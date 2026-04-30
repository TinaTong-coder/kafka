# SharePartitionManager 设计演化：从简单到复杂

本文档展示 SharePartitionManager 如何从最简单的设计一步步演化到 Kafka 实际实现的复杂版本。

**架构层级理解：**
- **SharePartitionManager** - 管理所有 share group 的所有 partition（本文重点）
- **SharePartition** - 管理单个 partition 的状态和 records（已在 sharepartition-evolution.md 中讲解）

---

## V1: 最简单的 Manager - 直接转发

### 设计

```java
class SharePartitionManager {
    Map<String, SharePartition> partitions;  // groupId-topicPartition → SharePartition

    CompletableFuture<Records> fetch(groupId, partition, memberId) {
        String key = groupId + "-" + partition;
        SharePartition sp = partitions.get(key);

        if (sp == null) {
            sp = new SharePartition(groupId, partition);
            partitions.put(key, sp);
        }

        return sp.fetch(memberId);
    }

    CompletableFuture<Void> acknowledge(groupId, partition, memberId, acks) {
        String key = groupId + "-" + partition;
        SharePartition sp = partitions.get(key);
        return sp.acknowledge(memberId, acks);
    }
}
```

### 特点
- 简单的 Map 存储
- 按需创建 SharePartition
- 直接转发请求

### 问题

**问题 1：从哪里读取 records？**

```
SharePartition 需要 records，但谁去 Kafka log 读取？
```

**问题 2：如何获取 log 数据？**

SharePartition 只管理状态，不应该直接访问磁盘。

---

## V2: 引入 ReplicaManager - 读取 Log

### 为什么需要这个复杂度？

**需求：需要从 Kafka log 读取实际的 record 数据**

### 设计

```java
class SharePartitionManager {
    Map<SharePartitionKey, SharePartition> partitions;
    ReplicaManager replicaManager;  // ← 新增：用于读取 log

    CompletableFuture<Records> fetch(groupId, partition, memberId, maxBytes) {
        SharePartitionKey key = new SharePartitionKey(groupId, partition);
        SharePartition sp = getOrCreate(key);

        // 1. 从 ReplicaManager 读取 log
        CompletableFuture<FetchResult> logFetch =
            replicaManager.fetchMessages(
                partition,
                offset = sp.nextFetchOffset(),  // SharePartition 告诉我们读哪里
                maxBytes
            );

        // 2. 让 SharePartition 处理（标记为 ACQUIRED）
        return logFetch.thenCompose(fetchResult -> {
            return sp.acquire(memberId, fetchResult.records);
        });
    }
}
```

### 流程

```
Consumer                SharePartitionManager       SharePartition      ReplicaManager
   |                            |                          |                   |
   | fetch() -----------------> |                          |                   |
   |                            | nextFetchOffset() -----> |                   |
   |                            | <--------------------- offset=100           |
   |                            |                          |                   |
   |                            | fetchMessages(partition, offset=100) ------> |
   |                            |                          |    读取磁盘
   |                            | <--------------------------------- records   |
   |                            |                          |                   |
   |                            | acquire(records) ------> |                   |
   |                            |              标记 ACQUIRED                  |
   |                            | <------------ acquired records              |
   | <------------------------- records                                        |
```

### 解决的问题
- ✅ 分离关注点：SharePartition 管理状态，ReplicaManager 读取数据
- ✅ 复用现有基础设施

### 新问题

**问题 3：key 管理太简单，容易冲突！**

```java
String key = groupId + "-" + partition;  // ← 字符串拼接，不安全

"group1" + "-" + "topic-0"  = "group1-topic-0"
"group1-topic" + "-" + "0"  = "group1-topic-0"  // 冲突！
```

**问题 4：partition 信息不完整！**

Topic 可能被删除后重建，topic name 相同但 topic ID 不同。

---

## V3: 引入 SharePartitionKey - 精确标识

### 为什么需要这个复杂度？

**需求 1：唯一标识一个 share partition**
**需求 2：支持 topic ID（KIP-516）**

### 设计

```java
class SharePartitionKey {
    String groupId;
    TopicIdPartition topicIdPartition;  // ← 包含 topicId + partition

    @Override
    public boolean equals(Object o) {
        SharePartitionKey that = (SharePartitionKey) o;
        return groupId.equals(that.groupId) &&
               topicIdPartition.equals(that.topicIdPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicIdPartition);
    }
}

class TopicIdPartition {
    Uuid topicId;      // ← topic 的唯一 ID
    String topic;      // ← topic name
    int partition;     // ← partition number
}

class SharePartitionManager {
    Map<SharePartitionKey, SharePartition> partitionCacheMap;  // ← 精确的 key

    SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition tip) {
        return new SharePartitionKey(groupId, tip);
    }

    CompletableFuture<Records> fetch(...) {
        SharePartitionKey key = sharePartitionKey(groupId, topicIdPartition);
        SharePartition sp = partitionCacheMap.get(key);
        ...
    }
}
```

### 解决的问题
- ✅ 精确标识，避免冲突
- ✅ 支持 topic ID，处理 topic 重建场景
- ✅ 类型安全（不是字符串拼接）

### 新问题

**问题 5：Consumer 断开连接后，它 acquire 的 records 怎么办？**

```
Consumer-A fetch → records 100-109 (ACQUIRED by A)
Consumer-A 断开连接
records 100-109 卡在 ACQUIRED 状态？
```

**问题 6：如何追踪 member 的活跃状态？**

---

## V4: 引入 Member Tracking 和 Session 管理

### 为什么需要这个复杂度？

**需求 1：检测 member 断开连接**
**需求 2：自动释放断开 member 的 records**

### 设计

```java
class SharePartitionManager {
    Map<SharePartitionKey, SharePartition> partitionCacheMap;
    ShareSessionCache sessionCache;  // ← 新增：管理 sessions

    Timer timer;  // ← 新增：定时任务

    CompletableFuture<Records> fetch(
        String groupId,
        String memberId,
        FetchParams params,
        Map<TopicIdPartition, Integer> partitions
    ) {
        // 1. 更新或创建 session
        ShareSession session = sessionCache.get(groupId, memberId);
        if (session == null) {
            session = new ShareSession(groupId, memberId);
            sessionCache.add(session);
        }
        session.updateLastUsedTime();

        // 2. 正常的 fetch 流程
        ...
    }

    void scheduleReleaseTimer(String memberId, String groupId) {
        // 定期检查 member 是否超时
        timer.schedule(() -> {
            ShareSession session = sessionCache.get(groupId, memberId);
            if (session.isExpired()) {
                // Member 超时了，释放它的所有 acquired records
                releaseAllAcquiredRecords(memberId, groupId);
                sessionCache.remove(groupId, memberId);
            }
        }, sessionTimeoutMs);
    }

    void releaseAllAcquiredRecords(String memberId, String groupId) {
        // 遍历所有该 group 的 partitions
        for (SharePartition sp : getPartitionsForGroup(groupId)) {
            sp.releaseAcquisitionLock(memberId);  // 释放该 member 的锁
        }
    }
}

class ShareSession {
    String groupId;
    String memberId;
    long lastUsedTimeMs;
    long sessionTimeoutMs;

    boolean isExpired() {
        return System.currentTimeMillis() - lastUsedTimeMs > sessionTimeoutMs;
    }
}
```

### 流程

```
Time    Consumer-A              SharePartitionManager               SharePartition
  |
  1     fetch() ---------------> 更新 session.lastUsedTime
  |                              启动 timer(30s)
  |                              ------------------------------->>> acquire()
  |     <---------------------------------- records 100-109
  |
  10    fetch() ---------------> 更新 session.lastUsedTime
  |                              重置 timer(30s)
  |
  |     [断开连接]
  |
  40    (timer 触发)
  |                              检查：40 - 10 = 30s > 30s ✓
  |                              session expired
  |                              ---------------------------->>> releaseAcquisitionLock(A)
  |                                                              records 100-109: AVAILABLE
```

### 解决的问题
- ✅ 自动检测 member 断开
- ✅ 自动释放 acquired records
- ✅ 防止资源泄漏

### 新问题

**问题 7：每个 partition 单独创建和管理太浪费资源！**

```
1000 个 partitions × 1 SharePartition 对象
= 1000 个独立的状态、锁、timer
```

**问题 8：怎么高效查找某个 group 的所有 partitions？**

```java
void releaseAllAcquiredRecords(String memberId, String groupId) {
    // 需要遍历整个 map？
    for (SharePartitionKey key : partitionCacheMap.keySet()) {
        if (key.groupId.equals(groupId)) {  // ← O(n) 太慢
            ...
        }
    }
}
```

---

## V5: 引入 Cached SharePartition - 延迟创建和缓存

### 为什么需要这个复杂度？

**需求 1：减少内存占用 - 不是所有 partition 都活跃**
**需求 2：提高查找效率 - 按 group 索引**

### 设计

```java
class SharePartitionManager {
    // 原来：直接存储 SharePartition
    // Map<SharePartitionKey, SharePartition> partitionCacheMap;

    // 现在：存储 CachedSharePartition（包装器）
    Map<SharePartitionKey, CachedSharePartition> partitionCacheMap;

    // 按 group 索引（加速查找）
    Map<String, Set<SharePartitionKey>> groupToPartitions;  // ← 新增

    CompletableFuture<Records> fetch(...) {
        SharePartitionKey key = sharePartitionKey(groupId, topicIdPartition);

        // 1. 查找或创建 CachedSharePartition
        CachedSharePartition cached = partitionCacheMap.computeIfAbsent(
            key,
            k -> new CachedSharePartition(key, this::createSharePartition)
        );

        // 2. 记录到 group 索引
        groupToPartitions
            .computeIfAbsent(groupId, g -> new HashSet<>())
            .add(key);

        // 3. 获取实际的 SharePartition（延迟创建）
        SharePartition sp = cached.partition();

        ...
    }

    void releaseAllAcquiredRecords(String memberId, String groupId) {
        // 现在可以高效查找
        Set<SharePartitionKey> keys = groupToPartitions.get(groupId);
        if (keys != null) {
            for (SharePartitionKey key : keys) {
                CachedSharePartition cached = partitionCacheMap.get(key);
                if (cached != null && cached.isLoaded()) {
                    cached.partition().releaseAcquisitionLock(memberId);
                }
            }
        }
    }
}

class CachedSharePartition {
    SharePartitionKey key;
    SharePartition partition;  // ← null 表示未创建
    Function<SharePartitionKey, SharePartition> factory;

    SharePartition partition() {
        if (partition == null) {
            partition = factory.apply(key);  // ← 延迟创建
        }
        return partition;
    }

    boolean isLoaded() {
        return partition != null;
    }
}
```

### 优化效果

**内存优化：**
```
Before V5:
  创建 SharePartition: 1000 × 每个都初始化完整状态
  = 大量内存

After V5:
  创建 CachedSharePartition: 1000 × 轻量级包装器
  只有被访问的才创建真正的 SharePartition
  实际活跃：100 个
  = 节省 90% 内存
```

**查找优化：**
```
Before V5: O(所有 partitions)
After V5:  O(该 group 的 partitions)
```

### 解决的问题
- ✅ 减少内存占用
- ✅ 加速 group 级别操作
- ✅ 延迟创建，按需初始化

### 新问题

**问题 9：SharePartition 什么时候该删除？**

```
Consumer 消费完一个 partition 后
SharePartition 一直占用内存
什么时候清理？
```

**问题 10：Partition leader 迁移怎么办？**

```
Broker-A 是 partition-0 的 leader
SharePartition 在 Broker-A 上
Partition leader 迁移到 Broker-B
Broker-A 上的 SharePartition 应该失效
```

---

## V6: 引入 Partition Listener - 响应 Leader 变更

### 为什么需要这个复杂度？

**需求：Partition leader 变更时，清理或转移状态**

### 设计

```java
class SharePartitionManager implements PartitionListener {
    // PartitionListener 接口
    @Override
    public void onLeadershipChange(
        Set<Partition> leaderPartitions,
        Set<Partition> followerPartitions
    ) {
        // 清理不再是 leader 的 partitions
        for (Partition p : followerPartitions) {
            removePartitionsForTopicPartition(p.topicPartition());
        }
    }

    void removePartitionsForTopicPartition(TopicPartition tp) {
        // 找到所有涉及这个 topic-partition 的 share partitions
        List<SharePartitionKey> toRemove = new ArrayList<>();

        for (SharePartitionKey key : partitionCacheMap.keySet()) {
            if (key.topicIdPartition.topicPartition().equals(tp)) {
                toRemove.add(key);
            }
        }

        // 清理
        for (SharePartitionKey key : toRemove) {
            CachedSharePartition cached = partitionCacheMap.remove(key);
            if (cached != null && cached.isLoaded()) {
                // 释放所有资源
                cached.partition().close();
            }

            // 从 group 索引中移除
            groupToPartitions.get(key.groupId()).remove(key);
        }
    }
}
```

### 流程

```
Time    Broker-A (leader)           Broker-B (follower)       ZooKeeper/Controller
  |
  1     SharePartition(group1, tp0)
  |     serving requests
  |
  |     [Leader 选举触发]
  |                                                            leader: tp0 → Broker-B
  |
  2     onLeadershipChange(
          leader: [],
          follower: [tp0]
        )
  |
  3     removePartitions(tp0)
  |     partitionCacheMap.remove(group1-tp0)
  |     partition.close()
  |
  4     [后续请求返回 NOT_LEADER_OR_FOLLOWER]
  |
  |                                 接收到新请求
  |                                 创建新的 SharePartition
  |                                 serving requests
```

### 解决的问题
- ✅ Leader 变更时自动清理
- ✅ 防止多个 broker 同时服务同一个 share partition
- ✅ 快速响应拓扑变化

### 新问题

**问题 11：状态持久化到哪里？怎么恢复？**

我们已经在 SharePartition 层面解决了这个问题（V7 引入 Persister），但 SharePartitionManager 需要协调。

**问题 12：Consumer 的 fetch 请求可能跨多个 partitions，怎么批量处理？**

```
Consumer fetch request:
  partition-0: maxBytes=1MB
  partition-1: maxBytes=1MB
  partition-2: maxBytes=1MB

每个 partition 单独处理 → 3 次 future 操作
能否批量优化？
```

---

## V7: 引入批量处理和 Future 合并

### 为什么需要这个复杂度？

**需求 1：减少 future 开销**
**需求 2：提高吞吐量**

### 设计

```java
class SharePartitionManager {
    CompletableFuture<Map<TopicIdPartition, PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        Map<TopicIdPartition, Integer> partitionMaxBytes  // ← 多个 partitions
    ) {
        // 收集所有 futures
        Map<TopicIdPartition, CompletableFuture<PartitionData>> futures = new HashMap<>();

        for (Map.Entry<TopicIdPartition, Integer> entry : partitionMaxBytes.entrySet()) {
            TopicIdPartition tip = entry.getKey();
            int maxBytes = entry.getValue();

            SharePartitionKey key = sharePartitionKey(groupId, tip);
            SharePartition sp = getOrCreate(key);

            // 启动异步 fetch
            CompletableFuture<PartitionData> future = fetchFromPartition(
                sp, memberId, fetchParams, maxBytes
            );

            futures.put(tip, future);
        }

        // 合并所有 futures
        return FutureUtils.combineFutures(futures);
    }

    CompletableFuture<PartitionData> fetchFromPartition(
        SharePartition sp,
        String memberId,
        FetchParams params,
        int maxBytes
    ) {
        // 1. 从 ReplicaManager 读取 log
        long offset = sp.nextFetchOffset();
        CompletableFuture<FetchResult> logFetch =
            replicaManager.fetchMessages(sp.topicPartition(), offset, maxBytes);

        // 2. 处理 fetch 结果
        return logFetch.thenCompose(fetchResult -> {
            if (fetchResult.error != Errors.NONE) {
                return CompletableFuture.completedFuture(
                    new PartitionData().setErrorCode(fetchResult.error.code())
                );
            }

            // 3. 让 SharePartition acquire
            return sp.acquire(memberId, params.maxFetchRecords, fetchResult)
                .thenApply(acquiredRecords -> {
                    return new PartitionData()
                        .setRecords(acquiredRecords.records())
                        .setAcquiredRecords(acquiredRecords.acquiredRecords());
                });
        });
    }
}

class FutureUtils {
    static <K, V> CompletableFuture<Map<K, V>> combineFutures(
        Map<K, CompletableFuture<V>> futures
    ) {
        // 等待所有 futures 完成
        CompletableFuture<Void> allOf = CompletableFuture.allOf(
            futures.values().toArray(new CompletableFuture[0])
        );

        // 收集结果
        return allOf.thenApply(v -> {
            Map<K, V> results = new HashMap<>();
            futures.forEach((key, future) -> {
                results.put(key, future.join());
            });
            return results;
        });
    }
}
```

### 流程

```
Consumer Request: fetch [partition-0, partition-1, partition-2]

SharePartitionManager:
  |
  ├─ fetchFromPartition(partition-0) → future1
  ├─ fetchFromPartition(partition-1) → future2
  └─ fetchFromPartition(partition-2) → future3
  |
  combineFutures([future1, future2, future3])
  |
  ↓ (并发执行)
  |
  partition-0: ReplicaManager.fetch → SharePartition.acquire → result1
  partition-1: ReplicaManager.fetch → SharePartition.acquire → result2
  partition-2: ReplicaManager.fetch → SharePartition.acquire → result3
  |
  ↓ (等待所有完成)
  |
  返回: {partition-0: result1, partition-1: result2, partition-2: result3}
```

### 解决的问题
- ✅ 并发处理多个 partitions
- ✅ 减少延迟（不是串行处理）
- ✅ 统一错误处理

### 新问题

**问题 13：Acknowledge 也需要批量处理！**

类似 fetch，consumer 可能同时 ACK 多个 partitions。

**问题 14：如何追踪 metrics？**

需要知道：
- 有多少 fetch 请求
- 有多少 ACK 请求
- 各种 ACK 类型的分布（ACCEPT/REJECT/RELEASE）
- Partition 加载时间

---

## V8: 引入 Metrics 和完整的批量 Acknowledge（Kafka 实际实现）

### 为什么需要这个复杂度？

**需求 1：监控系统健康**
**需求 2：性能调优**
**需求 3：问题诊断**

### 完整设计

```java
class SharePartitionManager implements PartitionListener {
    // === 核心数据结构 ===
    Map<SharePartitionKey, CachedSharePartition> partitionCacheMap;
    Map<String, Set<SharePartitionKey>> groupToPartitions;

    // === 依赖组件 ===
    ReplicaManager replicaManager;
    ShareSessionCache sessionCache;
    Persister persister;
    GroupConfigManager groupConfigManager;
    Timer timer;

    // === Metrics ===
    ShareGroupMetrics shareGroupMetrics;  // ← 新增

    // === 配置 ===
    int defaultRecordLockDurationMs;
    int maxDeliveryCount;
    int maxInFlightMessages;
    int maxFetchRecords;

    // ============ Fetch Messages ============
    public CompletableFuture<Map<TopicIdPartition, PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        Map<TopicIdPartition, Integer> partitionMaxBytes
    ) {
        CompletableFuture<Map<TopicIdPartition, PartitionData>> future =
            new CompletableFuture<>();

        // 封装成 ShareFetch 对象
        ShareFetch shareFetch = new ShareFetch(
            fetchParams,
            groupId,
            memberId,
            future,
            partitionMaxBytes,
            maxFetchRecords
        );

        // 处理
        processShareFetch(shareFetch);

        return future;
    }

    void processShareFetch(ShareFetch shareFetch) {
        Map<TopicPartition, ShareFetchPartitionData> fetchPartitions = new HashMap<>();

        // 1. 准备所有 partition 的 fetch 参数
        for (Map.Entry<TopicIdPartition, Integer> entry :
             shareFetch.partitionMaxBytes().entrySet()) {

            TopicIdPartition tip = entry.getKey();
            SharePartitionKey key = sharePartitionKey(shareFetch.groupId(), tip);

            // 获取或创建 CachedSharePartition
            CachedSharePartition cached = partitionCacheMap.computeIfAbsent(
                key,
                k -> {
                    long startTimeMs = time.milliseconds();
                    CachedSharePartition cp = new CachedSharePartition(...);

                    // 记录加载时间
                    shareGroupMetrics.partitionLoadTime(
                        time.milliseconds() - startTimeMs
                    );

                    return cp;
                }
            );

            SharePartition partition = cached.partition();
            long fetchOffset = partition.nextFetchOffset();

            fetchPartitions.put(
                tip.topicPartition(),
                new ShareFetchPartitionData(
                    tip,
                    fetchOffset,
                    entry.getValue(),
                    shareFetch.memberId(),
                    partition
                )
            );
        }

        // 2. 批量从 ReplicaManager 读取
        replicaManager.fetchMessages(
            shareFetch.fetchParams(),
            fetchPartitions.keySet(),
            responseCallback(shareFetch, fetchPartitions)
        );
    }

    BiConsumer<TopicPartition, FetchPartitionData> responseCallback(
        ShareFetch shareFetch,
        Map<TopicPartition, ShareFetchPartitionData> fetchPartitions
    ) {
        return (topicPartition, fetchResult) -> {
            ShareFetchPartitionData spData = fetchPartitions.get(topicPartition);
            SharePartition partition = spData.partition();

            if (fetchResult.error != Errors.NONE) {
                // 错误处理
                shareFetch.addPartitionData(
                    spData.topicIdPartition(),
                    errorPartitionData(fetchResult.error)
                );
                return;
            }

            // 3. SharePartition acquire
            try {
                ShareAcquiredRecords acquired = partition.acquire(
                    shareFetch.memberId(),
                    shareFetch.maxFetchRecords(),
                    fetchResult
                );

                shareFetch.addPartitionData(
                    spData.topicIdPartition(),
                    new PartitionData()
                        .setRecords(acquired.records())
                        .setAcquiredRecords(acquired.acquiredRecordsMetadata())
                );
            } catch (Exception e) {
                fencedSharePartitionHandler().accept(spData.key(), e);
                shareFetch.addPartitionData(
                    spData.topicIdPartition(),
                    errorPartitionData(Errors.forException(e))
                );
            }

            // 4. 检查是否所有 partitions 都完成了
            if (shareFetch.maybeComplete()) {
                shareFetch.future().complete(shareFetch.partitionsData());
            }
        };
    }

    // ============ Acknowledge ============
    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>>
    acknowledge(
        String memberId,
        String groupId,
        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics
    ) {
        // 记录 metrics
        shareGroupMetrics.shareAcknowledgement();

        Map<TopicIdPartition, CompletableFuture<Throwable>> futures = new HashMap<>();

        // 对每个 partition 执行 acknowledge
        acknowledgeTopics.forEach((topicIdPartition, ackBatches) -> {
            SharePartitionKey key = sharePartitionKey(groupId, topicIdPartition);
            SharePartition partition = partitionCacheMap.get(key);

            if (partition != null) {
                CompletableFuture<Throwable> future = new CompletableFuture<>();

                // 调用 SharePartition.acknowledge
                partition.acknowledge(memberId, ackBatches).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        fencedSharePartitionHandler().accept(key, throwable);
                        future.complete(throwable);
                        return;
                    }

                    // 记录 ACK metrics
                    ackBatches.forEach(batch ->
                        batch.acknowledgeTypes().forEach(
                            shareGroupMetrics::recordAcknowledgement
                        )
                    );

                    future.complete(null);
                });

                // ACK 完成后，触发等待的 fetch
                DelayedShareFetchKey delayedKey =
                    new DelayedShareFetchGroupKey(groupId, topicIdPartition);
                replicaManager.completeDelayedShareFetchRequest(delayedKey);

                futures.put(topicIdPartition, future);
            } else {
                futures.put(topicIdPartition,
                    CompletableFuture.completedFuture(
                        Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()
                    )
                );
            }
        });

        // 合并所有 futures
        return FutureUtils.waitForAll(futures).thenApply(results -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> response =
                new HashMap<>();

            results.forEach((tip, error) -> {
                response.put(tip, new ShareAcknowledgeResponseData.PartitionData()
                    .setPartitionIndex(tip.partition())
                    .setErrorCode(error == null ?
                        Errors.NONE.code() :
                        Errors.forException(error).code())
                );
            });

            return response;
        });
    }

    // ============ Member Lifecycle ============
    void releaseAcquiredRecords(String memberId, String groupId) {
        Set<SharePartitionKey> keys = groupToPartitions.get(groupId);
        if (keys != null) {
            for (SharePartitionKey key : keys) {
                CachedSharePartition cached = partitionCacheMap.get(key);
                if (cached != null && cached.isLoaded()) {
                    cached.partition().releaseAcquisitionLock(memberId);
                }
            }
        }
    }

    // ============ Partition Lifecycle ============
    @Override
    public void onLeadershipChange(
        Set<Partition> leaderPartitions,
        Set<Partition> followerPartitions
    ) {
        // 清理不再是 leader 的 partitions
        for (Partition p : followerPartitions) {
            removePartitionsForTopicPartition(p.topicPartition());
        }
    }

    void removePartitionsForTopicPartition(TopicPartition tp) {
        List<SharePartitionKey> toRemove = new ArrayList<>();

        for (SharePartitionKey key : partitionCacheMap.keySet()) {
            if (key.topicIdPartition().topicPartition().equals(tp)) {
                toRemove.add(key);
            }
        }

        for (SharePartitionKey key : toRemove) {
            CachedSharePartition cached = partitionCacheMap.remove(key);
            if (cached != null && cached.isLoaded()) {
                cached.partition().close();
            }

            Set<SharePartitionKey> groupKeys = groupToPartitions.get(key.groupId());
            if (groupKeys != null) {
                groupKeys.remove(key);
            }
        }
    }

    // ============ Fenced Partition Handler ============
    BiConsumer<SharePartitionKey, Throwable> fencedSharePartitionHandler() {
        return (key, throwable) -> {
            if (throwable instanceof FencedStateEpochException) {
                // State epoch 被 fence 了，移除这个 partition
                CachedSharePartition cached = partitionCacheMap.remove(key);
                if (cached != null && cached.isLoaded()) {
                    cached.partition().close();
                }

                Set<SharePartitionKey> groupKeys = groupToPartitions.get(key.groupId());
                if (groupKeys != null) {
                    groupKeys.remove(key);
                }
            }
        };
    }

    // ============ Cleanup ============
    @Override
    public void close() {
        partitionCacheMap.values().forEach(cached -> {
            if (cached.isLoaded()) {
                cached.partition().close();
            }
        });

        timer.close();
        shareGroupMetrics.close();
    }
}

// ============ Metrics ============
class ShareGroupMetrics {
    Sensor shareAcknowledgementSensor;
    Map<AcknowledgeType, Sensor> recordAcknowledgementSensors;
    Sensor partitionLoadTimeSensor;

    void shareAcknowledgement() {
        shareAcknowledgementSensor.record();
    }

    void recordAcknowledgement(AcknowledgeType type) {
        recordAcknowledgementSensors.get(type).record();
    }

    void partitionLoadTime(long timeMs) {
        partitionLoadTimeSensor.record(timeMs);
    }

    // Metrics:
    // - share-acknowledgement-rate: ACK 请求速率
    // - share-acknowledgement-count: 总 ACK 数量
    // - record-acknowledgement-rate (per type): 各类型 ACK 速率
    // - partition-load-time-avg: 平均 partition 加载时间
    // - partition-load-time-max: 最大 partition 加载时间
}
```

### 完整流程示例

```
Consumer Request:
  ShareFetch [partition-0, partition-1]
  ShareAcknowledge [partition-0: 100-109 ACCEPT]

Time    Consumer            SharePartitionManager           SharePartition          ReplicaManager
  |
  1     fetch(p0, p1) ----> processShareFetch()
  |                         创建 ShareFetch 对象
  |                         准备 fetch 参数
  |                         ------------------------------->>>
  |                                                          nextFetchOffset(p0) = 100
  |                                                          nextFetchOffset(p1) = 200
  |                         <----------------------------------
  |                         fetchMessages([p0@100, p1@200]) --------------->>>
  |                                                                              读取 log
  |                         <-------------------------------------------------- records
  |                         partition.acquire(p0, records) >>>
  |                                                          标记 100-109: ACQUIRED
  |                         <----------------------------------
  |                         partition.acquire(p1, records) >>>
  |                                                          标记 200-209: ACQUIRED
  |                         <----------------------------------
  2     <------------------ {p0: records 100-109, p1: records 200-209}
  |
  |     处理 records
  |
  3     acknowledge(
          p0: 100-109 ACCEPT
        ) -----------------> acknowledge()
  |                         记录 metrics
  |                         partition.acknowledge(p0) ------->>>
  |                                                          100-109: ACKNOWLEDGED
  |                                                          持久化到 __share_group_state
  |                         <----------------------------------
  |                         触发 DelayedShareFetch(p0)
  4     <------------------ ACK success
```

---

## 复杂度总结

| 版本 | 引入的复杂度 | 解决的问题 | 代价 |
|------|-------------|-----------|------|
| V1 | 简单 Map | 基础管理 | 缺少 log 读取 |
| V2 | ReplicaManager | 读取 log 数据 | 需要协调 |
| V3 | SharePartitionKey | 精确标识 | 更复杂的 key |
| V4 | Session + Timer | Member 生命周期 | 需要定时任务 |
| V5 | CachedSharePartition + 索引 | 内存优化 | 延迟创建复杂度 |
| V6 | PartitionListener | Leader 变更 | 需要监听机制 |
| V7 | 批量 Future | 多 partition 并发 | Future 合并复杂 |
| V8 | Metrics + 完整错误处理 | 监控和生产就绪 | 最高复杂度 |

---

## 关键设计思想

1. **分层管理**
   - SharePartitionManager: 管理所有 partitions，协调组件
   - SharePartition: 管理单个 partition 的状态

2. **延迟初始化**
   - CachedSharePartition 包装器
   - 只在需要时创建实际对象

3. **索引优化**
   - groupToPartitions: 快速查找 group 的所有 partitions
   - 避免全表扫描

4. **异步处理**
   - CompletableFuture 链式调用
   - 批量并发处理多个 partitions

5. **生命周期管理**
   - Session 管理 member
   - PartitionListener 响应 leader 变更
   - 自动清理和资源释放

6. **可观测性**
   - 详细的 metrics
   - 支持监控和调优

7. **错误处理**
   - Fenced partition 自动移除
   - 优雅降级（部分成功）

---

## SharePartitionManager vs SharePartition

| 方面 | SharePartitionManager | SharePartition |
|------|----------------------|----------------|
| 职责 | 管理多个 partitions | 管理单个 partition 的状态 |
| 范围 | 整个 broker | 单个 group-partition |
| 并发 | 多个 partitions 并发 | 单个 partition 串行（锁） |
| 生命周期 | Broker 生命周期 | Partition leader 生命周期 |
| 核心逻辑 | 路由、协调、批量处理 | 状态机、MVCC、持久化 |

每一层复杂度都是为了：
- ✅ 提高性能（并发、缓存、批量）
- ✅ 减少资源占用（延迟创建、索引）
- ✅ 保证正确性（生命周期管理、错误处理）
- ✅ 支持运维（metrics、监控）
