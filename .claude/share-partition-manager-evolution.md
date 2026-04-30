# SharePartitionManager 从简单到复杂的演化分析

本文档通过逐步演进的方式，展示 SharePartitionManager 的设计如何从简单到复杂，以及每一步复杂度的引入原因。

---

## Step 1: 最简单版本 - 单个 Partition 的消息获取

### 需求
Consumer 从一个 partition 获取消息。

### 实现
```java
class SimplePartitionManager {
    private final ReplicaManager replicaManager;

    // 核心方法：从 partition 获取消息
    List<Record> fetchMessages(TopicPartition partition, int maxBytes) {
        return replicaManager.fetchFromLog(partition, maxBytes);
    }
}
```

### 数据结构
```
┌─────────────────────────────────────────┐
│  SimplePartitionManager                 │
├─────────────────────────────────────────┤
│  - replicaManager                       │
│                                         │
│  + fetchMessages(partition, maxBytes)  │
└─────────────────────────────────────────┘
         │
         ▼ 调用
┌─────────────────────────────────────────┐
│  ReplicaManager                         │
│  - 管理底层的 Log                        │
└─────────────────────────────────────────┘
```

### 特点
- ✅ 简单直接
- ✅ 无状态
- ❌ 不支持多个 consumer
- ❌ 没有消息锁定机制

---

## Step 2: 引入 Share Group - 多消费者共享消费

### 新需求
**多个 consumer 需要共享消费同一个 partition 的消息，而不是像传统 consumer group 那样独占 partition。**

**为什么？** 传统 consumer group 的限制：
- 一个 partition 只能分配给一个 consumer
- 如果 partition 数量 < consumer 数量，部分 consumer 闲置
- Share Group 允许多个 consumer 并发消费同一个 partition

### 新增复杂度
1. **需要区分不同的 Share Group**
2. **需要为每个 (groupId, partition) 维护独立的状态**

### 实现
```java
class SharePartitionManager {
    // 核心：为每个 (groupId, partition) 维护一个 SharePartition 对象
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;

    CompletableFuture<PartitionData> fetchMessages(
        String groupId,  // 新增：group ID
        String memberId,
        TopicIdPartition partition
    ) {
        SharePartitionKey key = new SharePartitionKey(groupId, partition);
        SharePartition sharePartition = partitionCacheMap.get(key);
        return sharePartition.fetch(memberId);
    }
}

// 新增数据结构
class SharePartitionKey {
    String groupId;
    TopicIdPartition topicIdPartition;
}

class SharePartition {
    // 维护该 partition 的消费状态
    private long nextFetchOffset;
    private Set<String> activeMembers;
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  SharePartitionManager                                       │
├──────────────────────────────────────────────────────────────┤
│  partitionCacheMap: Map<SharePartitionKey, SharePartition>  │
│    ├─ ("group1", topic1-p0) -> SharePartition1              │
│    ├─ ("group1", topic1-p1) -> SharePartition2              │
│    ├─ ("group2", topic1-p0) -> SharePartition3  // 同一个 partition│
│    └─ ("group2", topic2-p0) -> SharePartition4              │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **SharePartitionKey**：组合键 `(groupId, topicIdPartition)`
2. **partitionCacheMap**：缓存每个 share partition 的状态
3. **为什么？** 不同的 group 消费同一个 partition 需要独立的状态

---

## Step 3: 引入消息锁定机制 - 防止重复消费

### 新需求
**Consumer 获取消息后，消息需要被"锁定"一段时间，防止其他 consumer 重复消费。**

**为什么？**
```
场景：
Consumer1 获取了 offset 100-110 的消息
Consumer2 立即也请求消息
  -> 如果没有锁定，Consumer2 也会拿到 100-110
  -> 导致重复消费！
```

### 新增复杂度
1. **记录哪些消息正在被"锁定"（in-flight）**
2. **锁定有过期时间（record lock duration）**
3. **需要 Timer 来处理锁过期**

### 实现
```java
class SharePartition {
    // 新增：记录正在被锁定的消息
    private final Map<String, Set<Long>> inFlightRecords;  // memberId -> offsets

    // 新增：锁过期时间
    private final int recordLockDurationMs;

    // 新增：Timer 处理锁超时
    private final Timer timer;

    CompletableFuture<List<Record>> acquire(String memberId, int maxBytes) {
        List<Record> records = fetchFromLog(maxBytes);

        // 锁定这些消息
        Set<Long> offsets = extractOffsets(records);
        inFlightRecords.put(memberId, offsets);

        // 设置锁超时
        timer.schedule(() -> {
            releaseLock(memberId, offsets);
        }, recordLockDurationMs);

        return records;
    }
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  SharePartition (单个 share partition 的状态)                │
├──────────────────────────────────────────────────────────────┤
│  nextFetchOffset: 150                                        │
│                                                               │
│  inFlightRecords: Map<String, Set<Long>>                    │
│    ├─ "member1" -> {100, 101, 102}  // 锁定中               │
│    ├─ "member2" -> {103, 104, 105}  // 锁定中               │
│    └─ "member3" -> {106, 107, 108}  // 锁定中               │
│                                                               │
│  timer: Timer  // 处理锁超时                                 │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **inFlightRecords**：跟踪锁定的消息
2. **Timer**：异步处理锁过期
3. **recordLockDurationMs**：配置锁的持续时间
4. **为什么？** 保证消息不被重复消费，同时允许失败重试

---

## Step 4: 引入 Acknowledgement 机制 - 消息确认

### 新需求
**Consumer 处理完消息后，需要显式确认（acknowledge），才能推进消费进度。**

**为什么？**
```
场景：
Consumer1 获取了 offset 100-110 的消息
  -> 如果处理成功：ACCEPT -> 可以继续消费 111+
  -> 如果处理失败：RELEASE -> 重新分配给其他 consumer
  -> 如果消息有问题：REJECT -> 标记为无法处理
```

### 新增复杂度
1. **支持多种 acknowledgement 类型**
2. **根据 ack 类型更新状态**
3. **需要持久化 ack 状态（防止重启后丢失）**

### 实现
```java
class SharePartition {
    // 新增：记录每个 offset 的状态
    private final Map<Long, RecordState> recordStates;

    // 新增：Persister 持久化状态
    private final Persister persister;

    CompletableFuture<Void> acknowledge(
        String memberId,
        List<AcknowledgementBatch> batches  // offset range + ack type
    ) {
        for (AcknowledgementBatch batch : batches) {
            switch (batch.acknowledgeType()) {
                case ACCEPT:
                    // 标记为已消费
                    markAsConsumed(batch.offsets());
                    break;
                case RELEASE:
                    // 释放锁，允许其他 consumer 消费
                    releaseLock(memberId, batch.offsets());
                    break;
                case REJECT:
                    // 标记为无法处理
                    markAsArchived(batch.offsets());
                    break;
            }
        }

        // 持久化状态变更
        return persister.persist(recordStates);
    }
}

enum AcknowledgeType {
    ACCEPT,  // 处理成功
    RELEASE, // 重新分配
    REJECT   // 无法处理
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  SharePartition                                              │
├──────────────────────────────────────────────────────────────┤
│  recordStates: Map<Long, RecordState>                       │
│    ├─ 100 -> ACQUIRED (member1)                             │
│    ├─ 101 -> ACKNOWLEDGED (已消费)                          │
│    ├─ 102 -> AVAILABLE (可分配)                             │
│    ├─ 103 -> ARCHIVED (被 reject)                           │
│    └─ 104 -> ACQUIRED (member2)                             │
│                                                               │
│  persister: Persister  // 持久化到 coordinator              │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **RecordState 状态机**：AVAILABLE -> ACQUIRED -> ACKNOWLEDGED/ARCHIVED
2. **Persister**：异步持久化到 share coordinator
3. **AcknowledgementBatch**：批量处理 ack，提升性能
4. **为什么？** 精确控制消息的处理状态，支持失败重试

---

## Step 5: 引入 ShareSession 机制 - 优化重复请求

### 新需求
**Consumer 会频繁发送 fetch 请求，每次都携带完整的 partition 列表太浪费带宽。**

**为什么？**
```
场景：
Consumer1 每次 fetch 都请求 [topic1-p0, topic1-p1, topic1-p2]
  -> 如果每次都发送完整列表：浪费网络带宽
  -> 改进：第一次发送完整列表，后续只发送变更（类似 HTTP 的 ETag）
```

### 新增复杂度
1. **为每个 (groupId, memberId) 维护一个 Session**
2. **Session 存储 cached partitions**
3. **支持增量更新（added, updated, removed）**
4. **Session 需要 epoch 防止乱序**

### 实现
```java
class SharePartitionManager {
    // 新增：Session 缓存
    private final ShareSessionCache cache;

    ShareFetchContext newContext(
        String groupId,
        Map<TopicIdPartition, SharePartitionData> shareFetchData,
        ShareRequestMetadata reqMetadata  // 包含 epoch
    ) {
        if (reqMetadata.epoch() == INITIAL_EPOCH) {
            // 第一次请求：创建新 session
            ShareSessionKey key = new ShareSessionKey(groupId, reqMetadata.memberId());
            ImplicitLinkedHashCollection<CachedSharePartition> cachedPartitions =
                new ImplicitLinkedHashCollection<>();
            shareFetchData.forEach((tp, data) ->
                cachedPartitions.add(new CachedSharePartition(tp, data)));

            cache.maybeCreateSession(groupId, reqMetadata.memberId(), cachedPartitions);
            return new ShareSessionContext(reqMetadata, shareFetchData);
        } else {
            // 后续请求：更新现有 session
            ShareSession session = cache.get(key);
            Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> modifications =
                session.update(shareFetchData, toForget);

            session.epoch = nextEpoch(session.epoch);
            return new ShareSessionContext(reqMetadata, session);
        }
    }
}

class ShareSession {
    int epoch;  // 防止乱序
    ImplicitLinkedHashCollection<CachedSharePartition> partitionMap;

    Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> update(
        Map<TopicIdPartition, SharePartitionData> newData,
        List<TopicIdPartition> toForget
    ) {
        // 计算 added, updated, removed
        // ...
    }
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  SharePartitionManager                                       │
├──────────────────────────────────────────────────────────────┤
│  cache: ShareSessionCache                                    │
│    ├─ ("group1", "member1") -> ShareSession                 │
│    │     epoch: 5                                            │
│    │     partitionMap: [topic1-p0, topic1-p1, topic1-p2]   │
│    │                                                          │
│    └─ ("group1", "member2") -> ShareSession                 │
│          epoch: 3                                            │
│          partitionMap: [topic1-p0, topic2-p0]              │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **ShareSessionCache**：缓存每个 member 的 session
2. **ShareSessionKey**：`(groupId, memberId)` 组合键
3. **epoch 管理**：防止请求乱序
4. **增量更新**：计算 added/updated/removed
5. **为什么？** 减少网络带宽，优化频繁请求的性能

---

## Step 6: 引入 DelayedShareFetch - 处理无数据场景

### 新需求
**Consumer 请求消息时，如果暂时没有可用消息（所有消息都被锁定），不应该立即返回空，而是等待一段时间。**

**为什么？**
```
场景：
Consumer1 请求 topic1-p0 的消息
  -> offset 100-110 都被其他 consumer 锁定
  -> 如果立即返回空：Consumer1 会频繁轮询，浪费资源
  -> 改进：等待直到有消息可用或超时
```

### 新增复杂度
1. **DelayedShareFetch 放入 Purgatory**
2. **Watch keys：当特定事件发生时唤醒**
3. **支持多种唤醒条件：ack 完成、锁超时、HWM 更新**

### 实现
```java
class SharePartitionManager {
    private void processShareFetch(ShareFetch shareFetch) {
        List<DelayedShareFetchKey> watchKeys = new ArrayList<>();

        for (TopicIdPartition tp : shareFetch.partitions()) {
            // Watch key 1: 当该 group 的该 partition 有 ack 时唤醒
            DelayedShareFetchKey groupKey = new DelayedShareFetchGroupKey(
                shareFetch.groupId(), tp.topicId(), tp.partition()
            );
            watchKeys.add(groupKey);

            // Watch key 2: 当该 partition 的 HWM 更新时唤醒
            DelayedShareFetchKey partitionKey = new DelayedShareFetchPartitionKey(
                tp.topicId(), tp.partition()
            );
            watchKeys.add(partitionKey);
        }

        // 创建 delayed fetch 并添加到 purgatory
        DelayedShareFetch delayedFetch = new DelayedShareFetch(
            shareFetch, replicaManager, sharePartitions
        );
        addDelayedShareFetch(delayedFetch, watchKeys);
    }
}

// 当 ack 完成时
public void acknowledge(...) {
    // ... ack 逻辑 ...

    // 唤醒等待的 fetch
    DelayedShareFetchKey key = new DelayedShareFetchGroupKey(groupId, topicId, partition);
    replicaManager.completeDelayedShareFetchRequest(key);
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  ReplicaManager (Purgatory)                                  │
├──────────────────────────────────────────────────────────────┤
│  delayedShareFetchPurgatory:                                 │
│    ├─ DelayedShareFetch1                                     │
│    │    watchKeys: [GroupKey("g1","t1",0), PartitionKey("t1",0)]│
│    │    timeout: 500ms                                        │
│    │                                                           │
│    └─ DelayedShareFetch2                                     │
│         watchKeys: [GroupKey("g1","t1",1), PartitionKey("t1",1)]│
│         timeout: 500ms                                        │
│                                                               │
│  唤醒条件：                                                   │
│    1. acknowledge 完成 -> completeByKey(GroupKey)           │
│    2. 锁超时释放      -> completeByKey(GroupKey)            │
│    3. HWM 更新        -> completeByKey(PartitionKey)        │
│    4. 超时            -> checkAndComplete()                  │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **DelayedShareFetch**：延迟操作对象
2. **DelayedShareFetchKey**：watch key 的抽象
3. **Purgatory 集成**：使用 Kafka 现有的 delayed operation 框架
4. **多个 watch key**：一个请求可能被多种事件唤醒
5. **为什么？** 避免空轮询，提升系统效率

---

## Step 7: 引入 Partition Listener - 处理 Leader 变更

### 新需求
**当 partition 的 leader 变更时，该 partition 对应的 SharePartition 需要被清理。**

**为什么？**
```
场景：
Broker1 是 topic1-p0 的 leader
  -> SharePartition 初始化完成，正在服务请求
  -> Leader 切换到 Broker2
  -> Broker1 上的 SharePartition 应该被移除（fence）
  -> Consumer 重新发现 Broker2 是新 leader
```

### 新增复杂度
1. **监听 Partition 事件**
2. **自动清理 fenced 的 SharePartition**
3. **每个 SharePartition 需要独立的 listener**

### 实现
```java
class SharePartitionManager {
    private SharePartition getOrCreateSharePartition(SharePartitionKey key) {
        return partitionCacheMap.computeIfAbsent(key, k -> {
            // 创建 listener
            SharePartitionListener listener = new SharePartitionListener(
                key, replicaManager, partitionCacheMap
            );

            // 注册 listener 到 ReplicaManager
            replicaManager.maybeAddListener(key.topicPartition(), listener);

            SharePartition partition = new SharePartition(..., listener);
            return partition;
        });
    }
}

static class SharePartitionListener implements PartitionListener {
    @Override
    public void onBecomingFollower(TopicPartition tp) {
        // Leader 变更：移除 SharePartition
        removeSharePartitionFromCache(sharePartitionKey, partitionCacheMap);
    }

    @Override
    public void onDeleted(TopicPartition tp) {
        // Partition 删除：移除 SharePartition
        removeSharePartitionFromCache(sharePartitionKey, partitionCacheMap);
    }

    @Override
    public void onFailed(TopicPartition tp) {
        // Partition 失败：移除 SharePartition
        removeSharePartitionFromCache(sharePartitionKey, partitionCacheMap);
    }
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  ReplicaManager                                              │
├──────────────────────────────────────────────────────────────┤
│  partitionListeners: Map<TopicPartition, List<Listener>>    │
│    ├─ topic1-p0 -> [SharePartitionListener1,                │
│    │                SharePartitionListener2]  // 多个 group  │
│    └─ topic1-p1 -> [SharePartitionListener3]                │
│                                                               │
│  当 leader 变更时：                                          │
│    notifyListeners(topic1-p0, "onBecomingFollower")        │
│      -> SharePartitionListener1.onBecomingFollower()       │
│      -> SharePartitionListener2.onBecomingFollower()       │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  SharePartitionManager                                       │
├──────────────────────────────────────────────────────────────┤
│  partitionCacheMap:                                          │
│    ├─ ("group1", topic1-p0) -> SharePartition1 + Listener1 │
│    └─ ("group2", topic1-p0) -> SharePartition2 + Listener2 │
│                                                               │
│  leader 变更后：                                             │
│    两个 SharePartition 都被移除 ✅                           │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **SharePartitionListener**：每个 SharePartition 一个 listener
2. **Listener 生命周期管理**：注册、移除
3. **为什么一个 partition 有多个 listener？** 因为不同的 share group 对应不同的 SharePartition
4. **为什么？** 自动处理 leader 变更，避免 consumer 访问错误的 broker

---

## Step 8: 引入异步初始化 - 优化启动性能

### 新需求
**SharePartition 需要从 persister 加载状态，这个过程可能很慢，不应该阻塞 fetch 请求。**

**为什么？**
```
场景：
Consumer 请求 topic1-p0 的消息
  -> SharePartition 还未初始化（需要从 coordinator 加载状态）
  -> 如果同步等待：请求被阻塞几百毫秒
  -> 改进：异步初始化 + 延迟请求
```

### 新增复杂度
1. **初始化状态：未初始化、初始化中、已初始化**
2. **初始化失败处理**
3. **初始化完成后唤醒等待的请求**

### 实现
```java
class SharePartition {
    private CompletableFuture<Void> initializationFuture;

    CompletableFuture<Void> maybeInitialize() {
        if (initializationFuture != null) {
            return initializationFuture;  // 已经在初始化或已完成
        }

        initializationFuture = new CompletableFuture<>();

        // 异步加载状态
        persister.readState(groupId, topicIdPartition).whenComplete((state, throwable) -> {
            if (throwable != null) {
                initializationFuture.completeExceptionally(throwable);
                return;
            }

            // 恢复状态
            this.recordStates.putAll(state.recordStates);
            this.startOffset = state.startOffset;

            initializationFuture.complete(null);
        });

        return initializationFuture;
    }
}

class SharePartitionManager {
    private void processShareFetch(ShareFetch shareFetch) {
        for (TopicIdPartition tp : shareFetch.partitions()) {
            SharePartition sharePartition = getOrCreateSharePartition(key);

            CompletableFuture<Void> initFuture = sharePartition.maybeInitialize();
            boolean initialized = initFuture.isDone();

            initFuture.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    handleInitializationException(key, shareFetch, throwable);
                }

                // 如果初始化是异步完成的，唤醒等待的请求
                if (!initialized) {
                    replicaManager.completeDelayedShareFetchRequest(delayedKey);
                }
            });
        }
    }
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  SharePartition 生命周期                                     │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. 创建                                                      │
│     initializationFuture = null                              │
│                                                               │
│  2. 第一次 fetch                                             │
│     maybeInitialize() 被调用                                 │
│     initializationFuture = new CompletableFuture()          │
│     异步从 persister 加载状态                                │
│     fetch 请求进入 purgatory 等待                            │
│                                                               │
│  3. 初始化完成                                               │
│     initializationFuture.complete(null)                     │
│     唤醒等待的 fetch 请求                                    │
│                                                               │
│  4. 后续 fetch                                               │
│     initializationFuture.isDone() == true                   │
│     直接处理，无需等待                                       │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **CompletableFuture 管理初始化状态**
2. **初始化失败处理**：移除 SharePartition，让 consumer 重试
3. **唤醒机制**：初始化完成后唤醒等待的请求
4. **为什么？** 避免阻塞，提升系统响应速度

---

## Step 9: 引入 Metrics - 监控和可观测性

### 新需求
**运维人员需要监控 Share Group 的运行状态：ack 速率、partition 加载时间等。**

### 新增复杂度
1. **定义 Metrics**
2. **在关键路径上记录 Metrics**

### 实现
```java
class ShareGroupMetrics {
    private final Sensor shareAcknowledgementSensor;
    private final Map<Byte, Sensor> recordAcksSensorMap;
    private final Sensor partitionLoadTimeSensor;

    void shareAcknowledgement() {
        shareAcknowledgementSensor.record();
    }

    void recordAcknowledgement(byte ackType) {
        recordAcksSensorMap.get(ackType).record();
    }

    void partitionLoadTime(long startMs) {
        partitionLoadTimeSensor.record(time.hiResClockMs() - startMs);
    }
}

class SharePartitionManager {
    private final ShareGroupMetrics shareGroupMetrics;

    public void acknowledge(...) {
        this.shareGroupMetrics.shareAcknowledgement();
        // ... ack 逻辑 ...
        batches.forEach(batch ->
            this.shareGroupMetrics.recordAcknowledgement(batch.ackType())
        );
    }

    private SharePartition getOrCreateSharePartition(SharePartitionKey key) {
        long start = time.hiResClockMs();
        SharePartition partition = new SharePartition(...);
        this.shareGroupMetrics.partitionLoadTime(start);
        return partition;
    }
}
```

### 引入的复杂度
1. **ShareGroupMetrics 类**
2. **在关键路径注入 metrics 记录**
3. **为什么？** 提供可观测性，帮助运维和调优

---

## 完整架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SharePartitionManager                               │
│  (核心管理器，协调所有组件)                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ partitionCacheMap: Map<SharePartitionKey, SharePartition>            │  │
│  │   存储所有 (groupId, partition) 的状态                                │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                         │                                                    │
│                         ├─> SharePartition1 (group1, topic1-p0)            │
│                         │     - recordStates: 消息状态机                    │
│                         │     - inFlightRecords: 锁定的消息                 │
│                         │     - timer: 锁超时处理                           │
│                         │     - persister: 状态持久化                       │
│                         │     - initializationFuture: 异步初始化            │
│                         │     - listener: 监听 partition 事件               │
│                         │                                                    │
│                         └─> SharePartition2 (group2, topic1-p0)            │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ cache: ShareSessionCache                                             │  │
│  │   存储所有 (groupId, memberId) 的 session                            │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                         │                                                    │
│                         ├─> ShareSession1 (group1, member1)                │
│                         │     - epoch: 防止乱序                             │
│                         │     - partitionMap: 缓存的 partitions            │
│                         │                                                    │
│                         └─> ShareSession2 (group1, member2)                │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │ 其他组件                                                              │  │
│  │  - replicaManager: 底层存储管理                                      │  │
│  │  - timer: 全局锁超时 timer                                           │  │
│  │  - persister: 状态持久化                                             │  │
│  │  - shareGroupMetrics: 监控指标                                       │  │
│  │  - groupConfigManager: 动态配置                                      │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  核心方法:                                                                   │
│  ├─ fetchMessages(): 获取消息 -> DelayedShareFetch                         │
│  ├─ acknowledge(): 确认消息 -> 更新状态 + 唤醒等待的 fetch                 │
│  ├─ releaseSession(): 释放 session + 释放锁定的消息                        │
│  ├─ newContext(): 创建/更新 ShareSession                                  │
│  └─ processShareFetch(): 核心 fetch 处理逻辑                              │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌────────────────────────────────────────────────────┐
        │  ReplicaManager (Purgatory)                        │
        │  - delayedShareFetchPurgatory                      │
        │  - partitionListeners                              │
        └────────────────────────────────────────────────────┘
```

---

## 复杂度总结表

| Step | 新增组件 | 引入原因 | 复杂度类型 |
|------|---------|---------|-----------|
| 1 | 基础 fetch | 基本功能 | - |
| 2 | SharePartitionKey, partitionCacheMap | 支持 Share Group | 数据结构 |
| 3 | inFlightRecords, Timer | 消息锁定机制 | 并发控制 |
| 4 | RecordState, Persister, Acknowledgement | 消息确认 + 持久化 | 状态机 + 存储 |
| 5 | ShareSession, epoch | 优化网络带宽 | 会话管理 |
| 6 | DelayedShareFetch, Purgatory | 避免空轮询 | 异步编程 |
| 7 | SharePartitionListener | 自动处理 leader 变更 | 事件驱动 |
| 8 | 异步初始化 | 优化启动性能 | 异步编程 |
| 9 | ShareGroupMetrics | 可观测性 | 监控 |

---

## 关键设计决策

### 1. 为什么用 ConcurrentHashMap 存储 partitionCacheMap？
- **并发访问**：多个线程可能同时创建/访问不同的 SharePartition
- **线程安全**：computeIfAbsent 是原子操作

### 2. 为什么每个 SharePartition 都有独立的 Timer？
- **隔离性**：一个 partition 的锁超时不影响其他 partition
- **可配置**：不同 group 可以有不同的锁超时时间

### 3. 为什么需要 ShareSession？
- **性能优化**：减少网络传输
- **类似 HTTP 的 ETag**：客户端和服务端协商增量更新

### 4. 为什么需要 DelayedShareFetch？
- **避免空轮询**：没有可用消息时不立即返回
- **事件驱动**：当条件满足（ack、锁超时、HWM 更新）时自动唤醒

### 5. 为什么需要 Listener？
- **自动化**：leader 变更时自动清理，无需手动管理
- **解耦**：ReplicaManager 不需要知道 SharePartition 的存在

---

## 总结

SharePartitionManager 从一个简单的 fetch 逻辑，逐步演化成一个复杂的分布式系统组件：

1. **数据结构演化**：从单个 partition → 多 group 支持 → session 管理
2. **并发控制**：消息锁定 → 超时处理 → 异步初始化
3. **性能优化**：session 缓存 → delayed fetch → 异步编程
4. **可靠性**：状态持久化 → leader 变更处理 → 异常处理
5. **可观测性**：metrics → 日志 → 监控

每一步的复杂度引入都有明确的业务需求或性能优化目标！
