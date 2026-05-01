# SharePartition vs SubscriptionState/TopicPartitionState 对比

对比 Share Group 和 Consumer Group 中负责管理消费进度和 records 的核心组件。

---

## 核心问题

**问题**：在 Consumer Group 中，谁负责类似 SharePartition 的功能？

**答案**：主要是 **SubscriptionState** 和其内部的 **TopicPartitionState**，配合 **Fetcher**。

---

## 组件对应关系

```
┌─────────────────────────────────────────────────────────────────┐
│                    Share Group                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  SharePartitionManager (Broker 端)                              │
│    └─> SharePartition                                           │
│          - 管理单个 (groupId, partition) 的状态                 │
│          - 跟踪消息状态 (AVAILABLE/ACQUIRED/ACKNOWLEDGED)       │
│          - 处理 acquire/acknowledge                             │
│          - 持久化到 ShareCoordinator                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  Consumer Group                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  SubscriptionState (Consumer 端 - 客户端)                       │
│    └─> TopicPartitionState                                      │
│          - 管理单个 partition 的消费状态                        │
│          - 跟踪 fetch position (消费进度)                       │
│          - 本地管理，不持久化（只在 commit 时持久化）           │
│                                                                  │
│  + Fetcher (Consumer 端)                                        │
│    - 从 Broker 获取 records                                     │
│    - 更新 SubscriptionState 的 position                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 详细对比

### 1. SharePartition (Share Group - Broker 端)

**位置**：`kafka.server.share.SharePartition`（Broker 端）

**职责**：
1. 管理消息的状态（消息级别）
2. 分配消息给 consumer（acquire）
3. 处理消息确认（acknowledge）
4. 持久化状态到 ShareCoordinator

**核心数据结构**：

```java
class SharePartition {
    // 核心：消息级别的状态管理
    TreeMap<Long, InFlightBatch> cachedState;  // baseOffset -> batch

    // 消费进度跟踪
    long nextFetchOffset;         // 下一次 fetch 的起始位置
    long startOffset;             // partition 的起始 offset
    long endOffset;               // partition 的结束 offset

    // 状态管理
    SharePartitionState state;    // LOADING, ACTIVE, FENCED

    // 锁和超时管理
    Timer timer;                  // 管理消息锁超时
    int recordLockDurationMs;     // 锁定时间

    // 持久化
    Persister persister;          // 持久化到 __share_group_state

    // 核心方法
    CompletableFuture<AcquiredRecords> acquire(String memberId, ...);
    CompletableFuture<Void> acknowledge(String memberId, List<AcknowledgementBatch> batches);
    CompletableFuture<Void> releaseAcquiredRecords(String memberId);
}

// 消息状态
class InFlightBatch {
    long firstOffset;
    long lastOffset;
    RecordState batchState;  // AVAILABLE, ACQUIRED, ACKNOWLEDGED, ARCHIVED
    Map<Long, OffsetMetadata> offsetState;  // offset -> 详细状态
}
```

**特点**：
- ✅ **Broker 端状态**：在 Broker 上维护
- ✅ **消息级别追踪**：每个 offset 都有状态
- ✅ **持久化**：状态持久化到 __share_group_state
- ✅ **锁机制**：消息有锁定 + 超时

---

### 2. SubscriptionState + TopicPartitionState (Consumer Group - Consumer 端)

**位置**：`org.apache.kafka.clients.consumer.internals.SubscriptionState`（Consumer 端）

**职责**：
1. 管理分配的 partitions
2. 跟踪每个 partition 的消费进度（position）
3. 管理 pause/resume 状态
4. 本地维护，不负责持久化（由 OffsetMetadataManager 负责）

**核心数据结构**：

```java
class SubscriptionState {
    // 核心：partition 级别的状态管理
    PartitionStates<TopicPartitionState> assignment;  // partition -> state

    // 订阅信息
    Set<String> subscription;       // 订阅的 topics
    SubscriptionType subscriptionType;  // AUTO_TOPICS, USER_ASSIGNED, etc.

    // Rebalance 监听器
    Optional<ConsumerRebalanceListener> rebalanceListener;

    // 核心方法（简化）
    void assignFromSubscribed(Collection<TopicPartition> partitions);
    void seek(TopicPartition tp, long offset);
    void seekValidated(TopicPartition tp, FetchPosition position);
    FetchPosition position(TopicPartition tp);
    void pause(TopicPartition tp);
    void resume(TopicPartition tp);
}

// 单个 partition 的状态
class TopicPartitionState {
    // 核心：fetch position（消费到哪里了）
    FetchPosition position;  // last consumed position
                            // { offset, epoch, currentLeaderEpoch }

    // Partition 的元数据
    Long highWatermark;      // HWM
    Long logStartOffset;     // LSO
    Long lastStableOffset;   // LSO（for transactions）

    // 状态
    FetchState fetchState;   // INITIALIZING, FETCHING, AWAIT_RESET, etc.
    boolean paused;          // 是否暂停
    AutoOffsetResetStrategy resetStrategy;  // earliest, latest, none

    // Preferred Read Replica (KIP-392)
    Integer preferredReadReplica;
    Long preferredReadReplicaExpireTimeMs;

    // 核心方法
    void seekValidated(FetchPosition position);
    void position(FetchPosition position);
    boolean isFetchable();
    void pause();
    void resume();
}

class FetchPosition {
    long offset;                // 消费到的 offset
    Optional<Integer> offsetEpoch;  // offset 对应的 epoch
    Metadata.LeaderAndEpoch currentLeader;  // 当前 leader
}
```

**特点**：
- ✅ **Consumer 端状态**：在客户端维护
- ✅ **Partition 级别追踪**：每个 partition 一个 position
- ✅ **不持久化**：只在内存中，commit 时才持久化
- ✅ **无锁机制**：partition 独占，不需要锁

---

### 3. Fetcher (Consumer Group - Consumer 端)

**位置**：`org.apache.kafka.clients.consumer.internals.Fetcher`（Consumer 端）

**职责**：
1. 发送 FetchRequest 到 Broker
2. 接收 FetchResponse 并解析 records
3. 更新 SubscriptionState 的 position

**核心方法**：

```java
class Fetcher {
    private final SubscriptionState subscriptions;
    private final FetchBuffer fetchBuffer;

    // 核心方法：获取 records
    Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<>();

        // 从 fetchBuffer 中取出已经 fetch 的数据
        int recordsRemaining = maxPollRecords;
        while (recordsRemaining > 0) {
            CompletedFetch completedFetch = fetchBuffer.nextInLineFetch();
            if (completedFetch == null) break;

            // 解析 records
            List<ConsumerRecord<K, V>> records = parseRecords(completedFetch);

            // 更新 SubscriptionState 的 position
            TopicPartition partition = completedFetch.partition;
            FetchPosition nextPosition = FetchPosition.of(
                completedFetch.nextFetchOffset,
                completedFetch.lastEpoch
            );
            subscriptions.position(partition, nextPosition);

            drained.put(partition, records);
            recordsRemaining -= records.size();
        }

        return drained;
    }

    // 发送 fetch 请求
    void sendFetches() {
        // 遍历所有可 fetchable 的 partitions
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap =
            prepareFetchRequests();

        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry :
            fetchRequestMap.entrySet()) {

            Node fetchTarget = entry.getKey();
            FetchSessionHandler.FetchRequestData data = entry.getValue();

            // 发送 FetchRequest
            client.send(fetchTarget, data.toSend())
                .addListener(new RequestFutureListener<ClientResponse>() {
                    @Override
                    public void onSuccess(ClientResponse resp) {
                        handleFetchResponse(resp, data);
                    }
                });
        }
    }
}
```

---

## 架构对比图

### Share Group 的完整流程

```
┌─────────────────────────────────────────────────────────────────┐
│                   Consumer (客户端)                              │
├─────────────────────────────────────────────────────────────────┤
│  KafkaShareConsumer.poll()                                      │
│    └─> ShareFetchRequest -> Broker                             │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Broker (SharePartitionManager)                   │
├─────────────────────────────────────────────────────────────────┤
│  processShareFetch()                                            │
│    ├─> SharePartition.acquire()                                │
│    │     ├─ 从 cachedState 找 AVAILABLE 消息                   │
│    │     ├─ 标记为 ACQUIRED                                    │
│    │     ├─ 设置锁超时                                         │
│    │     └─ 返回 records                                       │
│    └─> 返回 ShareFetchResponse                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Consumer (客户端)                                │
├─────────────────────────────────────────────────────────────────┤
│  处理 records                                                    │
│    └─> ShareAcknowledgeRequest -> Broker                       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Broker (SharePartitionManager)                   │
├─────────────────────────────────────────────────────────────────┤
│  acknowledge()                                                   │
│    └─> SharePartition.acknowledge()                            │
│          ├─ 更新 offsetState: ACQUIRED -> ACKNOWLEDGED         │
│          ├─ 持久化到 ShareCoordinator                          │
│          └─ 唤醒等待的 fetch                                   │
└─────────────────────────────────────────────────────────────────┘
```

**关键点**：
- ✅ Broker 端管理状态（SharePartition）
- ✅ 消息级别的状态追踪
- ✅ 需要显式 acknowledge

---

### Consumer Group 的完整流程

```
┌─────────────────────────────────────────────────────────────────┐
│                   Consumer (客户端)                              │
├─────────────────────────────────────────────────────────────────┤
│  KafkaConsumer.poll()                                           │
│    └─> Fetcher.fetchedRecords()                                │
│          ├─ 从 SubscriptionState 获取分配的 partitions         │
│          └─ 读取本地的 FetchPosition                           │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Consumer (Fetcher)                             │
├─────────────────────────────────────────────────────────────────┤
│  sendFetches()                                                   │
│    ├─> 构建 FetchRequest                                        │
│    │     ├─ partition: topic1-p0                                │
│    │     ├─ fetchOffset: 1000 (from SubscriptionState)         │
│    │     └─ maxBytes: 1MB                                       │
│    └─> 发送到 Broker                                            │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Broker (ReplicaManager)                        │
├─────────────────────────────────────────────────────────────────┤
│  处理 FetchRequest                                               │
│    ├─> 从 Log 读取数据 (offset 1000 开始)                      │
│    └─> 返回 FetchResponse                                       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Consumer (Fetcher)                             │
├─────────────────────────────────────────────────────────────────┤
│  handleFetchResponse()                                           │
│    ├─> 解析 records                                             │
│    ├─> 更新 SubscriptionState.position                         │
│    │     position(topic1-p0, FetchPosition(1010))  ← 本地更新  │
│    └─> 返回 records 给应用层                                    │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Consumer (应用层)                              │
├─────────────────────────────────────────────────────────────────┤
│  处理 records                                                    │
│    └─> commitSync()                                             │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 Consumer (ConsumerCoordinator)                   │
├─────────────────────────────────────────────────────────────────┤
│  commitOffsetsSync()                                            │
│    ├─> 从 SubscriptionState 读取 position                      │
│    ├─> 构建 OffsetCommitRequest                                │
│    │     topic1-p0 -> offset: 1010                             │
│    └─> 发送到 Coordinator                                       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Group Coordinator (Broker)                          │
├─────────────────────────────────────────────────────────────────┤
│  handleOffsetCommit()                                           │
│    └─> 持久化到 __consumer_offsets                             │
└─────────────────────────────────────────────────────────────────┘
```

**关键点**：
- ✅ Consumer 端管理状态（SubscriptionState）
- ✅ Partition 级别的 position 追踪
- ✅ 只在 commit 时持久化

---

## 数据结构对比表

| 维度 | SharePartition | SubscriptionState + TopicPartitionState |
|------|----------------|----------------------------------------|
| **位置** | Broker 端 | Consumer 端（客户端） |
| **粒度** | 消息级别（每个 offset） | Partition 级别（每个 partition 一个 position） |
| **状态类型** | RecordState (AVAILABLE/ACQUIRED/ACKNOWLEDGED/ARCHIVED) | FetchPosition (offset + epoch) |
| **持久化** | 实时持久化到 __share_group_state | 只在 commit 时持久化到 __consumer_offsets |
| **锁机制** | 有（消息锁定 + 超时） | 无（partition 独占） |
| **状态复杂度** | 高（每个消息都有状态） | 低（每个 partition 一个 offset） |
| **内存占用** | 高 | 低 |
| **管理者** | SharePartitionManager | Consumer 自己 |

---

## 核心方法对比

### SharePartition 的核心方法

```java
// 1. 获取消息（acquire）
CompletableFuture<AcquiredRecords> acquire(
    String memberId,
    FetchPartitionData fetchPartitionData
) {
    // 从 cachedState 中找 AVAILABLE 消息
    // 标记为 ACQUIRED
    // 设置锁超时
    // 返回 records
}

// 2. 确认消息（acknowledge）
CompletableFuture<Void> acknowledge(
    String memberId,
    List<AcknowledgementBatch> batches
) {
    // 更新 offsetState
    // ACQUIRED -> ACKNOWLEDGED/ARCHIVED
    // 持久化到 ShareCoordinator
}

// 3. 释放消息（release）
CompletableFuture<Void> releaseAcquiredRecords(String memberId) {
    // 将 ACQUIRED 的消息变回 AVAILABLE
    // 供其他 consumer 获取
}
```

---

### SubscriptionState + TopicPartitionState 的核心方法

```java
// 1. 设置消费位置（seek）
void seekValidated(TopicPartition tp, FetchPosition position) {
    TopicPartitionState state = assignedState(tp);
    state.seekValidated(position);  // 设置 position
}

// 2. 获取消费位置（position）
FetchPosition position(TopicPartition tp) {
    TopicPartitionState state = assignedState(tp);
    return state.position;  // 返回当前 position
}

// 3. 更新消费位置（由 Fetcher 调用）
void position(TopicPartition tp, FetchPosition position) {
    TopicPartitionState state = assignedState(tp);
    state.position(position);  // 更新 position
}

// 4. 暂停/恢复（pause/resume）
void pause(TopicPartition tp) {
    assignedState(tp).pause();  // 暂停 fetch
}

void resume(TopicPartition tp) {
    assignedState(tp).resume();  // 恢复 fetch
}
```

---

## 状态流转对比

### SharePartition 的状态流转

```
消息状态流转：
AVAILABLE ──acquire──> ACQUIRED ──acknowledge(ACCEPT)──> ACKNOWLEDGED
    ▲                     │
    │                     │ timeout / acknowledge(RELEASE)
    └─────────────────────┘

    ACQUIRED ──acknowledge(REJECT)──> deliveryCount++
                                          │
                                          │ if deliveryCount > maxDeliveryCount
                                          ▼
                                       ARCHIVED (永久归档)

Partition 状态流转：
LOADING ──初始化完成──> ACTIVE ──leader 变更──> FENCED
```

---

### SubscriptionState 的状态流转

```
FetchState 流转：
INITIALIZING ──seekValidated──> FETCHING ──pause──> PAUSED
                                    │                   │
                                    │ ←─────resume──────┘
                                    │
                                    │ ──需要 reset──> AWAIT_RESET
                                    │                   │
                                    │ ←─reset 完成──────┘

Position 更新：
1. 初始化：seekValidated(position)
2. Fetch 后：position(tp, new FetchPosition(nextOffset))
3. Commit：从 position 构建 OffsetCommitRequest
4. Rebalance：position 被清空，重新 seekValidated
```

---

## 配合的其他组件

### Share Group 的配合组件

```
SharePartitionManager
  └─> SharePartition (核心状态管理)
        ├─> InFlightBatch (消息状态)
        ├─> Timer (锁超时)
        ├─> Persister (持久化)
        └─> ShareCoordinator (远程状态存储)

ShareSessionCache
  └─> ShareSession (session 管理)

DelayedShareFetch (Purgatory)
  └─> 等待消息可用
```

---

### Consumer Group 的配合组件

```
SubscriptionState (核心状态管理)
  └─> TopicPartitionState (partition 状态)
        └─> FetchPosition (消费位置)

Fetcher (fetch 逻辑)
  ├─> 读取 SubscriptionState
  ├─> 发送 FetchRequest
  ├─> 更新 SubscriptionState
  └─> 返回 records

ConsumerCoordinator (group 管理)
  ├─> 处理 rebalance
  ├─> 读取 SubscriptionState.position
  └─> commitOffsets (持久化到 __consumer_offsets)

GroupMetadataManager (Broker 端)
  └─> OffsetMetadataManager (存储 committed offsets)
```

---

## 总结

### 对应关系总结

| Share Group | Consumer Group |
|-------------|----------------|
| **SharePartition** (Broker 端) | **SubscriptionState + TopicPartitionState** (Consumer 端) |
| SharePartitionManager | Fetcher + ConsumerCoordinator |
| InFlightBatch | FetchPosition |
| RecordState (消息状态) | FetchState (partition 状态) |
| Persister -> __share_group_state | CommitOffsets -> __consumer_offsets |

### 核心差异

1. **位置**：
   - SharePartition 在 **Broker 端**
   - SubscriptionState 在 **Consumer 端**

2. **粒度**：
   - SharePartition 管理 **消息级别** 的状态
   - SubscriptionState 管理 **partition 级别** 的 position

3. **持久化**：
   - SharePartition **实时持久化** 每个消息的状态
   - SubscriptionState **按需持久化**（commit 时）

4. **复杂度**：
   - SharePartition 需要管理锁、超时、状态机
   - SubscriptionState 只需要管理 offset

### 为什么设计不同？

**Consumer Group**：
- Partition 独占 → 不需要复杂的状态管理
- Consumer 端控制 → 状态在客户端，灵活
- 简单高效 → 只管理 offset，性能好

**Share Group**：
- Partition 共享 → 需要 Broker 协调消息分配
- Broker 端控制 → 状态在服务端，保证一致性
- 复杂精确 → 消息级别状态，支持失败重试

这就是为什么 Share Group 需要 SharePartition（Broker 端），而 Consumer Group 只需要 SubscriptionState（Consumer 端）！
