# Share Group vs Consumer Group 架构对比

深入对比 Kafka 的 Share Group 和传统 Consumer Group 的实现差异。

---

## 核心差异总览

```
┌─────────────────────────────────────────────────────────────────┐
│                    Consumer Group                                │
│  (传统模式 - Partition 独占)                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Topic: orders (3 partitions)                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │ Partition│  │ Partition│  │ Partition│                      │
│  │    0     │  │    1     │  │    2     │                      │
│  └─────┬────┘  └─────┬────┘  └─────┬────┘                      │
│        │             │             │                             │
│        │独占         │独占         │独占                         │
│        ▼             ▼             ▼                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                        │
│  │Consumer1│  │Consumer2│  │Consumer3│                        │
│  └─────────┘  └─────────┘  └─────────┘                        │
│                                                                  │
│  特点：                                                          │
│  ✓ 一个 partition 只能分配给一个 consumer                       │
│  ✓ Rebalance 时重新分配 partition                               │
│  ✓ 通过 offset commit 跟踪消费进度                              │
│  ✗ Consumer 数量不能超过 partition 数量                         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Share Group                                 │
│  (新模式 - 消息级别共享)                                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Topic: orders (3 partitions)                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │ Partition│  │ Partition│  │ Partition│                      │
│  │    0     │  │    1     │  │    2     │                      │
│  └─────┬────┘  └─────┬────┘  └─────┬────┘                      │
│        │             │             │                             │
│        │共享         │共享         │共享                         │
│        ▼             ▼             ▼                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│  │Consumer1│  │Consumer2│  │Consumer3│  │Consumer4│          │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘          │
│       ▲             ▲             ▲             ▲               │
│       └─────────────┴─────────────┴─────────────┘               │
│             所有 consumer 都可以从任意 partition 获取消息       │
│                                                                  │
│  特点：                                                          │
│  ✓ 多个 consumer 可以并发消费同一个 partition                   │
│  ✓ 消息级别的锁定（不是 partition 级别）                        │
│  ✓ 支持 ACCEPT/RELEASE/REJECT 确认                             │
│  ✓ Consumer 数量可以远超 partition 数量                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 架构对比

### 1. Consumer Group 架构

```
┌─────────────────────────────────────────────────────────────────┐
│              Group Coordinator (在 Broker 上)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ GroupMetadataManager                                   │    │
│  │  - 管理所有 Consumer Group                             │    │
│  └────────────────────────────────────────────────────────┘    │
│                         │                                        │
│                         ├─> ConsumerGroup1                      │
│                         │     - groupId: "my-group"             │
│                         │     - state: STABLE                   │
│                         │     - members: [m1, m2, m3]           │
│                         │     - targetAssignment:               │
│                         │       m1 -> [p0]                      │
│                         │       m2 -> [p1]                      │
│                         │       m3 -> [p2]                      │
│                         │                                        │
│                         └─> ConsumerGroup2                      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ OffsetMetadataManager                                  │    │
│  │  - 管理 offset 提交                                    │    │
│  │  - offsetsByGroup:                                     │    │
│  │    "my-group" -> {                                     │    │
│  │      "orders-0" -> offset: 1000                        │    │
│  │      "orders-1" -> offset: 2000                        │    │
│  │      "orders-2" -> offset: 3000                        │    │
│  │    }                                                    │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  持久化到: __consumer_offsets topic                             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              Consumer 端（客户端）                               │
├─────────────────────────────────────────────────────────────────┤
│  Consumer1:                                                      │
│    - 订阅 topic: "orders"                                       │
│    - 分配到: partition 0                                        │
│    - 本地维护 fetch offset                                      │
│    - poll() 时只 fetch partition 0 的数据                       │
│    - commitSync() 提交 offset 到 coordinator                    │
└─────────────────────────────────────────────────────────────────┘
```

**关键点**：
1. **Partition 分配**：由 Coordinator 决定，通过 rebalance 机制
2. **状态存储**：只存储 offset（每个 partition 一个 offset）
3. **Consumer 端逻辑**：Consumer 本地管理 fetch offset，定期 commit
4. **无消息状态**：不跟踪单个消息的状态

---

### 2. Share Group 架构

```
┌─────────────────────────────────────────────────────────────────┐
│           Share Coordinator (独立的 Coordinator)                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ ShareCoordinatorShard                                  │    │
│  │  - 管理 Share Group 的状态                             │    │
│  └────────────────────────────────────────────────────────┘    │
│                         │                                        │
│                         ├─> shareStateMap:                      │
│                         │   ("group1", "orders", 0) -> {        │
│                         │     startOffset: 1000,                │
│                         │     stateEpoch: 5                     │
│                         │   }                                    │
│                         │                                        │
│                         └─> offsetsManager:                     │
│                             ("group1", "orders", 0) -> 1050     │
│                                                                  │
│  持久化到: __share_group_state topic                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│            Broker 端（SharePartitionManager）                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ SharePartitionManager                                  │    │
│  │  - 每个 Broker 上独立运行                              │    │
│  │  - 管理本 Broker 作为 leader 的 share partitions       │    │
│  └────────────────────────────────────────────────────────┘    │
│                         │                                        │
│                         ├─> SharePartition1                     │
│                         │   (groupId="group1", topic="orders", partition=0)│
│                         │                                        │
│                         │   ┌──────────────────────────────┐    │
│                         │   │ InFlightBatch 状态           │    │
│                         │   │   offset 1000-1009:          │    │
│                         │   │     1000: ACQUIRED (m1)      │    │
│                         │   │     1001: ACKNOWLEDGED (m1)  │    │
│                         │   │     1002: AVAILABLE          │    │
│                         │   │     1003: ARCHIVED           │    │
│                         │   │     ...                      │    │
│                         │   └──────────────────────────────┘    │
│                         │                                        │
│                         │   - timer: 管理锁超时                 │
│                         │   - persister: 持久化状态             │
│                         │                                        │
│                         └─> SharePartition2                     │
│                             (groupId="group2", topic="orders", partition=0)│
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ ShareSessionCache                                      │    │
│  │  - 管理 member 的 session                              │    │
│  │  - ("group1", "member1") -> ShareSession               │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              Consumer 端（客户端）                               │
├─────────────────────────────────────────────────────────────────┤
│  Consumer1:                                                      │
│    - 订阅 share group: "group1", topic: "orders"               │
│    - 不分配固定 partition（可以从任意 partition 获取）          │
│    - poll() 从任意 partition 获取可用消息                       │
│    - acknowledge() 确认消息处理结果                             │
│      - ACCEPT: 处理成功                                         │
│      - RELEASE: 重新分配给其他 consumer                         │
│      - REJECT: 标记为无法处理                                   │
└─────────────────────────────────────────────────────────────────┘
```

**关键点**：
1. **无 Partition 分配**：Consumer 不绑定特定 partition
2. **消息级别状态**：跟踪每个 offset 的状态（AVAILABLE/ACQUIRED/ACKNOWLEDGED/ARCHIVED）
3. **Broker 端状态**：SharePartitionManager 在 Broker 上管理状态
4. **锁定机制**：消息被 acquire 后有超时时间

---

## 数据结构对比

### Consumer Group 的核心数据结构

```java
// 1. ConsumerGroup - 管理 group 的元数据
class ConsumerGroup {
    String groupId;
    ConsumerGroupState state;  // EMPTY, ASSIGNING, RECONCILING, STABLE

    // Members
    TimelineHashMap<String, ConsumerGroupMember> members;

    // Target assignment (由 Assignor 计算)
    TimelineHashMap<String, Assignment> targetAssignment;
    //   memberId -> Assignment{
    //     partitions: Set<TopicPartition>
    //   }

    // 倒排索引：哪些 member 拥有哪些 partition
    TimelineHashMap<TopicIdPartition, String> invertedTargetAssignment;
}

// 2. OffsetMetadataManager - 管理 offset 提交
class OffsetMetadataManager {
    // 三层嵌套：group -> topic -> partition -> offset
    TimelineHashMap<String,                    // groupId
        TimelineHashMap<String,                // topic
            TimelineHashMap<Integer,           // partition
                OffsetAndMetadata              // offset + metadata
            >
        >
    > offsetsByGroup;
}

// 3. Offset 数据
class OffsetAndMetadata {
    long offset;           // 消费到的 offset
    String metadata;       // 用户自定义元数据
    long commitTimestamp;  // 提交时间
    long expireTimestamp;  // 过期时间
}
```

**特点**：
- **轻量级**：只存储 offset，不存储消息状态
- **Partition 粒度**：以 partition 为单位管理
- **简单**：每个 partition 只有一个 offset

---

### Share Group 的核心数据结构

```java
// 1. SharePartition - 管理单个 (group, partition) 的状态
class SharePartition {
    String groupId;
    TopicIdPartition topicIdPartition;

    // 核心：消息级别的状态管理
    TreeMap<Long, InFlightBatch> cachedState;  // baseOffset -> batch

    long nextFetchOffset;         // 下一次 fetch 的起始位置
    long startOffset;             // partition 的起始 offset
    long endOffset;               // partition 的结束 offset

    Timer timer;                  // 管理锁超时
    Persister persister;          // 持久化状态
    int maxInFlightMessages;      // 最大 in-flight 消息数
    int maxDeliveryCount;         // 最大投递次数
}

// 2. InFlightBatch - 管理一批消息的状态
class InFlightBatch {
    long firstOffset;
    long lastOffset;

    // Batch 级别状态（优化：如果所有 offset 状态一致）
    RecordState batchState;
    String batchMemberId;
    long acquiredTimestamp;

    // Offset 级别状态（如果需要精细化追踪）
    Map<Long, OffsetMetadata> offsetState;  // offset -> metadata
}

// 3. OffsetMetadata - 单个消息的状态
class OffsetMetadata {
    RecordState state;  // AVAILABLE, ACQUIRED, ACKNOWLEDGED, ARCHIVED
    String memberId;    // 哪个 member 获取了这个消息
    long acquiredTimestamp;  // 获取时间
    byte deliveryCount;      // 投递次数
}

enum RecordState {
    AVAILABLE,      // 可获取
    ACQUIRED,       // 已锁定
    ACKNOWLEDGED,   // 已确认
    ARCHIVED        // 已归档（超过最大投递次数）
}

// 4. ShareSession - 管理 member 的 fetch session
class ShareSession {
    int epoch;  // 防止乱序
    ImplicitLinkedHashCollection<CachedSharePartition> partitionMap;
    // 缓存 member 订阅的 partition，支持增量更新
}
```

**特点**：
- **重量级**：跟踪每个消息的状态
- **消息粒度**：以 offset 为单位管理
- **复杂**：需要维护状态机、锁、超时等

---

## 功能对比表

| 功能维度 | Consumer Group | Share Group |
|---------|---------------|-------------|
| **Partition 分配** | 独占（一个 partition 一个 consumer） | 共享（多个 consumer 可访问同一个 partition） |
| **Consumer 数量限制** | ≤ partition 数量 | 无限制（可以远超 partition 数量） |
| **消息分配粒度** | Partition 级别 | 消息级别 |
| **消费进度跟踪** | Offset（每个 partition 一个） | 消息状态（每个 offset 一个状态） |
| **Rebalance** | 需要（partition 重新分配） | 不需要 |
| **消息锁定** | 无（partition 独占即锁定） | 有（消息级别锁定 + 超时） |
| **失败重试** | 手动 seek 或重启 consumer | 自动（RELEASE 消息） |
| **消息确认** | commitSync/commitAsync | acknowledge (ACCEPT/RELEASE/REJECT) |
| **状态持久化** | `__consumer_offsets` | `__share_group_state` |
| **状态复杂度** | 简单（只存 offset） | 复杂（每个消息的状态） |
| **内存占用** | 低 | 高（需要缓存消息状态） |
| **并发度** | 受限于 partition 数量 | 不受限 |

---

## 消费流程对比

### Consumer Group 的消费流程

```
1. Consumer 启动
   ├─ JoinGroup 请求 -> Coordinator
   ├─ Coordinator 触发 rebalance
   ├─ Assignor 计算 partition 分配
   ├─ SyncGroup 获取分配结果
   └─ Consumer 得到分配的 partitions: [p0, p1]

2. Fetch 消息
   ├─ poll()
   ├─ 从分配的 partitions 发送 FetchRequest 到对应的 Broker
   ├─ Broker 从 Log 读取数据
   └─ 返回 FetchResponse

3. 处理消息
   ├─ 应用层处理消息
   └─ (本地维护 offset)

4. 提交 Offset
   ├─ commitSync() 或 commitAsync()
   ├─ 发送 OffsetCommitRequest 到 Coordinator
   ├─ Coordinator 存储 offset 到 __consumer_offsets
   └─ 返回成功

5. Rebalance（如果有 member 加入/离开）
   ├─ 所有 member 停止消费
   ├─ 重新分配 partitions
   ├─ 从 committed offset 恢复
   └─ 继续消费

时序图：
Consumer        Coordinator       Broker (Leader of p0)
   │                 │                      │
   ├─JoinGroup──────>│                      │
   │<─Assignment─────┤                      │
   │                 │                      │
   ├─FetchRequest(p0)─────────────────────>│
   │<─FetchResponse(records)────────────────┤
   │                 │                      │
   ├─(process msgs)  │                      │
   │                 │                      │
   ├─CommitOffset───>│                      │
   │<─Success────────┤                      │
```

**关键特点**：
- **Consumer 主导**：Consumer 决定何时 fetch、何时 commit
- **Partition 绑定**：Consumer 只 fetch 分配给自己的 partition
- **无状态 Broker**：Broker 只负责读取 Log，不管理消费状态

---

### Share Group 的消费流程

```
1. Consumer 启动
   ├─ 无需 JoinGroup（没有 rebalance）
   └─ 开始发送 ShareFetch 请求

2. Fetch 消息（ShareFetch）
   ├─ ShareFetchRequest -> Broker (任意 partition 的 leader)
   ├─ Broker 的 SharePartitionManager 处理
   │   ├─ 从 SharePartition 查找 AVAILABLE 消息
   │   ├─ 将消息状态改为 ACQUIRED
   │   ├─ 设置锁超时 timer
   │   └─ 如果没有可用消息，放入 DelayedShareFetch purgatory
   ├─ 返回 ShareFetchResponse
   └─ Consumer 得到消息

3. 处理消息
   ├─ 应用层处理消息
   └─ 决定 ack 类型

4. 确认消息（Acknowledge）
   ├─ ShareAcknowledgeRequest -> Broker
   │   - offsets: [100, 101, 102]
   │   - ackType: ACCEPT
   ├─ SharePartitionManager 更新状态
   │   ├─ 将 offset 状态改为 ACKNOWLEDGED
   │   ├─ 持久化到 ShareCoordinator
   │   └─ 唤醒等待的 DelayedShareFetch
   └─ 返回成功

5. 失败处理
   ├─ 如果处理失败：ackType = RELEASE
   │   └─ 消息状态变回 AVAILABLE，其他 consumer 可以获取
   ├─ 如果无法处理：ackType = REJECT
   │   └─ deliveryCount++，如果超过 maxDeliveryCount，变为 ARCHIVED
   └─ 如果锁超时：自动 RELEASE

时序图：
Consumer        Broker(SharePartitionManager)   ShareCoordinator
   │                      │                             │
   ├─ShareFetchRequest──>│                             │
   │                      ├─查找 AVAILABLE 消息         │
   │                      ├─标记为 ACQUIRED             │
   │                      ├─设置锁超时                  │
   │<─ShareFetchResponse─┤                             │
   │                      │                             │
   ├─(process msgs)       │                             │
   │                      │                             │
   ├─ShareAckRequest────>│                             │
   │  (ACCEPT)            ├─更新状态为 ACKNOWLEDGED     │
   │                      ├─持久化请求──────────────────>│
   │<─Success─────────────┤<─持久化成功─────────────────┤
```

**关键特点**：
- **Broker 主导**：Broker 管理消息的分配和状态
- **无 Partition 绑定**：Consumer 可以从任意 partition 获取消息
- **有状态 Broker**：Broker 需要维护每个消息的状态

---

## 状态持久化对比

### Consumer Group 的持久化

```
Topic: __consumer_offsets
Key: (groupId, topic, partition)
Value: (offset, metadata, timestamp)

示例：
Key: ("my-group", "orders", 0)
Value: {
  offset: 1000,
  metadata: "",
  commitTimestamp: 1234567890,
  expireTimestamp: 1234567890 + 7days
}
```

**特点**：
- **简单**：每个 partition 一条记录
- **小**：只存储 offset 和元数据
- **批量写入**：一次 commit 写入多个 partition 的 offset

---

### Share Group 的持久化

```
Topic: __share_group_state
Key: (groupId, topicId, partition)
Value: 复杂的状态数据

示例：
Key: ("group1", topic-uuid, 0)
Value: {
  stateEpoch: 5,
  startOffset: 1000,
  stateBatches: [
    {
      firstOffset: 1000,
      lastOffset: 1009,
      deliveryState: ACKNOWLEDGED,
      deliveryCount: 1
    },
    {
      firstOffset: 1010,
      lastOffset: 1019,
      deliveryState: ACQUIRED,
      deliveryCount: 1,
      offsetMetadata: {
        1010: {state: ACQUIRED, memberId: "m1"},
        1011: {state: ACKNOWLEDGED, memberId: "m1"},
        1012: {state: AVAILABLE, memberId: null},
        ...
      }
    }
  ]
}
```

**特点**：
- **复杂**：需要存储每个消息的状态
- **大**：包含状态机、memberId、deliveryCount 等
- **频繁写入**：每次 ack 都可能触发持久化

---

## 性能和扩展性对比

### Consumer Group

**优点**：
- ✅ **轻量级**：状态简单，内存占用小
- ✅ **高吞吐**：Broker 无状态，只需读取 Log
- ✅ **顺序消费**：每个 partition 保证顺序
- ✅ **可预测**：partition 分配固定，容易调试

**缺点**：
- ❌ **扩展性受限**：consumer 数量 ≤ partition 数量
- ❌ **Rebalance 开销**：需要停止所有 consumer
- ❌ **负载不均衡**：如果 partition 数据量不均，会有热点

**适用场景**：
- 流式处理（需要顺序）
- ETL 任务
- 日志聚合
- partition 数量足够多

---

### Share Group

**优点**：
- ✅ **高并发**：consumer 数量不受限
- ✅ **负载均衡**：消息级别分配，自动平衡
- ✅ **无 Rebalance**：consumer 加入/离开不影响其他 consumer
- ✅ **灵活重试**：RELEASE 可以重新分配消息

**缺点**：
- ❌ **重量级**：需要维护大量状态
- ❌ **内存占用大**：每个消息都有状态
- ❌ **无顺序保证**：消息可能被乱序消费
- ❌ **持久化开销**：频繁写入 __share_group_state

**适用场景**：
- 任务队列（无序）
- 并发处理
- partition 数量少但消费者多
- 需要精确的失败重试控制

---

## 代码实现对比

### Consumer Group 的核心代码路径

```
Client 端：
├─ KafkaConsumer.poll()
│   ├─ ConsumerCoordinator.poll()
│   │   ├─ ensureActiveGroup()  // 处理 rebalance
│   │   └─ updateAssignment()
│   ├─ Fetcher.fetchedRecords()
│   │   └─ 从分配的 partitions fetch 数据
│   └─ 返回 ConsumerRecords

Broker 端：
├─ GroupCoordinator
│   ├─ GroupMetadataManager
│   │   ├─ ConsumerGroup (管理 group 元数据)
│   │   └─ 处理 JoinGroup/SyncGroup
│   └─ OffsetMetadataManager
│       └─ 处理 OffsetCommit/OffsetFetch

持久化：
└─ __consumer_offsets topic
```

---

### Share Group 的核心代码路径

```
Client 端：
├─ KafkaShareConsumer.poll()
│   ├─ ShareFetchRequest -> Broker
│   └─ 返回 ShareFetchResponse

Broker 端：
├─ SharePartitionManager (每个 Broker)
│   ├─ partitionCacheMap: Map<SharePartitionKey, SharePartition>
│   ├─ cache: ShareSessionCache
│   ├─ processShareFetch()
│   │   ├─ getOrCreateSharePartition()
│   │   ├─ sharePartition.acquire()
│   │   └─ addDelayedShareFetch()  // 如果无可用消息
│   └─ acknowledge()
│       ├─ sharePartition.acknowledge()
│       └─ persister.persist()
│
├─ SharePartition (单个 share partition)
│   ├─ cachedState: TreeMap<Long, InFlightBatch>
│   ├─ maybeInitialize()  // 从 persister 加载状态
│   ├─ acquire()  // 分配消息
│   ├─ acknowledge()  // 确认消息
│   └─ timer  // 管理锁超时
│
└─ ShareCoordinator (独立 Raft 组)
    ├─ ShareCoordinatorShard
    │   ├─ shareStateMap: 管理状态
    │   └─ offsetsManager: 管理 offset
    └─ 持久化到 __share_group_state topic
```

---

## 总结：何时选择哪种模式？

### 选择 Consumer Group 的场景

```
✅ 使用 Consumer Group 如果：
  1. 需要顺序消费（在 partition 内）
  2. Consumer 数量 ≤ partition 数量
  3. 状态简单（只需要 offset）
  4. 追求最高吞吐量
  5. 不需要复杂的失败重试逻辑

示例：
- 日志聚合：顺序处理每个 partition 的日志
- CDC (Change Data Capture)：保持数据变更顺序
- 流式 ETL：顺序转换数据
```

### 选择 Share Group 的场景

```
✅ 使用 Share Group 如果：
  1. 不关心消费顺序
  2. 需要高并发（consumer 数量 >> partition 数量）
  3. Partition 数量少但负载高
  4. 需要精确的失败重试控制
  5. 像使用消息队列（RabbitMQ/SQS）

示例：
- 任务队列：并发执行独立任务
- 图片处理：多个 worker 并发处理
- API 请求代理：负载均衡到多个 consumer
- 通知发送：并发发送邮件/短信
```

---

## 架构演进对比

```
Consumer Group 的演进：
  1990s: 传统消息队列（无顺序保证）
    ↓
  2011: Kafka 0.8 引入 Consumer Group（顺序 + 分区）
    ↓
  2017: Kafka 0.10.1 引入 Incremental Rebalance
    ↓
  2023: Kafka 3.x 引入 KRaft（无 ZooKeeper）

Share Group 的演进：
  2023: Kafka 提出 KIP-932 (Share Group)
    ↓
  2024: 实现 Share Group
    ↓
  目标：结合 Consumer Group 的吞吐 + 消息队列的灵活性
```

---

## 核心差异总结

| 维度 | Consumer Group | Share Group |
|-----|---------------|-------------|
| **模型** | Partition 独占 | 消息共享 |
| **状态** | Offset (partition 级别) | RecordState (消息级别) |
| **位置** | Coordinator (集中式) | Broker + Coordinator (分布式) |
| **复杂度** | 简单 | 复杂 |
| **并发度** | 受限 | 不受限 |
| **顺序** | 保证 (partition 内) | 不保证 |
| **用途** | 流式处理 | 任务队列 |

Share Group 本质上是**在 Kafka 上实现了消息队列的语义**，而 Consumer Group 保持了**流式处理的语义**。
