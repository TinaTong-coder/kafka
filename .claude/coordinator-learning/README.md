# Kafka Coordinator 架构学习指南

## 概览

Kafka 的 Coordinator 架构是一个通用的**复制状态机（Replicated State Machine）**框架，用于实现各种协调器：
- **ShareCoordinator**: 管理 share group 的状态
- **GroupCoordinator**: 管理 consumer group 的状态
- **TransactionCoordinator**: 管理事务状态

## 核心组件关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                     ShareCoordinatorService                      │
│  (对外API层，处理 ReadShareGroupState/WriteShareGroupState)      │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ 调用 scheduleWriteOperation()
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CoordinatorRuntime<S, U>                      │
│  S = ShareCoordinatorShard, U = CoordinatorRecord               │
│                                                                   │
│  核心职责:                                                         │
│  1. 管理多个 CoordinatorContext (按 TopicPartition 分片)         │
│  2. 事件队列调度 (通过 CoordinatorEventProcessor)                │
│  3. 写入batching和flushing                                       │
│  4. High watermark监听和deferred event管理                       │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ 每个 partition 一个 context
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CoordinatorContext                          │
│  (管理单个 partition 的所有状态)                                  │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  SnapshottableCoordinator                                │   │
│  │    │                                                      │   │
│  │    └─> ShareCoordinatorShard (implements CoordinatorShard)  │
│  │           ├─ TimelineHashMap<SharePartitionKey, offset>  │   │
│  │           ├─ SnapshotRegistry (MVCC 实现)                │   │
│  │           └─ replay() / writeState() / readState()       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
│  其他组件:                                                        │
│  - EventBasedCoordinatorTimer (定时器)                           │
│  - DeferredEventQueue (等待 commit 的事件)                       │
│  - CoordinatorBatch (当前pending的batch)                        │
│  - BufferSupplier (内存管理)                                     │
└─────────────────────────────────────────────────────────────────┘
```

## 关键接口

### 1. CoordinatorShard<U>
**状态机接口**，每个 coordinator 需要实现：

```java
public interface CoordinatorShard<U> {
    // 加载完成后的回调
    void onLoaded(MetadataImage newImage);

    // 元数据更新回调
    void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta);

    // 卸载时的清理
    void onUnloaded();

    // 核心: 重放记录到状态机
    void replay(long offset, long producerId, short producerEpoch, U record);
}
```

**ShareCoordinatorShard** 的实现：
- `replay()`: 根据 `ShareSnapshotKey` 或 `ShareUpdateKey` 更新内存中的 `shareStateMap`
- 使用 `TimelineHashMap` + `SnapshotRegistry` 实现 MVCC（多版本并发控制）

### 2. CoordinatorRuntime<S, U>
**运行时框架**，提供：

#### 写操作
```java
<T> CompletableFuture<T> scheduleWriteOperation(
    String name,
    TopicPartition tp,
    Duration timeout,
    CoordinatorWriteOperation<S, T, U> op
)
```

流程：
1. 创建 `CoordinatorWriteEvent`
2. 加入 `CoordinatorEventProcessor` 队列
3. 在 event processor 线程执行 `op.generateRecordsAndResult(shard)`
4. 生成的 records 通过 `append()` 写入 log 并 replay 到状态机
5. 将 event 加入 `deferredEventQueue`，等待 HW 推进后 complete

#### 读操作
```java
<T> CompletableFuture<T> scheduleReadOperation(
    String name,
    TopicPartition tp,
    CoordinatorReadOperation<S, T> op
)
```

流程：
1. 创建 `CoordinatorReadEvent`
2. 执行 `op.generateResponse(shard, lastCommittedOffset)`
3. **只能读取已提交的数据**（使用 SnapshotRegistry 的快照机制）
4. 立即 complete future

## 数据流示例：WriteShareGroupState

```
┌─────────────┐
│   Client    │
│  Request    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│ ShareCoordinatorService.writeState()                    │
│   ├─ 验证请求                                            │
│   └─ runtime.scheduleWriteOperation()                   │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼ 创建 CoordinatorWriteEvent
┌─────────────────────────────────────────────────────────┐
│ CoordinatorEventProcessor 队列                           │
│   (MultiThreadedEventProcessor 多线程处理)               │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼ event.run()
┌─────────────────────────────────────────────────────────┐
│ CoordinatorContext.withActiveContextOrThrow()           │
│   ├─ 获取 context.lock                                  │
│   ├─ 检查 state == ACTIVE                               │
│   └─ 执行写操作                                          │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ ShareCoordinatorShard.writeState()                      │
│   ├─ generateShareStateRecord()                         │
│   └─ 返回 CoordinatorResult<Response, Records>          │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ CoordinatorContext.append()                             │
│   ├─ 序列化 records                                      │
│   ├─ maybeAllocateNewBatch()                            │
│   ├─ replay records 到 shard (更新内存状态)              │
│   ├─ builder.append(record)                             │
│   ├─ deferredEvents.add(event)                          │
│   └─ maybeFlushCurrentBatch()                           │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼ 如果 batch 满了或超时
┌─────────────────────────────────────────────────────────┐
│ CoordinatorContext.flushCurrentBatch()                  │
│   ├─ partitionWriter.append(tp, records)                │
│   ├─ 返回写入的 offset                                   │
│   └─ deferredEventQueue.add(offset, deferredEvents)     │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼ 异步：log 复制完成
┌─────────────────────────────────────────────────────────┐
│ HighWatermarkListener.onHighWatermarkUpdated()         │
│   └─ 推送 HighWatermarkUpdate event 到队列              │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ CoordinatorContext 处理 HW 更新                          │
│   ├─ coordinator.updateLastCommittedOffset(newHW)       │
│   └─ deferredEventQueue.completeUpTo(newHW)            │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│ CoordinatorWriteEvent.complete(null)                    │
│   └─ future.complete(result.response())                │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
              返回响应给客户端
```

## 状态机的生命周期

```
INITIAL
  │
  │ scheduleLoadOperation()
  ▼
LOADING ───────────────┐
  │                    │ 加载失败
  │ 加载成功            ▼
  ▼                  FAILED
ACTIVE                 │
  │                    │ 可以重试加载
  │ 卸载或失败          │
  ▼                    │
CLOSED ◄───────────────┘
```

### 状态转换详解

1. **INITIAL → LOADING**:
   - `scheduleLoadOperation()` 被调用
   - 创建 `SnapshotRegistry` 和 `ShareCoordinatorShard`
   - 调用 `loader.load()` 从 log 中读取所有记录并 replay

2. **LOADING → ACTIVE**:
   - 所有历史记录replay完成
   - 注册 `HighWatermarkListener`
   - 调用 `shard.onLoaded()`
   - 开始接受读写请求

3. **ACTIVE → FAILED**:
   - Log 写入失败
   - Replay 失败
   - 触发 `unload()` 清理资源

4. **ANY → CLOSED**:
   - `scheduleUnloadOperation()` 被调用
   - Leadership 丢失
   - 触发 `unload()` 清理资源

## MVCC 实现：SnapshotRegistry + TimelineHashMap

### 为什么需要 MVCC？

**问题场景**：
```java
// Thread 1: Write operation (uncommitted)
shard.replay(100, record1);  // 修改了 offset -> 100
shard.replay(101, record2);  // 修改了 offset -> 101

// Thread 2: Read operation (同时执行)
shard.readState(committedOffset=99);  // 应该读到 offset 99 的数据
                                      // 但 Thread 1 已经修改到 101！
```

### 解决方案：TimelineHashMap

```java
public class ShareCoordinatorShard {
    // 使用 TimelineHashMap 而不是普通 HashMap
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;

    public ShareCoordinatorShard(SnapshotRegistry snapshotRegistry) {
        // 注册到 SnapshotRegistry
        this.shareStateMap = new TimelineHashMap<>(snapshotRegistry, 0);
    }
}
```

### 快照机制

```java
// CoordinatorRuntime 维护 lastCommittedOffset
coordinator.updateLastCommittedOffset(newHW);

// 读操作使用快照
CoordinatorReadEvent<T> {
    void run() {
        long committedOffset = context.coordinator.lastCommittedOffset();

        // 所有 TimelineHashMap 操作会使用这个 offset 的快照
        response = op.generateResponse(shard, committedOffset);
    }
}
```

## 关键设计点

### 1. 线程模型

```
┌─────────────────────────────────────────────────────────┐
│          CoordinatorEventProcessor                       │
│     (MultiThreadedEventProcessor: 多个线程)              │
│                                                           │
│  线程池: share-coordinator-event-processor-0..N          │
│                                                           │
│  保证: 同一个 TopicPartition 的事件串行处理               │
│  实现: event.key() 用于分片                              │
└─────────────────────────────────────────────────────────┘
```

- **每个 CoordinatorContext 有自己的 ReentrantLock**
- **同一 partition 的操作串行化**
- **不同 partition 的操作可以并行**

### 2. Batching 机制

```java
class CoordinatorBatch {
    final long baseOffset;              // batch 起始 offset
    final MemoryRecordsBuilder builder; // 累积 records
    final DeferredEventCollection deferredEvents;  // 关联的事件

    // 触发 flush 的条件:
    // 1. batch 满了: !builder.hasRoomFor()
    // 2. 超时: (currentTime - appendTimeMs) >= appendLingerMs
    // 3. 事务: builder.isTransactional()
}
```

**好处**：
- 减少磁盘写入次数
- 批量提交提高吞吐量
- 控制延迟（通过 appendLingerMs）

### 3. Deferred Event Queue

```java
class DeferredEventQueue {
    // offset -> List<DeferredEvent>
    private final Map<Long, List<DeferredEvent>> pendingEvents;

    void add(long offset, DeferredEvent event) {
        // 等待 HW 到达 offset 时 complete
    }

    void completeUpTo(long highWatermark) {
        // Complete 所有 offset <= highWatermark 的事件
    }
}
```

**为什么需要？**
- Kafka 的复制是异步的
- Records 写入本地 log 后，需要等待 follower 复制
- 只有 HW 推进后，数据才真正持久化，可以安全返回给客户端

### 4. Timer 机制

```java
class EventBasedCoordinatorTimer {
    // key -> TimerTask
    private final Map<String, TimerTask> tasks;

    void schedule(String key, long delay, TimeoutOperation op) {
        TimerTask task = new TimerTask() {
            void run() {
                // 推送一个 CoordinatorWriteEvent 到队列
                enqueueLast(new CoordinatorWriteEvent(...));
            }
        };
        timer.add(task);
    }
}
```

**关键点**：
- Timer 过期后，生成一个 **Event** 而不是直接执行
- 保证所有操作都通过 EventProcessor，维持线程模型一致性

## ShareCoordinator 的具体实现

### 数据结构

```java
class ShareCoordinatorShard {
    // 核心状态: group+topic+partition -> offset 信息
    TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;

    // Leader epoch 追踪
    TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;

    // 快照计数器 (用于决定何时写完整快照)
    TimelineHashMap<SharePartitionKey, Integer> snapshotUpdateCount;

    // State epoch (用于乐观锁)
    TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;
}
```

### 记录类型

```
┌─────────────────────────────────────────────────────────┐
│            __share_group_state topic                    │
│                                                           │
│  Record Key                    Record Value              │
│  ────────────────────────────────────────────────────   │
│                                                           │
│  ShareSnapshotKey             ShareSnapshotValue         │
│  - groupId                     - snapshotEpoch           │
│  - topicId                     - leaderEpoch             │
│  - partition                   - stateEpoch              │
│                                - startOffset             │
│                                - stateBatches[]          │
│                                                           │
│  ShareUpdateKey               ShareUpdateValue           │
│  - groupId                     - leaderEpoch             │
│  - topicId                     - startOffset             │
│  - partition                   - stateBatches[]          │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

**两种记录的区别**：
- **ShareSnapshot**: 完整快照，包含所有状态
- **ShareUpdate**: 增量更新，只包含变化的部分

**何时写哪种**：
```java
if (snapshotUpdateCount >= config.snapshotUpdateRecordsPerSnapshot()) {
    // 写完整快照
    return new ShareSnapshotRecord(...);
} else {
    // 写增量更新
    return new ShareUpdateRecord(...);
}
```

### WriteState 实现

```java
public CoordinatorResult<WriteShareGroupStateResponseData, CoordinatorRecord>
        writeState(WriteShareGroupStateRequestData request) {

    // 1. 验证请求
    validate(request);

    // 2. 生成记录
    CoordinatorRecord record = generateShareStateRecord(request);

    // 3. 构造响应
    WriteShareGroupStateResponseData response = buildResponse();

    // 4. 返回 Result (records 会被 runtime append 和 replay)
    return new CoordinatorResult<>(
        Collections.singletonList(record),
        response,
        true  // replayRecords = true
    );
}
```

### ReadState 实现

```java
public CoordinatorResult<ReadShareGroupStateResponseData, CoordinatorRecord>
        readState(ReadShareGroupStateRequestData request, long committedOffset) {

    // 使用 committedOffset 对应的快照读取
    ShareGroupOffset offset = shareStateMap.get(key);

    // 构造响应
    ReadShareGroupStateResponseData response = buildResponse(offset);

    // 读操作不产生 records
    return new CoordinatorResult<>(
        Collections.emptyList(),
        response
    );
}
```

## 总结：架构的优雅之处

1. **分层清晰**:
   - Service 层：API 和业务逻辑
   - Runtime 层：通用框架（事件调度、batching、MVCC）
   - Shard 层：具体状态机实现

2. **并发安全**:
   - EventProcessor 保证同 partition 串行
   - MVCC 保证读写隔离
   - Lock 保护 context 状态

3. **可扩展**:
   - 新 Coordinator 只需实现 `CoordinatorShard` 接口
   - 共享 Runtime 的所有能力

4. **容错**:
   - 基于 Kafka log 的复制
   - Leader 切换时自动 reload
   - Deferred events 保证只在数据安全后返回

5. **性能优化**:
   - Batching 减少 IO
   - MVCC 避免锁竞争
   - 异步处理提高吞吐

---

## 学习建议

1. 先理解 `CoordinatorShard` 接口和 `ShareCoordinatorShard` 实现
2. 看 `CoordinatorRuntime` 如何管理 `CoordinatorContext`
3. 跟踪一个完整的 write 请求流程
4. 理解 MVCC 如何保证读写隔离
5. 看 `DeferredEventQueue` 和 `HighWatermarkListener` 如何协作

## 下一步

查看具体代码文件：
- `CoordinatorRuntime.java:2553` - 核心框架
- `ShareCoordinatorShard.java:300` - Shard 实现
- `ShareCoordinatorService.java:200` - Service 层
- `CoordinatorShard.java:81` - 接口定义
