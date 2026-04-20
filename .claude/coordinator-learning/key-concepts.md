# Kafka Coordinator 核心概念详解

## 1. 复制状态机（Replicated State Machine）

### 概念
Kafka 的 Coordinator 本质是一个**复制状态机**：
- **状态机**：根据输入（records）确定性地更新状态
- **复制**：通过 Kafka log 将 records 复制到多个副本
- **一致性**：所有副本按相同顺序 replay records，达到相同状态

### 为什么这样设计？

```
传统方式（基于内存 + 同步RPC）:
┌─────────┐  写请求   ┌─────────┐
│ Leader  │ ────────> │ Memory  │
└─────────┘           └─────────┘
     │
     │ 同步复制 (Raft/Paxos)
     ▼
┌─────────┐
│Follower │
└─────────┘

问题：
1. 需要实现复杂的共识协议
2. Leader 切换时状态恢复困难
3. 难以扩展（所有状态都在内存）

Kafka 方式（基于 Log）:
┌─────────┐  写请求   ┌─────────────────┐
│ Leader  │ ────────> │  __state topic  │ (Kafka 内部 topic)
└─────────┘           └─────────────────┘
     │                         │
     │ replay                  │ 自动复制
     ▼                         ▼
┌─────────┐               ┌─────────┐
│ Memory  │               │Follower │
└─────────┘               └─────────┘

优势：
1. 复用 Kafka 的复制机制
2. Leader 切换简单（新 Leader replay log）
3. 状态可以很大（log compaction）
4. 自然支持审计和调试
```

## 2. MVCC (Multi-Version Concurrency Control)

### 问题：读写冲突

```java
// 问题场景
Thread 1: Write Operation
  ├─ 生成 record: offset 100
  ├─ 写入 log (本地完成，但未复制)
  ├─ replay(100)  ← 更新了内存状态
  └─ 等待 HW 推进...

Thread 2: Read Operation (同时发生)
  └─ 读取状态  ← 读到了 offset 100 的数据
                  但这数据还没有被复制！
                  如果 Leader crash，数据丢失！
```

### 解决方案：SnapshotRegistry + TimelineHashMap

```java
class SnapshotRegistry {
    // offset -> Snapshot
    private final Map<Long, Snapshot> snapshots;

    // 当前正在使用的 snapshot offset
    private long currentSnapshotOffset;

    // 创建快照
    public void getOrCreateSnapshot(long offset) {
        if (!snapshots.containsKey(offset)) {
            snapshots.put(offset, new Snapshot(offset));
        }
    }

    // 回到某个快照
    public void revertToSnapshot(long offset) {
        this.currentSnapshotOffset = offset;
    }
}

class TimelineHashMap<K, V> {
    // key -> Timeline
    // Timeline = List<(offset, value)>
    private final Map<K, Timeline<V>> data;
    private final SnapshotRegistry registry;

    public V get(K key) {
        Timeline<V> timeline = data.get(key);
        if (timeline == null) return null;

        // 返回 <= currentSnapshotOffset 的最新值
        long currentOffset = registry.currentSnapshotOffset();
        return timeline.getAtOffset(currentOffset);
    }

    public void put(K key, V value) {
        Timeline<V> timeline = data.computeIfAbsent(key, k -> new Timeline<>());

        // 记录当前 offset 对应的值
        long currentOffset = registry.currentSnapshotOffset();
        timeline.add(currentOffset, value);
    }
}

// 使用示例
SnapshotRegistry registry = new SnapshotRegistry();
TimelineHashMap<String, Integer> map = new TimelineHashMap<>(registry);

// 写操作 1
registry.getOrCreateSnapshot(100);
map.put("key", 10);  // Timeline: [(100, 10)]

// 写操作 2
registry.getOrCreateSnapshot(101);
map.put("key", 20);  // Timeline: [(100, 10), (101, 20)]

// 读操作 1: 只读已提交的数据 (假设 HW = 100)
registry.revertToSnapshot(100);
map.get("key");  // 返回 10

// 读操作 2: HW 推进后
registry.revertToSnapshot(101);
map.get("key");  // 返回 20
```

### 在 CoordinatorRuntime 中的应用

```java
class CoordinatorContext {
    SnapshottableCoordinator coordinator;

    // 写操作
    void append(List<U> records, DeferredEvent event) {
        for (U record : records) {
            // 使用 lastWrittenOffset (可能未提交)
            coordinator.replay(lastWrittenOffset++, record);
        }
    }

    // 读操作
    T read(CoordinatorReadOperation<S, T> op) {
        // 使用 lastCommittedOffset (已复制到多数派)
        long committedOffset = coordinator.lastCommittedOffset();
        return op.generateResponse(shard, committedOffset);
    }
}
```

## 3. Deferred Event Queue

### 为什么需要？

```
Kafka 的写入是分两步的：
1. 写入本地 log (很快，几毫秒)
2. 复制到 followers (较慢，可能几十毫秒)

只有第二步完成（HW 推进），数据才真正持久化。

问题：什么时候返回响应给客户端？
- 太早：客户端以为写成功了，但实际可能丢失
- 太晚：响应延迟太高

答案：等待 HW 到达写入的 offset 后再返回
```

### 实现

```java
class DeferredEventQueue {
    // offset -> List<DeferredEvent>
    private final TreeMap<Long, List<DeferredEvent>> pendingEvents;

    void add(long offset, DeferredEvent event) {
        pendingEvents
            .computeIfAbsent(offset, k -> new ArrayList<>())
            .add(event);
    }

    void completeUpTo(long highWatermark) {
        // 找到所有 offset <= highWatermark 的事件
        Iterator<Map.Entry<Long, List<DeferredEvent>>> it =
            pendingEvents.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Long, List<DeferredEvent>> entry = it.next();

            if (entry.getKey() <= highWatermark) {
                // Complete 所有事件
                for (DeferredEvent event : entry.getValue()) {
                    event.complete(null);
                }
                it.remove();
            } else {
                break;  // TreeMap 是有序的
            }
        }
    }
}

// 使用流程
void flushCurrentBatch() {
    // 1. 写入 log
    long offset = partitionWriter.append(records);

    // 2. 加入 deferred queue
    deferredEventQueue.add(offset, event);
}

void onHighWatermarkUpdated(long newHW) {
    // 3. HW 推进时，complete 所有已安全的事件
    deferredEventQueue.completeUpTo(newHW);
}
```

## 4. Batching 机制

### 为什么需要 Batching？

```
没有 batching:
Request 1 → flush → disk write (10ms)
Request 2 → flush → disk write (10ms)
Request 3 → flush → disk write (10ms)
总延迟: 30ms, 吞吐量: 3 req/30ms = 100 req/s

有 batching (linger 10ms):
Request 1 ──┐
Request 2 ──┤ batch → flush → disk write (10ms)
Request 3 ──┘
总延迟: 10ms (linger) + 10ms (write) = 20ms
吞吐量: 3 req/20ms = 150 req/s
```

### 实现

```java
class CoordinatorBatch {
    final long baseOffset;              // batch 起始 offset
    final long appendTimeMs;            // batch 创建时间
    final MemoryRecordsBuilder builder; // 累积 records
    final DeferredEventCollection deferredEvents;  // 关联的所有事件
    long nextOffset;                    // 下一个 record 的 offset
}

// 何时 flush？
void maybeFlushCurrentBatch(long currentTimeMs) {
    if (currentBatch == null) return;

    boolean shouldFlush =
        // 1. 事务写 (必须原子)
        currentBatch.builder.isTransactional() ||
        // 2. 超过 linger 时间
        (currentTimeMs - currentBatch.appendTimeMs) >= appendLingerMs ||
        // 3. batch 满了
        !currentBatch.builder.hasRoomFor(0);

    if (shouldFlush) {
        flushCurrentBatch();
    }
}
```

### Linger Timeout 实现

```java
void maybeAllocateNewBatch() {
    if (currentBatch != null) return;

    // 创建 batch
    currentBatch = new CoordinatorBatch(...);

    // 如果配置了 appendLingerMs，创建定时器
    if (appendLingerMs > 0) {
        TimerTask lingerTask = new TimerTask(appendLingerMs) {
            @Override
            public void run() {
                // 推送一个 flush 事件到队列头部
                enqueueFirst(new CoordinatorInternalEvent("FlushBatch", tp, () -> {
                    if (!this.isCancelled()) {
                        flushCurrentBatch();
                    }
                }));
            }
        };
        timer.add(lingerTask);
        currentBatch.lingerTimeoutTask = Optional.of(lingerTask);
    }
}
```

## 5. 事件驱动架构

### 为什么事件驱动？

```
传统线程模型:
每个请求一个线程 → 大量线程 → 上下文切换开销大 → 难以扩展

事件驱动模型:
请求 → Event → 队列 → 少量线程池处理 → 高效

额外好处:
1. 容易做 batching (多个 event 可以合并处理)
2. 容易做优先级 (enqueueFirst vs enqueueLast)
3. 容易做超时 (event 带创建时间)
4. 容易做监控 (队列长度、处理延迟)
```

### CoordinatorEventProcessor

```java
interface CoordinatorEvent {
    // 返回 partition，用于分片
    TopicPartition key();

    // 执行事件
    void run();

    // 完成事件（成功或失败）
    void complete(Throwable exception);
}

class MultiThreadedEventProcessor implements CoordinatorEventProcessor {
    // 线程池
    private final ExecutorService[] executors;

    // 每个 partition 一个队列
    private final Map<TopicPartition, Queue<CoordinatorEvent>> queues;

    @Override
    public void enqueue(CoordinatorEvent event) {
        TopicPartition tp = event.key();

        // 1. 选择线程（基于 partition hash）
        int threadIndex = Math.abs(tp.hashCode()) % executors.length;

        // 2. 加入对应的队列
        Queue<CoordinatorEvent> queue = queues.computeIfAbsent(tp,
            k -> new ConcurrentLinkedQueue<>());
        queue.add(event);

        // 3. 提交任务到线程池
        executors[threadIndex].submit(() -> {
            processEvents(tp);
        });
    }

    private void processEvents(TopicPartition tp) {
        Queue<CoordinatorEvent> queue = queues.get(tp);

        // 串行处理该 partition 的所有事件
        CoordinatorEvent event;
        while ((event = queue.poll()) != null) {
            try {
                event.run();
            } catch (Throwable t) {
                event.complete(t);
            }
        }
    }
}
```

**关键保证**：
- 同一 partition 的事件串行处理（避免并发问题）
- 不同 partition 的事件并行处理（提高吞吐量）

## 6. Timer 机制

### 为什么需要特殊的 Timer？

```
问题：普通 Timer 直接在回调中执行操作

Timer.schedule(() -> {
    // 这里直接修改状态
    coordinator.timeout(key);
}, 1000);

问题：
1. 在 Timer 线程执行，破坏了线程模型
2. 可能和其他操作并发，需要加锁
3. 难以测试（时间依赖）

解决方案：Timer 只生成 Event

Timer.schedule(() -> {
    // 只生成事件，不执行逻辑
    eventProcessor.enqueue(new CoordinatorWriteEvent(...));
}, 1000);

优势：
1. 保持线程模型一致性
2. 自动串行化（通过 EventProcessor）
3. 可以测试（mock EventProcessor）
```

### EventBasedCoordinatorTimer

```java
class EventBasedCoordinatorTimer implements CoordinatorTimer {
    // key -> TimerTask
    private final Map<String, TimerTask> tasks;

    @Override
    public void schedule(
        String key,
        long delay,
        TimeUnit unit,
        TimeoutOperation<U> operation
    ) {
        TimerTask task = new TimerTask(unit.toMillis(delay)) {
            @Override
            public void run() {
                // 创建写事件
                CoordinatorWriteEvent<Void> event = new CoordinatorWriteEvent<>(
                    "Timeout(" + key + ")",
                    tp,
                    coordinator -> {
                        // 检查是否被取消
                        if (!tasks.remove(key, this)) {
                            throw new RejectedExecutionException("Timer cancelled");
                        }

                        // 执行超时操作
                        return operation.generateRecords();
                    }
                );

                // 推送事件到队列
                enqueueLast(event);
            }
        };

        // 保存 task，支持取消
        TimerTask oldTask = tasks.put(key, task);
        if (oldTask != null) oldTask.cancel();

        // 添加到底层 timer
        timer.add(task);
    }

    @Override
    public void cancel(String key) {
        TimerTask task = tasks.remove(key);
        if (task != null) task.cancel();
    }
}
```

**使用示例**：

```java
// ShareGroup 的 heartbeat timeout
class ShareCoordinatorShard {
    void handleHeartbeat(String memberId) {
        // 每次心跳重置定时器
        timer.schedule(
            "heartbeat-" + memberId,
            sessionTimeoutMs,
            TimeUnit.MILLISECONDS,
            true,  // retry on failure
            () -> {
                // 超时处理逻辑
                return removeMemberRecords(memberId);
            }
        );
    }
}
```

## 7. 状态转换和 Loading

### Coordinator 生命周期

```
INITIAL
  │
  │ Leader 选举
  ▼
scheduleLoadOperation()
  │
  ▼
LOADING ──────────────┐
  │                   │ 加载失败
  │ replay 所有历史记录 │
  │                   ▼
  │                 FAILED
  ▼                   │
onLoaded()            │ 可以重试
  │                   │
  ▼                   │
ACTIVE ◄──────────────┘
  │
  │ Leadership 丢失
  ▼
scheduleUnloadOperation()
  │
  ▼
CLOSED
```

### Loading 流程

```java
void scheduleLoadOperation(TopicPartition tp, int epoch) {
    CoordinatorContext context = maybeCreateContext(tp);

    scheduleInternalOperation("Load", tp, () -> {
        context.lock.lock();
        try {
            if (context.epoch < epoch) {
                context.epoch = epoch;
                context.transitionTo(CoordinatorState.LOADING);
            }
        } finally {
            context.lock.unlock();
        }
    });
}

void transitionTo(CoordinatorState.LOADING) {
    // 1. 创建 SnapshotRegistry 和 Shard
    SnapshotRegistry registry = new SnapshotRegistry();
    ShareCoordinatorShard shard = new ShareCoordinatorShard(registry);
    this.coordinator = new SnapshottableCoordinator(registry, shard);

    // 2. 异步加载
    loader.load(tp, coordinator).whenComplete((summary, exception) -> {
        // 3. 加载完成后转换状态
        scheduleInternalOperation("CompleteLoad", tp, () -> {
            context.lock.lock();
            try {
                if (exception != null) {
                    context.transitionTo(CoordinatorState.FAILED);
                } else {
                    context.transitionTo(CoordinatorState.ACTIVE);
                    log.info("Loaded {} records in {}ms", summary.numRecords(), summary.duration());
                }
            } finally {
                context.lock.unlock();
            }
        });
    });
}
```

### Loader 实现

```java
interface CoordinatorLoader<U> {
    CompletableFuture<LoadSummary> load(
        TopicPartition tp,
        CoordinatorPlayback<U> coordinator
    );
}

class CoordinatorLoaderImpl implements CoordinatorLoader<U> {
    CompletableFuture<LoadSummary> load(TopicPartition tp, CoordinatorPlayback coordinator) {
        return CompletableFuture.supplyAsync(() -> {
            long startMs = time.milliseconds();
            long numRecords = 0;

            // 从头读取 log
            KafkaConsumer consumer = createConsumer();
            consumer.assign(Collections.singleton(tp));
            consumer.seekToBeginning(tp);

            while (true) {
                ConsumerRecords records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) break;

                for (ConsumerRecord record : records) {
                    // Replay 每条记录
                    U deserializedRecord = deserialize(record);
                    coordinator.replay(
                        record.offset(),
                        record.producerId(),
                        record.producerEpoch(),
                        deserializedRecord
                    );
                    numRecords++;
                }
            }

            return new LoadSummary(startMs, time.milliseconds(), numRecords);
        }, executorService);
    }
}
```

## 总结

### 核心思想

1. **复制状态机**: 将状态变更转换为 log records，利用 Kafka 的复制能力
2. **MVCC**: 支持未提交数据的写入，同时保证只读取已提交数据
3. **事件驱动**: 所有操作通过事件队列，保证线程安全和可测试性
4. **异步处理**: Write 立即返回 Future，等待复制完成后 complete
5. **Batching**: 减少磁盘 IO，提高吞吐量

### 关键数据结构

- `TimelineHashMap` + `SnapshotRegistry`: MVCC 实现
- `DeferredEventQueue`: 等待 HW 推进
- `CoordinatorBatch`: 累积写入
- `EventBasedCoordinatorTimer`: 定时器转事件

### 设计优势

1. **简单**: 不需要自己实现 Raft/Paxos
2. **高效**: Batching + 异步 + 多线程
3. **可靠**: 依赖 Kafka 的持久化保证
4. **可扩展**: 分区级并行
5. **可调试**: 所有变更都在 log 中
