# Kafka Coordinator 架构：从零开始的完整教程

## 目录
- [版本 1: 最简单的实现](#版本-1-最简单的实现)
- [版本 2: 为什么需要持久化？](#版本-2-为什么需要持久化)
- [版本 3: 为什么需要事件队列？](#版本-3-为什么需要事件队列)
- [版本 4: 为什么需要 Batching？](#版本-4-为什么需要-batching)
  - [深入解析：Batch 失败处理的权衡](#深入解析batch-失败处理的权衡)
- [版本 5: 为什么需要 Deferred Events？](#版本-5-为什么需要-deferred-events)
  - [深入解析：为什么不能只依赖 Producer ACK？](#深入解析为什么不能只依赖-producer-ack)
  - [深入解析：ACK vs HW 时序对比](#深入解析ack-vs-hw-时序对比)
- [版本 6: 为什么需要 MVCC？](#版本-6-为什么需要-mvcc)
  - [深入解析：TimelineHashMap 的正确实现](#深入解析timelinehashmap-的正确实现)
  - [深入解析：为什么 SnapshotRegistry 这么复杂？](#深入解析为什么-snapshotregistry-这么复杂)
- [版本 7: 完整的 CoordinatorRuntime](#版本-7-完整的-coordinatorruntime)
- [总结：为什么需要这些设计？](#总结为什么需要这些设计)
- [高级主题：Revert 回滚机制](#高级主题revert-回滚机制)

---

## 版本 1: 最简单的实现

### 需求
我们要实现一个 **ShareCoordinator**，管理 share group 的状态：
- 每个 group 对某个 topic-partition 有一个 offset
- 客户端可以写入新的 offset
- 客户端可以读取当前 offset

### 实现

```java
// 最简单的实现：就是一个 HashMap
class ShareCoordinatorV1 {
    // group + topic + partition -> offset
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();

    // 写入 offset
    public void writeOffset(String group, String topic, int partition, long offset) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        offsets.put(key, offset);
    }

    // 读取 offset
    public Long readOffset(String group, String topic, int partition) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        return offsets.get(key);
    }
}

// 使用示例
ShareCoordinatorV1 coordinator = new ShareCoordinatorV1();
coordinator.writeOffset("group1", "topic1", 0, 100L);
Long offset = coordinator.readOffset("group1", "topic1", 0);  // 返回 100
```

### 问题出现了！

#### 问题 1: 进程重启后数据丢失

```java
ShareCoordinatorV1 coordinator = new ShareCoordinatorV1();
coordinator.writeOffset("group1", "topic1", 0, 100L);

// 进程重启...
// 数据全部丢失！

ShareCoordinatorV1 coordinator2 = new ShareCoordinatorV1();
Long offset = coordinator2.readOffset("group1", "topic1", 0);  // 返回 null
```

**为什么是问题？**
- 客户端以为写入成功了
- 但重启后数据消失
- Consumer 会重复消费消息

#### 问题 2: 没有高可用

```java
// 只有一个进程运行 coordinator
// 如果这个进程挂了，整个系统不可用
```

**为什么是问题？**
- 单点故障
- 无法容忍机器故障

---

## 版本 2: 为什么需要持久化？

### 解决方案：写入 Kafka Log

```java
class ShareCoordinatorV2 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final KafkaProducer<byte[], byte[]> producer;
    private final String stateTopic = "__share_group_state";

    // 写入 offset
    public void writeOffset(String group, String topic, int partition, long offset) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);

        // 1. 先写入 Kafka log
        Record record = new Record(key, offset);
        producer.send(stateTopic, serialize(record));

        // 2. 再更新内存
        offsets.put(key, offset);
    }

    // 读取 offset
    public Long readOffset(String group, String topic, int partition) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        return offsets.get(key);
    }

    // 启动时从 log 恢复
    public void load() {
        KafkaConsumer<byte[], byte[]> consumer = createConsumer();
        consumer.assign(Collections.singleton(new TopicPartition(stateTopic, 0)));
        consumer.seekToBeginning();

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) break;

            for (ConsumerRecord<byte[], byte[]> record : records) {
                Record r = deserialize(record.value());
                // 重放记录到内存
                offsets.put(r.key, r.offset);
            }
        }
    }
}

// 使用示例
ShareCoordinatorV2 coordinator = new ShareCoordinatorV2();
coordinator.load();  // 从 log 恢复
coordinator.writeOffset("group1", "topic1", 0, 100L);

// 进程重启...
ShareCoordinatorV2 coordinator2 = new ShareCoordinatorV2();
coordinator2.load();  // 重新加载
Long offset = coordinator2.readOffset("group1", "topic1", 0);  // 返回 100，数据恢复了！
```

### 为什么这样设计？

#### 优势 1: 持久化
- 数据写入 Kafka log，即使进程重启也不会丢失

#### 优势 2: 高可用
- Kafka log 自动复制到多个 broker
- 任何一个 broker 可以成为新的 coordinator（通过 replay log）

#### 优势 3: 审计和调试
- 所有变更都在 log 中，可以追溯历史

### 问题出现了！

#### 问题 3: 并发问题

```java
// Thread 1
coordinator.writeOffset("group1", "topic1", 0, 100L);

// Thread 2 (同时)
coordinator.writeOffset("group1", "topic1", 0, 200L);

// 可能出现：
// - Thread 1 写 log 成功，Thread 2 写 log 失败
// - 但 Thread 2 先更新内存，Thread 1 后更新内存
// - 结果：内存是 100，但 log 里是 100
// - 重启后恢复成 100，但实际应该是 200
```

**为什么是问题？**
- 写 log 和更新内存不是原子操作
- 多线程并发访问 HashMap 不安全

#### 问题 4: 性能问题

```java
public void writeOffset(...) {
    // 每次写入都要等待 Kafka producer.send() 返回
    producer.send(stateTopic, record).get();  // 阻塞！每次 10-20ms
    offsets.put(key, offset);
}

// 1000 个请求 = 10-20 秒！
```

**为什么是问题？**
- 每次写入都等待磁盘 IO
- 吞吐量太低

---

## 版本 3: 为什么需要事件队列？

### 解决方案：所有操作通过事件队列串行化

```java
class ShareCoordinatorV3 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final KafkaProducer<byte[], byte[]> producer;
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private final Thread eventThread;

    public ShareCoordinatorV3() {
        // 启动事件处理线程
        eventThread = new Thread(() -> {
            while (true) {
                Event event = eventQueue.take();
                processEvent(event);
            }
        });
        eventThread.start();
    }

    // 写入 offset (异步)
    public CompletableFuture<Void> writeOffset(String group, String topic, int partition, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        // 创建事件
        WriteEvent event = new WriteEvent(
            new SharePartitionKey(group, topic, partition),
            offset,
            future
        );

        // 加入队列
        eventQueue.add(event);

        return future;
    }

    // 读取 offset (异步)
    public CompletableFuture<Long> readOffset(String group, String topic, int partition) {
        CompletableFuture<Long> future = new CompletableFuture<>();

        ReadEvent event = new ReadEvent(
            new SharePartitionKey(group, topic, partition),
            future
        );

        eventQueue.add(event);

        return future;
    }

    // 事件处理 (单线程，串行)
    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;
            try {
                // 1. 写 log
                Record record = new Record(e.key, e.offset);
                producer.send(stateTopic, serialize(record)).get();

                // 2. 更新内存
                offsets.put(e.key, e.offset);

                // 3. 完成 future
                e.future.complete(null);
            } catch (Exception ex) {
                e.future.completeExceptionally(ex);
            }

        } else if (event instanceof ReadEvent) {
            ReadEvent e = (ReadEvent) event;
            Long offset = offsets.get(e.key);
            e.future.complete(offset);
        }
    }
}

// 使用示例
ShareCoordinatorV3 coordinator = new ShareCoordinatorV3();

// 异步写入
CompletableFuture<Void> writeFuture = coordinator.writeOffset("group1", "topic1", 0, 100L);
writeFuture.thenRun(() -> System.out.println("写入成功"));

// 异步读取
CompletableFuture<Long> readFuture = coordinator.readOffset("group1", "topic1", 0);
readFuture.thenAccept(offset -> System.out.println("读到: " + offset));
```

### 为什么这样设计？

#### 优势 1: 线程安全
- 所有操作在单线程中执行
- 不需要 synchronized 或 Lock
- HashMap 的操作天然串行化

#### 优势 2: 异步非阻塞
- 请求立即返回 CompletableFuture
- 调用线程不会被阻塞
- 可以同时处理多个请求

#### 优势 3: 顺序保证
- 同一个 key 的操作按照加入队列的顺序执行
- 不会出现并发导致的顺序错乱

### 问题出现了！

#### 问题 5: 性能还是不够

```java
// 即使用了事件队列，每个 event 还是要等待写 log
processEvent(WriteEvent) {
    producer.send(record).get();  // 还是阻塞 10-20ms
    offsets.put(key, offset);
}

// 吞吐量：50-100 req/s (因为每个都要等 IO)
```

**为什么是问题？**
- 虽然是异步，但事件处理线程还是被 IO 阻塞
- 没有充分利用 Kafka 的批量写入能力

---

## 版本 4: 为什么需要 Batching？

### 解决方案：累积多个写入，批量 flush

```java
class ShareCoordinatorV4 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final KafkaProducer<byte[], byte[]> producer;
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    // Batch 相关
    private List<Record> currentBatch = new ArrayList<>();
    private List<CompletableFuture<Void>> pendingFutures = new ArrayList<>();
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public ShareCoordinatorV4() {
        startEventThread();
        startFlushTimer();
    }

    private void startEventThread() {
        new Thread(() -> {
            while (true) {
                Event event = eventQueue.take();
                processEvent(event);

                // 检查是否需要 flush
                if (currentBatch.size() >= 100) {  // batch 满了
                    flushBatch();
                }
            }
        }).start();
    }

    private void startFlushTimer() {
        // 每 10ms flush 一次
        scheduler.scheduleAtFixedRate(() -> {
            eventQueue.add(new FlushEvent());
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> writeOffset(String group, String topic, int partition, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        WriteEvent event = new WriteEvent(
            new SharePartitionKey(group, topic, partition),
            offset,
            future
        );
        eventQueue.add(event);
        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 1. 先更新内存 (立即生效)
            offsets.put(e.key, e.offset);

            // 2. 加入 batch (稍后 flush)
            Record record = new Record(e.key, e.offset);
            currentBatch.add(record);
            pendingFutures.add(e.future);

            // 注意：不立即写 log！

        } else if (event instanceof FlushEvent) {
            flushBatch();
        }
    }

    private void flushBatch() {
        if (currentBatch.isEmpty()) return;

        try {
            // 批量写入所有 records
            for (Record record : currentBatch) {
                producer.send(stateTopic, serialize(record));
            }
            producer.flush();  // 等待所有写入完成

            // 完成所有 futures
            for (CompletableFuture<Void> future : pendingFutures) {
                future.complete(null);
            }

            System.out.println("Flushed " + currentBatch.size() + " records");

        } catch (Exception e) {
            // 失败时完成所有 futures
            for (CompletableFuture<Void> future : pendingFutures) {
                future.completeExceptionally(e);
            }
        } finally {
            currentBatch.clear();
            pendingFutures.clear();
        }
    }
}

// 使用示例
ShareCoordinatorV4 coordinator = new ShareCoordinatorV4();

// 连续写入 1000 个
for (int i = 0; i < 1000; i++) {
    coordinator.writeOffset("group1", "topic1", i, 100L);
}

// 这些写入会被累积成几个 batch，大大提高吞吐量
// 吞吐量：5000+ req/s (batch 大小 100，每次 flush 10-20ms)
```

### 为什么这样设计？

#### 优势 1: 高吞吐量
```
没有 batching:
1000 个写入 × 10ms = 10 秒

有 batching (batch size 100):
10 个 batch × 20ms = 0.2 秒

提升 50 倍！
```

#### 优势 2: 减少磁盘 IO
- 多个小写入合并成一个大写入
- 减少磁盘 fsync 次数

#### 优势 3: 控制延迟
- 通过 flush timer 保证最大延迟 (10ms)
- 通过 batch size 保证吞吐量

### 问题出现了！

#### 问题 6: 数据可能丢失！

```java
processEvent(WriteEvent) {
    offsets.put(e.key, e.offset);  // 立即更新内存
    currentBatch.add(record);       // 加入 batch
    pendingFutures.add(e.future);   // 但还没 flush！
}

// 如果此时进程 crash...
// 内存中的数据丢失了，但 future 还没 complete
// 更严重的是，log 里也没有这些数据！
```

**为什么是问题？**
- 内存状态领先于 log
- 进程 crash 后恢复的状态是旧的

#### 问题 7: 返回太快了！

```java
flushBatch() {
    producer.flush();  // 写入本地 log

    // 立即完成 futures
    for (CompletableFuture<Void> future : pendingFutures) {
        future.complete(null);  // 告诉客户端"写入成功"
    }
}

// 但是！Kafka 的复制是异步的
// 数据可能还没有复制到 followers
// 如果 leader crash，数据还是会丢！
```

**为什么是问题？**
- 客户端收到"成功"响应
- 但数据实际还没有持久化到多数派
- 违反了 Kafka 的复制保证

---

### 深入解析：Batch 失败处理的权衡

#### 问题场景

当一个 batch 中的某个 record 失败时，我们有两个选择：

**选项 1: 部分成功**（让成功的成功，失败的失败）
**选项 2: 全部失败**（Kafka 的选择）

#### 为什么 Kafka 选择"全部失败"？

```java
void flushBatch() {
    try {
        // 批量写入所有 records
        for (Record record : batch) {
            producer.send(tp, record);
        }
        producer.flush();  // 等待所有写入完成

        // 所有写入成功，加入 deferred queue
        deferredEventQueue.add(lastWrittenOffset, batchFutures);

    } catch (Exception e) {
        // 任何一个失败，回滚所有状态
        coordinator.revertLastWrittenOffset(baseOffset);

        // Fail 所有 futures
        for (CompletableFuture<Void> future : batchFutures) {
            future.completeExceptionally(e);
        }
    }
}
```

**核心原因：保证状态机一致性**

```
如果允许部分成功：
- 内存有一些 replay
- Log 缺失一些 records
- 重启后无法恢复到正确状态

如果全部失败：
- 内存回滚到 batch 前
- Log 也没有这些 records
- 一致！
```

#### Trade-off 分析

| 维度 | 部分成功 | 全部失败（Kafka） |
|------|---------|-----------------|
| **状态一致性** | ❌ 可能不一致 | ✅ 始终一致 |
| **成功率** | ✅ 更高 | ❌ "连坐" |
| **原子性** | ❌ 难保证 | ✅ 保证 |
| **重启恢复** | ❌ 困难 | ✅ 可靠 |
| **实现复杂度** | ❌ 需要补偿逻辑 | ✅ 简单回滚 |

#### 为什么劣势是可接受的？

1. **失败应该是罕见的**：在正常情况下，write 失败非常罕见
2. **客户端可以重试**：因为状态已回滚，重试是安全的
3. **Batch 通常不大**：默认 100-1000 个 records，重试代价不大
4. **保证正确性 > 优化性能**：Coordinator 管理关键状态，正确性更重要

#### 实际例子：转账场景

```java
// 如果允许部分成功：
batch = [
    record1: account1 -= 100,  // 扣款成功 ✅
    record2: account2 += 100,  // 加款失败 ❌
]
// 结果：钱消失了！

// 全部失败：
两个都回滚 ← 安全！客户端可以重试整个转账
```

---

## 版本 5: 为什么需要 Deferred Events？

### 核心概念

Kafka 的复制分两步：
1. **写入本地 log** (很快，几毫秒)
2. **复制到 followers** (较慢，几十毫秒)

只有第二步完成，数据才真正安全。
producer ack 和 HW 更新是绑定的吗？

👉 ❌ 不是强绑定

ack：写入完成（写路径）
HW：对外可见（读路径）

当然你会问，内存里的版本不还是和log里不一致吗？这个问题留在下个版本解决。

### 解决方案：等待 High Watermark

```java
class ShareCoordinatorV5 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final KafkaProducer<byte[], byte[]> producer;
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    // Batch
    private List<Record> currentBatch = new ArrayList<>();
    private List<CompletableFuture<Void>> pendingFutures = new ArrayList<>();
    private long currentBatchOffset = -1;  // 这个 batch 对应的 offset

    // Deferred Events Queue
    private final TreeMap<Long, List<CompletableFuture<Void>>> deferredEvents = new TreeMap<>();

    // 当前已提交的 offset (High Watermark)
    private long lastCommittedOffset = -1;

    public ShareCoordinatorV5() {
        startEventThread();
        startFlushTimer();
        startHighWatermarkListener();
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 1. Replay 到内存
            offsets.put(e.key, e.offset);

            // 2. 加入 batch
            Record record = new Record(e.key, e.offset);
            currentBatch.add(record);
            pendingFutures.add(e.future);

        } else if (event instanceof FlushEvent) {
            flushBatch();

        } else if (event instanceof HighWatermarkEvent) {
            updateHighWatermark(((HighWatermarkEvent) event).offset);
        }
    }

    private void flushBatch() {
        if (currentBatch.isEmpty()) return;

        try {
            // 1. 批量写入 log
            for (Record record : currentBatch) {
                producer.send(stateTopic, serialize(record));
            }
            producer.flush();

            // 2. 获取写入的 offset
            // (简化：假设我们知道写入的 offset)
            long writtenOffset = getLastWrittenOffset();
            currentBatchOffset = writtenOffset;

            // 3. 不立即完成 futures！
            // 而是加入 deferred queue，等待 HW 推进
            deferredEvents.put(writtenOffset, new ArrayList<>(pendingFutures));

            System.out.println("Wrote batch at offset " + writtenOffset + ", waiting for HW...");

        } catch (Exception e) {
            // 写入失败，立即 fail
            for (CompletableFuture<Void> future : pendingFutures) {
                future.completeExceptionally(e);
            }
        } finally {
            currentBatch.clear();
            pendingFutures.clear();
        }
    }

    private void startHighWatermarkListener() {
        // 监听 partition 的 High Watermark 变化
        // (实际通过 PartitionWriter.Listener 实现)
        new Thread(() -> {
            while (true) {
                Thread.sleep(100);

                // 模拟 HW 推进
                long newHW = fetchHighWatermark();
                if (newHW > lastCommittedOffset) {
                    // 推送 HW 更新事件
                    eventQueue.add(new HighWatermarkEvent(newHW));
                }
            }
        }).start();
    }

    private void updateHighWatermark(long newHW) {
        System.out.println("High Watermark updated to " + newHW);

        lastCommittedOffset = newHW;

        // Complete 所有 offset <= newHW 的 deferred events
        Iterator<Map.Entry<Long, List<CompletableFuture<Void>>>> it =
            deferredEvents.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Long, List<CompletableFuture<Void>>> entry = it.next();

            if (entry.getKey() <= newHW) {
                // 这些数据已经安全了，可以返回给客户端
                for (CompletableFuture<Void> future : entry.getValue()) {
                    future.complete(null);
                }
                it.remove();

                System.out.println("Completed batch at offset " + entry.getKey());
            } else {
                break;  // TreeMap 是有序的
            }
        }
    }
}

// 使用示例
ShareCoordinatorV5 coordinator = new ShareCoordinatorV5();

CompletableFuture<Void> future = coordinator.writeOffset("group1", "topic1", 0, 100L);

// future 不会立即完成
// 只有当 HW 推进后才会 complete

future.thenRun(() -> {
    System.out.println("数据已经安全复制到多数派！");
});
```

### 为什么这样设计？

#### 优势 1: 数据安全
- 只在数据真正持久化后才返回成功
- 遵循 Kafka 的复制保证

#### 优势 2: 正确的语义
- 客户端收到成功响应 = 数据不会丢失
- 即使 leader crash，数据也在 followers 上

### 时序图

```
Client          Coordinator         Local Log       Followers       Client Future
  │                  │                  │                │                │
  │──writeOffset──>  │                  │                │                │
  │                  │                  │                │                │
  │                  │──replay──>       │                │                │
  │                  │  (update memory) │                │                │
  │                  │                  │                │                │
  │                  │──add to batch──> │                │                │
  │                  │                  │                │                │
  │  <─future────────│                  │                │                │
  │  (返回但未完成)    │                  │                │                │
  │                  │                  │                │                │
  │                  │──flush batch──>  │                │                │
  │                  │                  │                │                │
  │                  │                  │──replicate──> │                │
  │                  │                  │                │                │
  │                  │                  │<─ack─────────  │                │
  │                  │                  │                │                │
  │                  │                  │──HW updated──> │                │
  │                  │<──HW event────── │                │                │
  │                  │                  │                │                │
  │                  │──complete futures───────────────────────────────> │
  │                  │                  │                │                │
  │  <─success────────────────────────────────────────────────────── │
```

---

### 深入解析：为什么不能只依赖 Producer ACK？

#### Producer ACK 的局限

**Producer ACK 只保证"写入成功"，但 Coordinator 需要知道"数据何时对读操作可见"（即 High Watermark 何时推进）。**

#### 时间差问题

```
Time │ Leader             │ Producer ACK │ High Watermark │ 读操作能看到吗？
─────┼────────────────────┼──────────────┼────────────────┼─────────────────
  0  │ 写入 offset 100    │              │ 99             │ ❌
  1  │ 发送给 followers   │              │ 99             │ ❌
  2  │ followers 写入成功 │              │ 99             │ ❌
  3  │ 收到 followers ack │ ✅ ACK 返回  │ 99             │ ❌ ← 问题！
  4  │                    │              │ 99             │ ❌
  5  │ Fetch 请求到达     │              │ 99             │ ❌
  6  │ Leader 更新 HW     │              │ 100 ← 推进     │ ✅
```

**关键点：**
- T3: Producer 收到 ACK（数据已安全）
- T3-T6: **HW 还是 99**（读操作看不到 offset 100）
- T6: HW 推进到 100（读操作才能看到）

#### HW 推进机制

HW 推进不是写入时触发的，而是 **下一次 Fetch 请求** 时触发的：

```java
// Leader 处理 Fetch 时更新 HW
void handleFetchRequest(FetchRequest req) {
    // 根据 follower 的 fetchOffset，推进 HW
    updateHighWatermark(req.replica, req.fetchOffset);

    // 然后返回数据
    return fetchRecords();
}
```

**可能有几毫秒到几百毫秒的延迟**

#### 如果只用 Producer ACK 会怎样？

```java
// 错误的实现
void processWriteEvent(WriteEvent event) {
    // 1. Replay 到内存
    offsets.put(event.key, event.offset);

    // 2. 写入 log
    RecordMetadata metadata = producer.send(record).get();  // 等待 ACK

    // 3. 立即完成 future ← 错误！
    event.future.complete(null);
}

// 问题场景
Thread 1: Write
  producer.send().get();  // ACK 返回
  future.complete(null);  // 告诉客户端"成功"

Thread 2: Read (同时)
  Long offset = offsets.get(key);  // 读到了！
  // 但实际上：
  // - HW 还没推进
  // - 如果 leader crash，新 leader 可能没有这个数据
  // - 导致不一致！
```

#### Deferred Event Queue 的作用

```java
void processWriteEvent(WriteEvent event) {
    // 1. Replay 到内存
    offsets.put(event.key, event.offset);

    // 2. 写入 log
    RecordMetadata metadata = producer.send(record).get();
    long writtenOffset = metadata.offset();

    // 3. 不立即完成！而是加入 deferred queue
    deferredEventQueue.add(writtenOffset, event);

    // future 暂时不 complete
}

// 单独的监听器
class HighWatermarkListener {
    void onHighWatermarkUpdated(long newHW) {
        // HW 推进时，complete 所有已安全的事件
        deferredEventQueue.completeUpTo(newHW);
    }
}
```

#### 核心思想

```
Producer ACK:  "数据已写入，不会丢失"
HW 推进:       "数据已提交，对读操作可见"

Coordinator 需要的是：HW 推进！
```

---

### 深入解析：ACK vs HW 时序对比

#### 方式 1: 只用 Producer ACK（错误）

```
Time  │ Coordinator      │ Broker              │ Client          │ 问题
──────┼──────────────────┼─────────────────────┼─────────────────┼─────────────────
  0   │ 收到写请求        │                     │                 │
  1   │ 更新内存: v=100   │                     │                 │
  2   │ 调用 send()      │                     │                 │
  3   │                  │ 写入 leader         │                 │
  4   │                  │ 复制到 follower1    │                 │
  5   │                  │ 复制到 follower2    │                 │
  6   │                  │ 所有 ISR 确认       │                 │
  7   │ ← ACK 返回       │                     │                 │
  8   │ future.complete()│                     │ ← 收到"成功"    │ ← 太早！
  9   │                  │                     │                 │
 10   │                  │                     │ 发起读请求       │
 11   │ 返回 v=100       │                     │ ← 读到 100      │ ← Dirty Read!
      │                  │ HW = 99             │                 │    HW 还是 99
 12   │                  │ ↓                   │                 │
 13   │                  │ Follower Fetch请求  │                 │
 14   │                  │ 更新 replica offset │                 │
 15   │                  │ HW 推进到 100       │                 │ ← 太晚！
```

**问题：**
- T8: 客户端收到成功，但 HW = 99
- T11: 读操作读到了 uncommitted 数据（offset 100）
- 如果 T12 时 leader crash，offset 100 可能丢失，但客户端已经看到了！

#### 方式 2: 使用 Deferred Event Queue（正确）

```
Time  │ Coordinator      │ Broker              │ Deferred Queue  │ Client
──────┼──────────────────┼─────────────────────┼─────────────────┼─────────────────
  0   │ 收到写请求        │                     │                 │
  1   │ 更新内存: v=100   │                     │                 │
  2   │ 调用 send()      │                     │                 │
  3   │                  │ 写入 leader         │                 │
  4   │                  │ 复制到 follower1    │                 │
  5   │                  │ 复制到 follower2    │                 │
  6   │                  │ 所有 ISR 确认       │                 │
  7   │ ← ACK 返回       │                     │                 │
  8   │                  │                     │ ← add(100, fut) │ ← 加入队列
  9   │                  │                     │ [waiting...]    │ [waiting...]
 10   │ 收到读请求        │                     │                 │
 11   │ 返回 null        │                     │                 │ ← 读到 null
      │ (HW=99, v=100)   │ HW = 99             │                 │    正确！
 12   │                  │ ↓                   │                 │
 13   │                  │ Follower Fetch请求  │                 │
 14   │                  │ 更新 replica offset │                 │
 15   │                  │ HW 推进到 100       │                 │
 16   │ ← HW 更新事件    │                     │                 │
 17   │                  │                     │ completeUpTo(100)│
 18   │                  │                     │ → future.complete│ ← 收到"成功"
 19   │ 收到读请求        │                     │                 │
 20   │ 返回 v=100       │                     │                 │ ← 读到 100
      │ (HW=100, v=100)  │ HW = 100            │                 │    正确！
```

**正确：**
- T8: ACK 返回，但不立即返回成功，而是加入 deferred queue
- T11: 读操作读到 null（因为 HW=99，offset 100 还未 committed）
- T18: HW 推进到 100 后，才返回成功给客户端
- T20: 读操作可以读到 100（因为 HW=100）

#### 类比：银行转账

```
只用 Producer ACK:
  你: "我已经转账了" (ACK 返回)
  对方: "我还没收到钱" (HW 还没推进)
  → 不一致！

使用 Deferred Event Queue:
  你: "我发起了转账" (写入 log)
  银行: "转账处理中..." (deferred queue)
  对方: "我收到钱了" (HW 推进)
  银行: "转账成功" (future complete)
  → 一致！
```

---

### 问题出现了！

#### 问题 8: 读取到未提交的数据

```java
// Thread 1: Write
coordinator.writeOffset("group1", "topic1", 0, 100L);
// 此时：内存是 100，但 HW 还没推进

// Thread 2: Read (同时)
Long offset = coordinator.readOffset("group1", "topic1", 0);
// 读到 100！但这个数据还没有被复制到多数派
// 如果 leader crash，这个数据会丢失
// 但 reader 已经看到了！
```

**为什么是问题？**
- 读操作看到了未提交的数据（Dirty Read）
- 违反了一致性保证

---

## 版本 6: 为什么需要 MVCC？

### 核心概念

**MVCC (Multi-Version Concurrency Control)** = 多版本并发控制

- **写操作**：创建新版本，使用 `lastWrittenOffset`
- **读操作**：读取旧版本，使用 `lastCommittedOffset`

### 解决方案：TimelineHashMap + SnapshotRegistry

```java
// 简化版的 TimelineHashMap
class TimelineHashMap<K, V> {
    // key -> Timeline
    // Timeline = List<(offset, value)>
    private final Map<K, List<VersionedValue<V>>> data = new HashMap<>();
    private final SnapshotRegistry registry;

    static class VersionedValue<V> {
        final long offset;
        final V value;

        VersionedValue(long offset, V value) {
            this.offset = offset;
            this.value = value;
        }
    }

    public TimelineHashMap(SnapshotRegistry registry) {
        this.registry = registry;
    }

    public V get(K key) {
        List<VersionedValue<V>> timeline = data.get(key);
        if (timeline == null) return null;

        // 使用 registry 中的 currentOffset
        long currentOffset = registry.currentOffset();

        // 找到 <= currentOffset 的最新值
        V result = null;
        for (VersionedValue<V> vv : timeline) {
            if (vv.offset <= currentOffset) {
                result = vv.value;
            } else {
                break;
            }
        }
        return result;
    }

    public void put(K key, V value) {
        List<VersionedValue<V>> timeline = data.computeIfAbsent(key, k -> new ArrayList<>());

        // 使用 registry 中的 currentOffset
        long currentOffset = registry.currentOffset();

        // 添加新版本
        timeline.add(new VersionedValue<>(currentOffset, value));
    }
}

class SnapshotRegistry {
    private long currentOffset = 0;

    public long currentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }
}

// 使用 MVCC 的 Coordinator
class ShareCoordinatorV6 {
    private final SnapshotRegistry registry = new SnapshotRegistry();
    private final TimelineHashMap<SharePartitionKey, Long> offsets = new TimelineHashMap<>(registry);

    private long lastWrittenOffset = 0;     // 已写入 log 的最新 offset
    private long lastCommittedOffset = 0;   // 已复制到多数派的 offset

    public CompletableFuture<Void> writeOffset(String group, String topic, int partition, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        eventQueue.add(new WriteEvent(..., future));
        return future;
    }

    public CompletableFuture<Long> readOffset(String group, String topic, int partition) {
        CompletableFuture<Long> future = new CompletableFuture<>();

        eventQueue.add(new ReadEvent(..., future));
        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 使用 lastWrittenOffset (未提交)
            registry.setCurrentOffset(lastWrittenOffset);

            // Replay 到内存
            offsets.put(e.key, e.offset);

            // 加入 batch
            currentBatch.add(new Record(e.key, e.offset));
            pendingFutures.add(e.future);

        } else if (event instanceof ReadEvent) {
            ReadEvent e = (ReadEvent) event;

            // 使用 lastCommittedOffset (已提交)
            registry.setCurrentOffset(lastCommittedOffset);

            // 读取已提交的数据
            Long offset = offsets.get(e.key);

            e.future.complete(offset);

        } else if (event instanceof FlushEvent) {
            flushBatch();

        } else if (event instanceof HighWatermarkEvent) {
            updateHighWatermark(((HighWatermarkEvent) event).offset);
        }
    }

    private void flushBatch() {
        if (currentBatch.isEmpty()) return;

        // 写入 log
        for (Record record : currentBatch) {
            producer.send(stateTopic, serialize(record));
        }
        producer.flush();

        // 更新 lastWrittenOffset
        lastWrittenOffset += currentBatch.size();

        // 加入 deferred queue
        deferredEvents.put(lastWrittenOffset, new ArrayList<>(pendingFutures));

        currentBatch.clear();
        pendingFutures.clear();
    }

    private void updateHighWatermark(long newHW) {
        // 更新 lastCommittedOffset
        lastCommittedOffset = newHW;

        // Complete deferred events
        // ...
    }
}

// 使用示例
ShareCoordinatorV6 coordinator = new ShareCoordinatorV6();

// Write
coordinator.writeOffset("group1", "topic1", 0, 100L);
// 内存: offset 100 at version lastWrittenOffset

// Read (同时)
coordinator.readOffset("group1", "topic1", 0);
// 读取 version lastCommittedOffset 的数据
// 如果 lastCommittedOffset < lastWrittenOffset，读不到 100
```

### MVCC 示例

```java
// 初始状态
lastCommittedOffset = 0
lastWrittenOffset = 0
offsets = {}

// Write 1: offset 100
registry.setCurrentOffset(0);
offsets.put("key1", 100L);
// Timeline: [(0, 100)]
lastWrittenOffset = 1

// Write 2: offset 200
registry.setCurrentOffset(1);
offsets.put("key1", 200L);
// Timeline: [(0, 100), (1, 200)]
lastWrittenOffset = 2

// Read (HW 还没推进)
registry.setCurrentOffset(lastCommittedOffset);  // 0
Long val = offsets.get("key1");
// 返回: null (因为没有 offset <= 0 的版本)

// HW 推进到 1
lastCommittedOffset = 1

// Read again
registry.setCurrentOffset(lastCommittedOffset);  // 1
Long val = offsets.get("key1");
// 返回: 200 (找到 offset 1 的版本)
```

### 为什么这样设计？

#### 优势 1: 读写隔离
- 写操作立即更新内存，不阻塞
- 读操作只看已提交的数据，保证一致性

#### 优势 2: 无锁并发
- 写操作和读操作不需要加锁
- TimelineHashMap 支持多版本，天然支持并发

#### 优势 3: 时间旅行
- 可以读取任意 offset 的历史状态
- 方便调试和审计

---

### 深入解析：TimelineHashMap 的正确实现

#### 核心问题

**如何高效地获取 "小于等于 currentOffset 的最大 offset 对应的值"？**

这是 MVCC (Multi-Version Concurrency Control) 的核心操作。

#### 错误实现：线性遍历（O(n)）

```java
// ❌ 错误：O(n) 时间复杂度
V get(K key) {
    List<VersionedValue<V>> timeline = data.get(key);
    if (timeline == null) return null;

    V result = null;
    for (VersionedValue<V> vv : timeline) {
        if (vv.offset <= currentOffset) {
            result = vv.value;  // 不断覆盖，最后得到最大的
        } else {
            break;
        }
    }
    return result;
}
```

**问题：** 时间复杂度 O(n)，性能随版本数线性增长

#### 正确实现：TreeMap.floorEntry()（O(log n)）

```java
// ✅ 正确：O(log n) 时间复杂度
static class TimelineHashMap<K, V> {
    private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
    private long currentOffset = 0;

    V get(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return null;

        // floorEntry(offset): 返回 <= offset 的最大 key 的 entry
        Map.Entry<Long, V> entry = timeline.floorEntry(currentOffset);
        return entry != null ? entry.getValue() : null;
    }

    void put(K key, V value) {
        data.computeIfAbsent(key, k -> new TreeMap<>())
            .put(currentOffset, value);
    }
}
```

#### TreeMap.floorEntry() 原理

`TreeMap` 是基于**红黑树**实现的有序 Map：

```java
TreeMap<Long, V> timeline = new TreeMap<>();
timeline.put(0L, "v0");
timeline.put(5L, "v5");
timeline.put(10L, "v10");

// floorEntry(offset): 返回 <= offset 的最大 key
timeline.floorEntry(3L);   // (0, "v0")  - 最大的 <= 3 的 key 是 0
timeline.floorEntry(5L);   // (5, "v5")  - 最大的 <= 5 的 key 是 5
timeline.floorEntry(7L);   // (5, "v5")  - 最大的 <= 7 的 key 是 5
timeline.floorEntry(15L);  // (10, "v10") - 最大的 <= 15 的 key 是 10
```

**时间复杂度：O(log n)** - 红黑树的查找时间

#### MVCC 读取示例

```java
TimelineHashMap<String, Integer> state = new TimelineHashMap<>();

// 模拟 3 次写入
state.setCurrentOffset(0);
state.put("balance", 100);  // offset=0: balance=100

state.setCurrentOffset(5);
state.put("balance", 200);  // offset=5: balance=200

state.setCurrentOffset(10);
state.put("balance", 300);  // offset=10: balance=300

// Timeline: {0→100, 5→200, 10→300}

// 读取不同 offset 时刻的值
state.setCurrentOffset(0);
System.out.println(state.get("balance"));  // 100

state.setCurrentOffset(3);
System.out.println(state.get("balance"));  // 100 (使用 offset=0 的值)

state.setCurrentOffset(7);
System.out.println(state.get("balance"));  // 200 (使用 offset=5 的值)

state.setCurrentOffset(15);
System.out.println(state.get("balance"));  // 300 (使用 offset=10 的值)
```

#### 性能对比

| 操作 | List 线性遍历 | TreeMap.floorEntry() |
|------|---------------|----------------------|
| **get()** | O(n) | O(log n) |
| **put()** | O(1) | O(log n) |
| **回滚** | O(1) | O(1) |

**结论：** TreeMap 的 **get() 性能远优于线性遍历**，在版本数较多时差异更明显

---

### 深入解析：为什么 SnapshotRegistry 这么复杂？

#### 简化版 vs Kafka 的实现

| 特性 | 简化版 | Kafka SnapshotRegistry |
|------|--------|----------------------|
| **核心实现** | 单个 `TimelineHashMap` | `SnapshotRegistry` + `Snapshot` + `Revertable` + `Delta` |
| **代码行数** | ~100 行 | ~500+ 行 |
| **回滚机制** | 修改 `currentOffset` | 删除 Snapshot 并调用 `executeRevert()` |
| **内存管理** | 手动 `deleteAfter()` | 自动 + WeakReference |
| **多数据结构** | 单个 HashMap | 多个 Timeline 数据结构 |

#### 核心区别 1: 中心化管理

**简化版：** 每个 TimelineHashMap 独立管理

```java
// 需要手动同步 3 个 TimelineHashMap
TimelineHashMap<String, Integer> map1 = new TimelineHashMap<>();
TimelineHashMap<String, String> map2 = new TimelineHashMap<>();
TimelineHashMap<String, List<String>> map3 = new TimelineHashMap<>();

// 回滚（需要手动同步）
map1.revertTo(5);
map2.revertTo(5);
map3.revertTo(5);
// 问题：如果忘记回滚 map3，状态不一致！
```

**Kafka：** 中心化管理

```java
// 创建中心化的 SnapshotRegistry
SnapshotRegistry registry = new SnapshotRegistry(new LogContext());

// 所有数据结构自动注册
TimelineHashMap<String, Integer> map1 = new TimelineHashMap<>(registry, 10);
TimelineHashMap<String, String> map2 = new TimelineHashMap<>(registry, 10);
TimelineHashMap<String, List<String>> map3 = new TimelineHashMap<>(registry, 10);

// 回滚（一次调用，所有数据结构自动同步）
registry.revertToSnapshot(5);
// ✅ map1, map2, map3 都回滚到 epoch 5
```

#### 核心区别 2: 显式 Snapshot 对象

**简化版：** 隐式快照

```java
// 没有显式的 Snapshot 对象
// 快照隐含在 TreeMap 的 entry 中
Map<K, TreeMap<Long, V>> data;
```

**Kafka：** 显式 Snapshot

```java
class Snapshot {
    private final long epoch;
    private IdentityHashMap<Revertable, Delta> map;
    private Snapshot prev;  // 双向链表
    private Snapshot next;
}

// Snapshot 双向链表（按 epoch 排序）
head ↔ Snapshot(0) ↔ Snapshot(5) ↔ Snapshot(10) ↔ Snapshot(15)
```

**优势：**
- ✅ 独立管理每个 Snapshot
- ✅ O(1) 访问和遍历
- ✅ 可以合并和删除快照

#### 核心区别 3: Delta 增量存储

**简化版：** 存储完整值

```java
// 每个版本存储完整的值
TreeMap<Long, V> timeline;
timeline.put(0, "value at offset 0");    // 存储完整值
timeline.put(5, "value at offset 5");    // 存储完整值
timeline.put(10, "value at offset 10");  // 存储完整值
```

**Kafka：** 增量 Delta

```java
// 初始状态: {a=1, b=2, c=3}

// Snapshot(0): 空 Delta（初始状态）

// Snapshot(5): Delta {a=10}  // 只记录 a 的变更
// 当前状态: {a=10, b=2, c=3}

// Snapshot(10): Delta {b=20}  // 只记录 b 的变更
// 当前状态: {a=10, b=20, c=3}
```

**优势：** 只存储变更，减少内存占用

#### 核心区别 4: WeakReference 自动内存管理

**简化版：** 手动清理

```java
void deleteAfter(long offset) {
    for (TreeMap<Long, V> timeline : data.values()) {
        timeline.tailMap(offset + 1).clear();  // 手动删除
    }
}
// 需要手动调用，容易忘记导致内存泄漏
```

**Kafka：** 自动清理

```java
class SnapshotRegistry {
    // 使用 WeakReference 存储 Revertable
    private List<WeakReference<Revertable>> revertables;

    void register(Revertable revertable) {
        revertables.add(new WeakReference<>(revertable));
        if (numRegistrationsSinceScrub > maxRegistrationsSinceScrub) {
            scrub();  // 自动清理过期引用
        }
    }
}
```

#### 为什么 Kafka 需要这么复杂？

**1. 多数据结构同步回滚**

ShareCoordinatorShard 需要管理多个 Timeline 数据结构：

```java
class ShareCoordinatorShard {
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;
    private final TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;
    private final TimelineHashMap<SharePartitionKey, Integer> snapshotUpdateCount;
    private final TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;

    // 使用 SnapshotRegistry：一行代码同步回滚
    void revert(long offset) {
        registry.revertToSnapshot(offset);  // ✅ 自动同步
    }
}
```

**2. 生产环境需求**

| 需求 | 简化版 | Kafka |
|------|--------|-------|
| 多数据结构同步回滚 | ❌ 手动 | ✅ 自动 |
| 防止内存泄漏 | ❌ 手动清理 | ✅ WeakReference |
| 快照合并优化 | ❌ 不支持 | ✅ 智能合并 |
| 增量存储 | ❌ 完整值 | ✅ Delta |
| 一致性保证 | ❌ 易出错 | ✅ 强一致性 |

#### 总结

| 维度 | 简化版 | Kafka SnapshotRegistry |
|------|--------|----------------------|
| **目标** | 教学演示 | 生产级系统 |
| **复杂度** | 简单 | 复杂 |
| **功能** | 基础 MVCC | 完整快照管理框架 |
| **内存管理** | 手动 | 自动 + 优化 |
| **一致性** | 弱（易出错） | 强（原子操作） |
| **扩展性** | 单一数据结构 | 多种数据结构 |

**关键洞察：**

简化版是为了**教学**，帮助快速理解 MVCC 核心原理。

Kafka 的 SnapshotRegistry 是为了**生产**，解决大规模分布式系统的实际问题：
- 多数据结构同步
- 内存管理
- 一致性保证
- 扩展性

两者的**核心思想一致**（MVCC + 多版本存储），但**工程实现复杂度**完全不同！

---

## 版本 7: 完整的 CoordinatorRuntime

现在我们把所有功能组合起来，形成通用的框架。

### 架构分层

```
┌─────────────────────────────────────────┐
│     ShareCoordinatorService             │  ← 业务层：API 接口
│  (writeShareGroupState, readShareGroupState) │
└────────────────┬────────────────────────┘
                 │
                 │ 调用
                 ▼
┌─────────────────────────────────────────┐
│       CoordinatorRuntime<S, U>          │  ← 框架层：通用能力
│  - Event Queue                          │
│  - Batching                             │
│  - Deferred Events                      │
│  - MVCC                                 │
│  - High Watermark Listener              │
└────────────────┬────────────────────────┘
                 │
                 │ 管理
                 ▼
┌─────────────────────────────────────────┐
│      CoordinatorContext (per partition) │  ← 分区层：每个 partition 一个
│  - SnapshottableCoordinator             │
│  - CoordinatorBatch                     │
│  - DeferredEventQueue                   │
│  - EventBasedCoordinatorTimer           │
└────────────────┬────────────────────────┘
                 │
                 │ 包含
                 ▼
┌─────────────────────────────────────────┐
│      ShareCoordinatorShard              │  ← 状态机层：业务逻辑
│  - TimelineHashMap<key, offset>        │
│  - replay()                             │
│  - writeState()                         │
│  - readState()                          │
└─────────────────────────────────────────┘
```

### 关键组件

#### 1. CoordinatorShard 接口

```java
// 状态机接口，业务层实现
public interface CoordinatorShard<U> {
    // 重放记录到状态机
    void replay(long offset, long producerId, short producerEpoch, U record);

    // 加载完成后的回调
    void onLoaded(MetadataImage newImage);

    // 卸载时的清理
    void onUnloaded();
}
```

#### 2. CoordinatorRuntime

```java
public class CoordinatorRuntime<S extends CoordinatorShard<U>, U> {
    // 每个 partition 一个 context
    private final Map<TopicPartition, CoordinatorContext> coordinators;

    // 事件处理器
    private final CoordinatorEventProcessor eventProcessor;

    // 调度写操作
    public <T> CompletableFuture<T> scheduleWriteOperation(
        String name,
        TopicPartition tp,
        Duration timeout,
        CoordinatorWriteOperation<S, T, U> op
    ) {
        CoordinatorWriteEvent<T> event = new CoordinatorWriteEvent<>(name, tp, timeout, op);
        eventProcessor.enqueue(event);
        return event.future;
    }

    // 调度读操作
    public <T> CompletableFuture<T> scheduleReadOperation(
        String name,
        TopicPartition tp,
        CoordinatorReadOperation<S, T> op
    ) {
        CoordinatorReadEvent<T> event = new CoordinatorReadEvent<>(name, tp, op);
        eventProcessor.enqueue(event);
        return event.future;
    }
}
```

#### 3. ShareCoordinatorShard

```java
public class ShareCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) {
        // 根据记录类型更新状态
        if (record.key() instanceof ShareSnapshotKey) {
            ShareSnapshotKey key = (ShareSnapshotKey) record.key();
            ShareSnapshotValue value = (ShareSnapshotValue) record.value();
            shareStateMap.put(SharePartitionKey.from(key), ShareGroupOffset.fromRecord(value));
        }
    }

    // 写操作
    public CoordinatorResult<WriteResponse, CoordinatorRecord> writeState(WriteRequest request) {
        // 1. 验证请求
        validate(request);

        // 2. 生成 record
        CoordinatorRecord record = generateRecord(request);

        // 3. 构造响应
        WriteResponse response = new WriteResponse(SUCCESS);

        // 4. 返回 result
        // CoordinatorRuntime 会：
        //   a) replay record 到状态机
        //   b) 写入 log
        //   c) 等待 HW 后返回 response
        return new CoordinatorResult<>(
            Collections.singletonList(record),
            response,
            true  // replayRecords = true
        );
    }

    // 读操作
    public ReadResponse readState(ReadRequest request, long committedOffset) {
        // 使用 committedOffset 的快照
        ShareGroupOffset offset = shareStateMap.get(key);
        return new ReadResponse(offset);
    }
}
```

#### 4. ShareCoordinatorService

```java
public class ShareCoordinatorService {
    private final CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime;

    public CompletableFuture<WriteShareGroupStateResponse> writeShareGroupState(
        WriteShareGroupStateRequest request
    ) {
        TopicPartition tp = partitionFor(request.groupId());

        return runtime.scheduleWriteOperation(
            "WriteShareGroupState",
            tp,
            Duration.ofSeconds(30),
            shard -> shard.writeState(request)
        );
    }

    public CompletableFuture<ReadShareGroupStateResponse> readShareGroupState(
        ReadShareGroupStateRequest request
    ) {
        TopicPartition tp = partitionFor(request.groupId());

        return runtime.scheduleReadOperation(
            "ReadShareGroupState",
            tp,
            (shard, committedOffset) -> shard.readState(request, committedOffset)
        );
    }
}
```

### 完整流程

```
Client Request
     │
     ▼
ShareCoordinatorService.writeShareGroupState()
     │
     ▼
runtime.scheduleWriteOperation(tp, shard -> shard.writeState(request))
     │
     ▼
创建 CoordinatorWriteEvent，加入 EventProcessor 队列
     │
     ▼
EventProcessor 线程取出 event
     │
     ▼
event.run() {
    获取 CoordinatorContext
    获取 context.lock
    op.generateRecordsAndResult(shard)  // 调用 shard.writeState()
    context.append(records, this)
}
     │
     ▼
context.append() {
    registry.setCurrentOffset(lastWrittenOffset)
    shard.replay(record)  // 更新内存
    currentBatch.add(record)
    deferredEvents.add(event)
    maybeFlushCurrentBatch()
}
     │
     ▼
context.flushCurrentBatch() {
    partitionWriter.append(records)
    lastWrittenOffset = returnedOffset
    deferredEventQueue.add(offset, deferredEvents)
}
     │
     ▼
[异步] Log 复制到 followers
     │
     ▼
HighWatermarkListener.onHighWatermarkUpdated(newHW)
     │
     ▼
推送 HighWatermarkUpdate event 到队列
     │
     ▼
context 处理 HW 更新 {
    lastCommittedOffset = newHW
    deferredEventQueue.completeUpTo(newHW)
}
     │
     ▼
event.complete(null)
     │
     ▼
future.complete(response)
     │
     ▼
Client 收到响应
```

---

## 总结：为什么需要这些设计？

| 版本 | 问题 | 解决方案 | 收益 |
|------|------|----------|------|
| V1 | 数据丢失 | 写入 Kafka log | 持久化 + 高可用 |
| V2 | 并发冲突 | 事件队列串行化 | 线程安全 + 异步 |
| V3 | 性能低 | Batching | 吞吐量提升 50 倍 |
| V4 | 数据不安全 | Deferred Events | 等待复制完成 |
| V5 | Dirty Read | MVCC | 读写隔离 |
| V6 | - | CoordinatorRuntime | 通用框架 |

### 核心思想

1. **复制状态机**: 通过 log 复制保证一致性
2. **事件驱动**: 串行化操作，避免并发问题
3. **Batching**: 提高吞吐量，减少 IO
4. **异步处理**: 写入立即返回 Future，等待后台完成
5. **MVCC**: 读已提交数据，写不阻塞

### 为什么这样复杂？

**因为我们要同时满足**：
- ✅ 高性能 (Batching)
- ✅ 一致性 (MVCC + Deferred Events)
- ✅ 高可用 (基于 Kafka log)
- ✅ 线程安全 (Event Queue)
- ✅ 可扩展 (分区级并行)

每一层设计都解决了一个实际问题，缺一不可！

---

## 高级主题：Revert 回滚机制

### 功能概述

基于 TimelineHashMap 的 MVCC 特性，我们可以实现状态回滚功能，这在以下场景非常有用：

#### 使用场景

**场景 1: 错误恢复**

```java
// 当前状态正确
coordinator.writeState(key, correctState, "正常操作");
coordinator.createCheckpoint("before_update");

// 由于 bug，写入了错误的状态
coordinator.writeState(key, buggyState, "Bug 导致的错误状态");

// 发现错误，立即回滚
coordinator.revertToCheckpoint("before_update");
```

**优势：** 秒级恢复，无需重启服务，避免数据丢失

**场景 2: 事务支持**

```java
// BEGIN TRANSACTION
long txStart = coordinator.getCurrentOffset();

try {
    coordinator.writeState(key1, state1, "操作 1");
    coordinator.writeState(key2, state2, "操作 2");
    coordinator.writeState(key3, state3, "操作 3");

    // 验证一致性
    if (!validateConsistency(key1, key2, key3)) {
        throw new Exception("状态不一致");
    }

    // COMMIT (do nothing, changes are already written)

} catch (Exception e) {
    // ROLLBACK
    coordinator.revertToVersion(txStart);
    System.err.println("事务回滚: " + e.getMessage());
}
```

**场景 3: 时间旅行调试**

```java
// 列出所有版本
List<Long> versions = coordinator.listVersions(key);
System.out.println("历史版本: " + versions);  // [0, 5, 10, 15, 20]

// 查看每个版本的状态
for (Long version : versions) {
    ShareGroupState state = coordinator.getStateAtVersion(key, version);
    System.out.println("Version " + version + ": " + state);
}

// 回到问题发生前的版本
coordinator.revertToVersion(15);

// 重新执行操作，观察是否能重现问题
coordinator.writeState(key, newState, "重现问题");
```

### 实现原理

#### 1. TimelineHashMap 的 MVCC 特性

```java
class TimelineHashMap<K, V> {
    // 每个 key 对应一个 TreeMap<Long, V>
    private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
    private long currentOffset = 0;

    void put(K key, V value) {
        // 在当前 offset 写入新版本
        data.computeIfAbsent(key, k -> new TreeMap<>())
            .put(currentOffset, value);
    }

    V get(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return null;

        // floorEntry: 返回 <= currentOffset 的最大 offset
        Map.Entry<Long, V> entry = timeline.floorEntry(currentOffset);
        return entry != null ? entry.getValue() : null;
    }
}
```

**关键点：**
- `put()` 不覆盖老版本，而是添加新版本
- `get()` 使用 `floorEntry()` 获取当前 offset 可见的版本
- 回滚只需修改 `currentOffset`，不需要删除数据

#### 2. 回滚机制

```java
void revertTo(long targetOffset) {
    // 修改 currentOffset
    this.currentOffset = targetOffset;
}
```

**示例：**

```
Timeline: {0→v0, 5→v1, 10→v2, 15→v3}

currentOffset = 15:
  get() 返回 v3 (floorEntry(15) = 15)

revertTo(7):
  currentOffset = 7
  get() 返回 v1 (floorEntry(7) = 5)

revertTo(0):
  currentOffset = 0
  get() 返回 v0 (floorEntry(0) = 0)
```

**核心优势：**
- O(1) 回滚时间
- 不需要复制数据
- 可以前进和后退

#### 3. 检查点机制

```java
class EnhancedShareCoordinatorShard {
    private final Map<String, Long> checkpoints = new HashMap<>();

    void createCheckpoint(String name) {
        long offset = shareStateMap.getCurrentOffset();
        checkpoints.put(name, offset);
    }

    void revertToCheckpoint(String name) {
        Long offset = checkpoints.get(name);
        if (offset != null) {
            revertToVersion(offset);
        }
    }
}
```

**类比数据库的 SAVEPOINT：**

```sql
-- SQL
SAVEPOINT sp1;
UPDATE ...;
ROLLBACK TO sp1;

-- Coordinator
createCheckpoint("sp1");
writeState(...);
revertToCheckpoint("sp1");
```

### API 设计

```java
public interface VersionedCoordinator {
    /**
     * 回滚到指定版本
     */
    void revertToVersion(long targetOffset);

    /**
     * 列出某个 key 的所有历史版本
     */
    List<Long> listVersions(SharePartitionKey key);

    /**
     * 获取指定版本的状态（不改变当前 offset）
     */
    ShareGroupState getStateAtVersion(SharePartitionKey key, long offset);

    /**
     * 创建命名检查点
     */
    void createCheckpoint(String name);

    /**
     * 回滚到检查点
     */
    void revertToCheckpoint(String name);

    /**
     * 清理历史版本
     */
    void cleanupOldVersions(long keepAfterOffset);
}
```

### 性能分析

| 操作 | 时间复杂度 | 说明 |
|------|-----------|------|
| `writeState()` | O(log n) | TreeMap.put() |
| `readState()` | O(log n) | TreeMap.floorEntry() |
| `revertToVersion()` | O(1) | 只修改 currentOffset |
| `listVersions()` | O(k) | k = 版本数 |
| `getStateAtVersion()` | O(log n) | 临时修改 offset + get() |
| `createCheckpoint()` | O(1) | HashMap.put() |
| `cleanupOldVersions()` | O(m × k) | m = key 数，k = 待删除版本数 |

### 与原版 Kafka 的对比

**原版 Kafka ShareCoordinator：**

```java
class ShareCoordinatorShard {
    // 只保留当前版本
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;

    // 写入会覆盖旧状态（通过 MVCC 的 offset 推进）
    void replay(long offset, CoordinatorRecord record) {
        shareStateMap.put(key, newState);
    }

    // 无法回滚
}
```

**限制：**
- ❌ 无法回滚到历史版本
- ❌ 错误数据一旦写入，只能通过新的写入来修正
- ❌ 无法查看历史状态
- ✅ 内存占用小（只保留必要的历史版本）

**增强版 ShareCoordinator：**

```java
class EnhancedShareCoordinatorShard {
    // 保留多个版本
    private final TimelineHashMap<SharePartitionKey, ShareGroupState> shareStateMap;
    private final Map<Long, String> operationLog;
    private final Map<String, Long> checkpoints;

    // 支持回滚
    void revertToVersion(long targetOffset) { ... }

    // 支持查看历史
    List<Long> listVersions(SharePartitionKey key) { ... }

    // 支持检查点
    void createCheckpoint(String name) { ... }
}
```

**优势：**
- ✅ 支持回滚
- ✅ 支持查看历史
- ✅ 支持事务语义
- ✅ 便于调试和问题分析
- ⚠️ 内存占用增加（需要清理策略）

### 适用场景

✅ **适合：**
- 需要错误恢复的生产环境
- 需要调试的开发环境
- 需要事务语义的场景
- 需要历史查询的分析场景

❌ **不适合：**
- 内存极度受限的环境
- 写入频率极高（> 10K/s）且版本保留时间长
- 不需要回滚功能的简单场景

### 总结

| 维度 | 价值 |
|------|------|
| **可靠性** | 快速从错误中恢复，降低故障影响 |
| **可调试性** | 时间旅行调试，快速定位问题 |
| **可测试性** | 回滚测试数据，无需重启服务 |
| **灵活性** | 支持事务、A/B测试等高级功能 |

**实现要点：**
1. 利用 MVCC：TimelineHashMap 天然支持多版本
2. 低成本回滚：修改 currentOffset 即可，无需数据拷贝
3. 内存管理：定期清理历史版本
4. 同步一致性：多个 TimelineHashMap 需要同步回滚
5. 检查点机制：提供用户友好的回滚接口

---

## 深入理解：SnapshotRegistry 的演进史

在 V6 的 MVCC 部分，我们提到了 Kafka 使用 `SnapshotRegistry` 来统一管理所有 Timeline 数据结构。但为什么需要这么复杂的设计？让我们通过代码演进来理解。

**学习方式：** 通过可运行的代码展示每个版本如何解决前一个版本的具体问题。

**完整代码：** `.claude/coordinator-learning/snapshotregistry-evolution.java`

### 版本 1: 每个数据结构自己管理版本

**问题场景：** ShareCoordinator 需要管理多个 TimelineHashMap，最简单的想法是每个 Map 自己维护 currentOffset。

```java
class TimelineHashMap<K, V> {
    private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
    private long currentOffset = 0;  // 每个 Map 自己的 offset
    
    public void setOffset(long offset) {
        this.currentOffset = offset;
    }
    
    public void put(K key, V value) {
        data.computeIfAbsent(key, k -> new TreeMap<>())
            .put(currentOffset, value);
    }
    
    public V get(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return null;
        
        Map.Entry<Long, V> entry = timeline.floorEntry(currentOffset);
        return entry != null ? entry.getValue() : null;
    }
}

class ShareCoordinator {
    // 需要同步管理 3 个 TimelineHashMap
    private final TimelineHashMap<String, Integer> stateMap = new TimelineHashMap<>();
    private final TimelineHashMap<String, Integer> leaderEpochMap = new TimelineHashMap<>();
    private final TimelineHashMap<String, Integer> stateEpochMap = new TimelineHashMap<>();
    
    public void writeState(String key, int state, int leaderEpoch, int stateEpoch, long offset) {
        // 必须手动同步所有 Map 的 offset
        stateMap.setOffset(offset);
        leaderEpochMap.setOffset(offset);
        stateEpochMap.setOffset(offset);
        
        stateMap.put(key, state);
        leaderEpochMap.put(key, leaderEpoch);
        stateEpochMap.put(key, stateEpoch);
    }
    
    public void revertToSnapshot(long offset) {
        // 必须手动回滚所有 Map
        stateMap.revertTo(offset);
        leaderEpochMap.revertTo(offset);
        stateEpochMap.revertTo(offset);
    }
}
```

**问题暴露：**

❌ **问题 1：** 如果忘记同步某个 Map 的 offset？
- 例如：只设置了 `stateMap.setOffset(1)`，忘了 `leaderEpochMap`
- 结果：状态不一致！stateMap 在版本 1，leaderEpochMap 在版本 0

❌ **问题 2：** 新增一个 TimelineHashMap 怎么办？
- 需要修改所有 `writeState()` 和 `revertToSnapshot()` 的代码
- 容易遗漏，导致 bug

❌ **问题 3：** 代码重复
- 每次都要写 3 次 `setOffset()`，容易出错

---

### 版本 2: 中心化的 SnapshotRegistry

**解决方案：** 创建一个中心化的 SnapshotRegistry，所有 TimelineHashMap 注册到 registry，由它统一管理 offset。

```java
class SnapshotRegistry {
    private long currentOffset = 0;
    private final List<Revertable> revertables = new ArrayList<>();
    
    public long currentOffset() {
        return currentOffset;
    }
    
    public void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }
    
    public void register(Revertable revertable) {
        revertables.add(revertable);
    }
    
    public void revertToSnapshot(long targetOffset) {
        // 自动回滚所有注册的数据结构
        for (Revertable revertable : revertables) {
            revertable.revert(targetOffset);
        }
        this.currentOffset = targetOffset;
    }
}

interface Revertable {
    void revert(long targetOffset);
}

class TimelineHashMap<K, V> implements Revertable {
    private final SnapshotRegistry registry;
    
    public TimelineHashMap(SnapshotRegistry registry) {
        this.registry = registry;
        registry.register(this);  // 自动注册
    }
    
    public void put(K key, V value) {
        long offset = registry.currentOffset();  // 使用统一的 offset
        // ...
    }
}

class ShareCoordinator {
    private final SnapshotRegistry registry = new SnapshotRegistry();
    private final TimelineHashMap<String, Integer> stateMap;
    private final TimelineHashMap<String, Integer> leaderEpochMap;
    private final TimelineHashMap<String, Integer> stateEpochMap;
    
    public ShareCoordinator() {
        // 自动注册到 registry
        this.stateMap = new TimelineHashMap<>(registry);
        this.leaderEpochMap = new TimelineHashMap<>(registry);
        this.stateEpochMap = new TimelineHashMap<>(registry);
    }
    
    public void writeState(String key, int state, int leaderEpoch, int stateEpoch, long offset) {
        // 只需设置一次 offset
        registry.setCurrentOffset(offset);
        
        stateMap.put(key, state);
        leaderEpochMap.put(key, leaderEpoch);
        stateEpochMap.put(key, stateEpoch);
    }
    
    public void revertToSnapshot(long offset) {
        // 一行代码，自动回滚所有 Map
        registry.revertToSnapshot(offset);
    }
}
```

**优势：**

✅ 统一管理 offset，不会不一致  
✅ 新增 Map 只需 `new TimelineHashMap(registry)`  
✅ 回滚一行代码，自动同步所有数据结构

**新问题：**

❌ **如何管理多个 snapshot？**
- 当前只能记住一个 currentOffset
- 如果想保留多个快照（例如 offset 0, 5, 10），无法实现

---

### 版本 3: Snapshot 对象

**解决方案：** 引入显式的 Snapshot 对象，使用 HashMap 存储多个 snapshot。

```java
class Snapshot {
    private final long epoch;
    
    public Snapshot(long epoch) {
        this.epoch = epoch;
    }
    
    public long epoch() {
        return epoch;
    }
}

class SnapshotRegistry {
    private final Map<Long, Snapshot> snapshots = new HashMap<>();
    private long currentOffset = 0;
    private final List<Revertable> revertables = new ArrayList<>();
    
    // 创建快照
    public void createSnapshot(long epoch) {
        Snapshot snapshot = new Snapshot(epoch);
        snapshots.put(epoch, snapshot);
    }
    
    // 回滚到快照
    public void revertToSnapshot(long targetEpoch) {
        Snapshot target = snapshots.get(targetEpoch);
        if (target == null) {
            throw new RuntimeException("快照不存在: " + targetEpoch);
        }
        
        // 删除所有 > targetEpoch 的快照
        snapshots.keySet().removeIf(epoch -> epoch > targetEpoch);
        
        // 回滚所有数据结构
        for (Revertable revertable : revertables) {
            revertable.revert(targetEpoch);
        }
        
        this.currentOffset = targetEpoch;
    }
    
    public List<Long> listSnapshots() {
        return new ArrayList<>(snapshots.keySet()).stream()
            .sorted()
            .toList();
    }
}
```

**使用示例：**

```java
SnapshotRegistry registry = new SnapshotRegistry();
TimelineHashMap<String, Integer> map = new TimelineHashMap<>(registry);

// 创建版本 0
registry.createSnapshot(0);
map.put("key1", 100);

// 创建版本 5
registry.createSnapshot(5);
map.put("key1", 200);

// 创建版本 10
registry.createSnapshot(10);
map.put("key1", 300);

System.out.println("快照列表: " + registry.listSnapshots());  // [0, 5, 10]

// 回滚到版本 5
registry.revertToSnapshot(5);
System.out.println("剩余快照: " + registry.listSnapshots());  // [0, 5]
```

**优势：**

✅ 可以保留多个快照  
✅ 回滚时自动删除未来的快照

**新问题：**

❌ **内存占用**
- 每个版本都存储完整的值
- 如果值很大（例如 1KB），10 个版本 = 10KB
- 能否只存储变更？

---

### 版本 4: Delta 机制

**解决方案：** Snapshot 不存储完整值，只存储 Delta（变更）。回滚时应用 Delta 来恢复状态。

```java
interface Delta {
    void apply();
    void mergeFrom(Delta other);
}

class HashMapDelta<K, V> implements Delta {
    private final Map<K, V> oldValues = new HashMap<>();  // 记录旧值
    private final Set<K> removedKeys = new HashSet<>();
    
    public void recordPut(K key, V oldValue) {
        oldValues.put(key, oldValue);
    }
    
    @Override
    public void apply() {
        // 应用 Delta：恢复旧值
        for (Map.Entry<K, V> entry : oldValues.entrySet()) {
            // 恢复到旧值
        }
    }
}

class Snapshot {
    private final long epoch;
    private final Map<Revertable, Delta> deltas = new IdentityHashMap<>();
    
    public void setDelta(Revertable owner, Delta delta) {
        deltas.put(owner, delta);
    }
    
    public void applyDeltas() {
        for (Delta delta : deltas.values()) {
            delta.apply();
        }
    }
}

class SnapshotRegistry {
    public void createSnapshot(long epoch) {
        Snapshot snapshot = new Snapshot(epoch);
        snapshots.put(epoch, snapshot);
        
        // 让每个 Revertable 创建自己的 Delta
        for (Revertable revertable : revertables) {
            Delta delta = revertable.createDelta(epoch);
            if (delta != null) {
                snapshot.setDelta(revertable, delta);
            }
        }
    }
    
    public void revertToSnapshot(long targetEpoch) {
        Snapshot target = snapshots.get(targetEpoch);
        
        // 应用 Delta 回滚
        target.applyDeltas();
        
        this.currentOffset = targetEpoch;
    }
}
```

**内存对比：**

```
假设一个 HashMap 有 100 个 key，每个 value 1KB：

完整快照存储（V3）:
  每个快照: 100 × 1KB = 100KB
  10 个快照: 1MB

Delta 增量存储（V4）:
  每个快照: 只存储变更的 key（假设 10 个）= 10KB
  10 个快照: 100KB
  
内存节省 10 倍！
```

**优势：**

✅ 每个快照只存储变更（Delta）  
✅ 内存占用大大减少

**新问题：**

❌ **内存泄漏**
- 如果 TimelineHashMap 对象被 GC 回收了
- 但 `registry.revertables` 还持有强引用
- 会导致内存泄漏！

---

### 版本 5: WeakReference

**解决方案：** 使用 WeakReference 存储 Revertable，允许 GC 回收不再使用的数据结构。

```java
class SnapshotRegistry {
    // 使用 WeakReference 而不是强引用
    private List<WeakReference<Revertable>> revertables = new ArrayList<>();
    private int numRegistrations = 0;
    private int numScrubs = 0;
    
    public void register(Revertable revertable) {
        revertables.add(new WeakReference<>(revertable));
        numRegistrations++;
        
        // 每 5 次注册，清理一次过期的 WeakReference
        if (numRegistrations % 5 == 0) {
            scrub();
        }
    }
    
    // 清理过期的 WeakReference
    private void scrub() {
        List<WeakReference<Revertable>> newList = new ArrayList<>();
        for (WeakReference<Revertable> ref : revertables) {
            if (ref.get() != null) {  // 对象还存活
                newList.add(ref);
            }
        }
        revertables = newList;
        numScrubs++;
    }
    
    public void createSnapshot(long epoch) {
        Snapshot snapshot = new Snapshot(epoch);
        snapshots.put(epoch, snapshot);
        
        // 遍历所有还存活的 Revertable
        for (WeakReference<Revertable> ref : revertables) {
            Revertable revertable = ref.get();
            if (revertable != null) {
                Delta delta = revertable.createDelta(epoch);
                if (delta != null) {
                    snapshot.setDelta(revertable, delta);
                }
            }
        }
    }
}
```

**示例：**

```java
SnapshotRegistry registry = new SnapshotRegistry();

{
    TimelineHashMap<String, Integer> map = new TimelineHashMap<>(registry, "Map1");
    // map 被注册到 registry
    
    // ... 使用 map ...
    
} // map 离开作用域

// 如果使用强引用，registry 会一直持有 map，导致内存泄漏
// 使用 WeakReference，GC 可以回收 map

System.gc();  // 触发 GC
Thread.sleep(100);

// scrub() 会在下次注册时自动清理过期引用
```

**优势：**

✅ 使用 WeakReference，允许 GC 回收  
✅ 定期 scrub 清理过期引用  
✅ 防止内存泄漏

**新问题：**

❌ **删除中间快照的正确性**
- 如果有快照 [0, 5, 10, 15]
- 删除快照 10 后，查询 offset=12 会得到什么？
- 答案应该是快照 10 的状态，但快照 10 已经被删了！

---

### 版本 6: Snapshot 合并

**解决方案：** 删除快照时，将其 Delta 合并到前一个快照，这样删除中间快照不会影响查询结果。

```java
class Snapshot {
    private final long epoch;
    private Map<String, Delta> deltas = new HashMap<>();
    
    // 双向链表
    private Snapshot prev = this;
    private Snapshot next = this;
    
    public Snapshot prev() { return prev; }
    public Snapshot next() { return next; }
    
    public void appendNext(Snapshot newNext) {
        newNext.prev = this;
        newNext.next = this.next;
        this.next.prev = newNext;
        this.next = newNext;
    }
    
    // 从链表中移除
    public void erase() {
        this.next.prev = this.prev;
        this.prev.next = this.next;
        this.deltas = null;  // 清空 Delta
    }
    
    // 合并另一个快照的 Delta 到当前快照
    public void mergeFrom(Snapshot source) {
        for (Map.Entry<String, Delta> entry : source.deltas.entrySet()) {
            String owner = entry.getKey();
            Delta sourceDelta = entry.getValue();
            
            Delta myDelta = this.deltas.get(owner);
            if (myDelta == null) {
                // 我没有这个 Delta，直接拷贝
                this.deltas.put(owner, sourceDelta);
            } else {
                // 我有这个 Delta，合并
                myDelta.mergeFrom(sourceDelta);
            }
        }
    }
}

class SnapshotRegistry {
    private final Map<Long, Snapshot> snapshots = new HashMap<>();
    private final Snapshot head = new Snapshot(Long.MIN_VALUE);  // 哨兵节点
    
    public void createSnapshot(long epoch) {
        Snapshot last = head.prev();
        
        Snapshot snapshot = new Snapshot(epoch);
        last.appendNext(snapshot);  // 插入双向链表
        snapshots.put(epoch, snapshot);
    }
    
    public void deleteSnapshot(long epoch) {
        Snapshot snapshot = snapshots.get(epoch);
        if (snapshot == null) return;
        
        Snapshot prev = snapshot.prev();
        
        if (prev != head) {
            // 将当前快照的 Delta 合并到前一个快照
            prev.mergeFrom(snapshot);
        }
        
        // 从链表中移除
        snapshot.erase();
        snapshots.remove(epoch);
    }
}
```

**示例：**

```
初始状态:
  Snapshot(0): {key1=100}
  Snapshot(5): Delta {key1=200}     → 状态: {key1=200}
  Snapshot(10): Delta {key2=300}    → 状态: {key1=200, key2=300}
  
删除 Snapshot(5):
  1. 合并 Delta {key1=200} 到 Snapshot(0)
  2. Snapshot(0): Delta {key1=200}
  3. 删除 Snapshot(5)
  
结果:
  Snapshot(0): Delta {key1=200}  → 状态: {key1=200}
  Snapshot(10): Delta {key2=300} → 状态: {key1=200, key2=300}
  
现在查询 offset=7 时，得到 Snapshot(0) 的状态 {key1=200}（正确！）
```

**优势：**

✅ 删除中间快照时，Delta 被合并到前一个快照  
✅ 查询结果保持正确  
✅ 不会丢失历史信息

---

## 总结：SnapshotRegistry 的演进

| 版本 | 问题 | 解决方案 | 关键技术 |
|------|------|----------|---------|
| V1 | 手动同步多个 Map，容易出错 | 中心化管理 | Revertable 接口 |
| V2 | 只能记住一个 offset | 支持多个快照 | Snapshot 对象 + HashMap |
| V3 | 内存占用大（存储完整值） | 增量存储 | Delta 机制 |
| V4 | 内存泄漏（强引用） | 自动 GC | WeakReference + scrub |
| V5 | 删除快照导致查询错误 | 合并 Delta | Snapshot 双向链表 + mergeFrom |

### Kafka 的最终设计

**核心组件：**

1. **SnapshotRegistry**
   - 管理所有 Snapshot
   - Snapshot 双向链表（按 epoch 排序）
   - HashMap<Long, Snapshot> 快速查找
   - List<WeakReference<Revertable>> 自动清理

2. **Snapshot**
   - IdentityHashMap<Revertable, Delta> 存储 Delta
   - 双向链表（prev/next 指针）
   - mergeFrom() 合并 Delta
   - erase() 从链表移除

3. **Revertable 接口**
   - TimelineHashMap、TimelineHashSet、TimelineInteger... 都实现这个接口
   - executeRevert() 应用 Delta 回滚
   - reset() 恢复初始值

4. **Delta**
   - 只存储变更，不存储完整值
   - mergeFrom() 合并多个 Delta
   - 内存占用远小于完整快照

### 为什么需要 500+ 行代码？

**因为要同时满足：**

- ✅ **统一管理**：一个 registry 管理所有数据结构
- ✅ **多快照支持**：保留多个历史版本
- ✅ **内存优化**：Delta 增量存储
- ✅ **防内存泄漏**：WeakReference + scrub
- ✅ **删除正确性**：Snapshot 合并
- ✅ **高性能**：O(1) 回滚，O(log n) 快照查找

**每一层设计都解决了一个具体问题，缺一不可！**

### 运行完整演示

```bash
cd .claude/coordinator-learning
javac snapshotregistry-evolution.java
java SnapshotRegistryEvolution
```

输出将展示从 V1 到 V6 的完整演进过程，以及每个版本如何解决前一个版本的问题。

