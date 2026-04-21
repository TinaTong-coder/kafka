# Kafka Coordinator 架构：从零开始的演进式教程

## 目录
- [版本 1: 最简单的实现](#版本-1-最简单的实现)
- [版本 2: 为什么需要持久化？](#版本-2-为什么需要持久化)
- [版本 3: 为什么需要事件队列？](#版本-3-为什么需要事件队列)
- [版本 4: 为什么需要 Batching？](#版本-4-为什么需要-batching)
- [版本 5: 为什么需要 Deferred Events？](#版本-5-为什么需要-deferred-events)
- [版本 6: 为什么需要 MVCC？](#版本-6-为什么需要-mvcc)
- [版本 7: 完整的 CoordinatorRuntime](#版本-7-完整的-coordinatorruntime)

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
  │  <─success────────────────────────────────────────────────────────── │
```

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
