/**
 * Kafka Coordinator 架构演进代码示例
 * 每个版本都是可运行的完整代码
 */

import java.util.*;
import java.util.concurrent.*;

// ============ 版本 1: 最简单的实现 ============

class SharePartitionKey {
    final String group;
    final String topic;
    final int partition;

    SharePartitionKey(String group, String topic, int partition) {
        this.group = group;
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SharePartitionKey)) return false;
        SharePartitionKey k = (SharePartitionKey) o;
        return group.equals(k.group) && topic.equals(k.topic) && partition == k.partition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, topic, partition);
    }
}

class ShareCoordinatorV1 {
    // 核心：就是一个 HashMap
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();

    public void writeOffset(String group, String topic, int partition, long offset) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        offsets.put(key, offset);
        System.out.println("[V1] Written: " + group + "/" + topic + "/" + partition + " = " + offset);
    }

    public Long readOffset(String group, String topic, int partition) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        Long offset = offsets.get(key);
        System.out.println("[V1] Read: " + group + "/" + topic + "/" + partition + " = " + offset);
        return offset;
    }

    public static void main(String[] args) {
        System.out.println("===== 版本 1: 最简单的实现 =====\n");

        ShareCoordinatorV1 coordinator = new ShareCoordinatorV1();

        // 写入
        coordinator.writeOffset("group1", "topic1", 0, 100L);

        // 读取
        Long offset = coordinator.readOffset("group1", "topic1", 0);

        System.out.println("\n问题：进程重启后数据丢失！");
        System.out.println("问题：没有高可用，单点故障！\n");
    }
}

// ============ 版本 3: 为什么需要事件队列？ ============

interface Event {
    void process();
}

class WriteEvent implements Event {
    final SharePartitionKey key;
    final long offset;
    final CompletableFuture<Void> future;

    WriteEvent(SharePartitionKey key, long offset, CompletableFuture<Void> future) {
        this.key = key;
        this.offset = offset;
        this.future = future;
    }

    @Override
    public void process() {
        // 在事件处理线程中执行
    }
}

class ReadEvent implements Event {
    final SharePartitionKey key;
    final CompletableFuture<Long> future;

    ReadEvent(SharePartitionKey key, CompletableFuture<Long> future) {
        this.key = key;
        this.future = future;
    }

    @Override
    public void process() {
        // 在事件处理线程中执行
    }
}

class ShareCoordinatorV3 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private final Thread eventThread;
    private volatile boolean running = true;

    public ShareCoordinatorV3() {
        // 启动事件处理线程（单线程，保证串行化）
        eventThread = new Thread(() -> {
            System.out.println("[V3] Event processing thread started");
            while (running) {
                try {
                    Event event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        processEvent(event);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        eventThread.start();
    }

    // 异步写入
    public CompletableFuture<Void> writeOffset(String group, String topic, int partition, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        WriteEvent event = new WriteEvent(key, offset, future);

        eventQueue.add(event);
        System.out.println("[V3] Enqueued write event: " + group + "/" + topic + "/" + partition + " = " + offset);

        return future;
    }

    // 异步读取
    public CompletableFuture<Long> readOffset(String group, String topic, int partition) {
        CompletableFuture<Long> future = new CompletableFuture<>();

        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        ReadEvent event = new ReadEvent(key, future);

        eventQueue.add(event);
        System.out.println("[V3] Enqueued read event: " + group + "/" + topic + "/" + partition);

        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;
            try {
                // 模拟写 log 的延迟
                Thread.sleep(10);

                // 更新内存
                offsets.put(e.key, e.offset);

                // 完成 future
                e.future.complete(null);

                System.out.println("[V3] Processed write: " + e.key.group + "/" + e.key.topic + "/" + e.key.partition + " = " + e.offset);
            } catch (Exception ex) {
                e.future.completeExceptionally(ex);
            }

        } else if (event instanceof ReadEvent) {
            ReadEvent e = (ReadEvent) event;
            Long offset = offsets.get(e.key);
            e.future.complete(offset);

            System.out.println("[V3] Processed read: " + e.key.group + "/" + e.key.topic + "/" + e.key.partition + " = " + offset);
        }
    }

    public void shutdown() {
        running = false;
        try {
            eventThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("===== 版本 3: 为什么需要事件队列？ =====\n");

        ShareCoordinatorV3 coordinator = new ShareCoordinatorV3();

        // 异步写入
        System.out.println("客户端：发起写请求");
        CompletableFuture<Void> writeFuture = coordinator.writeOffset("group1", "topic1", 0, 100L);
        System.out.println("客户端：立即返回 future（未阻塞）\n");

        // 等待完成
        writeFuture.thenRun(() -> System.out.println("客户端：写入完成！\n"));

        // 异步读取
        System.out.println("客户端：发起读请求");
        CompletableFuture<Long> readFuture = coordinator.readOffset("group1", "topic1", 0);
        System.out.println("客户端：立即返回 future（未阻塞）\n");

        readFuture.thenAccept(offset -> System.out.println("客户端：读到 offset = " + offset + "\n"));

        // 等待所有操作完成
        Thread.sleep(500);

        System.out.println("优势：");
        System.out.println("1. 线程安全：所有操作在单线程中串行执行");
        System.out.println("2. 异步非阻塞：调用线程不会被阻塞");
        System.out.println("3. 顺序保证：相同 key 的操作按照加入队列的顺序执行\n");

        coordinator.shutdown();
    }
}

// ============ 版本 4: 为什么需要 Batching？ ============

class FlushEvent implements Event {
    @Override
    public void process() {}
}

class ShareCoordinatorV4 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private final Thread eventThread;
    private volatile boolean running = true;

    // Batching 相关
    private final List<WriteEvent> currentBatch = new ArrayList<>();
    private final int maxBatchSize = 5;  // 简化：batch size = 5
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public ShareCoordinatorV4() {
        // 启动事件处理线程
        eventThread = new Thread(() -> {
            while (running) {
                try {
                    Event event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        processEvent(event);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        eventThread.start();

        // 启动 flush 定时器（每 100ms flush 一次）
        scheduler.scheduleAtFixedRate(() -> {
            eventQueue.add(new FlushEvent());
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> writeOffset(String group, String topic, int partition, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        WriteEvent event = new WriteEvent(key, offset, future);

        eventQueue.add(event);
        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 1. 立即更新内存
            offsets.put(e.key, e.offset);

            // 2. 加入 batch（稍后 flush）
            currentBatch.add(e);
            System.out.println("[V4] Added to batch: " + e.key.group + "/" + e.key.topic + "/" + e.key.partition + " = " + e.offset +
                    " (batch size: " + currentBatch.size() + ")");

            // 3. 检查是否需要 flush
            if (currentBatch.size() >= maxBatchSize) {
                System.out.println("[V4] Batch full, flushing...");
                flushBatch();
            }

        } else if (event instanceof FlushEvent) {
            if (!currentBatch.isEmpty()) {
                System.out.println("[V4] Flush timer triggered");
                flushBatch();
            }
        }
    }

    private void flushBatch() {
        if (currentBatch.isEmpty()) return;

        try {
            // 模拟批量写入 log 的延迟
            System.out.println("[V4] Flushing " + currentBatch.size() + " records...");
            Thread.sleep(20);  // 批量写入只需要 20ms

            // 完成所有 futures
            for (WriteEvent e : currentBatch) {
                e.future.complete(null);
            }

            System.out.println("[V4] Flush completed!\n");

        } catch (Exception e) {
            for (WriteEvent we : currentBatch) {
                we.future.completeExceptionally(e);
            }
        } finally {
            currentBatch.clear();
        }
    }

    public void shutdown() {
        running = false;
        scheduler.shutdown();
        try {
            eventThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("===== 版本 4: 为什么需要 Batching？ =====\n");

        ShareCoordinatorV4 coordinator = new ShareCoordinatorV4();

        long startTime = System.currentTimeMillis();

        // 连续写入 10 个
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CompletableFuture<Void> future = coordinator.writeOffset("group1", "topic1", i, 100L + i);
            futures.add(future);
        }

        System.out.println("\n等待所有写入完成...\n");

        // 等待所有完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long endTime = System.currentTimeMillis();

        System.out.println("总耗时: " + (endTime - startTime) + "ms");
        System.out.println("\n计算：");
        System.out.println("没有 batching: 10 个请求 × 10ms = 100ms");
        System.out.println("有 batching (batch size 5): 2 个 batch × 20ms = 40ms");
        System.out.println("性能提升：2.5 倍！\n");

        coordinator.shutdown();
    }
}

// ============ 版本 5: 为什么需要 Deferred Events？ ============

class HighWatermarkEvent implements Event {
    final long offset;

    HighWatermarkEvent(long offset) {
        this.offset = offset;
    }

    @Override
    public void process() {}
}

class ShareCoordinatorV5 {
    private final Map<SharePartitionKey, Long> offsets = new HashMap<>();
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private final Thread eventThread;
    private volatile boolean running = true;

    // Batching
    private final List<WriteEvent> currentBatch = new ArrayList<>();
    private final int maxBatchSize = 3;

    // Deferred Events
    private final TreeMap<Long, List<CompletableFuture<Void>>> deferredEvents = new TreeMap<>();
    private long lastWrittenOffset = 0;
    private long lastCommittedOffset = 0;

    public ShareCoordinatorV5() {
        // 启动事件处理线程
        eventThread = new Thread(() -> {
            while (running) {
                try {
                    Event event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (event != null) {
                        processEvent(event);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        eventThread.start();

        // 启动 flush 定时器
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            eventQueue.add(new FlushEvent());
        }, 100, 100, TimeUnit.MILLISECONDS);

        // 模拟 High Watermark 推进
        new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(200);

                    // 模拟 HW 推进
                    if (lastCommittedOffset < lastWrittenOffset) {
                        long newHW = lastCommittedOffset + 1;
                        System.out.println("[V5] High Watermark 推进: " + lastCommittedOffset + " -> " + newHW);
                        eventQueue.add(new HighWatermarkEvent(newHW));
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();
    }

    public CompletableFuture<Void> writeOffset(String group, String topic, int partition, long offset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);
        WriteEvent event = new WriteEvent(key, offset, future);

        eventQueue.add(event);
        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 更新内存
            offsets.put(e.key, e.offset);
            currentBatch.add(e);

            System.out.println("[V5] Added to batch: " + e.key.group + "/" + e.key.topic + "/" + e.key.partition +
                    " = " + e.offset + " (batch size: " + currentBatch.size() + ")");

            if (currentBatch.size() >= maxBatchSize) {
                flushBatch();
            }

        } else if (event instanceof FlushEvent) {
            if (!currentBatch.isEmpty()) {
                flushBatch();
            }

        } else if (event instanceof HighWatermarkEvent) {
            updateHighWatermark(((HighWatermarkEvent) event).offset);
        }
    }

    private void flushBatch() {
        if (currentBatch.isEmpty()) return;

        System.out.println("[V5] Flushing batch...");

        // 模拟写入 log
        lastWrittenOffset += currentBatch.size();
        long batchOffset = lastWrittenOffset;

        System.out.println("[V5] Batch written at offset " + batchOffset);

        // 收集所有 futures
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (WriteEvent e : currentBatch) {
            futures.add(e.future);
        }

        // 加入 deferred queue（等待 HW 推进）
        deferredEvents.put(batchOffset, futures);
        System.out.println("[V5] Batch added to deferred queue, waiting for HW...\n");

        currentBatch.clear();
    }

    private void updateHighWatermark(long newHW) {
        lastCommittedOffset = newHW;

        System.out.println("[V5] High Watermark updated to " + newHW);

        // Complete 所有 offset <= newHW 的 deferred events
        Iterator<Map.Entry<Long, List<CompletableFuture<Void>>>> it = deferredEvents.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Long, List<CompletableFuture<Void>>> entry = it.next();

            if (entry.getKey() <= newHW) {
                System.out.println("[V5] Completing batch at offset " + entry.getKey());

                for (CompletableFuture<Void> future : entry.getValue()) {
                    future.complete(null);
                }

                it.remove();
            } else {
                break;
            }
        }

        System.out.println();
    }

    public void shutdown() {
        running = false;
        try {
            eventThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("===== 版本 5: 为什么需要 Deferred Events？ =====\n");

        ShareCoordinatorV5 coordinator = new ShareCoordinatorV5();

        // 写入 3 个（触发 flush）
        System.out.println("写入 3 个 offset...\n");
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        CompletableFuture<Void> f1 = coordinator.writeOffset("group1", "topic1", 0, 100L);
        f1.thenRun(() -> System.out.println("客户端：offset 0 写入成功！"));
        futures.add(f1);

        CompletableFuture<Void> f2 = coordinator.writeOffset("group1", "topic1", 1, 101L);
        f2.thenRun(() -> System.out.println("客户端：offset 1 写入成功！"));
        futures.add(f2);

        CompletableFuture<Void> f3 = coordinator.writeOffset("group1", "topic1", 2, 102L);
        f3.thenRun(() -> System.out.println("客户端：offset 2 写入成功！"));
        futures.add(f3);

        // 等待所有完成
        System.out.println("\n客户端：等待所有写入完成（会等待 HW 推进）...\n");
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        System.out.println("\n关键点：");
        System.out.println("1. 写入本地 log 后，不立即返回成功");
        System.out.println("2. 而是加入 deferred queue，等待 HW 推进");
        System.out.println("3. 只有数据复制到多数派后，才返回成功");
        System.out.println("4. 保证客户端收到成功 = 数据不会丢失\n");

        coordinator.shutdown();
    }
}

// ============ 版本 6: 为什么需要 MVCC？ ============

// 简化版的 TimelineHashMap
class TimelineHashMap<K, V> {
    // key -> Timeline (List of versioned values)
    private final Map<K, List<VersionedValue<V>>> data = new HashMap<>();
    private long currentOffset = 0;

    static class VersionedValue<V> {
        final long offset;
        final V value;

        VersionedValue(long offset, V value) {
            this.offset = offset;
            this.value = value;
        }
    }

    public void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }

    public V get(K key) {
        List<VersionedValue<V>> timeline = data.get(key);
        if (timeline == null) return null;

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
        timeline.add(new VersionedValue<>(currentOffset, value));
    }

    public void printTimeline(K key) {
        List<VersionedValue<V>> timeline = data.get(key);
        if (timeline != null) {
            System.out.print("Timeline for " + key + ": ");
            for (VersionedValue<V> vv : timeline) {
                System.out.print("(" + vv.offset + ", " + vv.value + ") ");
            }
            System.out.println();
        }
    }
}

class ShareCoordinatorV6 {
    private final TimelineHashMap<SharePartitionKey, Long> offsets = new TimelineHashMap<>();
    private long lastWrittenOffset = 0;
    private long lastCommittedOffset = 0;

    public void writeOffset(String group, String topic, int partition, long offset) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);

        // 使用 lastWrittenOffset (未提交的数据)
        offsets.setCurrentOffset(lastWrittenOffset);
        offsets.put(key, offset);

        System.out.println("[V6] Write at offset " + lastWrittenOffset + ": " + group + "/" + topic + "/" + partition + " = " + offset);

        lastWrittenOffset++;
    }

    public Long readOffset(String group, String topic, int partition) {
        SharePartitionKey key = new SharePartitionKey(group, topic, partition);

        // 使用 lastCommittedOffset (只读已提交的数据)
        offsets.setCurrentOffset(lastCommittedOffset);
        Long offset = offsets.get(key);

        System.out.println("[V6] Read at committed offset " + lastCommittedOffset + ": " + group + "/" + topic + "/" + partition + " = " + offset);

        return offset;
    }

    public void updateHighWatermark(long newHW) {
        lastCommittedOffset = newHW;
        System.out.println("[V6] High Watermark updated to " + newHW + "\n");
    }

    public static void main(String[] args) {
        System.out.println("===== 版本 6: 为什么需要 MVCC？ =====\n");

        ShareCoordinatorV6 coordinator = new ShareCoordinatorV6();
        SharePartitionKey key = new SharePartitionKey("group1", "topic1", 0);

        // 写入 offset 100
        System.out.println("操作 1: 写入 offset 100");
        coordinator.writeOffset("group1", "topic1", 0, 100L);
        coordinator.offsets.printTimeline(key);
        System.out.println();

        // 写入 offset 200
        System.out.println("操作 2: 写入 offset 200");
        coordinator.writeOffset("group1", "topic1", 0, 200L);
        coordinator.offsets.printTimeline(key);
        System.out.println();

        // 读取（HW 还没推进）
        System.out.println("操作 3: 读取（HW = 0）");
        Long val = coordinator.readOffset("group1", "topic1", 0);
        System.out.println("读到: " + val + " (null，因为没有已提交的数据)");
        System.out.println();

        // HW 推进到 1
        System.out.println("操作 4: HW 推进到 1");
        coordinator.updateHighWatermark(1);

        // 再次读取
        System.out.println("操作 5: 读取（HW = 1）");
        val = coordinator.readOffset("group1", "topic1", 0);
        System.out.println("读到: " + val);
        System.out.println();

        // HW 推进到 2
        System.out.println("操作 6: HW 推进到 2");
        coordinator.updateHighWatermark(2);

        // 再次读取
        System.out.println("操作 7: 读取（HW = 2）");
        val = coordinator.readOffset("group1", "topic1", 0);
        System.out.println("读到: " + val);
        System.out.println();

        System.out.println("关键点：");
        System.out.println("1. 写操作使用 lastWrittenOffset，立即更新内存");
        System.out.println("2. 读操作使用 lastCommittedOffset，只读已提交的数据");
        System.out.println("3. TimelineHashMap 支持多版本，根据 offset 返回不同的值");
        System.out.println("4. 实现了读写隔离，避免 Dirty Read\n");
    }
}
