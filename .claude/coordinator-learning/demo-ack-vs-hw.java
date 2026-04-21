/**
 * 演示：为什么 Producer ACK 不够，需要等待 High Watermark
 */

import java.util.*;
import java.util.concurrent.*;

// ============ 模拟：只用 Producer ACK (错误方式) ============

class WrongCoordinator {
    private final Map<String, Long> offsets = new HashMap<>();
    private final SimulatedProducer producer = new SimulatedProducer();
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private volatile boolean running = true;

    // 模拟的 Producer
    static class SimulatedProducer {
        long nextOffset = 0;

        // 模拟写入，等待 ISR 确认
        CompletableFuture<Long> send(String key, long value) {
            long offset = nextOffset++;
            return CompletableFuture.supplyAsync(() -> {
                try {
                    // 模拟写入延迟：10ms
                    Thread.sleep(10);
                    System.out.println("  [Producer] ACK 返回，offset=" + offset);
                    return offset;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    interface Event {}

    static class WriteEvent implements Event {
        final String key;
        final long value;
        final CompletableFuture<Void> future;

        WriteEvent(String key, long value, CompletableFuture<Void> future) {
            this.key = key;
            this.value = value;
            this.future = future;
        }
    }

    static class ReadEvent implements Event {
        final String key;
        final CompletableFuture<Long> future;

        ReadEvent(String key, CompletableFuture<Long> future) {
            this.key = key;
            this.future = future;
        }
    }

    public WrongCoordinator() {
        new Thread(() -> {
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
        }).start();
    }

    public CompletableFuture<Void> writeOffset(String key, long value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.add(new WriteEvent(key, value, future));
        System.out.println("[Wrong] 客户端发起写入: " + key + " = " + value);
        return future;
    }

    public CompletableFuture<Long> readOffset(String key) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        eventQueue.add(new ReadEvent(key, future));
        System.out.println("[Wrong] 客户端发起读取: " + key);
        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 1. 更新内存
            offsets.put(e.key, e.value);
            System.out.println("  [Wrong] 更新内存: " + e.key + " = " + e.value);

            // 2. 写入 log，等待 ACK
            producer.send(e.key, e.value).thenAccept(offset -> {
                // 3. ACK 返回后，立即 complete future ← 这是错误的！
                e.future.complete(null);
                System.out.println("  [Wrong] 立即返回成功给客户端");
            });

        } else if (event instanceof ReadEvent) {
            ReadEvent e = (ReadEvent) event;
            Long value = offsets.get(e.key);
            e.future.complete(value);
            System.out.println("  [Wrong] 读取到: " + e.key + " = " + value);
        }
    }

    public void shutdown() {
        running = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("===== 演示 1: 只用 Producer ACK (错误方式) =====\n");

        WrongCoordinator coordinator = new WrongCoordinator();

        // 写入
        CompletableFuture<Void> writeFuture = coordinator.writeOffset("key1", 100L);

        // 等待写入"成功"
        writeFuture.get();
        System.out.println("[Wrong] 客户端收到写入成功\n");

        // 立即读取
        CompletableFuture<Long> readFuture = coordinator.readOffset("key1");
        Long value = readFuture.get();
        System.out.println("[Wrong] 客户端读到: " + value);

        System.out.println("\n问题：");
        System.out.println("1. ACK 返回时，HW 可能还没推进");
        System.out.println("2. 客户端以为写入成功，但数据可能还未 committed");
        System.out.println("3. 如果此时 leader crash，数据可能丢失");
        System.out.println("4. 但客户端已经读到了这个数据，导致不一致！\n");

        coordinator.shutdown();
    }
}

// ============ 模拟：使用 Deferred Event Queue (正确方式) ============

class CorrectCoordinator {
    private final Map<String, Long> offsets = new HashMap<>();
    private final SimulatedBroker broker = new SimulatedBroker();
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
    private final DeferredEventQueue deferredEventQueue = new DeferredEventQueue();
    private volatile boolean running = true;

    // 模拟的 Broker (包含 Producer + HW 推进)
    static class SimulatedBroker {
        long nextOffset = 0;
        long highWatermark = -1;
        Timer timer = new Timer();

        // 写入数据
        CompletableFuture<Long> append(String key, long value) {
            long offset = nextOffset++;
            return CompletableFuture.supplyAsync(() -> {
                try {
                    // 模拟写入延迟
                    Thread.sleep(10);
                    System.out.println("  [Broker] 数据已写入，offset=" + offset + "，等待复制...");

                    // 模拟 HW 推进延迟 (50ms 后推进)
                    timer.schedule(new TimerTask() {
                        @Override
                        public void run() {
                            highWatermark = offset;
                            System.out.println("  [Broker] HW 推进到 " + offset);
                            notifyHighWatermarkUpdate(offset);
                        }
                    }, 50);

                    return offset;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // HW 监听器
        private final List<HighWatermarkListener> listeners = new ArrayList<>();

        void registerListener(HighWatermarkListener listener) {
            listeners.add(listener);
        }

        void notifyHighWatermarkUpdate(long newHW) {
            for (HighWatermarkListener listener : listeners) {
                listener.onHighWatermarkUpdated(newHW);
            }
        }
    }

    interface HighWatermarkListener {
        void onHighWatermarkUpdated(long offset);
    }

    // Deferred Event Queue
    static class DeferredEventQueue {
        private final TreeMap<Long, List<CompletableFuture<Void>>> pending = new TreeMap<>();

        void add(long offset, CompletableFuture<Void> future) {
            pending.computeIfAbsent(offset, k -> new ArrayList<>()).add(future);
            System.out.println("  [Deferred] 加入队列，等待 HW 到达 " + offset);
        }

        void completeUpTo(long highWatermark) {
            Iterator<Map.Entry<Long, List<CompletableFuture<Void>>>> it = pending.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<Long, List<CompletableFuture<Void>>> entry = it.next();

                if (entry.getKey() <= highWatermark) {
                    System.out.println("  [Deferred] HW 已到达 " + entry.getKey() + "，完成 futures");
                    for (CompletableFuture<Void> future : entry.getValue()) {
                        future.complete(null);
                    }
                    it.remove();
                } else {
                    break;
                }
            }
        }
    }

    interface Event {}

    static class WriteEvent implements Event {
        final String key;
        final long value;
        final CompletableFuture<Void> future;

        WriteEvent(String key, long value, CompletableFuture<Void> future) {
            this.key = key;
            this.value = value;
            this.future = future;
        }
    }

    static class ReadEvent implements Event {
        final String key;
        final CompletableFuture<Long> future;

        ReadEvent(String key, CompletableFuture<Long> future) {
            this.key = key;
            this.future = future;
        }
    }

    static class HighWatermarkEvent implements Event {
        final long offset;

        HighWatermarkEvent(long offset) {
            this.offset = offset;
        }
    }

    public CorrectCoordinator() {
        // 启动事件处理线程
        new Thread(() -> {
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
        }).start();

        // 注册 HW 监听器
        broker.registerListener(offset -> {
            eventQueue.add(new HighWatermarkEvent(offset));
        });
    }

    public CompletableFuture<Void> writeOffset(String key, long value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.add(new WriteEvent(key, value, future));
        System.out.println("[Correct] 客户端发起写入: " + key + " = " + value);
        return future;
    }

    public CompletableFuture<Long> readOffset(String key) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        eventQueue.add(new ReadEvent(key, future));
        System.out.println("[Correct] 客户端发起读取: " + key);
        return future;
    }

    private void processEvent(Event event) {
        if (event instanceof WriteEvent) {
            WriteEvent e = (WriteEvent) event;

            // 1. 更新内存
            offsets.put(e.key, e.value);
            System.out.println("  [Correct] 更新内存: " + e.key + " = " + e.value);

            // 2. 写入 broker
            broker.append(e.key, e.value).thenAccept(offset -> {
                // 3. 不立即 complete！而是加入 deferred queue
                eventQueue.add(new Event() {
                    @Override
                    public String toString() {
                        return "AddToDeferred";
                    }
                });
            });

            // 实际应该在上面的 thenAccept 中处理，这里简化
            try {
                Thread.sleep(20);
                deferredEventQueue.add(broker.nextOffset - 1, e.future);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

        } else if (event instanceof ReadEvent) {
            ReadEvent e = (ReadEvent) event;
            // 只读取 HW 之前的数据
            Long value = offsets.get(e.key);

            // 简化：这里应该检查 offset 是否 <= HW
            if (value != null && broker.nextOffset - 1 > broker.highWatermark) {
                System.out.println("  [Correct] 数据还未 committed，返回 null");
                e.future.complete(null);
            } else {
                System.out.println("  [Correct] 读取到已 committed 的数据: " + e.key + " = " + value);
                e.future.complete(value);
            }

        } else if (event instanceof HighWatermarkEvent) {
            HighWatermarkEvent e = (HighWatermarkEvent) event;
            System.out.println("  [Correct] 处理 HW 更新事件: " + e.offset);
            deferredEventQueue.completeUpTo(e.offset);
        }
    }

    public void shutdown() {
        running = false;
        broker.timer.cancel();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("\n===== 演示 2: 使用 Deferred Event Queue (正确方式) =====\n");

        CorrectCoordinator coordinator = new CorrectCoordinator();

        // 写入
        CompletableFuture<Void> writeFuture = coordinator.writeOffset("key1", 100L);

        System.out.println("[Correct] 客户端等待写入成功（会等待 HW 推进）...\n");

        // 在写入完成前尝试读取
        Thread.sleep(30);  // 等待 ACK 返回，但 HW 还没推进
        System.out.println("\n[Correct] 此时 ACK 已返回，但 HW 还没推进，尝试读取...");
        CompletableFuture<Long> readFuture1 = coordinator.readOffset("key1");
        Long value1 = readFuture1.get();
        System.out.println("[Correct] 读到: " + value1 + " (null，因为 HW 还没推进)\n");

        // 等待写入真正完成（HW 推进）
        writeFuture.get();
        System.out.println("\n[Correct] 客户端收到写入成功（HW 已推进）\n");

        // 再次读取
        System.out.println("[Correct] HW 已推进，再次尝试读取...");
        CompletableFuture<Long> readFuture2 = coordinator.readOffset("key1");
        Long value2 = readFuture2.get();
        System.out.println("[Correct] 读到: " + value2);

        System.out.println("\n优势：");
        System.out.println("1. 只在 HW 推进后才返回成功");
        System.out.println("2. 读操作只看到已 committed 的数据");
        System.out.println("3. 即使 leader crash，数据也不会丢失");
        System.out.println("4. 保证了强一致性！\n");

        coordinator.shutdown();
    }
}

// ============ 演示：两者对比 ============

class ComparisonDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Producer ACK vs High Watermark: 为什么需要 Deferred Events? ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");

        // 运行错误方式
        WrongCoordinator.main(args);

        Thread.sleep(1000);

        // 运行正确方式
        CorrectCoordinator.main(args);

        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║  核心区别总结                                                  ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");

        System.out.println("\nProducer ACK 的语义：");
        System.out.println("  ✓ 数据已写入所有 ISR");
        System.out.println("  ✓ 数据不会丢失");
        System.out.println("  ✗ 但 HW 可能还没推进");
        System.out.println("  ✗ 读操作可能看不到");

        System.out.println("\nHigh Watermark 的语义：");
        System.out.println("  ✓ Leader 知道所有 ISR 的进度");
        System.out.println("  ✓ HW = 所有 ISR 都有的 offset");
        System.out.println("  ✓ 读操作可以安全读取");
        System.out.println("  ✓ 即使 leader crash，数据也在");

        System.out.println("\nDeferred Event Queue 的作用：");
        System.out.println("  ✓ 弥补 ACK 和 HW 之间的时间差");
        System.out.println("  ✓ 确保只在数据真正 committed 后返回成功");
        System.out.println("  ✓ 保证读操作的一致性");
        System.out.println("  ✓ 实现 Coordinator 的强一致性语义\n");
    }
}
