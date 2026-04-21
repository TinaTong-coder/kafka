/**
 * 演示：Batch 失败的两种处理方式
 * 1. 部分成功 (看起来更好，但有问题)
 * 2. 全部失败 (Kafka 的选择)
 */

import java.util.*;
import java.util.concurrent.*;

// ============ 方式 1: 部分成功 (错误) ============

class PartialSuccessCoordinator {
    private final Map<String, Integer> state = new HashMap<>();
    private final List<String> log = new ArrayList<>();
    private final Random random = new Random();

    static class WriteEvent {
        final String key;
        final int value;
        final CompletableFuture<Void> future;

        WriteEvent(String key, int value) {
            this.key = key;
            this.value = value;
            this.future = new CompletableFuture<>();
        }
    }

    public List<CompletableFuture<Void>> writeBatch(List<WriteEvent> batch) {
        System.out.println("\n[部分成功] 开始处理 batch，包含 " + batch.size() + " 个操作");

        // 1. Replay 到内存
        for (WriteEvent event : batch) {
            state.put(event.key, event.value);
            System.out.println("  [内存] " + event.key + " = " + event.value);
        }

        // 2. 逐个写入 log（模拟部分失败）
        for (int i = 0; i < batch.size(); i++) {
            WriteEvent event = batch.get(i);

            try {
                // 模拟：第 2 个操作失败
                if (i == 1) {
                    throw new RuntimeException("磁盘写入失败");
                }

                // 写入 log
                log.add(event.key + "=" + event.value);
                System.out.println("  [Log] 写入成功: " + event.key + "=" + event.value);

                // Complete future
                event.future.complete(null);

            } catch (Exception e) {
                System.out.println("  [Log] 写入失败: " + event.key + " (" + e.getMessage() + ")");

                // Fail 这个 future
                event.future.completeExceptionally(e);
            }
        }

        return batch.stream().map(e -> e.future).toList();
    }

    public void printState() {
        System.out.println("\n当前状态:");
        System.out.println("  内存: " + state);
        System.out.println("  Log:  " + log);
    }

    public void restart() {
        System.out.println("\n[模拟进程重启]");

        // 清空内存
        state.clear();

        // 从 log 恢复
        System.out.println("  从 log 恢复状态...");
        for (String record : log) {
            String[] parts = record.split("=");
            state.put(parts[0], Integer.parseInt(parts[1]));
        }

        System.out.println("  恢复后的内存: " + state);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("===== 演示 1: 部分成功（错误方式） =====");

        PartialSuccessCoordinator coordinator = new PartialSuccessCoordinator();

        // 创建 batch
        List<WriteEvent> batch = Arrays.asList(
            new WriteEvent("balance", 100),
            new WriteEvent("balance", 50),   // 这个会失败
            new WriteEvent("balance", 20)
        );

        // 处理 batch
        List<CompletableFuture<Void>> futures = coordinator.writeBatch(batch);

        // 检查结果
        System.out.println("\n客户端收到的结果:");
        for (int i = 0; i < futures.size(); i++) {
            try {
                futures.get(i).get();
                System.out.println("  操作 " + i + ": 成功 ✅");
            } catch (Exception e) {
                System.out.println("  操作 " + i + ": 失败 ❌ (" + e.getCause().getMessage() + ")");
            }
        }

        // 打印状态
        coordinator.printState();

        // 问题：内存和 log 不一致！
        System.out.println("\n⚠️  问题：");
        System.out.println("  - 内存有 balance=20");
        System.out.println("  - 但 log 里缺少 balance=50 这一步");
        System.out.println("  - 如果重启...");

        // 重启
        coordinator.restart();

        System.out.println("\n⚠️  结果：");
        System.out.println("  - 重启后 balance=20（从 log 恢复）");
        System.out.println("  - 但逻辑上应该是 20（100 -> 50 -> 20）");
        System.out.println("  - 实际是 20（100 -> 20），缺了中间步骤！");
        System.out.println("  - 状态机损坏！");
    }
}

// ============ 方式 2: 全部失败（正确）============

class AllOrNothingCoordinator {
    /**
     * TimelineHashMap: 使用 TreeMap 实现的多版本并发控制 (MVCC)
     *
     * 核心原理：
     * 1. 每个 key 对应一个 TreeMap<Long, V>，存储 (offset → value) 的映射
     * 2. get() 使用 floorEntry() 返回 <= currentOffset 的最大 offset 对应的值
     * 3. revertTo() 通过修改 currentOffset 来"回滚"到历史版本
     *
     * 这是 Kafka TimelineHashMap 的简化版本
     */
    static class TimelineHashMap<K, V> {
        // 每个 key 对应一个 TreeMap，存储 offset → value 的映射
        private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
        private long currentOffset = 0;

        void setCurrentOffset(long offset) {
            this.currentOffset = offset;
        }

        void put(K key, V value) {
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(currentOffset, value);
        }

        /**
         * 获取 key 在 currentOffset 时刻的值
         *
         * 原理：使用 TreeMap.floorEntry(offset)
         * - 返回 <= offset 的最大 key 对应的 entry
         * - O(log n) 时间复杂度
         *
         * 示例：
         *   timeline: {0→100, 5→200, 10→300}
         *   currentOffset=7 → 返回 200 (offset=5 的值)
         *   currentOffset=10 → 返回 300 (offset=10 的值)
         *   currentOffset=15 → 返回 300 (offset=10 的值)
         */
        V get(K key) {
            TreeMap<Long, V> timeline = data.get(key);
            if (timeline == null) return null;

            // floorEntry: 返回 <= currentOffset 的最大 offset 的 entry
            Map.Entry<Long, V> entry = timeline.floorEntry(currentOffset);
            return entry != null ? entry.getValue() : null;
        }

        /**
         * 回滚到指定 offset
         *
         * 注意：这里只修改 currentOffset，不删除数据
         * 因为未来可能需要 "前进" 到更大的 offset
         *
         * 如果要真正删除数据（释放内存），需要调用 deleteUpTo(offset)
         */
        void revertTo(long offset) {
            setCurrentOffset(offset);
        }

        /**
         * 删除所有 > offset 的版本（释放内存）
         *
         * 在 Kafka 中，当 HW 推进后，老版本不再需要，会被删除
         */
        void deleteUpTo(long offset) {
            for (TreeMap<Long, V> timeline : data.values()) {
                // tailMap(offset, inclusive): 返回 >= offset 的子 map
                // 这里用 offset + 1 表示 > offset
                timeline.tailMap(offset + 1, true).clear();
            }
        }

        void printTimeline(K key) {
            System.out.print("  Timeline [" + key + "]: ");
            TreeMap<Long, V> timeline = data.get(key);
            if (timeline != null) {
                for (Map.Entry<Long, V> entry : timeline.entrySet()) {
                    System.out.print("(" + entry.getKey() + "→" + entry.getValue() + ") ");
                }
            }
            System.out.print("| currentOffset=" + currentOffset);
            System.out.println();
        }
    }

    private final TimelineHashMap<String, Integer> state = new TimelineHashMap<>();
    private final List<String> log = new ArrayList<>();
    private long currentOffset = 0;

    static class WriteEvent {
        final String key;
        final int value;
        final CompletableFuture<Void> future;

        WriteEvent(String key, int value) {
            this.key = key;
            this.value = value;
            this.future = new CompletableFuture<>();
        }
    }

    public List<CompletableFuture<Void>> writeBatch(List<WriteEvent> batch) {
        System.out.println("\n[全部失败] 开始处理 batch，包含 " + batch.size() + " 个操作");

        // 保存 baseOffset（batch 开始前的 offset）
        long baseOffset = currentOffset;
        System.out.println("  [快照] baseOffset = " + baseOffset);

        try {
            // 1. Replay 到内存
            for (WriteEvent event : batch) {
                state.setCurrentOffset(currentOffset);
                state.put(event.key, event.value);
                System.out.println("  [内存] offset=" + currentOffset + ", " + event.key + " = " + event.value);
                currentOffset++;
            }

            // 打印 timeline
            state.printTimeline("balance");

            // 2. 批量写入 log（模拟失败）
            System.out.println("\n  [Log] 开始批量写入...");
            for (int i = 0; i < batch.size(); i++) {
                WriteEvent event = batch.get(i);

                // 模拟：第 2 个操作失败
                if (i == 1) {
                    throw new RuntimeException("磁盘写入失败");
                }

                log.add(event.key + "=" + event.value);
                System.out.println("  [Log] 写入: " + event.key + "=" + event.value);
            }

            // 所有写入成功
            System.out.println("  [Log] 批量写入成功");

            // Complete 所有 futures
            for (WriteEvent event : batch) {
                event.future.complete(null);
            }

        } catch (Exception e) {
            System.out.println("  [Log] 批量写入失败: " + e.getMessage());

            // 回滚状态
            System.out.println("\n  [回滚] 恢复到 offset " + baseOffset);
            state.revertTo(baseOffset);
            currentOffset = baseOffset;

            // 打印回滚后的 timeline
            state.printTimeline("balance");

            // Fail 所有 futures
            for (WriteEvent event : batch) {
                event.future.completeExceptionally(e);
            }
        }

        return batch.stream().map(e -> e.future).toList();
    }

    public void printState() {
        System.out.println("\n当前状态:");
        System.out.print("  内存: balance=" + state.get("balance"));
        System.out.println(" (offset=" + currentOffset + ")");
        System.out.println("  Log:  " + log);
    }

    public void restart() {
        System.out.println("\n[模拟进程重启]");

        // 清空内存
        state.setCurrentOffset(0);
        currentOffset = 0;

        // 从 log 恢复
        System.out.println("  从 log 恢复状态...");
        for (String record : log) {
            String[] parts = record.split("=");
            state.put(parts[0], Integer.parseInt(parts[1]));
            currentOffset++;
        }

        System.out.println("  恢复后的内存: balance=" + state.get("balance"));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("\n\n===== 演示 2: 全部失败（正确方式） =====");

        AllOrNothingCoordinator coordinator = new AllOrNothingCoordinator();

        // 创建 batch
        List<WriteEvent> batch = Arrays.asList(
            new WriteEvent("balance", 100),
            new WriteEvent("balance", 50),   // 这个会失败
            new WriteEvent("balance", 20)
        );

        // 处理 batch
        List<CompletableFuture<Void>> futures = coordinator.writeBatch(batch);

        // 检查结果
        System.out.println("\n客户端收到的结果:");
        for (int i = 0; i < futures.size(); i++) {
            try {
                futures.get(i).get();
                System.out.println("  操作 " + i + ": 成功 ✅");
            } catch (Exception e) {
                System.out.println("  操作 " + i + ": 失败 ❌ (" + e.getCause().getMessage() + ")");
            }
        }

        // 打印状态
        coordinator.printState();

        System.out.println("\n✅ 优势：");
        System.out.println("  - 内存和 log 一致（都是空的）");
        System.out.println("  - 状态已回滚到 batch 前");
        System.out.println("  - 客户端可以安全重试");

        System.out.println("\n客户端重试整个 batch...");

        // 重试（这次不会失败）
        List<WriteEvent> retryBatch = Arrays.asList(
            new WriteEvent("balance", 100),
            new WriteEvent("balance", 50),
            new WriteEvent("balance", 20)
        );

        // 修改代码让重试成功
        coordinator.writeBatchSuccess(retryBatch);

        coordinator.printState();

        System.out.println("\n✅ 结果：");
        System.out.println("  - 重试后所有操作都成功");
        System.out.println("  - 状态正确：balance=20");
        System.out.println("  - 状态机完整！");
    }

    // 成功的版本（用于重试）
    public List<CompletableFuture<Void>> writeBatchSuccess(List<WriteEvent> batch) {
        System.out.println("\n[重试] 开始处理 batch，包含 " + batch.size() + " 个操作");

        long baseOffset = currentOffset;

        try {
            // Replay
            for (WriteEvent event : batch) {
                state.setCurrentOffset(currentOffset);
                state.put(event.key, event.value);
                System.out.println("  [内存] offset=" + currentOffset + ", " + event.key + " = " + event.value);
                currentOffset++;
            }

            // 写入 log（这次成功）
            System.out.println("  [Log] 批量写入...");
            for (WriteEvent event : batch) {
                log.add(event.key + "=" + event.value);
                System.out.println("  [Log] 写入: " + event.key + "=" + event.value);
            }

            System.out.println("  [Log] 批量写入成功 ✅");

            // Complete 所有 futures
            for (WriteEvent event : batch) {
                event.future.complete(null);
            }

        } catch (Exception e) {
            state.revertTo(baseOffset);
            currentOffset = baseOffset;

            for (WriteEvent event : batch) {
                event.future.completeExceptionally(e);
            }
        }

        return batch.stream().map(e -> e.future).toList();
    }
}

// ============ 对比演示 ============

class ComparisonDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Batch 失败处理：部分成功 vs 全部失败                    ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");

        // 运行方式 1
        PartialSuccessCoordinator.main(args);

        // 运行方式 2
        AllOrNothingCoordinator.main(args);

        // 总结
        System.out.println("\n\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  总结                                                     ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");

        System.out.println("\n部分成功的问题：");
        System.out.println("  ✗ 内存和 log 不一致");
        System.out.println("  ✗ 重启后状态错误");
        System.out.println("  ✗ 状态机可能损坏");
        System.out.println("  ✗ 难以保证原子性");

        System.out.println("\n全部失败的优势：");
        System.out.println("  ✓ 内存和 log 始终一致");
        System.out.println("  ✓ 重启后状态正确");
        System.out.println("  ✓ 状态机完整");
        System.out.println("  ✓ 支持原子性操作");
        System.out.println("  ✓ 客户端可以安全重试");

        System.out.println("\nKafka 的选择：");
        System.out.println("  \"一个失败，全部失败\" 是正确的设计");
        System.out.println("  因为状态机的一致性比部分成功更重要");

        System.out.println("\n类比：");
        System.out.println("  银行转账：扣款成功但加款失败 → 必须回滚扣款");
        System.out.println("  数据库事务：一条 SQL 失败 → 整个事务回滚");
        System.out.println("  Coordinator batch：一个 record 失败 → 整个 batch 回滚");
    }
}
