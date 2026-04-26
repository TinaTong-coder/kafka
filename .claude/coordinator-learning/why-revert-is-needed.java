/**
 * 为什么 Kafka Coordinator 需要 revert to version (offset)?
 *
 * 通过真实场景展示回滚功能的必要性
 */

import java.util.*;
import java.util.concurrent.*;

// ============================================================
// 场景 1: Batch 写入失败后的回滚
// ============================================================

class Scenario1_BatchFailure {
    /**
     * 问题：Batch 写入时，部分成功、部分失败怎么办？
     *
     * 回顾 V4 的设计：
     * 1. Replay records 到内存（更新 lastWrittenOffset）
     * 2. 批量写入 Kafka log
     * 3. 如果写入失败？内存状态已经被修改了！
     */

    static class CoordinatorContext {
        private final Map<String, Integer> state = new HashMap<>();
        private long lastWrittenOffset = 0;
        private long lastCommittedOffset = 0;

        public void processBatch(List<Record> batch) {
            long baseOffset = lastWrittenOffset;

            try {
                // 1. Replay 所有 records 到内存
                for (Record record : batch) {
                    state.put(record.key, record.value);
                    lastWrittenOffset++;
                    System.out.println("  [内存] offset=" + (lastWrittenOffset - 1) +
                                     ", " + record.key + "=" + record.value);
                }

                System.out.println("  内存状态: " + state);

                // 2. 批量写入 log
                System.out.println("  [Log] 批量写入...");
                for (int i = 0; i < batch.size(); i++) {
                    if (i == 1) {
                        // 模拟第 2 条写入失败
                        throw new RuntimeException("磁盘写入失败");
                    }
                }

                System.out.println("  [Log] 写入成功");

            } catch (Exception e) {
                System.out.println("  [Log] 写入失败: " + e.getMessage());

                // ❌ 问题：内存已经被修改了！
                System.out.println("  ❌ 问题：内存状态 = " + state +
                                 ", 但 log 里没有这些数据！");
                System.out.println("  ❌ 重启后会丢失这些状态！");

                // ✅ 解决方案：回滚到 baseOffset
                System.out.println("  ✅ 回滚到 offset " + baseOffset);
                lastWrittenOffset = baseOffset;
                // 但是！state 已经被修改了，怎么恢复？
                // 这就是为什么需要 MVCC + SnapshotRegistry！
            }
        }
    }

    static class Record {
        String key;
        int value;

        Record(String key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    public static void main(String[] args) {
        System.out.println("=== 场景 1: Batch 写入失败 ===\n");

        CoordinatorContext context = new CoordinatorContext();

        List<Record> batch = Arrays.asList(
            new Record("balance", 100),
            new Record("balance", 200),  // 这条会失败
            new Record("balance", 300)
        );

        context.processBatch(batch);

        System.out.println("\n💡 为什么需要回滚？");
        System.out.println("  1. Batch 中一条失败，所有都要回滚");
        System.out.println("  2. 内存状态必须与 log 保持一致");
        System.out.println("  3. 普通 HashMap 无法回滚");
        System.out.println("  4. 需要 TimelineHashMap + SnapshotRegistry");
    }
}

// ============================================================
// 场景 2: Leader 变更时的状态同步
// ============================================================

class Scenario2_LeaderChange {
    /**
     * 问题：Leader 变更时，新 Leader 需要从 log 恢复状态
     *
     * 时间线：
     * 1. Leader A 处理到 offset 100
     * 2. Leader A 的内存有 offset 101-105（已 replay，但未提交）
     * 3. Leader A crash
     * 4. Leader B 接管，从 log 恢复
     * 5. Log 只有 offset 0-100（HW=100）
     * 6. Leader B 需要清除 offset 101-105 的内存状态
     */

    static class Partition {
        String id;
        Map<String, Integer> memory = new HashMap<>();
        long lastWrittenOffset = 0;
        long highWatermark = 0;

        Partition(String id) {
            this.id = id;
        }

        void processRecords(long startOffset, Map<String, Integer> records) {
            System.out.println("\n[" + id + "] 处理 records:");
            for (Map.Entry<String, Integer> entry : records.entrySet()) {
                memory.put(entry.getKey(), entry.getValue());
                lastWrittenOffset++;
                System.out.println("  offset=" + (lastWrittenOffset - 1) + ", " +
                                 entry.getKey() + "=" + entry.getValue());
            }
            System.out.println("  内存: " + memory + ", lastWrittenOffset=" + lastWrittenOffset);
        }

        void updateHW(long newHW) {
            System.out.println("\n[" + id + "] HW 推进到 " + newHW);
            highWatermark = newHW;
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== 场景 2: Leader 变更 ===\n");

        // Leader A 处理 records
        Partition leaderA = new Partition("Leader A");

        Map<String, Integer> batch1 = new HashMap<>();
        batch1.put("key1", 100);
        batch1.put("key2", 200);
        leaderA.processRecords(0, batch1);
        leaderA.updateHW(1);  // HW 推进到 1

        Map<String, Integer> batch2 = new HashMap<>();
        batch2.put("key1", 300);
        leaderA.processRecords(2, batch2);
        // 注意：这批还没有推进 HW！

        System.out.println("\n[Leader A] lastWrittenOffset=" + leaderA.lastWrittenOffset +
                         ", HW=" + leaderA.highWatermark);

        System.out.println("\n💥 Leader A crash!");

        // Leader B 接管
        System.out.println("\n[Leader B] 从 log 恢复...");
        Partition leaderB = new Partition("Leader B");

        System.out.println("  Log 只有 offset 0-" + leaderA.highWatermark);
        System.out.println("  ❌ 问题：Leader A 的 offset 2 数据还在内存，但 log 里没有");
        System.out.println("  ❌ 如果 Leader B 直接继续，状态会不一致");

        System.out.println("\n  ✅ 解决方案：Leader B 恢复时");
        System.out.println("    1. 从 log 读取 offset 0-" + leaderA.highWatermark);
        System.out.println("    2. 回滚到 offset " + leaderA.highWatermark);
        System.out.println("    3. 丢弃 offset > " + leaderA.highWatermark + " 的内存状态");

        // Leader B 正确恢复
        leaderB.processRecords(0, batch1);
        leaderB.updateHW(1);
        System.out.println("\n[Leader B] 恢复完成: " + leaderB.memory +
                         ", lastWrittenOffset=" + leaderB.lastWrittenOffset);

        System.out.println("\n💡 为什么需要回滚？");
        System.out.println("  1. Leader 变更时，新 Leader 只知道 HW 之前的数据");
        System.out.println("  2. 需要丢弃 > HW 的未提交状态");
        System.out.println("  3. 使用 SnapshotRegistry.revertToSnapshot(HW)");
    }
}

// ============================================================
// 场景 3: 读已提交（Read Committed）隔离级别
// ============================================================

class Scenario3_ReadCommitted {
    /**
     * 问题：如何保证读操作只看到已提交的数据？
     *
     * MVCC 的核心：
     * - 写操作使用 lastWrittenOffset（最新）
     * - 读操作使用 lastCommittedOffset（已提交）
     *
     * 这就需要在不同 offset 之间"切换"
     */

    static class MVCCCoordinator {
        private final Map<String, TreeMap<Long, Integer>> data = new HashMap<>();
        private long lastWrittenOffset = 0;
        private long lastCommittedOffset = 0;

        public void write(String key, int value) {
            // 写入使用 lastWrittenOffset
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(lastWrittenOffset, value);

            System.out.println("[写入] offset=" + lastWrittenOffset +
                             ", " + key + "=" + value);
            lastWrittenOffset++;
        }

        public Integer read(String key) {
            // 读取使用 lastCommittedOffset
            TreeMap<Long, Integer> timeline = data.get(key);
            if (timeline == null) return null;

            Map.Entry<Long, Integer> entry = timeline.floorEntry(lastCommittedOffset);
            return entry != null ? entry.getValue() : null;
        }

        public void commit(long offset) {
            lastCommittedOffset = offset;
            System.out.println("[提交] HW 推进到 " + offset);
        }

        public void printState() {
            System.out.println("  lastWrittenOffset=" + lastWrittenOffset +
                             ", lastCommittedOffset=" + lastCommittedOffset);
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== 场景 3: Read Committed 隔离级别 ===\n");

        MVCCCoordinator coordinator = new MVCCCoordinator();

        // 写入 offset 0
        coordinator.write("balance", 100);
        coordinator.commit(0);

        System.out.println("\n读取 balance: " + coordinator.read("balance"));
        coordinator.printState();

        // 写入 offset 1（未提交）
        System.out.println();
        coordinator.write("balance", 200);
        // 注意：没有 commit！

        System.out.println("\n读取 balance: " + coordinator.read("balance"));
        coordinator.printState();
        System.out.println("  ✅ 读到 100（已提交），没有读到 200（未提交）");

        // 提交 offset 1
        System.out.println();
        coordinator.commit(1);

        System.out.println("\n读取 balance: " + coordinator.read("balance"));
        coordinator.printState();
        System.out.println("  ✅ 现在读到 200");

        System.out.println("\n💡 为什么需要回滚？");
        System.out.println("  1. 不是真正的'回滚'，而是'切换视图'");
        System.out.println("  2. 写操作看 lastWrittenOffset 的视图");
        System.out.println("  3. 读操作看 lastCommittedOffset 的视图");
        System.out.println("  4. SnapshotRegistry.setCurrentOffset() 实现视图切换");
    }
}

// ============================================================
// 场景 4: 事务支持（未来可能的功能）
// ============================================================

class Scenario4_Transaction {
    /**
     * 问题：如何支持事务？
     *
     * 事务语义：
     * - BEGIN: 创建快照
     * - COMMIT: 提交，删除快照
     * - ROLLBACK: 回滚到快照
     */

    static class TransactionalCoordinator {
        private final Map<String, TreeMap<Long, Integer>> data = new HashMap<>();
        private long currentOffset = 0;
        private final Stack<Long> transactionStack = new Stack<>();

        public void beginTransaction() {
            transactionStack.push(currentOffset);
            System.out.println("[BEGIN TX] 保存快照 at offset " + currentOffset);
        }

        public void write(String key, int value) {
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(currentOffset, value);
            System.out.println("  [写入] offset=" + currentOffset + ", " + key + "=" + value);
            currentOffset++;
        }

        public void commit() {
            if (!transactionStack.isEmpty()) {
                long txStart = transactionStack.pop();
                System.out.println("[COMMIT TX] 从 offset " + txStart + " 提交");
            }
        }

        public void rollback() {
            if (!transactionStack.isEmpty()) {
                long txStart = transactionStack.pop();
                System.out.println("[ROLLBACK TX] 回滚到 offset " + txStart);

                // 删除 > txStart 的所有版本
                for (TreeMap<Long, Integer> timeline : data.values()) {
                    timeline.tailMap(txStart + 1, true).clear();
                }

                currentOffset = txStart;
            }
        }

        public void printState() {
            System.out.println("  当前状态:");
            for (Map.Entry<String, TreeMap<Long, Integer>> entry : data.entrySet()) {
                Map.Entry<Long, Integer> latest = entry.getValue().floorEntry(currentOffset);
                if (latest != null) {
                    System.out.println("    " + entry.getKey() + "=" + latest.getValue());
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== 场景 4: 事务支持 ===\n");

        TransactionalCoordinator coordinator = new TransactionalCoordinator();

        // 初始状态
        coordinator.write("balance", 100);
        coordinator.printState();

        // 事务 1: 成功
        System.out.println();
        coordinator.beginTransaction();
        coordinator.write("balance", 200);
        coordinator.write("credit", 50);
        coordinator.printState();
        coordinator.commit();
        System.out.println("  ✅ 事务提交");

        // 事务 2: 回滚
        System.out.println();
        coordinator.beginTransaction();
        coordinator.write("balance", 300);
        coordinator.write("credit", 100);
        coordinator.printState();

        System.out.println("\n  检测到错误，回滚事务");
        coordinator.rollback();
        coordinator.printState();
        System.out.println("  ✅ 状态恢复到事务前");

        System.out.println("\n💡 为什么需要回滚？");
        System.out.println("  1. 实现类似数据库的事务语义");
        System.out.println("  2. BEGIN 创建快照，ROLLBACK 回滚到快照");
        System.out.println("  3. 多个操作原子性执行");
    }
}

// ============================================================
// 场景 5: 调试和问题重现（真实案例）
// ============================================================

class Scenario5_Debugging {
    /**
     * 真实场景：生产环境出现 bug，需要分析状态演进
     *
     * 问题：某个 group 的 offset 突然变成 0，为什么？
     */

    static class AuditableCoordinator {
        private final Map<String, TreeMap<Long, Integer>> data = new HashMap<>();
        private final Map<Long, String> operationLog = new TreeMap<>();
        private long currentOffset = 0;

        public void write(String key, int value, String description) {
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(currentOffset, value);
            operationLog.put(currentOffset, description + ": " + key + "=" + value);
            currentOffset++;
        }

        public void debugProblem() {
            System.out.println("【问题】offset 10 时，balance 突然变成 0");

            System.out.println("\n【分析】查看操作历史:");
            for (Map.Entry<Long, String> entry : operationLog.entrySet()) {
                System.out.println("  offset " + entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("\n【分析】查看每个版本的状态:");
            for (long offset = 0; offset < currentOffset; offset++) {
                TreeMap<Long, Integer> timeline = data.get("balance");
                if (timeline != null) {
                    Map.Entry<Long, Integer> entry = timeline.floorEntry(offset);
                    if (entry != null) {
                        System.out.println("  offset " + offset + ": balance=" + entry.getValue());
                    }
                }
            }

            System.out.println("\n【定位】在 offset 5-6 之间，balance 从 100 变成 0");
            System.out.println("  原因：Client 错误地发送了 balance=0 的请求");
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== 场景 5: 调试和问题重现 ===\n");

        AuditableCoordinator coordinator = new AuditableCoordinator();

        coordinator.write("balance", 100, "初始化");
        coordinator.write("balance", 200, "消费者 A 更新");
        coordinator.write("balance", 300, "消费者 B 更新");
        coordinator.write("balance", 400, "消费者 C 更新");
        coordinator.write("balance", 500, "消费者 D 更新");
        coordinator.write("balance", 0, "消费者 E 错误更新");  // Bug!
        coordinator.write("balance", 600, "消费者 F 更新");

        coordinator.debugProblem();

        System.out.println("\n💡 为什么需要回滚？");
        System.out.println("  1. 保留所有历史版本，方便调试");
        System.out.println("  2. 可以'回到过去'重现问题");
        System.out.println("  3. 分析状态演进过程");
        System.out.println("  4. 找到 bug 的根本原因");
    }
}

// ============================================================
// 总结
// ============================================================

class WhyRevertIsNeeded {
    public static void main(String[] args) throws Exception {
        Scenario1_BatchFailure.main(args);
        Scenario2_LeaderChange.main(args);
        Scenario3_ReadCommitted.main(args);
        Scenario4_Transaction.main(args);
        Scenario5_Debugging.main(args);

        System.out.println("\n\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  总结：为什么 Kafka 需要 revert to version?           ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        System.out.println("\n1. Batch 失败回滚（核心需求）");
        System.out.println("   - Batch 中一条失败，所有都要回滚");
        System.out.println("   - 保证内存和 log 一致");
        System.out.println("   - 使用: registry.revertToSnapshot(baseOffset)");

        System.out.println("\n2. Leader 变更（高可用）");
        System.out.println("   - 新 Leader 只知道 HW 之前的数据");
        System.out.println("   - 丢弃未提交的状态");
        System.out.println("   - 使用: registry.revertToSnapshot(highWatermark)");

        System.out.println("\n3. Read Committed（隔离级别）");
        System.out.println("   - 读操作看已提交数据");
        System.out.println("   - 写操作看最新数据");
        System.out.println("   - 使用: registry.setCurrentOffset(committedOffset/writtenOffset)");

        System.out.println("\n4. 事务支持（未来功能）");
        System.out.println("   - BEGIN/COMMIT/ROLLBACK 语义");
        System.out.println("   - 多个操作原子执行");
        System.out.println("   - 使用: registry 的快照机制");

        System.out.println("\n5. 调试和审计（运维）");
        System.out.println("   - 查看历史状态");
        System.out.println("   - 重现问题");
        System.out.println("   - 分析状态演进");

        System.out.println("\n🔑 关键洞察:");
        System.out.println("  'Revert' 不仅是回滚，更是'视图切换'");
        System.out.println("  - 同一份数据，不同 offset 看到不同的版本");
        System.out.println("  - 这就是 MVCC 的核心思想");
        System.out.println("  - SnapshotRegistry 提供了统一的机制");
    }
}
