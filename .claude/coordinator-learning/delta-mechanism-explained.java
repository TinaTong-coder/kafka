/**
 * Delta 机制详解：到底存储什么？
 *
 * 回答问题：如果每个 snapshot 都已经是增量了，怎么计算新的 delta？
 */

import java.util.*;

// ============================================================
// 错误理解：Delta 存储"相对于上一个快照的变更"
// ============================================================

class WrongApproach {
    /**
     * ❌ 错误想法：Delta 存储 "从 snapshot N-1 到 snapshot N 的变更"
     *
     * 问题：如果删除 snapshot N-1，就无法恢复 snapshot N 的状态了！
     */

    static class WrongDelta {
        Map<String, Integer> changes = new HashMap<>();  // 相对于上一个快照的变更

        void recordChange(String key, int newValue) {
            changes.put(key, newValue);
        }
    }

    static class WrongSnapshot {
        long epoch;
        WrongDelta delta;  // 相对变更

        WrongSnapshot(long epoch) {
            this.epoch = epoch;
        }
    }

    public static void main(String[] args) {
        System.out.println("=== 错误理解：Delta 存储相对变更 ===\n");

        // 快照 0: {key1=100}
        WrongSnapshot s0 = new WrongSnapshot(0);
        s0.delta = new WrongDelta();
        s0.delta.recordChange("key1", 100);
        System.out.println("Snapshot 0: key1=100 (完整状态)");

        // 快照 5: {key1=200}
        WrongSnapshot s5 = new WrongSnapshot(5);
        s5.delta = new WrongDelta();
        s5.delta.recordChange("key1", 200);  // 只记录变更
        System.out.println("Snapshot 5: Delta {key1: 100→200}");

        // 快照 10: {key1=200, key2=300}
        WrongSnapshot s10 = new WrongSnapshot(10);
        s10.delta = new WrongDelta();
        s10.delta.recordChange("key2", 300);  // 只记录新增的 key2
        System.out.println("Snapshot 10: Delta {key2: null→300}");

        System.out.println("\n问题：如果删除 Snapshot 5...");
        System.out.println("  剩余: Snapshot 0, Snapshot 10");
        System.out.println("  查询 epoch=7 时，应该返回 {key1=200}");
        System.out.println("  但 Snapshot 5 被删了，无法知道 key1 在 epoch 5-10 之间是 200！");
        System.out.println("  ❌ 错误！");
    }
}

// ============================================================
// 正确理解：Delta 存储"回滚信息"
// ============================================================

class CorrectApproach {
    /**
     * ✅ 正确理解：Delta 存储 "回滚到这个快照需要的信息"
     *
     * Delta 记录的是：
     * - 在这个快照创建之后，哪些数据被修改了
     * - 回滚时需要恢复的旧值是什么
     */

    static class CorrectDelta {
        // 记录"未来的变更"以及"旧值"
        // key -> 这个快照时刻的值（回滚时需要恢复的值）
        Map<String, Integer> rollbackValues = new HashMap<>();

        void recordModification(String key, Integer valueAtThisEpoch) {
            rollbackValues.put(key, valueAtThisEpoch);
        }

        void apply() {
            System.out.println("    回滚：恢复这些值 -> " + rollbackValues);
        }

        void mergeFrom(CorrectDelta other) {
            // 合并：如果 other 有某个 key 的旧值，而我没有，就拷贝过来
            for (Map.Entry<String, Integer> entry : other.rollbackValues.entrySet()) {
                rollbackValues.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
    }

    static class CorrectSnapshot {
        long epoch;
        CorrectDelta delta = new CorrectDelta();

        CorrectSnapshot(long epoch) {
            this.epoch = epoch;
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== 正确理解：Delta 存储回滚信息 ===\n");

        Map<String, Integer> currentState = new HashMap<>();

        // === 创建快照 0 ===
        System.out.println("【操作】创建快照 0");
        CorrectSnapshot s0 = new CorrectSnapshot(0);
        currentState.put("key1", 100);
        System.out.println("  当前状态: " + currentState);
        System.out.println("  快照 0 的 Delta: {} (初始快照，无需回滚信息)");

        // === 创建快照 5 之前，先记录变更 ===
        System.out.println("\n【操作】修改 key1: 100 → 200");
        System.out.println("  在快照 0 的 Delta 中记录：key1 的旧值是 100");
        s0.delta.recordModification("key1", 100);  // 记录旧值！

        currentState.put("key1", 200);

        System.out.println("\n【操作】创建快照 5");
        CorrectSnapshot s5 = new CorrectSnapshot(5);
        System.out.println("  当前状态: " + currentState);
        System.out.println("  快照 0 的 Delta: {key1=100} (回滚到快照 0 需要恢复 key1=100)");
        System.out.println("  快照 5 的 Delta: {} (刚创建，还没有未来的变更)");

        // === 创建快照 10 之前，先记录变更 ===
        System.out.println("\n【操作】新增 key2: null → 300");
        System.out.println("  在快照 5 的 Delta 中记录：key2 的旧值是 null");
        s5.delta.recordModification("key2", null);  // 记录旧值（null 表示不存在）

        currentState.put("key2", 300);

        System.out.println("\n【操作】创建快照 10");
        CorrectSnapshot s10 = new CorrectSnapshot(10);
        System.out.println("  当前状态: " + currentState);
        System.out.println("  快照 5 的 Delta: {key2=null} (回滚到快照 5 需要删除 key2)");
        System.out.println("  快照 10 的 Delta: {} (刚创建)");

        // === 回滚到快照 5 ===
        System.out.println("\n【操作】回滚到快照 5");
        s5.delta.apply();
        currentState.remove("key2");
        System.out.println("  恢复后状态: " + currentState);

        // === 删除快照 5，合并 Delta ===
        System.out.println("\n【操作】删除快照 5，合并 Delta 到快照 0");
        s0.delta.mergeFrom(s5.delta);
        System.out.println("  快照 0 的 Delta 变成: " + s0.delta.rollbackValues);
        System.out.println("  现在回滚到快照 0 会：");
        System.out.println("    - 恢复 key1=100");
        System.out.println("    - 删除 key2（因为合并了快照 5 的 Delta）");
    }
}

// ============================================================
// 完整示例：TimelineHashMap 如何创建 Delta
// ============================================================

class FullExample {
    /**
     * 真实场景：TimelineHashMap 在每次写入时，如何为已存在的快照创建 Delta
     */

    interface Delta {
        void apply();
        void mergeFrom(Delta other);
    }

    static class HashMapDelta implements Delta {
        // 存储这个快照的数据（用于回滚）
        private final Map<String, VersionedValue> snapshotData = new HashMap<>();

        static class VersionedValue {
            final long epoch;
            final Integer value;

            VersionedValue(long epoch, Integer value) {
                this.epoch = epoch;
                this.value = value;
            }
        }

        void recordValue(String key, long epoch, Integer value) {
            // 只记录第一次修改前的值
            snapshotData.putIfAbsent(key, new VersionedValue(epoch, value));
        }

        @Override
        public void apply() {
            System.out.println("      回滚数据: " + snapshotData);
        }

        @Override
        public void mergeFrom(Delta other) {
            HashMapDelta otherDelta = (HashMapDelta) other;
            // 合并：如果 other 有某个 key，而我没有，就拷贝
            for (Map.Entry<String, VersionedValue> entry : otherDelta.snapshotData.entrySet()) {
                snapshotData.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public String toString() {
            return snapshotData.toString();
        }
    }

    static class Snapshot {
        final long epoch;
        final HashMapDelta delta = new HashMapDelta();

        Snapshot(long epoch) {
            this.epoch = epoch;
        }
    }

    static class TimelineHashMap {
        // 实际数据：key -> (epoch -> value)
        private final Map<String, TreeMap<Long, Integer>> data = new HashMap<>();

        // 当前所有快照
        private final List<Snapshot> snapshots = new ArrayList<>();

        public void createSnapshot(long epoch) {
            Snapshot snapshot = new Snapshot(epoch);
            snapshots.add(snapshot);
            System.out.println("  [创建快照 " + epoch + "]");
        }

        public void put(String key, long epoch, Integer value) {
            System.out.println("\n【写入】epoch=" + epoch + ", " + key + "=" + value);

            // 1. 找到当前值（如果有）
            TreeMap<Long, Integer> timeline = data.get(key);
            Integer oldValue = null;
            long oldEpoch = -1;

            if (timeline != null && !timeline.isEmpty()) {
                Map.Entry<Long, Integer> entry = timeline.floorEntry(epoch - 1);
                if (entry != null) {
                    oldValue = entry.getValue();
                    oldEpoch = entry.getKey();
                }
            }

            System.out.println("  旧值: " + (oldValue == null ? "null" : oldValue + " (at epoch " + oldEpoch + ")"));

            // 2. 为所有 epoch < 当前 epoch 的快照记录这个变更
            for (Snapshot snapshot : snapshots) {
                if (snapshot.epoch < epoch) {
                    // 这个快照创建时，key 的值是 oldValue
                    // 如果将来回滚到这个快照，需要恢复 oldValue
                    snapshot.delta.recordValue(key, snapshot.epoch, oldValue);
                    System.out.println("  [快照 " + snapshot.epoch + "] 记录旧值: " + key + "=" + oldValue);
                }
            }

            // 3. 写入新值
            data.computeIfAbsent(key, k -> new TreeMap<>()).put(epoch, value);
        }

        public void printSnapshots() {
            System.out.println("\n=== 当前所有快照的 Delta ===");
            for (Snapshot snapshot : snapshots) {
                System.out.println("  快照 " + snapshot.epoch + ": " + snapshot.delta);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== 完整示例：TimelineHashMap 如何创建 Delta ===\n");

        TimelineHashMap map = new TimelineHashMap();

        // 创建快照 0
        map.createSnapshot(0);
        map.put("key1", 0, 100);

        // 创建快照 5
        map.createSnapshot(5);
        map.put("key1", 5, 200);  // 会在快照 0 的 Delta 中记录 key1=100

        // 创建快照 10
        map.createSnapshot(10);
        map.put("key2", 10, 300);  // 会在快照 0 和 5 的 Delta 中记录 key2=null

        map.printSnapshots();

        System.out.println("\n✅ 关键理解:");
        System.out.println("  1. Delta 不是存储'变更'，而是存储'回滚信息'");
        System.out.println("  2. 每次写入时，更新所有老快照的 Delta");
        System.out.println("  3. 快照的 Delta 记录的是：回滚到这个快照需要恢复哪些旧值");
    }
}

// ============================================================
// 运行所有演示
// ============================================================

class DeltaMechanismExplained {
    public static void main(String[] args) {
        WrongApproach.main(args);
        CorrectApproach.main(args);
        FullExample.main(args);

        System.out.println("\n\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  总结：Delta 到底存储什么？                           ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        System.out.println("\n❌ 错误理解：");
        System.out.println("  Delta 存储 '从快照 N-1 到快照 N 的变更'");
        System.out.println("  问题：删除中间快照会丢失信息");

        System.out.println("\n✅ 正确理解：");
        System.out.println("  Delta 存储 '回滚到这个快照需要恢复的旧值'");
        System.out.println("  每次写入都更新所有老快照的 Delta");

        System.out.println("\n📝 示例：");
        System.out.println("  快照 0 创建时: Delta = {}");
        System.out.println("  写入 key1=100: 快照 0 的 Delta 不变（因为 key1 在快照 0 之后写入）");
        System.out.println("  快照 5 创建");
        System.out.println("  写入 key1=200: 快照 0 的 Delta 记录 {key1=100}（回滚时需要恢复）");
        System.out.println("  快照 10 创建");
        System.out.println("  写入 key2=300: 快照 0 和 5 的 Delta 都记录 {key2=null}");

        System.out.println("\n🔑 为什么这样设计？");
        System.out.println("  1. 删除中间快照时，可以合并 Delta");
        System.out.println("  2. 回滚时只需应用一个快照的 Delta");
        System.out.println("  3. 内存占用 = 变更次数，而不是快照数");
    }
}
