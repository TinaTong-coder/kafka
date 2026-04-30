/**
 * 为什么删除中间 snapshot 时需要 mergeFrom？
 *
 * 核心问题：Delta 存储的是"回滚到这个 snapshot 需要恢复的值"
 * 如果删除中间 snapshot，后面 snapshot 的 Delta 就失效了！
 */

import java.util.*;

class WhyMergeDelta {
    public static void main(String[] args) {
        System.out.println("=== 场景：删除中间 snapshot 为什么需要 merge ===\n");

        // 场景：3 个 snapshot，删除中间的
        DemoWithMerge();
        System.out.println();
        DemoWithoutMerge();
    }

    static void DemoWithMerge() {
        System.out.println("✅ 正确做法：删除 snapshot 时 merge Delta");
        System.out.println("-------------------------------------------");

        TimelineMap map = new TimelineMap();

        // offset 100: 初始状态
        map.createSnapshot(100);
        map.put("x", 1);
        map.put("y", 2);
        System.out.println("offset 100: x=1, y=2");
        System.out.println("  Snapshot(100).delta = {x: null, y: null}");

        // offset 101: 修改 x
        map.createSnapshot(101);
        map.put("x", 10);
        System.out.println("\noffset 101: x=10, y=2");
        System.out.println("  Snapshot(101).delta = {x: 1}  ← 回滚到 101 需要恢复 x=1");

        // offset 102: 修改 y
        map.createSnapshot(102);
        map.put("y", 20);
        System.out.println("\noffset 102: x=10, y=20");
        System.out.println("  Snapshot(102).delta = {y: 2}  ← 回滚到 102 需要恢复 y=2");

        System.out.println("\n当前状态: x=" + map.getCurrentValue("x") + ", y=" + map.getCurrentValue("y"));

        // 删除 snapshot 101 (中间的)
        System.out.println("\n🗑️  删除 Snapshot(101) - 执行 merge");
        map.deleteSnapshotWithMerge(101);
        System.out.println("  Snapshot(100).delta = {x: 1, y: null}  ← merge 了 101 的 Delta");
        System.out.println("  Snapshot(102).delta = {y: 2}");

        // 测试：回滚到 102 还能正确工作吗？
        System.out.println("\n📍 回滚到 offset 102:");
        map.revertTo(102);
        System.out.println("  x=" + map.getCurrentValue("x") + ", y=" + map.getCurrentValue("y") + " ✅ 正确！");

        // 测试：回滚到 100 还能正确工作吗？
        System.out.println("\n📍 回滚到 offset 100:");
        map.revertTo(100);
        System.out.println("  x=" + map.getCurrentValue("x") + ", y=" + map.getCurrentValue("y") + " ✅ 正确！");
    }

    static void DemoWithoutMerge() {
        System.out.println("❌ 错误做法：删除 snapshot 不 merge Delta");
        System.out.println("-------------------------------------------");

        TimelineMapBroken map = new TimelineMapBroken();

        // offset 100: 初始状态
        map.createSnapshot(100);
        map.put("x", 1);
        map.put("y", 2);
        System.out.println("offset 100: x=1, y=2");
        System.out.println("  Snapshot(100).delta = {x: null, y: null}");

        // offset 101: 修改 x
        map.createSnapshot(101);
        map.put("x", 10);
        System.out.println("\noffset 101: x=10, y=2");
        System.out.println("  Snapshot(101).delta = {x: 1}");

        // offset 102: 修改 y
        map.createSnapshot(102);
        map.put("y", 20);
        System.out.println("\noffset 102: x=10, y=20");
        System.out.println("  Snapshot(102).delta = {y: 2}");

        System.out.println("\n当前状态: x=" + map.getCurrentValue("x") + ", y=" + map.getCurrentValue("y"));

        // 删除 snapshot 101 (中间的) - 不 merge
        System.out.println("\n🗑️  删除 Snapshot(101) - 不 merge，直接删除");
        map.deleteSnapshotWithoutMerge(101);
        System.out.println("  Snapshot(100).delta = {x: null, y: null}  ← 没变！");
        System.out.println("  Snapshot(102).delta = {y: 2}");

        // 测试：回滚到 102
        System.out.println("\n📍 回滚到 offset 102:");
        map.revertTo(102);
        System.out.println("  期望: x=10, y=2");
        System.out.println("  实际: x=" + map.getCurrentValue("x") + ", y=" + map.getCurrentValue("y"));
        System.out.println("  ❌ 错误！x 的值丢失了，因为 Snapshot(101) 的 Delta {x: 1} 被删除了！");

        System.out.println("\n💡 问题分析:");
        System.out.println("  - Snapshot(102).delta 只记录了 {y: 2}");
        System.out.println("  - Snapshot(101) 被删除了，{x: 1} 的回滚信息丢失");
        System.out.println("  - 回滚到 102 时，无法恢复 x 的正确值！");
    }
}

// ============ 正确实现 ============
class TimelineMap {
    private final Map<String, Integer> data = new HashMap<>();
    private final Map<Long, Snapshot> snapshots = new TreeMap<>();

    static class Snapshot {
        long epoch;
        Map<String, Integer> delta = new HashMap<>();  // 回滚到这个 epoch 需要恢复的值

        Snapshot(long epoch) {
            this.epoch = epoch;
        }

        void mergeFrom(Snapshot source) {
            // 把 source 的 delta 合并到自己
            for (Map.Entry<String, Integer> entry : source.delta.entrySet()) {
                // 只在自己没有这个 key 时才添加
                delta.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
    }

    void createSnapshot(long epoch) {
        snapshots.put(epoch, new Snapshot(epoch));
    }

    void put(String key, Integer value) {
        Integer oldValue = data.get(key);

        // 更新所有 snapshot 的 delta
        for (Snapshot snapshot : snapshots.values()) {
            if (!snapshot.delta.containsKey(key)) {
                snapshot.delta.put(key, oldValue);
            }
        }

        data.put(key, value);
    }

    Integer getCurrentValue(String key) {
        return data.get(key);
    }

    void deleteSnapshotWithMerge(long epoch) {
        Snapshot toDelete = snapshots.get(epoch);
        if (toDelete == null) return;

        // 找到前一个 snapshot
        Snapshot prev = null;
        for (Snapshot s : snapshots.values()) {
            if (s.epoch < epoch) {
                prev = s;
            } else {
                break;
            }
        }

        // 如果有前一个 snapshot，merge delta
        if (prev != null) {
            prev.mergeFrom(toDelete);
        }

        snapshots.remove(epoch);
    }

    void revertTo(long targetEpoch) {
        Snapshot snapshot = snapshots.get(targetEpoch);
        if (snapshot == null) return;

        // 使用 delta 恢复状态
        for (Map.Entry<String, Integer> entry : snapshot.delta.entrySet()) {
            if (entry.getValue() == null) {
                data.remove(entry.getKey());
            } else {
                data.put(entry.getKey(), entry.getValue());
            }
        }
    }
}

// ============ 错误实现 ============
class TimelineMapBroken {
    private final Map<String, Integer> data = new HashMap<>();
    private final Map<Long, Snapshot> snapshots = new TreeMap<>();

    static class Snapshot {
        long epoch;
        Map<String, Integer> delta = new HashMap<>();

        Snapshot(long epoch) {
            this.epoch = epoch;
        }
    }

    void createSnapshot(long epoch) {
        snapshots.put(epoch, new Snapshot(epoch));
    }

    void put(String key, Integer value) {
        Integer oldValue = data.get(key);

        for (Snapshot snapshot : snapshots.values()) {
            if (!snapshot.delta.containsKey(key)) {
                snapshot.delta.put(key, oldValue);
            }
        }

        data.put(key, value);
    }

    Integer getCurrentValue(String key) {
        return data.get(key);
    }

    void deleteSnapshotWithoutMerge(long epoch) {
        // 直接删除，不 merge
        snapshots.remove(epoch);
    }

    void revertTo(long targetEpoch) {
        Snapshot snapshot = snapshots.get(targetEpoch);
        if (snapshot == null) return;

        for (Map.Entry<String, Integer> entry : snapshot.delta.entrySet()) {
            if (entry.getValue() == null) {
                data.remove(entry.getKey());
            } else {
                data.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
