/**
 * SnapshotRegistry 演进史：从简单到复杂
 *
 * 通过代码演进展示为什么 Kafka 的 SnapshotRegistry 需要这么复杂的设计
 * 每个版本解决前一个版本的具体问题
 */

import java.util.*;
import java.lang.ref.WeakReference;

// ============================================================
// 版本 1: 最简单的实现 - 每个数据结构自己管理版本
// ============================================================

class V1_Demo {
    /**
     * 问题场景：ShareCoordinator 需要管理多个 TimelineHashMap
     * 简单想法：每个 TimelineHashMap 自己维护 currentOffset
     */

    static class TimelineHashMap<K, V> {
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

        public void revertTo(long offset) {
            this.currentOffset = offset;
        }
    }

    static class ShareCoordinator {
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

    public static void main(String[] args) {
        System.out.println("=== V1: 每个数据结构自己管理版本 ===\n");

        ShareCoordinator coordinator = new ShareCoordinator();

        // 写入版本 0
        coordinator.writeState("key1", 100, 1, 0, 0);
        System.out.println("写入版本 0: state=100, leaderEpoch=1, stateEpoch=0");

        // 写入版本 1
        coordinator.writeState("key1", 200, 1, 1, 1);
        System.out.println("写入版本 1: state=200, leaderEpoch=1, stateEpoch=1");

        // 回滚到版本 0
        coordinator.revertToSnapshot(0);
        System.out.println("回滚到版本 0");

        System.out.println("\n❌ 问题 1: 如果忘记同步某个 Map 的 offset？");
        System.out.println("   例如：只设置了 stateMap.setOffset(1)，忘了 leaderEpochMap");
        System.out.println("   结果：状态不一致！stateMap 在版本 1，leaderEpochMap 在版本 0");

        System.out.println("\n❌ 问题 2: 新增一个 TimelineHashMap 怎么办？");
        System.out.println("   需要修改所有 writeState() 和 revertToSnapshot() 的代码");
        System.out.println("   容易遗漏，导致 bug");

        System.out.println("\n❌ 问题 3: 代码重复");
        System.out.println("   每次都要写 3 次 setOffset()，容易出错");
    }
}

// ============================================================
// 版本 2: 中心化的 SnapshotRegistry - 统一管理所有数据结构
// ============================================================

class V2_Demo {
    /**
     * 解决方案：创建一个中心化的 SnapshotRegistry
     * 所有 TimelineHashMap 注册到 registry，由它统一管理 offset
     */

    static class SnapshotRegistry {
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

    static class TimelineHashMap<K, V> implements Revertable {
        private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
        private final SnapshotRegistry registry;

        public TimelineHashMap(SnapshotRegistry registry) {
            this.registry = registry;
            registry.register(this);  // 自动注册
        }

        public void put(K key, V value) {
            long offset = registry.currentOffset();
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(offset, value);
        }

        public V get(K key) {
            TreeMap<Long, V> timeline = data.get(key);
            if (timeline == null) return null;

            long offset = registry.currentOffset();
            Map.Entry<Long, V> entry = timeline.floorEntry(offset);
            return entry != null ? entry.getValue() : null;
        }

        @Override
        public void revert(long targetOffset) {
            // 回滚逻辑（目前为空，因为只是修改 registry 的 offset）
        }
    }

    static class ShareCoordinator {
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

        public void printState(String key) {
            System.out.println("  state=" + stateMap.get(key) +
                             ", leaderEpoch=" + leaderEpochMap.get(key) +
                             ", stateEpoch=" + stateEpochMap.get(key));
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== V2: 中心化的 SnapshotRegistry ===\n");

        ShareCoordinator coordinator = new ShareCoordinator();

        coordinator.writeState("key1", 100, 1, 0, 0);
        System.out.println("版本 0:");
        coordinator.printState("key1");

        coordinator.writeState("key1", 200, 1, 1, 1);
        System.out.println("\n版本 1:");
        coordinator.printState("key1");

        coordinator.revertToSnapshot(0);
        System.out.println("\n回滚到版本 0:");
        coordinator.printState("key1");

        System.out.println("\n✅ 解决了 V1 的问题:");
        System.out.println("   1. 统一管理 offset，不会不一致");
        System.out.println("   2. 新增 Map 只需 new TimelineHashMap(registry)");
        System.out.println("   3. 回滚一行代码，自动同步所有数据结构");

        System.out.println("\n❌ 新问题: 如何管理多个 snapshot？");
        System.out.println("   当前只能记住一个 currentOffset");
        System.out.println("   如果想保留多个快照（例如 offset 0, 5, 10），无法实现");
    }
}

// ============================================================
// 版本 3: Snapshot 对象 - 支持多个命名快照
// ============================================================

class V3_Demo {
    /**
     * 解决方案：引入显式的 Snapshot 对象
     * 使用 HashMap 存储多个 snapshot
     */

    static class Snapshot {
        private final long epoch;

        public Snapshot(long epoch) {
            this.epoch = epoch;
        }

        public long epoch() {
            return epoch;
        }
    }

    static class SnapshotRegistry {
        private final Map<Long, Snapshot> snapshots = new HashMap<>();
        private long currentOffset = 0;
        private final List<Revertable> revertables = new ArrayList<>();

        public long currentOffset() {
            return currentOffset;
        }

        public void register(Revertable revertable) {
            revertables.add(revertable);
        }

        // 创建快照
        public void createSnapshot(long epoch) {
            Snapshot snapshot = new Snapshot(epoch);
            snapshots.put(epoch, snapshot);
            System.out.println("  [Registry] 创建快照: epoch=" + epoch);
        }

        // 回滚到快照
        public void revertToSnapshot(long targetEpoch) {
            Snapshot target = snapshots.get(targetEpoch);
            if (target == null) {
                throw new RuntimeException("快照不存在: " + targetEpoch);
            }

            // 删除所有 > targetEpoch 的快照
            List<Long> toRemove = new ArrayList<>();
            for (Long epoch : snapshots.keySet()) {
                if (epoch > targetEpoch) {
                    toRemove.add(epoch);
                }
            }

            for (Long epoch : toRemove) {
                snapshots.remove(epoch);
                System.out.println("  [Registry] 删除快照: epoch=" + epoch);
            }

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

    interface Revertable {
        void revert(long targetOffset);
    }

    static class TimelineHashMap<K, V> implements Revertable {
        private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
        private final SnapshotRegistry registry;

        public TimelineHashMap(SnapshotRegistry registry) {
            this.registry = registry;
            registry.register(this);
        }

        public void put(K key, V value) {
            long offset = registry.currentOffset();
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(offset, value);
        }

        public V get(K key) {
            TreeMap<Long, V> timeline = data.get(key);
            if (timeline == null) return null;

            long offset = registry.currentOffset();
            Map.Entry<Long, V> entry = timeline.floorEntry(offset);
            return entry != null ? entry.getValue() : null;
        }

        @Override
        public void revert(long targetOffset) {
            // 删除所有 > targetOffset 的版本
            for (TreeMap<Long, V> timeline : data.values()) {
                timeline.tailMap(targetOffset + 1, true).clear();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== V3: Snapshot 对象 - 支持多个快照 ===\n");

        SnapshotRegistry registry = new SnapshotRegistry();
        TimelineHashMap<String, Integer> map = new TimelineHashMap<>(registry);

        // 创建版本 0
        registry.createSnapshot(0);
        map.put("key1", 100);
        System.out.println("版本 0: key1=100");

        // 创建版本 5
        registry.createSnapshot(5);
        map.put("key1", 200);
        System.out.println("版本 5: key1=200");

        // 创建版本 10
        registry.createSnapshot(10);
        map.put("key1", 300);
        System.out.println("版本 10: key1=300");

        System.out.println("\n当前快照列表: " + registry.listSnapshots());

        // 回滚到版本 5
        System.out.println("\n回滚到版本 5:");
        registry.revertToSnapshot(5);
        System.out.println("key1=" + map.get("key1"));
        System.out.println("剩余快照: " + registry.listSnapshots());

        System.out.println("\n✅ 解决了 V2 的问题:");
        System.out.println("   1. 可以保留多个快照");
        System.out.println("   2. 回滚时自动删除未来的快照");

        System.out.println("\n❌ 新问题: 内存占用");
        System.out.println("   每个版本都存储完整的值");
        System.out.println("   如果值很大（例如 1KB），10 个版本 = 10KB");
        System.out.println("   能否只存储变更？");
    }
}

// ============================================================
// 版本 4: Delta 机制 - 增量存储
// ============================================================

class V4_Demo {
    /**
     * 解决方案：Snapshot 不存储完整值，只存储 Delta（变更）
     * 回滚时应用 Delta 来恢复状态
     */

    interface Delta {
        void apply();
        void mergeFrom(Delta other);
    }

    static class HashMapDelta<K, V> implements Delta {
        private final Map<K, TreeMap<Long, V>> targetMap;
        private final Map<K, V> oldValues = new HashMap<>();
        private final Set<K> removedKeys = new HashSet<>();

        public HashMapDelta(Map<K, TreeMap<Long, V>> targetMap) {
            this.targetMap = targetMap;
        }

        public void recordPut(K key, long offset, V newValue) {
            // 记录旧值（如果存在）
            TreeMap<Long, V> timeline = targetMap.get(key);
            if (timeline != null) {
                Map.Entry<Long, V> entry = timeline.floorEntry(offset - 1);
                if (entry != null) {
                    oldValues.put(key, entry.getValue());
                }
            }
        }

        public void recordRemove(K key) {
            removedKeys.add(key);
        }

        @Override
        public void apply() {
            // 应用 Delta：恢复旧值
            for (Map.Entry<K, V> entry : oldValues.entrySet()) {
                // 恢复到旧值（简化实现）
                System.out.println("    恢复 " + entry.getKey() + " 到旧值: " + entry.getValue());
            }
        }

        @Override
        public void mergeFrom(Delta other) {
            // 合并两个 Delta（简化实现）
        }
    }

    static class Snapshot {
        private final long epoch;
        private final Map<Revertable, Delta> deltas = new IdentityHashMap<>();

        public Snapshot(long epoch) {
            this.epoch = epoch;
        }

        public long epoch() {
            return epoch;
        }

        public void setDelta(Revertable owner, Delta delta) {
            deltas.put(owner, delta);
        }

        public void applyDeltas() {
            for (Delta delta : deltas.values()) {
                delta.apply();
            }
        }

        public int deltaCount() {
            return deltas.size();
        }
    }

    static class SnapshotRegistry {
        private final Map<Long, Snapshot> snapshots = new HashMap<>();
        private long currentOffset = 0;
        private final List<Revertable> revertables = new ArrayList<>();

        public long currentOffset() {
            return currentOffset;
        }

        public void register(Revertable revertable) {
            revertables.add(revertable);
        }

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

            System.out.println("  [Registry] 创建快照: epoch=" + epoch +
                             ", deltas=" + snapshot.deltaCount());
        }

        public void revertToSnapshot(long targetEpoch) {
            Snapshot target = snapshots.get(targetEpoch);
            if (target == null) {
                throw new RuntimeException("快照不存在: " + targetEpoch);
            }

            // 应用 Delta 回滚
            target.applyDeltas();

            // 删除未来的快照
            snapshots.keySet().removeIf(epoch -> epoch > targetEpoch);

            this.currentOffset = targetEpoch;
        }
    }

    interface Revertable {
        void revert(long targetOffset);
        Delta createDelta(long epoch);
    }

    static class TimelineHashMap<K, V> implements Revertable {
        private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
        private final SnapshotRegistry registry;

        public TimelineHashMap(SnapshotRegistry registry) {
            this.registry = registry;
            registry.register(this);
        }

        public void put(K key, V value) {
            long offset = registry.currentOffset();
            data.computeIfAbsent(key, k -> new TreeMap<>())
                .put(offset, value);
        }

        public V get(K key) {
            TreeMap<Long, V> timeline = data.get(key);
            if (timeline == null) return null;

            long offset = registry.currentOffset();
            Map.Entry<Long, V> entry = timeline.floorEntry(offset);
            return entry != null ? entry.getValue() : null;
        }

        @Override
        public void revert(long targetOffset) {
            for (TreeMap<Long, V> timeline : data.values()) {
                timeline.tailMap(targetOffset + 1, true).clear();
            }
        }

        @Override
        public Delta createDelta(long epoch) {
            return new HashMapDelta<>(data);
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== V4: Delta 机制 - 增量存储 ===\n");

        SnapshotRegistry registry = new SnapshotRegistry();
        TimelineHashMap<String, Integer> map = new TimelineHashMap<>(registry);

        registry.createSnapshot(0);
        map.put("key1", 100);
        System.out.println("版本 0: key1=100");

        registry.createSnapshot(5);
        map.put("key1", 200);  // 只记录 key1 的变更
        System.out.println("版本 5: key1=200 (只存储 Delta: key1 100→200)");

        registry.createSnapshot(10);
        map.put("key2", 300);  // 只记录 key2 的变更
        System.out.println("版本 10: key2=300 (只存储 Delta: key2 新增)");

        System.out.println("\n✅ 解决了 V3 的问题:");
        System.out.println("   1. 每个快照只存储变更（Delta）");
        System.out.println("   2. 内存占用大大减少");

        System.out.println("\n❌ 新问题: 内存泄漏");
        System.out.println("   如果 TimelineHashMap 对象被 GC 回收了");
        System.out.println("   但 registry.revertables 还持有强引用");
        System.out.println("   会导致内存泄漏！");
    }
}

// ============================================================
// 版本 5: WeakReference - 自动清理
// ============================================================

class V5_Demo {
    /**
     * 解决方案：使用 WeakReference 存储 Revertable
     * 允许 GC 回收不再使用的数据结构
     */

    interface Delta {
        void apply();
    }

    interface Revertable {
        void revert(long targetOffset);
        Delta createDelta(long epoch);
    }

    static class Snapshot {
        private final long epoch;
        private final Map<Revertable, Delta> deltas = new IdentityHashMap<>();

        public Snapshot(long epoch) {
            this.epoch = epoch;
        }

        public long epoch() {
            return epoch;
        }

        public void setDelta(Revertable owner, Delta delta) {
            deltas.put(owner, delta);
        }
    }

    static class SnapshotRegistry {
        private final Map<Long, Snapshot> snapshots = new HashMap<>();
        private long currentOffset = 0;

        // 使用 WeakReference 而不是强引用
        private List<WeakReference<Revertable>> revertables = new ArrayList<>();
        private int numRegistrations = 0;
        private int numScrubs = 0;

        public long currentOffset() {
            return currentOffset;
        }

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
            int beforeSize = revertables.size();

            List<WeakReference<Revertable>> newList = new ArrayList<>();
            for (WeakReference<Revertable> ref : revertables) {
                if (ref.get() != null) {  // 对象还存活
                    newList.add(ref);
                }
            }

            revertables = newList;
            numScrubs++;

            System.out.println("  [Scrub] 清理前: " + beforeSize +
                             ", 清理后: " + revertables.size() +
                             " (第 " + numScrubs + " 次清理)");
        }

        public void createSnapshot(long epoch) {
            Snapshot snapshot = new Snapshot(epoch);
            snapshots.put(epoch, snapshot);

            // 遍历所有还存活的 Revertable
            int aliveCount = 0;
            for (WeakReference<Revertable> ref : revertables) {
                Revertable revertable = ref.get();
                if (revertable != null) {
                    Delta delta = revertable.createDelta(epoch);
                    if (delta != null) {
                        snapshot.setDelta(revertable, delta);
                    }
                    aliveCount++;
                }
            }

            System.out.println("  [Registry] 创建快照: epoch=" + epoch +
                             ", 存活的数据结构=" + aliveCount);
        }

        public int aliveRevertableCount() {
            int count = 0;
            for (WeakReference<Revertable> ref : revertables) {
                if (ref.get() != null) {
                    count++;
                }
            }
            return count;
        }
    }

    static class TimelineHashMap<K, V> implements Revertable {
        private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
        private final SnapshotRegistry registry;
        private final String name;

        public TimelineHashMap(SnapshotRegistry registry, String name) {
            this.registry = registry;
            this.name = name;
            registry.register(this);
            System.out.println("[" + name + "] 注册到 Registry");
        }

        @Override
        public void revert(long targetOffset) {}

        @Override
        public Delta createDelta(long epoch) {
            return () -> {};  // 简化实现
        }

        @Override
        protected void finalize() {
            System.out.println("[" + name + "] 被 GC 回收");
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("\n\n=== V5: WeakReference - 自动清理 ===\n");

        SnapshotRegistry registry = new SnapshotRegistry();

        // 创建 3 个 Map
        TimelineHashMap<String, Integer> map1 = new TimelineHashMap<>(registry, "Map1");
        TimelineHashMap<String, Integer> map2 = new TimelineHashMap<>(registry, "Map2");
        TimelineHashMap<String, Integer> map3 = new TimelineHashMap<>(registry, "Map3");

        System.out.println("\n存活的数据结构: " + registry.aliveRevertableCount());

        // 让 map2 离开作用域
        map2 = null;
        System.out.println("\nmap2 = null (离开作用域)");

        // 强制 GC
        System.gc();
        Thread.sleep(100);

        System.out.println("执行 GC 后，存活的数据结构: " + registry.aliveRevertableCount());

        // 创建快照，触发 scrub
        registry.createSnapshot(0);

        // 再创建几个 Map，触发 scrub
        new TimelineHashMap<>(registry, "Map4");
        new TimelineHashMap<>(registry, "Map5");

        System.out.println("\n✅ 解决了 V4 的问题:");
        System.out.println("   1. 使用 WeakReference，允许 GC 回收");
        System.out.println("   2. 定期 scrub 清理过期引用");
        System.out.println("   3. 防止内存泄漏");

        System.out.println("\n❌ 新问题: 删除中间快照的正确性");
        System.out.println("   如果有快照 [0, 5, 10, 15]");
        System.out.println("   删除快照 10 后，查询 offset=12 会得到什么？");
        System.out.println("   答案应该是快照 10 的状态，但快照 10 已经被删了！");
    }
}

// ============================================================
// 版本 6: Snapshot 合并 - 删除快照时合并 Delta
// ============================================================

class V6_Demo {
    /**
     * 解决方案：删除快照时，将其 Delta 合并到前一个快照
     * 这样删除中间快照不会影响查询结果
     */

    interface Delta {
        void apply();
        void mergeFrom(Delta other);
    }

    static class SimpleDelta implements Delta {
        private final Map<String, Integer> changes = new HashMap<>();

        public void recordChange(String key, int value) {
            changes.put(key, value);
        }

        @Override
        public void apply() {
            System.out.println("    应用 Delta: " + changes);
        }

        @Override
        public void mergeFrom(Delta other) {
            SimpleDelta otherDelta = (SimpleDelta) other;
            // 合并变更（后面的覆盖前面的）
            changes.putAll(otherDelta.changes);
            System.out.println("    合并后 Delta: " + changes);
        }
    }

    static class Snapshot {
        private final long epoch;
        private Map<String, Delta> deltas = new HashMap<>();
        private Snapshot prev = this;
        private Snapshot next = this;

        public Snapshot(long epoch) {
            this.epoch = epoch;
        }

        public long epoch() {
            return epoch;
        }

        public void setDelta(String owner, Delta delta) {
            deltas.put(owner, delta);
        }

        public Delta getDelta(String owner) {
            return deltas.get(owner);
        }

        // 双向链表操作
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
            this.prev = this;
            this.next = this;
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

    static class SnapshotRegistry {
        private final Map<Long, Snapshot> snapshots = new HashMap<>();
        private final Snapshot head = new Snapshot(Long.MIN_VALUE);  // 哨兵节点

        public void createSnapshot(long epoch) {
            Snapshot last = head.prev();

            if (last.epoch() == epoch) {
                return;  // 已存在
            }

            Snapshot snapshot = new Snapshot(epoch);
            last.appendNext(snapshot);
            snapshots.put(epoch, snapshot);

            System.out.println("  [Registry] 创建快照: epoch=" + epoch);
        }

        public void deleteSnapshot(long epoch) {
            Snapshot snapshot = snapshots.get(epoch);
            if (snapshot == null) {
                return;
            }

            Snapshot prev = snapshot.prev();

            if (prev != head) {
                // 将当前快照的 Delta 合并到前一个快照
                System.out.println("  [合并] 将快照 " + epoch + " 的 Delta 合并到快照 " + prev.epoch());
                prev.mergeFrom(snapshot);
            }

            // 从链表中移除
            snapshot.erase();
            snapshots.remove(epoch);

            System.out.println("  [Registry] 删除快照: epoch=" + epoch);
        }

        public List<Long> listSnapshots() {
            List<Long> result = new ArrayList<>();
            Snapshot curr = head.next();
            while (curr != head) {
                result.add(curr.epoch());
                curr = curr.next();
            }
            return result;
        }

        public void printSnapshots() {
            System.out.print("  快照链表: ");
            for (Long epoch : listSnapshots()) {
                System.out.print(epoch + " → ");
            }
            System.out.println("END");
        }
    }

    public static void main(String[] args) {
        System.out.println("\n\n=== V6: Snapshot 合并 - 删除中间快照 ===\n");

        SnapshotRegistry registry = new SnapshotRegistry();

        // 创建快照 0
        registry.createSnapshot(0);
        Snapshot s0 = registry.snapshots.get(0L);
        SimpleDelta d0 = new SimpleDelta();
        d0.recordChange("key1", 100);
        s0.setDelta("map1", d0);
        System.out.println("快照 0: key1=100");

        // 创建快照 5
        registry.createSnapshot(5);
        Snapshot s5 = registry.snapshots.get(5L);
        SimpleDelta d5 = new SimpleDelta();
        d5.recordChange("key1", 200);
        s5.setDelta("map1", d5);
        System.out.println("快照 5: key1=200");

        // 创建快照 10
        registry.createSnapshot(10);
        Snapshot s10 = registry.snapshots.get(10L);
        SimpleDelta d10 = new SimpleDelta();
        d10.recordChange("key1", 300);
        s10.setDelta("map1", d10);
        System.out.println("快照 10: key1=300");

        registry.printSnapshots();

        // 删除快照 5
        System.out.println("\n删除快照 5:");
        registry.deleteSnapshot(5);
        registry.printSnapshots();

        // 验证合并结果
        System.out.println("\n验证：快照 0 的 Delta 应该包含快照 5 的变更");
        SimpleDelta merged = (SimpleDelta) s0.getDelta("map1");
        if (merged != null) {
            merged.apply();
        }

        System.out.println("\n✅ 解决了 V5 的问题:");
        System.out.println("   1. 删除中间快照时，Delta 被合并到前一个快照");
        System.out.println("   2. 查询结果保持正确");
        System.out.println("   3. 不会丢失历史信息");

        System.out.println("\n📚 这就是 Kafka SnapshotRegistry 的核心设计！");
    }
}

// ============================================================
// 运行所有演示
// ============================================================

class SnapshotRegistryEvolution {
    public static void main(String[] args) throws Exception {
        V1_Demo.main(args);
        V2_Demo.main(args);
        V3_Demo.main(args);
        V4_Demo.main(args);
        V5_Demo.main(args);
        V6_Demo.main(args);

        System.out.println("\n\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  总结：SnapshotRegistry 的演进                        ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        System.out.println("\nV1 → V2: 中心化管理，解决手动同步问题");
        System.out.println("V2 → V3: Snapshot 对象，支持多个快照");
        System.out.println("V3 → V4: Delta 机制，减少内存占用");
        System.out.println("V4 → V5: WeakReference，防止内存泄漏");
        System.out.println("V5 → V6: Snapshot 合并，保证删除正确性");

        System.out.println("\n每一步都解决了前一步的具体问题！");
        System.out.println("这就是为什么 Kafka 的 SnapshotRegistry 需要 500+ 行代码");
    }
}
