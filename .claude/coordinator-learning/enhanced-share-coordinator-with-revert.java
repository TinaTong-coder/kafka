/**
 * Enhanced ShareCoordinator 实现
 *
 * 新增功能：Revert to Previous Version
 *
 * 使用场景：
 * 1. 回滚错误的状态更新
 * 2. 撤销失败的事务
 * 3. 恢复到某个历史快照
 * 4. 调试和测试
 *
 * 实现原理：
 * - 利用 TimelineHashMap 的 MVCC 特性
 * - 维护 offset → snapshot 的映射
 * - 支持回滚到任意历史 offset
 */

import java.util.*;
import java.util.concurrent.*;

// ============ 核心数据结构 ============

/**
 * ShareGroupState: Share Group 的状态
 */
class ShareGroupState {
    final String groupId;
    final String topicId;
    final int partition;
    final long startOffset;
    final int leaderEpoch;
    final int stateEpoch;
    final List<StateBatch> stateBatches;

    ShareGroupState(String groupId, String topicId, int partition,
                    long startOffset, int leaderEpoch, int stateEpoch,
                    List<StateBatch> stateBatches) {
        this.groupId = groupId;
        this.topicId = topicId;
        this.partition = partition;
        this.startOffset = startOffset;
        this.leaderEpoch = leaderEpoch;
        this.stateEpoch = stateEpoch;
        this.stateBatches = new ArrayList<>(stateBatches);
    }

    @Override
    public String toString() {
        return String.format("ShareGroupState{group=%s, topic=%s, partition=%d, " +
                           "startOffset=%d, leaderEpoch=%d, stateEpoch=%d, batches=%d}",
            groupId, topicId, partition, startOffset, leaderEpoch, stateEpoch, stateBatches.size());
    }

    // 创建副本（用于快照）
    public ShareGroupState copy() {
        return new ShareGroupState(groupId, topicId, partition, startOffset,
                                  leaderEpoch, stateEpoch, new ArrayList<>(stateBatches));
    }
}

/**
 * StateBatch: 表示一批连续的 offset 的状态
 */
class StateBatch {
    final long firstOffset;
    final long lastOffset;
    final String deliveryState;  // "Available", "Acquired", "Acknowledged"
    final int deliveryCount;

    StateBatch(long firstOffset, long lastOffset, String deliveryState, int deliveryCount) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.deliveryState = deliveryState;
        this.deliveryCount = deliveryCount;
    }

    @Override
    public String toString() {
        return String.format("[%d-%d:%s(%d)]", firstOffset, lastOffset, deliveryState, deliveryCount);
    }
}

/**
 * SharePartitionKey: 唯一标识一个 share partition
 */
class SharePartitionKey {
    final String groupId;
    final String topicId;
    final int partition;

    SharePartitionKey(String groupId, String topicId, int partition) {
        this.groupId = groupId;
        this.topicId = topicId;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SharePartitionKey)) return false;
        SharePartitionKey that = (SharePartitionKey) o;
        return partition == that.partition &&
               Objects.equals(groupId, that.groupId) &&
               Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicId, partition);
    }

    @Override
    public String toString() {
        return groupId + ":" + topicId + ":" + partition;
    }
}

// ============ TimelineHashMap 实现 ============

/**
 * TimelineHashMap: 支持多版本的 HashMap
 *
 * 核心特性：
 * 1. 每个 key 对应一个 TreeMap<Long, V>，存储 (offset → value)
 * 2. get() 返回 <= currentOffset 的最大 offset 对应的值
 * 3. revertTo(offset) 可以回滚到历史版本
 */
class TimelineHashMap<K, V> {
    private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
    private long currentOffset = 0;

    void setCurrentOffset(long offset) {
        this.currentOffset = offset;
    }

    long getCurrentOffset() {
        return currentOffset;
    }

    void put(K key, V value) {
        data.computeIfAbsent(key, k -> new TreeMap<>())
            .put(currentOffset, value);
    }

    V get(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return null;

        Map.Entry<Long, V> entry = timeline.floorEntry(currentOffset);
        return entry != null ? entry.getValue() : null;
    }

    boolean containsKey(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return false;
        return timeline.floorEntry(currentOffset) != null;
    }

    /**
     * 回滚到指定 offset
     */
    void revertTo(long offset) {
        this.currentOffset = offset;
    }

    /**
     * 获取所有历史版本（用于调试）
     */
    List<Long> getVersions(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return Collections.emptyList();
        return new ArrayList<>(timeline.keySet());
    }

    /**
     * 删除 > offset 的所有版本（释放内存）
     */
    void deleteAfter(long offset) {
        for (TreeMap<Long, V> timeline : data.values()) {
            timeline.tailMap(offset + 1, true).clear();
        }
    }
}

// ============ Enhanced ShareCoordinatorShard ============

/**
 * 增强版的 ShareCoordinatorShard
 *
 * 新增功能：
 * 1. revertToVersion(offset): 回滚到指定版本
 * 2. listVersions(key): 列出所有历史版本
 * 3. getStateAtVersion(key, offset): 获取指定版本的状态
 * 4. createCheckpoint(): 创建检查点
 * 5. revertToCheckpoint(name): 回滚到检查点
 */
class EnhancedShareCoordinatorShard {
    private final TimelineHashMap<SharePartitionKey, ShareGroupState> shareStateMap;
    private final TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;
    private final TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;

    // 用于记录每个 offset 对应的操作描述
    private final Map<Long, String> operationLog;

    // 用于命名的检查点
    private final Map<String, Long> checkpoints;

    private long currentOffset = 0;

    public EnhancedShareCoordinatorShard() {
        this.shareStateMap = new TimelineHashMap<>();
        this.leaderEpochMap = new TimelineHashMap<>();
        this.stateEpochMap = new TimelineHashMap<>();
        this.operationLog = new TreeMap<>();
        this.checkpoints = new HashMap<>();
    }

    /**
     * 写入状态（类似原版的 writeState）
     */
    public void writeState(SharePartitionKey key, ShareGroupState state, String description) {
        System.out.println("\n[Offset " + currentOffset + "] " + description);

        // 更新 offset
        shareStateMap.setCurrentOffset(currentOffset);
        leaderEpochMap.setCurrentOffset(currentOffset);
        stateEpochMap.setCurrentOffset(currentOffset);

        // 写入状态
        shareStateMap.put(key, state);
        leaderEpochMap.put(key, state.leaderEpoch);
        stateEpochMap.put(key, state.stateEpoch);

        // 记录操作
        operationLog.put(currentOffset, description);

        System.out.println("  状态: " + state);
        System.out.println("  版本历史: " + shareStateMap.getVersions(key));

        currentOffset++;
    }

    /**
     * 读取当前状态
     */
    public ShareGroupState readState(SharePartitionKey key) {
        return shareStateMap.get(key);
    }

    /**
     * 新功能 1: 回滚到指定版本
     */
    public void revertToVersion(long targetOffset) {
        if (targetOffset >= currentOffset) {
            System.out.println("\n[警告] 无法回滚到未来的 offset: " + targetOffset);
            return;
        }

        System.out.println("\n[回滚] 从 offset " + currentOffset + " 回滚到 offset " + targetOffset);

        // 回滚所有 TimelineHashMap
        shareStateMap.revertTo(targetOffset);
        leaderEpochMap.revertTo(targetOffset);
        stateEpochMap.revertTo(targetOffset);

        // 更新 currentOffset
        shareStateMap.setCurrentOffset(targetOffset);
        leaderEpochMap.setCurrentOffset(targetOffset);
        stateEpochMap.setCurrentOffset(targetOffset);

        System.out.println("  回滚完成");
        System.out.println("  当前 offset: " + targetOffset);
    }

    /**
     * 新功能 2: 列出某个 key 的所有历史版本
     */
    public List<Long> listVersions(SharePartitionKey key) {
        return shareStateMap.getVersions(key);
    }

    /**
     * 新功能 3: 获取指定版本的状态（不改变当前 offset）
     */
    public ShareGroupState getStateAtVersion(SharePartitionKey key, long offset) {
        // 保存当前 offset
        long savedOffset = shareStateMap.getCurrentOffset();

        // 临时切换到目标 offset
        shareStateMap.setCurrentOffset(offset);
        ShareGroupState state = shareStateMap.get(key);

        // 恢复 offset
        shareStateMap.setCurrentOffset(savedOffset);

        return state;
    }

    /**
     * 新功能 4: 创建命名检查点
     */
    public void createCheckpoint(String name) {
        long offset = shareStateMap.getCurrentOffset();
        checkpoints.put(name, offset);
        System.out.println("\n[检查点] 创建检查点 '" + name + "' at offset " + offset);
    }

    /**
     * 新功能 5: 回滚到检查点
     */
    public void revertToCheckpoint(String name) {
        Long offset = checkpoints.get(name);
        if (offset == null) {
            System.out.println("\n[错误] 检查点 '" + name + "' 不存在");
            System.out.println("  可用检查点: " + checkpoints.keySet());
            return;
        }

        System.out.println("\n[回滚] 恢复到检查点 '" + name + "'");
        revertToVersion(offset);
    }

    /**
     * 显示操作历史
     */
    public void showHistory() {
        System.out.println("\n=== 操作历史 ===");
        for (Map.Entry<Long, String> entry : operationLog.entrySet()) {
            String marker = entry.getKey() == shareStateMap.getCurrentOffset() ? " <-- 当前位置" : "";
            System.out.println("  [Offset " + entry.getKey() + "] " + entry.getValue() + marker);
        }
    }

    /**
     * 显示当前所有状态
     */
    public void showCurrentState(SharePartitionKey key) {
        System.out.println("\n=== 当前状态 ===");
        System.out.println("  当前 offset: " + shareStateMap.getCurrentOffset());
        System.out.println("  State: " + readState(key));
        System.out.println("  Leader Epoch: " + leaderEpochMap.get(key));
        System.out.println("  State Epoch: " + stateEpochMap.get(key));
    }

    /**
     * 清理历史版本（释放内存）
     */
    public void cleanupOldVersions(long keepAfterOffset) {
        System.out.println("\n[清理] 删除 offset <= " + keepAfterOffset + " 的历史版本");
        shareStateMap.deleteAfter(keepAfterOffset);
        leaderEpochMap.deleteAfter(keepAfterOffset);
        stateEpochMap.deleteAfter(keepAfterOffset);
    }
}

// ============ 演示场景 ============

class EnhancedShareCoordinatorDemo {
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  Enhanced ShareCoordinator with Revert Capability    ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        EnhancedShareCoordinatorShard coordinator = new EnhancedShareCoordinatorShard();
        SharePartitionKey key = new SharePartitionKey("group1", "topic1", 0);

        // ========== 场景 1: 正常的状态演进 ==========
        System.out.println("\n【场景 1】正常的状态演进");
        System.out.println("─────────────────────────────");

        // 操作 1: 初始化
        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 0, 1, 0,
                Arrays.asList(
                    new StateBatch(0, 99, "Available", 0)
                )),
            "初始化 share group"
        );

        // 操作 2: 消费者获取消息
        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 0, 1, 1,
                Arrays.asList(
                    new StateBatch(0, 49, "Acquired", 1),
                    new StateBatch(50, 99, "Available", 0)
                )),
            "消费者 A 获取 offset 0-49"
        );

        // 操作 3: 确认消息
        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 50, 1, 2,
                Arrays.asList(
                    new StateBatch(50, 99, "Available", 0)
                )),
            "消费者 A 确认 offset 0-49，startOffset 推进到 50"
        );

        // 创建检查点
        coordinator.createCheckpoint("stable_state");

        // 操作 4: 更多消费
        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 50, 1, 3,
                Arrays.asList(
                    new StateBatch(50, 74, "Acquired", 1),
                    new StateBatch(75, 99, "Available", 0)
                )),
            "消费者 B 获取 offset 50-74"
        );

        coordinator.showCurrentState(key);

        // ========== 场景 2: 查看历史版本 ==========
        System.out.println("\n\n【场景 2】查看历史版本");
        System.out.println("─────────────────────────────");

        List<Long> versions = coordinator.listVersions(key);
        System.out.println("所有版本: " + versions);

        for (Long version : versions) {
            ShareGroupState state = coordinator.getStateAtVersion(key, version);
            System.out.println("  Version " + version + ": " + state);
        }

        // ========== 场景 3: 回滚到历史版本 ==========
        System.out.println("\n\n【场景 3】回滚操作");
        System.out.println("─────────────────────────────");

        System.out.println("\n假设消费者 B 遇到了问题，需要回滚到 offset 2");
        coordinator.revertToVersion(2);
        coordinator.showCurrentState(key);

        System.out.println("\n现在可以重新尝试操作...");
        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 50, 1, 3,
                Arrays.asList(
                    new StateBatch(50, 99, "Acquired", 1)
                )),
            "消费者 C 获取 offset 50-99（重新尝试）"
        );

        // ========== 场景 4: 使用检查点回滚 ==========
        System.out.println("\n\n【场景 4】使用检查点回滚");
        System.out.println("─────────────────────────────");

        // 做一些可能出错的操作
        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 50, 2, 4,
                Arrays.asList(
                    new StateBatch(50, 99, "Available", 0)
                )),
            "错误操作：leaderEpoch 错误地增加到 2"
        );

        coordinator.writeState(key,
            new ShareGroupState("group1", "topic1", 0, 50, 3, 5,
                Arrays.asList(
                    new StateBatch(50, 99, "Available", 0)
                )),
            "错误操作：leaderEpoch 继续错误增长"
        );

        System.out.println("\n发现错误！回滚到之前的稳定状态...");
        coordinator.revertToCheckpoint("stable_state");
        coordinator.showCurrentState(key);

        // ========== 场景 5: 操作历史查看 ==========
        System.out.println("\n\n【场景 5】操作历史");
        System.out.println("─────────────────────────────");
        coordinator.showHistory();

        // ========== 场景 6: 内存清理 ==========
        System.out.println("\n\n【场景 6】清理历史版本");
        System.out.println("─────────────────────────────");
        System.out.println("当前版本列表: " + coordinator.listVersions(key));

        // 保留 offset 2 之后的版本
        coordinator.cleanupOldVersions(2);
        System.out.println("清理后版本列表: " + coordinator.listVersions(key));

        // ========== 总结 ==========
        System.out.println("\n\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║  总结：Revert 功能的价值                              ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");

        System.out.println("\n1. 错误恢复");
        System.out.println("   - 可以快速回滚到正确的状态");
        System.out.println("   - 避免级联错误");

        System.out.println("\n2. 调试支持");
        System.out.println("   - 查看每个版本的状态");
        System.out.println("   - 重现问题");

        System.out.println("\n3. 事务支持");
        System.out.println("   - 可以实现类似事务的 begin/commit/rollback");
        System.out.println("   - 检查点机制类似 savepoint");

        System.out.println("\n4. 时间旅行调试");
        System.out.println("   - 可以在不同版本之间切换");
        System.out.println("   - 分析状态演进过程");

        System.out.println("\n5. 实现原理");
        System.out.println("   - 利用 TimelineHashMap 的 MVCC 特性");
        System.out.println("   - 不需要额外的快照存储");
        System.out.println("   - O(log n) 的版本切换性能");
    }
}
