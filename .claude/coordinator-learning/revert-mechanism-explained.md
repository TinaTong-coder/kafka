# Revert to Previous Version 功能详解

## 目录
1. [功能概述](#功能概述)
2. [使用场景](#使用场景)
3. [实现原理](#实现原理)
4. [API 设计](#api-设计)
5. [完整示例](#完整示例)
6. [性能分析](#性能分析)
7. [与原版 Kafka 的对比](#与原版-kafka-的对比)

---

## 功能概述

**Revert to Previous Version** 是基于 TimelineHashMap 的 MVCC 特性实现的状态回滚功能。

### 核心能力

| 功能 | 描述 | 时间复杂度 |
|------|------|-----------|
| `revertToVersion(offset)` | 回滚到指定 offset | O(1) |
| `listVersions(key)` | 列出所有历史版本 | O(n) |
| `getStateAtVersion(key, offset)` | 查看历史版本（不改变当前状态） | O(log n) |
| `createCheckpoint(name)` | 创建命名检查点 | O(1) |
| `revertToCheckpoint(name)` | 回滚到检查点 | O(1) |

---

## 使用场景

### 场景 1：错误恢复

**问题：** 由于 bug 或配置错误，写入了错误的状态

```java
// 当前状态正确
coordinator.writeState(key, correctState, "正常操作");
coordinator.createCheckpoint("before_update");

// 由于 bug，写入了错误的状态
coordinator.writeState(key, buggyState, "Bug 导致的错误状态");

// 发现错误，立即回滚
coordinator.revertToCheckpoint("before_update");
```

**优势：**
- 秒级恢复
- 无需重启服务
- 避免数据丢失

---

### 场景 2：事务支持

**问题：** 多个操作需要原子性地执行

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

---

### 场景 3：调试和问题重现

**问题：** 生产环境出现问题，需要分析状态演进过程

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

---

### 场景 4：A/B 测试和灰度发布

**问题：** 需要在不同版本之间切换

```java
// 当前稳定版本
coordinator.createCheckpoint("stable_v1");

// 部署新版本
coordinator.writeState(key, newVersionState, "新版本 v2");
coordinator.createCheckpoint("test_v2");

// 测试新版本
boolean success = runTests();

if (!success) {
    // 测试失败，回滚到稳定版本
    coordinator.revertToCheckpoint("stable_v1");
} else {
    // 测试成功，清理老版本
    coordinator.cleanupOldVersions(currentOffset - 1);
}
```

---

### 场景 5：时间旅行调试

**问题：** 需要回到过去的某个时刻，分析当时的状态

```java
// 显示操作历史
coordinator.showHistory();
// [Offset 0] 初始化
// [Offset 5] 消费者 A 获取消息
// [Offset 10] 消费者 A 确认
// [Offset 15] 消费者 B 获取消息
// [Offset 20] 消费者 B 超时 <-- 问题发生

// 回到问题发生前
coordinator.revertToVersion(15);

// 查看当时的状态
ShareGroupState stateBefore = coordinator.readState(key);
System.out.println("问题发生前: " + stateBefore);

// 前进到问题发生后
coordinator.revertToVersion(20);
ShareGroupState stateAfter = coordinator.readState(key);
System.out.println("问题发生后: " + stateAfter);

// 对比差异
compareDiff(stateBefore, stateAfter);
```

---

## 实现原理

### 1. TimelineHashMap 的 MVCC 特性

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

---

### 2. 回滚机制

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

---

### 3. 多个 TimelineHashMap 的同步

ShareCoordinatorShard 维护多个 TimelineHashMap，需要同步回滚：

```java
class EnhancedShareCoordinatorShard {
    private final TimelineHashMap<SharePartitionKey, ShareGroupState> shareStateMap;
    private final TimelineHashMap<SharePartitionKey, Integer> leaderEpochMap;
    private final TimelineHashMap<SharePartitionKey, Integer> stateEpochMap;

    void revertToVersion(long targetOffset) {
        // 同步回滚所有 TimelineHashMap
        shareStateMap.revertTo(targetOffset);
        leaderEpochMap.revertTo(targetOffset);
        stateEpochMap.revertTo(targetOffset);

        // 更新 currentOffset
        shareStateMap.setCurrentOffset(targetOffset);
        leaderEpochMap.setCurrentOffset(targetOffset);
        stateEpochMap.setCurrentOffset(targetOffset);
    }
}
```

---

### 4. 检查点机制

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

---

### 5. 内存管理

**问题：** 历史版本会占用内存

**解决方案：** 定期清理老版本

```java
void cleanupOldVersions(long keepAfterOffset) {
    for (TreeMap<Long, V> timeline : data.values()) {
        // 删除所有 <= keepAfterOffset 的版本
        timeline.headMap(keepAfterOffset, true).clear();
    }
}
```

**清理策略：**

| 策略 | 适用场景 | 实现 |
|------|---------|------|
| 保留最近 N 个版本 | 调试场景 | 当版本数 > N 时，删除最老的版本 |
| 保留最近 T 时间的版本 | 生产环境 | 根据时间戳删除 |
| 按 HW 清理 | Kafka 集成 | 当 HW 推进后，删除 < HW 的版本 |
| 检查点清理 | 事务场景 | 事务提交后，删除检查点之前的版本 |

---

## API 设计

### 核心接口

```java
public interface VersionedCoordinator {
    /**
     * 回滚到指定版本
     * @param targetOffset 目标 offset
     * @throws IllegalArgumentException 如果 targetOffset >= currentOffset
     */
    void revertToVersion(long targetOffset);

    /**
     * 列出某个 key 的所有历史版本
     * @param key 分区 key
     * @return 版本列表（按 offset 排序）
     */
    List<Long> listVersions(SharePartitionKey key);

    /**
     * 获取指定版本的状态（不改变当前 offset）
     * @param key 分区 key
     * @param offset 目标 offset
     * @return 该版本的状态，如果不存在返回 null
     */
    ShareGroupState getStateAtVersion(SharePartitionKey key, long offset);

    /**
     * 创建命名检查点
     * @param name 检查点名称
     */
    void createCheckpoint(String name);

    /**
     * 回滚到检查点
     * @param name 检查点名称
     * @throws IllegalArgumentException 如果检查点不存在
     */
    void revertToCheckpoint(String name);

    /**
     * 删除检查点
     * @param name 检查点名称
     */
    void deleteCheckpoint(String name);

    /**
     * 清理历史版本
     * @param keepAfterOffset 保留 > keepAfterOffset 的版本
     */
    void cleanupOldVersions(long keepAfterOffset);
}
```

---

### 扩展接口（高级功能）

```java
public interface AdvancedVersionedCoordinator extends VersionedCoordinator {
    /**
     * 创建快照（深拷贝当前状态）
     * @param name 快照名称
     */
    void createSnapshot(String name);

    /**
     * 恢复快照（替换当前状态）
     * @param name 快照名称
     */
    void restoreSnapshot(String name);

    /**
     * 导出历史记录
     * @param key 分区 key
     * @param fromOffset 起始 offset
     * @param toOffset 结束 offset
     * @return 历史记录列表
     */
    List<VersionedState<ShareGroupState>> exportHistory(
        SharePartitionKey key, long fromOffset, long toOffset);

    /**
     * 比较两个版本的差异
     * @param key 分区 key
     * @param offset1 版本 1
     * @param offset2 版本 2
     * @return 差异信息
     */
    StateDiff diffVersions(SharePartitionKey key, long offset1, long offset2);
}
```

---

## 完整示例

### 示例 1：错误恢复

```java
EnhancedShareCoordinatorShard coordinator = new EnhancedShareCoordinatorShard();
SharePartitionKey key = new SharePartitionKey("group1", "topic1", 0);

// 正常操作序列
coordinator.writeState(key, initialState, "初始化");
coordinator.writeState(key, state1, "消费者 A 获取消息");
coordinator.writeState(key, state2, "消费者 A 确认");

// 创建稳定检查点
coordinator.createCheckpoint("stable");

// 执行可能失败的操作
try {
    coordinator.writeState(key, riskyState1, "风险操作 1");
    coordinator.writeState(key, riskyState2, "风险操作 2");

    if (detectError()) {
        throw new Exception("检测到错误");
    }
} catch (Exception e) {
    // 回滚到稳定状态
    coordinator.revertToCheckpoint("stable");
    System.out.println("已回滚到稳定状态");
}
```

---

### 示例 2：事务实现

```java
class TransactionCoordinator {
    private final EnhancedShareCoordinatorShard coordinator;
    private final Stack<Long> transactionStack = new Stack<>();

    void beginTransaction() {
        long txOffset = coordinator.getCurrentOffset();
        transactionStack.push(txOffset);
        System.out.println("BEGIN TX at offset " + txOffset);
    }

    void commitTransaction() {
        if (!transactionStack.isEmpty()) {
            long txOffset = transactionStack.pop();
            System.out.println("COMMIT TX from offset " + txOffset);
            // 可选：清理 txOffset 之前的版本
            coordinator.cleanupOldVersions(txOffset);
        }
    }

    void rollbackTransaction() {
        if (!transactionStack.isEmpty()) {
            long txOffset = transactionStack.pop();
            coordinator.revertToVersion(txOffset);
            System.out.println("ROLLBACK TX to offset " + txOffset);
        }
    }

    void execute(TransactionCallback callback) {
        beginTransaction();
        try {
            callback.run();
            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction();
            throw e;
        }
    }
}

// 使用示例
TransactionCoordinator txCoordinator = new TransactionCoordinator();

txCoordinator.execute(() -> {
    coordinator.writeState(key1, state1, "操作 1");
    coordinator.writeState(key2, state2, "操作 2");
    coordinator.writeState(key3, state3, "操作 3");

    if (!validateConsistency()) {
        throw new RuntimeException("一致性检查失败");
    }
});
```

---

### 示例 3：时间旅行调试

```java
class TimelineDebugger {
    private final EnhancedShareCoordinatorShard coordinator;

    void analyzeProblem(SharePartitionKey key, long problemOffset) {
        System.out.println("=== 问题分析 ===");

        // 获取所有版本
        List<Long> versions = coordinator.listVersions(key);
        System.out.println("历史版本: " + versions);

        // 找到问题发生前的版本
        long beforeOffset = -1;
        for (Long version : versions) {
            if (version < problemOffset) {
                beforeOffset = version;
            }
        }

        // 对比问题前后的状态
        ShareGroupState before = coordinator.getStateAtVersion(key, beforeOffset);
        ShareGroupState after = coordinator.getStateAtVersion(key, problemOffset);

        System.out.println("\n问题发生前 (offset " + beforeOffset + "):");
        System.out.println("  " + before);

        System.out.println("\n问题发生后 (offset " + problemOffset + "):");
        System.out.println("  " + after);

        System.out.println("\n差异:");
        printDiff(before, after);

        // 回到问题发生前，尝试重现
        coordinator.revertToVersion(beforeOffset);
        System.out.println("\n已回到 offset " + beforeOffset + "，可以尝试重现问题");
    }

    void printDiff(ShareGroupState before, ShareGroupState after) {
        if (before.startOffset != after.startOffset) {
            System.out.println("  startOffset: " + before.startOffset + " → " + after.startOffset);
        }
        if (before.leaderEpoch != after.leaderEpoch) {
            System.out.println("  leaderEpoch: " + before.leaderEpoch + " → " + after.leaderEpoch);
        }
        if (before.stateEpoch != after.stateEpoch) {
            System.out.println("  stateEpoch: " + before.stateEpoch + " → " + after.stateEpoch);
        }
        // ... 更多字段对比
    }
}
```

---

## 性能分析

### 时间复杂度

| 操作 | 时间复杂度 | 说明 |
|------|-----------|------|
| `writeState()` | O(log n) | TreeMap.put() |
| `readState()` | O(log n) | TreeMap.floorEntry() |
| `revertToVersion()` | O(1) | 只修改 currentOffset |
| `listVersions()` | O(k) | k = 版本数 |
| `getStateAtVersion()` | O(log n) | 临时修改 offset + get() |
| `createCheckpoint()` | O(1) | HashMap.put() |
| `revertToCheckpoint()` | O(1) | HashMap.get() + revertToVersion() |
| `cleanupOldVersions()` | O(m × k) | m = key 数，k = 待删除版本数 |

---

### 空间复杂度

**内存占用 = 基础状态大小 × 版本数**

**示例：**
- 单个 ShareGroupState: ~1 KB
- 保留 100 个版本: ~100 KB
- 1000 个 partition: ~100 MB

**优化策略：**
1. **增量存储**：只存储变更的字段
2. **压缩**：使用更紧凑的数据结构
3. **定期清理**：删除过期版本
4. **分层存储**：热数据在内存，冷数据持久化

---

### 性能对比

| 场景 | 无版本控制 | 完整快照 | TimelineHashMap (本实现) |
|------|-----------|---------|-------------------------|
| **写入性能** | O(1) | O(n) 拷贝 | O(log n) |
| **读取性能** | O(1) | O(1) | O(log n) |
| **回滚性能** | N/A | O(n) 拷贝 | O(1) |
| **内存占用** | 1x | k × n (k=版本数) | k × Δ (增量) |
| **支持并发读** | 否 | 是 | 是 |

---

## 与原版 Kafka 的对比

### 原版 Kafka ShareCoordinator

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

---

### 增强版 ShareCoordinator

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
    ShareGroupState getStateAtVersion(SharePartitionKey key, long offset) { ... }

    // 支持检查点
    void createCheckpoint(String name) { ... }
    void revertToCheckpoint(String name) { ... }
}
```

**优势：**
- ✅ 支持回滚
- ✅ 支持查看历史
- ✅ 支持事务语义
- ✅ 便于调试和问题分析
- ⚠️ 内存占用增加（需要清理策略）

---

## 总结

### Revert 功能的价值

| 维度 | 价值 |
|------|------|
| **可靠性** | 快速从错误中恢复，降低故障影响 |
| **可调试性** | 时间旅行调试，快速定位问题 |
| **可测试性** | 回滚测试数据，无需重启服务 |
| **灵活性** | 支持事务、A/B测试等高级功能 |

---

### 实现要点

1. **利用 MVCC**：TimelineHashMap 天然支持多版本
2. **低成本回滚**：修改 currentOffset 即可，无需数据拷贝
3. **内存管理**：定期清理历史版本
4. **同步一致性**：多个 TimelineHashMap 需要同步回滚
5. **检查点机制**：提供用户友好的回滚接口

---

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

---

### 下一步优化

1. **增量存储**：只存储变更的字段，减少内存占用
2. **持久化**：将历史版本持久化到磁盘
3. **压缩**：使用更紧凑的数据结构
4. **分布式快照**：支持跨 partition 的一致性快照
5. **自动清理策略**：基于时间、版本数、内存占用自动清理

---

## 参考资料

- **Kafka TimelineHashMap**: `org.apache.kafka.timeline.TimelineHashMap`
- **MVCC**: Multi-Version Concurrency Control
- **Database SAVEPOINT**: SQL 事务检查点
- **Git**: 版本控制系统（类似的回滚机制）
- **ZooKeeper**: 支持历史版本查询的分布式协调服务
