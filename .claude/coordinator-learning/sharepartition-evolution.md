# SharePartition 设计演化：从简单到复杂

本文档展示 SharePartition 如何从最简单的设计一步步演化到 Kafka 实际实现的复杂版本。每个版本都解决前一个版本的具体问题。

---

## V1: 最简单的共享队列 - Round Robin

### 设计

```
SharePartition {
    Queue<Record> records;

    fetch(consumer) {
        record = records.poll();
        return record;
    }
}
```

### 特点
- 简单的 FIFO 队列
- 每个 record 只发给一个 consumer
- 发出去就删除

### 问题

**问题 1：Consumer 处理失败怎么办？**

```
Consumer-A fetch → record-1
Consumer-A 处理时崩溃了
record-1 丢失了！
```

**问题 2：无法保证至少一次语义（At-Least-Once）**

---

## V2: 引入状态管理 - ACQUIRED 状态

### 为什么需要这个复杂度？

**需求：Consumer 失败后，record 要能重新投递给其他 consumer**

### 设计

```java
enum RecordState {
    AVAILABLE,
    ACQUIRED,
    ACKNOWLEDGED
}

class InFlightRecord {
    Record record;
    RecordState state;
    String memberId;  // 谁拿走了
}

SharePartition {
    Map<Long, InFlightRecord> records;  // offset → record

    fetch(consumer) {
        for (record : records) {
            if (record.state == AVAILABLE) {
                record.state = ACQUIRED;
                record.memberId = consumer;
                return record;
            }
        }
        return null;
    }

    acknowledge(consumer, offset, type) {
        record = records.get(offset);
        if (record.memberId == consumer) {
            if (type == ACCEPT) {
                record.state = ACKNOWLEDGED;
            } else if (type == REJECT) {
                record.state = AVAILABLE;  // 放回队列
                record.memberId = null;
            }
        }
    }
}
```

### 流程

```
Time    State                   Consumer-A          Consumer-B
  |
  1     offset-10: AVAILABLE
  |                             fetch() →
  2     offset-10: ACQUIRED     ← record-10
        memberId: A
  |
  3                                                 fetch() →
        offset-10: ACQUIRED     (skip, 已被A拿走)
        (state != AVAILABLE)                        ← null
  |
  4                             处理失败
                                acknowledge(REJECT) →
  5     offset-10: AVAILABLE
        memberId: null
  |
  6                                                 fetch() →
  7     offset-10: ACQUIRED                         ← record-10
        memberId: B
```

### 解决的问题
- ✅ Consumer 失败后可以重试
- ✅ 保证 At-Least-Once
- ✅ 防止重复投递（同一时刻只有一个 consumer 持有 record）

### 新问题

**问题 3：Consumer 拿了 record 后崩溃，永远不 ACK 怎么办？**

```
Consumer-A fetch → record-10 (ACQUIRED)
Consumer-A 崩溃
record-10 永远卡在 ACQUIRED 状态，别人拿不到！
```

---

## V3: 引入 Acquisition Lock Timeout

### 为什么需要这个复杂度？

**需求：Consumer 崩溃后，要能自动释放锁定的 records**

### 设计

```java
class InFlightRecord {
    Record record;
    RecordState state;
    String memberId;
    long acquisitionLockTimeoutMs;  // ← 新增：超时时间
    long acquiredTimestamp;         // ← 新增：拿走的时间
}

SharePartition {
    Map<Long, InFlightRecord> records;

    fetch(consumer) {
        // 先检查超时的 records
        releaseTimedOutRecords();

        for (record : records) {
            if (record.state == AVAILABLE) {
                record.state = ACQUIRED;
                record.memberId = consumer;
                record.acquiredTimestamp = now();
                return record;
            }
        }
        return null;
    }

    void releaseTimedOutRecords() {
        for (record : records) {
            if (record.state == ACQUIRED) {
                long elapsed = now() - record.acquiredTimestamp;
                if (elapsed > record.acquisitionLockTimeoutMs) {
                    // 超时了，释放回去
                    record.state = AVAILABLE;
                    record.memberId = null;
                }
            }
        }
    }
}
```

### 流程

```
Time    State                   Consumer-A
  |
  0     offset-10: AVAILABLE
  |                             fetch() →
  1     offset-10: ACQUIRED     ← record-10
        memberId: A
        acquiredTime: 1000
        timeout: 30000
  |
  |     [Consumer-A 崩溃]
  |
 31     (定期检查)
        elapsed = 31000 - 1000 = 30000 > 30000 ✓
        offset-10: AVAILABLE     ← 自动释放
        memberId: null
```

### 解决的问题
- ✅ Consumer 崩溃后自动释放
- ✅ 防止 records 永久卡住

### 新问题

**问题 4：每次 fetch 都要遍历所有 records 检查超时，太慢了！**

如果有 100 万条 records：
```java
releaseTimedOutRecords() {
    for (1,000,000 records) {  // ← O(n) 太慢！
        check timeout...
    }
}
```

**问题 5：每个 record 单独管理太浪费内存！**

```
1 million records × (state + memberId + timestamp + timeout)
= 大量内存
```

---

## V4: 引入 Batch 管理 - InFlightBatch

### 为什么需要这个复杂度？

**需求 1：减少内存占用 - 批量管理状态**
**需求 2：提高性能 - 批量操作**

### 设计

```java
class InFlightBatch {
    long firstOffset;
    long lastOffset;
    int batchSize;

    RecordState batchState;  // ← 整个 batch 的状态
    String memberId;
    long acquiredTimestamp;

    // offset 级别的状态（按需初始化）
    Map<Long, OffsetMetadata> offsetState;  // null 表示未初始化
}

SharePartition {
    TreeMap<Long, InFlightBatch> cachedState;  // baseOffset → batch

    fetch(consumer, maxRecords) {
        for (batch : cachedState) {
            if (batch.batchState == AVAILABLE) {
                // 整个 batch 一起获取
                batch.batchState = ACQUIRED;
                batch.memberId = consumer;
                batch.acquiredTimestamp = now();
                return batch.records[0:maxRecords];
            }
        }
    }
}
```

### 优化效果

**内存优化：**
```
Before V4:
  1000 records × 每个都有 state/memberId/timestamp
  = 1000 个对象

After V4:
  1 batch (包含 1000 records) × 1 个 state/memberId/timestamp
  = 1 个对象
```

**性能优化：**
```
Before V4: O(所有 records 数量)
After V4:  O(batch 数量) - 通常少 100-1000 倍
```

### 解决的问题
- ✅ 大幅减少内存占用
- ✅ 提高检查超时的性能
- ✅ 批量操作更高效

### 新问题

**问题 6：不同 consumer 处理速度不同，怎么办？**

```
Batch: offset 100-199 (100 条)

Consumer-A (慢): 只想要 10 条
Consumer-B (快): 想要 50 条

如果整个 batch 一起给 A，浪费了！
其他 90 条 B 也拿不到。
```

**问题 7：部分成功怎么办？**

```
Consumer-A fetch batch 100-199
  处理 100-149: 成功 ✓
  处理 150-199: 失败 ✗

怎么 ACK？
- 全部 ACCEPT？ 不对，150-199 失败了
- 全部 REJECT？ 不对，100-149 成功了
```

---

## V5: 引入 Offset-Level 状态追踪

### 为什么需要这个复杂度？

**需求 1：支持部分 fetch - 一个 batch 可以被多个 consumer 分割获取**
**需求 2：支持部分 ACK - 一个 batch 内不同 offset 可以有不同状态**

### 设计

```java
class OffsetMetadata {
    RecordState state;
    String memberId;
    long acquiredTimestamp;
    byte deliveryCount;  // 投递次数
}

class InFlightBatch {
    long firstOffset;
    long lastOffset;

    // Batch 级别状态（优化：如果所有 offset 状态一样）
    RecordState batchState;
    String batchMemberId;

    // Offset 级别状态（按需初始化，只在需要时展开）
    Map<Long, OffsetMetadata> offsetState;  // ← 关键！

    void maybeInitializeOffsetStateUpdate() {
        if (offsetState == null) {
            offsetState = new HashMap<>();
            // 从 batch 状态初始化每个 offset 的状态
            for (offset = firstOffset; offset <= lastOffset; offset++) {
                offsetState.put(offset, new OffsetMetadata(
                    batchState,
                    batchMemberId,
                    acquiredTimestamp
                ));
            }
        }
    }
}

SharePartition {
    acquire(consumer, maxRecords) {
        for (batch : cachedState) {
            // 检查是否需要 offset 级别追踪
            boolean needOffsetTracking =
                (请求只要部分 offset) || (已经有 offsetState);

            if (needOffsetTracking) {
                batch.maybeInitializeOffsetStateUpdate();

                // 逐个 offset 检查和获取
                int acquired = 0;
                for (offset = requestStart; offset <= requestEnd; offset++) {
                    OffsetMetadata meta = batch.offsetState.get(offset);
                    if (meta.state == AVAILABLE) {
                        meta.state = ACQUIRED;
                        meta.memberId = consumer;
                        acquired++;
                        if (acquired >= maxRecords) break;
                    }
                }
            } else {
                // 整个 batch 获取（快速路径）
                if (batch.batchState == AVAILABLE) {
                    batch.batchState = ACQUIRED;
                    batch.batchMemberId = consumer;
                }
            }
        }
    }

    acknowledge(consumer, ackBatches) {
        for (ackBatch : ackBatches) {
            batch = cachedState.get(ackBatch.firstOffset);

            if (batch.offsetState != null) {
                // Offset 级别 ACK
                for (ack : ackBatch.acknowledgeTypes) {
                    for (offset = ack.firstOffset; offset <= ack.lastOffset; offset++) {
                        OffsetMetadata meta = batch.offsetState.get(offset);
                        if (ack.type == ACCEPT) {
                            meta.state = ACKNOWLEDGED;
                        } else if (ack.type == REJECT) {
                            meta.state = AVAILABLE;
                            meta.memberId = null;
                        }
                    }
                }
            } else {
                // Batch 级别 ACK（快速路径）
                if (ackBatch.type == ACCEPT) {
                    batch.batchState = ACKNOWLEDGED;
                } else if (ackBatch.type == REJECT) {
                    batch.batchState = AVAILABLE;
                    batch.batchMemberId = null;
                }
            }
        }
    }
}
```

### 流程示例

```
Batch: offset 100-109 (10 条)

Time    State                                Consumer-A              Consumer-B
  |
  1     batchState: AVAILABLE
        offsetState: null
  |                                          fetch(maxRecords=5) →
  2     检测到：请求是 subset                  需要 offset tracking
        maybeInitializeOffsetStateUpdate()
        offsetState: {
          100: AVAILABLE, 101: AVAILABLE, ...
        }
  |
  3     offsetState: {
          100: ACQUIRED (A)                  ← offset 100-104
          101: ACQUIRED (A)
          102: ACQUIRED (A)
          103: ACQUIRED (A)
          104: ACQUIRED (A)
          105: AVAILABLE
          106: AVAILABLE
          ...
        }
  |
  4                                                                  fetch(maxRecords=3) →
  5     offsetState: {
          100: ACQUIRED (A)
          ...
          104: ACQUIRED (A)
          105: ACQUIRED (B)                                          ← offset 105-107
          106: ACQUIRED (B)
          107: ACQUIRED (B)
          108: AVAILABLE
          109: AVAILABLE
        }
  |
  6                                          处理 100-104
                                             100-102: 成功
                                             103-104: 失败
                                             acknowledge([
                                               100-102: ACCEPT,
                                               103-104: REJECT
                                             ]) →
  7     offsetState: {
          100: ACKNOWLEDGED ✓
          101: ACKNOWLEDGED ✓
          102: ACKNOWLEDGED ✓
          103: AVAILABLE (释放)
          104: AVAILABLE (释放)
          105: ACQUIRED (B)
          ...
        }
```

### 解决的问题
- ✅ 支持部分 fetch（一个 batch 可以被多个 consumer 分享）
- ✅ 支持细粒度 ACK（部分成功、部分失败）
- ✅ 提高并发度（不同 consumer 可以处理同一个 batch 的不同部分）

### 优化策略

**两级状态管理：**

1. **Batch 级别**（快速路径）
   - 如果整个 batch 状态一致，只用 `batchState`
   - 内存少，检查快

2. **Offset 级别**（慢速路径，按需初始化）
   - 只有在需要时才展开 `offsetState`
   - 支持复杂场景

### 新问题

**问题 8：怎么知道哪些 records 可以被删除？**

```
offsetState: {
  100: ACKNOWLEDGED
  101: ACKNOWLEDGED
  102: ACKNOWLEDGED
  103: AVAILABLE (被 reject 了多次)
  104: ACKNOWLEDGED
  ...
}

可以删除 100-102，但 103 还在等待重试
怎么高效找到可以删除的连续区间？
```

**问题 9：Delivery count 怎么管理？**

某些 records 可能被反复 reject：
```
offset-103:
  Consumer-A: REJECT
  Consumer-B: REJECT
  Consumer-C: REJECT
  ...
  已经投递 10 次了，要停止重试！
```

---

## V6: 引入 ARCHIVED 状态和 Delivery Count

### 为什么需要这个复杂度？

**需求 1：防止毒丸消息（Poison Pill）- 永远处理失败的 record**
**需求 2：管理 offset 推进 - 确定哪些可以删除**

### 设计

```java
enum RecordState {
    AVAILABLE,
    ACQUIRED,
    ACKNOWLEDGED,
    ARCHIVED  // ← 新增：投递次数超限
}

class OffsetMetadata {
    RecordState state;
    String memberId;
    long acquiredTimestamp;
    byte deliveryCount;  // ← 关键：投递次数
}

class InFlightBatch {
    // ... 之前的字段 ...
    byte maxDeliveryCount;  // 配置：最大投递次数

    InFlightState tryUpdateOffsetState(
        long offset,
        RecordState newState,
        String memberId
    ) {
        OffsetMetadata meta = offsetState.get(offset);

        if (newState == ACQUIRED) {
            meta.deliveryCount++;

            // 检查是否超过最大投递次数
            if (meta.deliveryCount > maxDeliveryCount) {
                meta.state = ARCHIVED;  // ← 不再投递
                return InFlightState.ARCHIVED;
            }

            meta.state = ACQUIRED;
            meta.memberId = memberId;
        } else if (newState == ACKNOWLEDGED) {
            meta.state = ACKNOWLEDGED;
        } else if (newState == AVAILABLE) {
            // REJECT - 放回队列，但 deliveryCount 保留
            meta.state = AVAILABLE;
            meta.memberId = null;
        }

        return InFlightState.UPDATED;
    }
}

SharePartition {
    acquire(consumer, maxRecords) {
        for (batch : cachedState) {
            for (offset : batch.offsets) {
                meta = batch.offsetState.get(offset);
                if (meta.state == AVAILABLE) {
                    result = batch.tryUpdateOffsetState(
                        offset, ACQUIRED, consumer
                    );

                    if (result == ARCHIVED) {
                        // 跳过这个 offset，它已经被标记为 ARCHIVED
                        continue;
                    }

                    // 成功 acquire
                    acquiredRecords.add(offset);
                }
            }
        }
    }

    // 定期清理已完成的 batches
    void maybeUpdateNextFetchOffset() {
        // 找到第一个不是 ACKNOWLEDGED/ARCHIVED 的 offset
        for (batch : cachedState) {
            if (batch.offsetState != null) {
                for (offset : batch) {
                    meta = batch.offsetState.get(offset);
                    if (meta.state != ACKNOWLEDGED &&
                        meta.state != ARCHIVED) {
                        nextFetchOffset = offset;
                        return;
                    }
                }
            } else {
                if (batch.batchState == ACKNOWLEDGED ||
                    batch.batchState == ARCHIVED) {
                    continue;
                } else {
                    nextFetchOffset = batch.firstOffset;
                    return;
                }
            }
        }
    }
}
```

### 流程示例

```
配置: maxDeliveryCount = 3

Time    offset-103 状态              操作
  |
  1     AVAILABLE
        deliveryCount: 0             Consumer-A fetch →
  |
  2     ACQUIRED (A)
        deliveryCount: 1             Consumer-A 处理失败
                                     acknowledge(REJECT) →
  |
  3     AVAILABLE
        deliveryCount: 1 (保留!)     Consumer-B fetch →
  |
  4     ACQUIRED (B)
        deliveryCount: 2             Consumer-B 处理失败
                                     acknowledge(REJECT) →
  |
  5     AVAILABLE
        deliveryCount: 2             Consumer-C fetch →
  |
  6     ACQUIRED (C)
        deliveryCount: 3             Consumer-C 处理失败
                                     acknowledge(REJECT) →
  |
  7     deliveryCount: 3 == maxDeliveryCount
        状态: ARCHIVED               ← 不再投递
  |
  8     (后续 fetch 都会跳过 offset-103)
```

### 解决的问题
- ✅ 防止毒丸消息无限重试
- ✅ 可以推进 offset（ARCHIVED 的 offset 可以跳过）
- ✅ 保护系统资源（不会一直卡在坏数据上）

### 新问题

**问题 10：状态要持久化吗？Broker 重启怎么办？**

```
Broker 重启前:
  offset-100: ACQUIRED (Consumer-A)
  offset-101: ACKNOWLEDGED
  offset-102: AVAILABLE, deliveryCount=2

Broker 重启后:
  所有状态都丢失了！
  offset-100 可能被重复投递
  offset-102 的 deliveryCount 丢失，可能超过最大投递次数
```

---

## V7: 引入持久化 - Persister

### 为什么需要这个复杂度？

**需求：Broker 重启后恢复状态，保证 exactly-once/at-least-once 语义**

### 设计

```java
interface Persister {
    CompletableFuture<Void> persistState(
        String groupId,
        TopicIdPartition partition,
        List<PartitionStateBatchData> stateBatches
    );

    CompletableFuture<PartitionAllData> loadState(
        String groupId,
        TopicIdPartition partition
    );
}

class SharePartition {
    Persister persister;

    acknowledge(consumer, ackBatches) {
        List<InFlightState> updatedStates = new ArrayList<>();

        // 1. 更新内存状态
        for (ackBatch : ackBatches) {
            updatedStates.add(updateInMemoryState(ackBatch));
        }

        // 2. 持久化到 __share_group_state
        List<PartitionStateBatchData> stateBatches =
            convertToStateBatches(updatedStates);

        CompletableFuture<Void> future = persister.persistState(
            groupId,
            topicIdPartition,
            stateBatches
        );

        return future;
    }

    // Broker 重启时调用
    void initialize() {
        CompletableFuture<PartitionAllData> future =
            persister.loadState(groupId, topicIdPartition);

        future.thenAccept(data -> {
            // 恢复所有 batch 的状态
            for (stateBatch : data.stateBatches) {
                InFlightBatch batch = new InFlightBatch(
                    stateBatch.firstOffset,
                    stateBatch.lastOffset
                );

                // 恢复 offset 状态
                for (offsetState : stateBatch.offsetStates) {
                    batch.offsetState.put(
                        offsetState.offset,
                        new OffsetMetadata(
                            offsetState.state,
                            offsetState.memberId,
                            offsetState.deliveryCount
                        )
                    );
                }

                cachedState.put(batch.firstOffset, batch);
            }

            state = SharePartitionState.ACTIVE;
        });
    }
}
```

### 持久化数据结构

```java
// 写入 __share_group_state topic
class PartitionStateBatchData {
    long firstOffset;
    long lastOffset;
    byte deliveryCount;  // batch 级别（如果一致）

    // Offset 级别状态（如果不一致）
    List<OffsetStateData> offsetStates;
}

class OffsetStateData {
    long offset;
    RecordState state;
    byte deliveryCount;
}
```

### 流程

```
正常运行:
  Memory:
    offset-100: ACQUIRED (A), deliveryCount=1
    offset-101: ACKNOWLEDGED

  acknowledge(100-101: ACCEPT) →

  Memory:
    offset-100: ACKNOWLEDGED
    offset-101: ACKNOWLEDGED

  Persist to __share_group_state:
    write record: {
      firstOffset: 100,
      lastOffset: 101,
      state: ACKNOWLEDGED,
      deliveryCount: 1
    }

Broker 重启:
  1. Load from __share_group_state
  2. Restore memory state:
     offset-100: ACKNOWLEDGED
     offset-101: ACKNOWLEDGED
  3. 继续服务
```

### 解决的问题
- ✅ Broker 重启后恢复状态
- ✅ 保证语义（不会丢失 ACK 状态）
- ✅ deliveryCount 不会重置

### 新问题

**问题 11：每次 ACK 都写磁盘太慢了！**

```
每个 consumer ACK 1 条 record
→ 写一次 __share_group_state
→ 磁盘 I/O 延迟 ~1-5ms

如果 1000 个 consumer 同时 ACK
→ 1000 次写入
→ 延迟和吞吐都受影响
```

**问题 12：读取历史数据和最新数据冲突？**

```
Consumer-A 正在读 offset-1000 的数据（历史）
Consumer-B 正在读 offset-5000 的数据（最新）

两个 fetch 请求都要从 Kafka log 读数据
能否优化？
```

---

## V8: 引入缓存和批量持久化（Kafka 实际实现）

### 为什么需要这个复杂度？

**需求 1：提高持久化性能 - 批量写入**
**需求 2：减少重复读取 - 缓存 records**
**需求 3：管理并发 - 读写锁**

### 完整设计

```java
class SharePartition {
    // 核心数据结构
    TreeMap<Long, InFlightBatch> cachedState;  // offset → batch

    // 并发控制
    ReadWriteLock lock;

    // 持久化
    Persister persister;

    // 延迟 fetch 支持
    DelayedShareFetchPurgatory purgatory;

    // 配置
    int maxInFlightMessages;
    int maxDeliveryCount;
    long acquisitionLockTimeoutMs;

    // === 核心方法 ===

    // 1. Fetch records
    public ShareAcquiredRecords acquire(
        String memberId,
        int maxFetchRecords,
        FetchPartitionData fetchPartitionData
    ) {
        lock.writeLock().lock();
        try {
            // 检查是否已经在 cache 中
            NavigableMap<Long, InFlightBatch> subMap =
                cachedState.subMap(firstOffset, lastOffset);

            if (subMap.isEmpty()) {
                // 新数据，创建 batch
                return acquireNewBatchRecords(memberId, fetchData);
            } else {
                // 已存在，检查是否可以 acquire
                return acquireExistingBatchRecords(memberId, subMap);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 2. Acknowledge records
    public CompletableFuture<Void> acknowledge(
        String memberId,
        List<ShareAcknowledgementBatch> ackBatches
    ) {
        List<PersisterStateBatch> stateBatches = new ArrayList<>();

        lock.writeLock().lock();
        try {
            for (ackBatch : ackBatches) {
                // 更新内存状态
                InFlightState state = updateInMemoryState(ackBatch);

                // 收集需要持久化的数据
                if (state.shouldPersist()) {
                    stateBatches.add(state.toStateBatch());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        // 批量持久化（在锁外执行）
        if (!stateBatches.isEmpty()) {
            return persister.persistState(
                groupId,
                topicIdPartition,
                stateBatches
            ).thenRun(() -> {
                // 持久化成功后，触发等待的 fetch
                purgatory.checkAndComplete(
                    new DelayedShareFetchKey(groupId, topicIdPartition)
                );
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    // 3. 批量管理和优化
    private AcquiredRecords acquireNewBatchRecords(
        String memberId,
        Iterable<RecordBatch> batches,
        long firstOffset,
        long lastOffset,
        int maxRecords
    ) {
        // 限制 in-flight 数量
        if (cachedState.size() >= maxInFlightMessages) {
            return AcquiredRecords.empty();
        }

        InFlightBatch batch = new InFlightBatch(
            firstOffset,
            lastOffset
        );

        // 尝试 acquire 整个 batch
        InFlightState state = batch.tryUpdateBatchState(
            RecordState.ACQUIRED,
            true,
            maxDeliveryCount,
            memberId
        );

        if (state == InFlightState.UPDATED) {
            cachedState.put(firstOffset, batch);
            return AcquiredRecords.from(batch, maxRecords);
        }

        return AcquiredRecords.empty();
    }

    // 4. Timeout 管理
    void releaseAcquisitionLock(String memberId) {
        lock.writeLock().lock();
        try {
            long now = time.milliseconds();

            for (InFlightBatch batch : cachedState.values()) {
                if (batch.offsetState != null) {
                    // Offset 级别检查
                    for (OffsetMetadata meta : batch.offsetState.values()) {
                        if (meta.state == ACQUIRED &&
                            meta.memberId.equals(memberId) &&
                            now - meta.acquiredTimestamp > acquisitionLockTimeoutMs) {
                            // 超时，释放
                            meta.state = AVAILABLE;
                            meta.memberId = null;
                        }
                    }
                } else {
                    // Batch 级别检查
                    if (batch.batchState == ACQUIRED &&
                        batch.batchMemberId.equals(memberId) &&
                        now - batch.acquiredTimestamp > acquisitionLockTimeoutMs) {
                        batch.batchState = AVAILABLE;
                        batch.batchMemberId = null;
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 5. 清理和 GC
    void maybeCleanupBatches() {
        lock.writeLock().lock();
        try {
            Iterator<InFlightBatch> iterator = cachedState.values().iterator();

            while (iterator.hasNext()) {
                InFlightBatch batch = iterator.next();

                // 检查整个 batch 是否可以删除
                if (canRemoveBatch(batch)) {
                    iterator.remove();

                    // 更新 nextFetchOffset
                    if (batch.lastOffset >= nextFetchOffset) {
                        nextFetchOffset = batch.lastOffset + 1;
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    boolean canRemoveBatch(InFlightBatch batch) {
        if (batch.offsetState != null) {
            // Offset 级别：所有 offset 都是 ACKNOWLEDGED 或 ARCHIVED
            for (OffsetMetadata meta : batch.offsetState.values()) {
                if (meta.state != ACKNOWLEDGED && meta.state != ARCHIVED) {
                    return false;
                }
            }
            return true;
        } else {
            // Batch 级别
            return batch.batchState == ACKNOWLEDGED ||
                   batch.batchState == ARCHIVED;
        }
    }
}
```

### 关键优化点

#### 1. **读写锁（ReadWriteLock）**

```java
// 多个 fetch 可以并发（读锁）
acquire() {
    lock.writeLock().lock();  // 实际是写操作（修改状态）
}

// 但读取配置等可以用读锁
getConfig() {
    lock.readLock().lock();
}
```

#### 2. **批量持久化**

```java
// 不是每个 ACK 都写磁盘
acknowledge(batch1, batch2, batch3) {
    // 1. 批量更新内存
    // 2. 一次性持久化所有变更
    persister.persistState([batch1, batch2, batch3]);
}
```

#### 3. **缓存管理**

```java
TreeMap<Long, InFlightBatch> cachedState;

优点:
  - 按 offset 排序
  - 快速查找：O(log n)
  - 范围查询：subMap(start, end)
  - 自动清理旧数据
```

#### 4. **延迟 Fetch**

```java
// 如果没有可用数据，不是立即返回空
// 而是等待一段时间，看看是否有 ACK 释放数据

DelayedShareFetchPurgatory purgatory;

fetch() {
    if (no available records) {
        // 注册延迟操作
        purgatory.tryCompleteElseWatch(
            delayedFetch,
            keys = [groupId-partition]
        );
    }
}

acknowledge() {
    // ACK 后触发等待的 fetch
    purgatory.checkAndComplete(groupId-partition);
}
```

### 完整流程示例

```
Time    Memory State            Consumer-A                  Consumer-B              Persister
  |
  1     cachedState: {}         fetch(max=10) →
                                (从 Kafka log 读取)
  |
  2     cachedState: {
          100: InFlightBatch
            [100-109]
            state: ACQUIRED (A)  ← records 100-109
        }
  |
  3                                                          fetch(max=5) →
                                                             (检查 cachedState)
                                                             batch 100 已被 A 占用
                                                             (从 Kafka log 读新数据)
  |
  4     cachedState: {
          100: ACQUIRED (A)
          110: InFlightBatch
            [110-114]
            state: ACQUIRED (B)                              ← records 110-114
        }
  |
  5                             处理 100-104: 成功
                                处理 105-109: 失败
                                acknowledge([
                                  100-104: ACCEPT,
                                  105-109: REJECT
                                ]) →
  |
  6     Lock acquired
        Update memory:
          100: offsetState={
            100: ACK, 101: ACK,
            102: ACK, 103: ACK,
            104: ACK,
            105: AVAIL, 106: AVAIL,
            107: AVAIL, 108: AVAIL,
            109: AVAIL
          }
        Lock released
  |                                                                                  Write to
  7                                                                                  __share_group_state:
                                                                                     {100-104: ACK,
                                                                                      105-109: AVAIL}
  |
  8                                                          fetch(max=5) →
                                                             找到 105-109: AVAILABLE
        cachedState: {
          100: mixed state
          105: ACQUIRED (B)
          106: ACQUIRED (B)
          107: ACQUIRED (B)
          108: ACQUIRED (B)
          109: ACQUIRED (B)                                  ← records 105-109
          110: ACQUIRED (B)
        }
```

---

## 复杂度总结

| 版本 | 引入的复杂度 | 解决的问题 | 代价 |
|------|-------------|-----------|------|
| V1 | 简单队列 | 基础共享 | 无容错 |
| V2 | 状态机（ACQUIRED） | At-Least-Once | 内存增加 |
| V3 | Timeout | Consumer 崩溃恢复 | 需要定期检查 |
| V4 | Batch 管理 | 内存和性能优化 | 代码复杂度 |
| V5 | Offset 级别状态 | 部分 fetch/ACK | 更多内存，按需初始化 |
| V6 | ARCHIVED + DeliveryCount | 毒丸消息 | 需要计数器 |
| V7 | 持久化 | Broker 重启 | I/O 开销 |
| V8 | 批量持久化 + 缓存 + 锁 | 性能优化 | 最高复杂度 |

---

## 核心设计思想

1. **两级状态管理** - Batch 级别（快） + Offset 级别（慢，按需）
2. **延迟初始化** - offsetState 只在需要时创建
3. **批量操作** - 持久化、ACK、清理都是批量的
4. **并发控制** - ReadWriteLock 保护共享状态
5. **超时保护** - 防止 consumer 崩溃导致数据卡住
6. **限流保护** - maxInFlightMessages 防止内存爆炸
7. **毒丸保护** - maxDeliveryCount 防止无限重试

每一层复杂度都是为了解决真实的生产问题！
