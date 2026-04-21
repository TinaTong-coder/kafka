# Batch 失败的 Trade-off：为什么一个失败，全部失败？

## TL;DR

**如果 batch 中一个 record 失败，所有 futures 都失败，是为了保持状态机的一致性。因为状态已经被 replay 了，但 log 写入失败，必须回滚所有状态。**

---

## 问题场景

```java
// Batch 包含 3 个写操作
batch = [
    WriteEvent("key1", 100, future1),
    WriteEvent("key2", 200, future2),
    WriteEvent("key3", 300, future3),
]

// 处理流程
for (event : batch) {
    // 1. Replay 到内存
    offsets.put(event.key, event.value);

    // 2. 序列化成 record
    records.add(serialize(event));
}

// 3. 批量写入 log
producer.send(records);  // ← 如果这里失败了？
```

**问题**：
- 内存状态已经更新了（key1=100, key2=200, key3=300）
- 但 log 写入失败了
- 如何处理？

---

## 选项 1：部分成功（看起来更好？）

### 实现

```java
void flushBatch() {
    List<CompletableFuture<RecordMetadata>> sendFutures = new ArrayList<>();

    // 逐个发送
    for (int i = 0; i < batch.size(); i++) {
        WriteEvent event = batch.get(i);
        CompletableFuture<RecordMetadata> sendFuture = producer.send(record[i]);
        sendFutures.add(sendFuture);

        sendFuture.whenComplete((metadata, error) -> {
            if (error == null) {
                // 成功：complete 这个 future
                event.future.complete(null);
            } else {
                // 失败：fail 这个 future
                event.future.completeExceptionally(error);
            }
        });
    }
}
```

### 问题出现了！

```
场景：
- Record 0 (key1=100): 写入成功 ✅
- Record 1 (key2=200): 写入失败 ❌
- Record 2 (key3=300): 写入成功 ✅

内存状态：
key1 = 100  ✅ 已 replay
key2 = 200  ✅ 已 replay（但 log 写入失败！）
key3 = 300  ✅ 已 replay

Log 状态：
[...][key1=100][key3=300]  ← key2 缺失！

客户端视角：
future1.get() → 成功 ✅
future2.get() → 失败 ❌
future3.get() → 成功 ✅
```

#### 问题 1: 内存和 Log 不一致

```
内存：[key1=100][key2=200][key3=300]
Log:  [...][key1=100]          [key3=300]
                      ↑ 缺失！

如果进程重启：
从 log 恢复：key2 = null（丢失了！）
但客户端以为只有 key2 失败
实际上 key3 也受影响（依赖 key2 的状态）
```

#### 问题 2: 状态机可能损坏

```java
// 假设这些操作有依赖关系
operation1: balance = 100  ← 成功
operation2: balance -= 50  ← 失败（但已 replay！）
operation3: withdraw 30    ← 成功

内存：balance = 20 (100 - 50 - 30)
Log:  balance = 70 (100 - 30)
       ↑ 缺少了 -50 这步

重启后恢复：balance = 70 ← 不对！
```

#### 问题 3: Offset 不连续

```
成功的 records 写入了 offset 100, 102
但 offset 101 缺失

这会导致：
1. Log compaction 可能出问题
2. Consumer 从 offset 100 读取，会困惑为什么跳过 101
3. 状态机 replay 时，offset 不连续，难以处理
```

---

## 选项 2：全部失败（Kafka 的选择）

### 实现

```java
void flushBatch() {
    try {
        // 批量写入所有 records
        for (Record record : batch) {
            producer.send(tp, record);
        }
        producer.flush();  // 等待所有写入完成

        // 所有写入成功，加入 deferred queue
        deferredEventQueue.add(lastWrittenOffset, batchFutures);

    } catch (Exception e) {
        // 任何一个失败，回滚所有状态
        coordinator.revertLastWrittenOffset(baseOffset);

        // Fail 所有 futures
        for (CompletableFuture<Void> future : batchFutures) {
            future.completeExceptionally(e);
        }
    }
}
```

### 关键：回滚状态

```java
// SnapshotRegistry 支持回滚
class SnapshottableCoordinator {
    void revertLastWrittenOffset(long offset) {
        // 回滚到 baseOffset 的快照
        snapshotRegistry.revertToSnapshot(offset);
    }
}

// TimelineHashMap 会恢复到之前的状态
// 所有在这个 batch 中的 replay 都会被撤销
```

### 流程

```
1. 开始 batch (baseOffset = 100)
   内存: key1=0, key2=0, key3=0

2. Replay batch 的 3 个 records
   内存: key1=100, key2=200, key3=300

3. 尝试写入 log
   - Record 0: 成功
   - Record 1: 失败 ❌
   - Record 2: 没有尝试（因为 1 失败了）

4. 捕获异常，回滚状态
   coordinator.revertToSnapshot(baseOffset=100)
   内存: key1=0, key2=0, key3=0  ← 恢复了！

5. Fail 所有 futures
   future1.completeExceptionally(error)
   future2.completeExceptionally(error)
   future3.completeExceptionally(error)

6. 客户端收到失败，可以重试
```

---

## 为什么这样设计？

### 原因 1: 保证状态机一致性

```
核心原则：内存状态必须与 Log 完全一致

如果允许部分成功：
- 内存有一些 replay
- Log 缺失一些 records
- 重启后无法恢复到正确状态

如果全部失败：
- 内存回滚到 batch 前
- Log 也没有这些 records
- 一致！
```

### 原因 2: Replay 的幂等性

```java
// 状态机的设计假设：
// replay(offset, record) 是确定性的

replay(100, record1) → state1
replay(101, record2) → state2
replay(102, record3) → state3

// 如果 record2 缺失：
replay(100, record1) → state1
replay(102, record3) → ??? ← 可能出错（依赖 state2）

// 状态机无法保证在缺失 record 时正确工作
```

### 原因 3: 避免复杂的补偿逻辑

```java
// 如果允许部分成功，需要：

if (record2 失败) {
    // 如何撤销 record2 的 replay？
    // - 可能需要反向操作（undo）
    // - 但不是所有操作都可逆

    // 例如：
    operation: "删除 key1"
    如何 undo？ "恢复 key1" ← 但恢复成什么值？

    // 又例如：
    operation: "发送通知"
    如何 undo？ ← 通知已经发出去了！
}
```

### 原因 4: 原子性保证

```java
// 很多业务场景需要原子性

// 例子：转账
batch = [
    record1: account1 -= 100,  // 扣款
    record2: account2 += 100,  // 加款
]

// 如果允许部分成功：
record1 成功：account1 -= 100 ✅
record2 失败：account2 += 100 ❌

// 结果：钱消失了！

// 全部失败：
两个都回滚 ← 安全！
```

---

## Trade-off 分析

### 选项 1: 部分成功

**优势**：
- ✅ 更高的"成功率"（能成功的尽量成功）
- ✅ 客户端可以知道哪些成功、哪些失败
- ✅ 减少不必要的重试

**劣势**：
- ❌ 状态机可能不一致
- ❌ Log 有空洞（offset 不连续）
- ❌ 需要复杂的 undo 逻辑
- ❌ 难以保证原子性
- ❌ 重启后难以恢复

### 选项 2: 全部失败（Kafka 的选择）

**优势**：
- ✅ 状态机始终一致
- ✅ Log 没有空洞
- ✅ 简单的错误处理（回滚）
- ✅ 保证原子性
- ✅ 重启后可靠恢复

**劣势**：
- ❌ 一个失败，所有都失败（"连坐"）
- ❌ 客户端需要重试整个 batch
- ❌ 看起来"成功率"低

---

## 为什么劣势是可接受的？

### 1. 失败应该是罕见的

```
在正常情况下，write 失败是非常罕见的：
- Kafka 的可靠性很高
- 网络问题通常会重试
- 磁盘满的情况很少见

如果频繁失败，说明系统有问题，应该修复根本原因
而不是通过"部分成功"来掩盖问题
```

### 2. 客户端可以重试

```java
// 客户端代码
try {
    coordinator.writeOffset(key, value).get();
} catch (Exception e) {
    // 重试
    coordinator.writeOffset(key, value).get();
}

// 因为状态已回滚，重试是安全的
```

### 3. Batch 通常不大

```
默认 batch size: 100-1000 个 records
默认 linger time: 10-100ms

所以即使全部失败，重试的代价也不大
```

### 4. 保证正确性 > 优化性能

```
Coordinator 管理的是关键状态：
- Consumer group offsets
- Transaction states
- Share group states

这些状态的正确性比性能更重要
宁可慢一点，也不能错
```

---

## 实际代码中如何实现？

### 1. 使用 SnapshotRegistry

```java
class CoordinatorContext {
    void append(List<U> records, DeferredEvent event) {
        // 保存当前 offset（batch 开始点）
        long baseOffset = coordinator.lastWrittenOffset();

        try {
            // Replay 所有 records
            for (U record : records) {
                coordinator.replay(currentOffset++, record);
            }

            // 写入 log
            flushBatch();

        } catch (Exception e) {
            // 回滚到 baseOffset
            coordinator.revertLastWrittenOffset(baseOffset);

            // Fail 所有 futures
            event.completeExceptionally(e);

            throw e;
        }
    }
}
```

### 2. TimelineHashMap 的回滚

```java
class TimelineHashMap<K, V> {
    // key -> Timeline (List of versioned values)
    Map<K, List<VersionedValue<V>>> data;

    void put(K key, V value) {
        long currentOffset = registry.currentOffset();
        data.computeIfAbsent(key, k -> new ArrayList<>())
            .add(new VersionedValue(currentOffset, value));
    }

    // 回滚：设置 currentOffset
    void revert(long offset) {
        registry.setCurrentOffset(offset);

        // get() 会自动返回 <= offset 的最新值
        // 后续的版本会被"忽略"
    }
}
```

### 3. 垃圾清理

```java
// 虽然回滚了，但 Timeline 中的版本还在
// 需要定期清理

class SnapshotRegistry {
    void cleanupOldSnapshots() {
        // 删除所有 < lastCommittedOffset 的版本
        // 因为这些版本不会再被访问了
    }
}
```

---

## 其他系统的做法

### 数据库的做法

```sql
-- 数据库也是"全部失败"
BEGIN TRANSACTION;
  UPDATE account1 SET balance = balance - 100;  -- 成功
  UPDATE account2 SET balance = balance + 100;  -- 失败
COMMIT;  -- 整个事务回滚

-- 不会出现：account1 成功，account2 失败
```

### Kafka Streams 的做法

```java
// Kafka Streams 也是相同的设计
// 一个 record 处理失败，整个 batch 回滚

streams.process(records, (key, value) -> {
    state.put(key, value);  // 更新状态
    return newRecord;       // 生成输出
});

// 如果某个 record 失败：
// - 状态回滚
// - 输出撤销
// - 重新处理整个 batch
```

---

## 能否改进？

### 改进 1: 更小的 Batch

```java
// 如果担心"连坐"影响太大，可以减小 batch size

CoordinatorRuntime.Builder()
    .withMaxBatchSize(10)      // 减小 batch（默认 100）
    .withAppendLingerMs(5)     // 减小 linger（默认 10ms）
```

**Trade-off**：
- ✅ 减少"连坐"影响
- ❌ 吞吐量下降

### 改进 2: 重试逻辑

```java
// 在 Producer 层面自动重试

producer.send(record, new Callback() {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null && isRetriable(e)) {
            // 自动重试
            producer.send(record, this);
        }
    }
});
```

**Trade-off**：
- ✅ 减少失败概率
- ❌ 增加延迟

### 改进 3: 监控和告警

```java
// 如果 batch 失败频繁，应该告警

if (batchFailureRate > threshold) {
    alert("Coordinator batch failure rate too high!");
}
```

这样可以及时发现问题，而不是通过"部分成功"掩盖问题。

---

## 总结

### 为什么一个失败，全部失败？

| 原因 | 解释 |
|------|------|
| **状态一致性** | 内存状态必须与 Log 一致 |
| **Replay 确定性** | 状态机假设 offset 连续，不能有空洞 |
| **原子性保证** | 某些操作需要"全部成功或全部失败" |
| **简化实现** | 避免复杂的 undo 逻辑 |
| **正确性优先** | 宁可慢一点，也不能错 |

### Trade-off

```
部分成功：
  + 看起来更高效
  - 状态可能不一致
  - 难以保证原子性
  - 实现复杂

全部失败：
  - 一个失败影响其他
  + 状态始终一致
  + 保证原子性
  + 实现简单
```

### Kafka 的选择

**Coordinator 优先选择正确性，而不是优化少数失败场景。**

这是合理的，因为：
1. 失败应该是罕见的
2. 可以通过重试解决
3. 状态正确性是核心要求

---

## 类比

### 银行转账

```
情况 1: 部分成功
  扣款成功 ✅
  加款失败 ❌
  → 钱消失了！不可接受

情况 2: 全部失败
  扣款回滚 ✅
  加款回滚 ✅
  → 重试整个转账，正确！
```

### 搬家

```
情况 1: 部分成功
  搬了一半家具 ✅
  另一半搬不动 ❌
  → 两边都不完整，住不了

情况 2: 全部失败
  所有家具都还在原来的家 ✅
  → 明天重新搬，没问题
```

---

**结论**: "一个失败，全部失败" 是为了保证状态机的一致性和正确性。虽然看起来效率低，但这是正确的设计选择。
