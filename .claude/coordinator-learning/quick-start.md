# Kafka Coordinator 架构 - 快速入门

## 🎯 5 分钟快速理解

### 核心思想

Kafka Coordinator 是一个**基于 Log 的复制状态机**框架：

```
客户端请求 → 生成 Records → 写入 Kafka Log → Replay 到内存 → 等待复制 → 返回响应
```

**关键点**：
1. 所有状态变更通过 **Kafka Log** 持久化（不需要自己实现 Raft）
2. 使用 **事件队列** 串行化操作（避免并发问题）
3. 使用 **Batching** 提高吞吐量（减少磁盘 IO）
4. 使用 **Deferred Events** 等待复制完成（保证一致性）
5. 使用 **MVCC** 实现读写分离（避免 Dirty Read）

---

## 📚 学习路径

### Step 1: 理解问题和演进 (30 分钟)

**阅读**: `step-by-step-tutorial.md`

这个文档从最简单的 HashMap 开始，逐步引入每个设计：
- **版本 1**: 最简单的 HashMap → 暴露数据丢失、无高可用问题
- **版本 2**: 加入持久化 (写入 Kafka Log) → 暴露并发问题
- **版本 3**: 加入事件队列 → 暴露性能问题
- **版本 4**: 加入 Batching → 暴露数据安全问题
- **版本 5**: 加入 Deferred Events → 暴露 Dirty Read 问题
- **版本 6**: 加入 MVCC → 完整解决方案
- **版本 7**: 封装成 CoordinatorRuntime 框架

**关键**：每一步都解释"为什么需要这个改进"。

### Step 2: 运行代码示例 (30 分钟)

**阅读**: `code-evolution.java`

这个文件包含每个版本的完整可运行代码：

```bash
# 编译
javac code-evolution.java

# 运行各个版本
java ShareCoordinatorV1
java ShareCoordinatorV3
java ShareCoordinatorV4
java ShareCoordinatorV5
java ShareCoordinatorV6
```

每个版本都会打印详细的执行过程，帮助你理解：
- V3: 事件如何入队、如何处理
- V4: Batching 如何累积、何时 flush
- V5: Deferred Events 如何等待 HW
- V6: MVCC 如何根据 offset 返回不同版本

### Step 3: 理解完整架构 (1 小时)

**阅读**: `README.md`

这个文档包含：
- 完整的架构图
- 各组件的职责
- 一次完整的 Write 请求流程（从客户端到返回响应）
- ShareCoordinator 的具体实现

**重点关注**：
- `CoordinatorRuntime` 提供了哪些通用能力
- `ShareCoordinatorShard` 需要实现什么
- `CoordinatorContext` 如何管理单个 partition 的状态

---

## 🔑 核心概念速查

### 1. CoordinatorShard（状态机）

**你需要实现的接口**：

```java
interface CoordinatorShard<U> {
    // 重放记录到状态机
    void replay(long offset, U record);

    // 生成需要写入的 records
    CoordinatorResult<Response, Record> writeState(Request request);

    // 读取状态（只能读已提交的数据）
    Response readState(Request request, long committedOffset);
}
```

**关键点**：
- `writeState()` **不直接修改状态**，只生成 records
- 状态修改由 `replay()` 完成
- `readState()` 使用 `committedOffset` 的快照

### 2. CoordinatorRuntime（框架）

**提供的能力**：

```java
// 调度写操作
CompletableFuture<Response> scheduleWriteOperation(
    TopicPartition tp,
    CoordinatorWriteOperation op
)

// 调度读操作
CompletableFuture<Response> scheduleReadOperation(
    TopicPartition tp,
    CoordinatorReadOperation op
)
```

**自动处理**：
- ✅ 事件调度（EventProcessor）
- ✅ 并发控制（单 partition 串行化）
- ✅ Batching（累积写入）
- ✅ Deferred Events（等待 HW）
- ✅ MVCC（SnapshotRegistry + TimelineHashMap）
- ✅ 状态机加载/卸载
- ✅ High Watermark 监听

### 3. TimelineHashMap（多版本存储）

**核心思想**：

```java
// 内部存储：key -> List<(offset, value)>
put("key1", 100);  // Timeline: [(0, 100)]
put("key1", 200);  // Timeline: [(0, 100), (1, 200)]

// 读取时指定 offset
setCurrentOffset(0);
get("key1");  // 返回 100

setCurrentOffset(1);
get("key1");  // 返回 200
```

**用途**：
- 写操作使用 `lastWrittenOffset`（未提交）
- 读操作使用 `lastCommittedOffset`（已提交）
- 实现读写隔离

### 4. Deferred Events（延迟完成）

**流程**：

```
1. 写入 log（返回 offset）
2. 加入 deferredEventQueue.add(offset, event)
3. 等待 HW 推进...
4. HW 到达 offset 时，complete event
5. 客户端收到响应
```

**目的**：只在数据真正安全后才返回成功。

### 5. Event 类型

```java
// 写操作
CoordinatorWriteEvent
  ├─ 执行业务逻辑 (shard.writeState)
  ├─ 生成 records
  ├─ Replay + 写入 log
  └─ 加入 deferred queue

// 读操作
CoordinatorReadEvent
  ├─ 使用 committedOffset 快照
  ├─ 执行业务逻辑 (shard.readState)
  └─ 立即返回

// 内部事件
HighWatermarkUpdate
  └─ 更新 committedOffset + complete deferred events

FlushBatch
  └─ 触发 batch flush
```

---

## 🎓 常见问题

### Q1: 为什么不直接用数据库？

**A**:
- Coordinator 需要高可用、高吞吐量
- 基于 Kafka Log 可以复用 Kafka 的复制机制
- 不需要自己实现共识协议（Raft/Paxos）
- Log-based 架构天然支持审计、调试

### Q2: 为什么 writeState() 不直接修改状态？

**A**:
```java
// 错误方式
writeState(request) {
    offsets.put(key, value);  // 直接修改
    return response;
}

// 问题：
// 1. 状态已经改了，但 log 可能写入失败
// 2. 难以实现 MVCC（需要保留多个版本）

// 正确方式
writeState(request) {
    record = generateRecord(request);  // 只生成 record
    return new Result(record, response);
}

// CoordinatorRuntime 会：
// 1. replay(record)  ← 修改状态
// 2. append(record)  ← 写入 log
// 3. 等待 HW 后返回 response
```

### Q3: 为什么需要两个 offset？

**A**:
- `lastWrittenOffset`: 已写入本地 log，但可能未复制
- `lastCommittedOffset`: 已复制到多数派，安全的

**用途**：
- 写操作用 `lastWrittenOffset`（允许写入未提交的数据）
- 读操作用 `lastCommittedOffset`（只读安全的数据）

### Q4: Batching 会导致延迟增加吗？

**A**: 会，但可以控制：

```java
// 两种触发 flush 的条件
if (batch.size() >= maxBatchSize) {
    flush();  // 条件 1: batch 满了
}

if (currentTime - batchStartTime >= lingerMs) {
    flush();  // 条件 2: 超时了
}

// 配置建议
maxBatchSize = 100    // 吞吐量优先
lingerMs = 10ms       // 延迟优先
```

**权衡**：
- 增加 10ms 延迟，换取 10-50 倍吞吐量提升
- 对大多数应用，这是值得的

### Q5: 如何实现一个新的 Coordinator？

**A**: 只需 3 步：

```java
// Step 1: 定义 Shard
class MyCoordinatorShard implements CoordinatorShard<MyRecord> {
    TimelineHashMap<Key, Value> state;

    void replay(long offset, MyRecord record) {
        // 根据 record 更新 state
    }

    CoordinatorResult writeState(Request req) {
        // 生成 record
        return new Result(record, response);
    }

    Response readState(Request req, long committedOffset) {
        // 读取 state (使用 committedOffset 快照)
        return response;
    }
}

// Step 2: 创建 Runtime
CoordinatorRuntime<MyCoordinatorShard, MyRecord> runtime =
    new CoordinatorRuntime.Builder()
        .withEventProcessor(...)
        .withPartitionWriter(...)
        .withLoader(...)
        .withCoordinatorShardBuilderSupplier(() -> new MyCoordinatorShard.Builder())
        .build();

// Step 3: 调度操作
CompletableFuture<Response> future = runtime.scheduleWriteOperation(
    "MyOperation",
    tp,
    Duration.ofSeconds(30),
    shard -> shard.writeState(request)
);
```

**就这样！** Runtime 会自动处理：
- 事件调度
- Batching
- 持久化
- 复制
- MVCC

---

## 🚀 下一步

1. **阅读 step-by-step-tutorial.md** (理解演进过程)
2. **运行 code-evolution.java** (看实际效果)
3. **阅读 README.md** (理解完整架构)
4. **阅读 Kafka 源码**:
   - `CoordinatorRuntime.java:2553`
   - `ShareCoordinatorShard.java:300`
   - `ShareCoordinatorService.java:200`

---

## 📖 术语表

| 术语 | 解释 |
|------|------|
| **Coordinator** | 管理共享状态的组件（如 ShareCoordinator, GroupCoordinator） |
| **Shard** | 单个 partition 对应的状态机 |
| **Runtime** | 通用框架，提供事件调度、Batching 等能力 |
| **Replay** | 将 log 中的 record 应用到内存状态 |
| **Batch** | 累积多个写操作，批量 flush |
| **Deferred Event** | 等待 HW 推进后才 complete 的事件 |
| **MVCC** | 多版本并发控制，支持读写隔离 |
| **HW (High Watermark)** | 已复制到多数派的最大 offset |
| **Committed Offset** | = High Watermark，安全的 offset |
| **Written Offset** | 已写入本地 log 的最大 offset |

---

## 💡 关键洞察

1. **基于 Log 而非 RPC**
   - 传统系统：Leader 通过 RPC 同步状态到 Follower
   - Kafka 系统：所有节点 replay 相同的 log

2. **写入分两阶段**
   - 阶段 1: 写入本地 log（快，几毫秒）
   - 阶段 2: 复制到 followers（慢，几十毫秒）
   - 只有阶段 2 完成才返回成功

3. **读写隔离**
   - 写操作：可以看到未提交的数据（提高并发）
   - 读操作：只能看到已提交的数据（保证一致性）

4. **Batching 是性能关键**
   - 单个写入：10-20ms
   - 批量写入：20-30ms（无论多少个）
   - 吞吐量提升：10-50 倍

5. **框架与业务分离**
   - Runtime 提供通用能力（事件、Batching、MVCC）
   - Shard 只需实现业务逻辑（replay、writeState、readState）
   - 新 Coordinator 只需 100 行代码！

---

好了，开始学习吧！🎉

如果有任何问题，参考 `step-by-step-tutorial.md` 中的详细解释。
