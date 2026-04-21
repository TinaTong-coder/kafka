# Producer ACK vs High Watermark - 可视化对比

## 时间线对比

### 方式 1: 只用 Producer ACK (错误)

```
Time  │ Coordinator      │ Broker              │ Client          │ 问题
──────┼──────────────────┼─────────────────────┼─────────────────┼─────────────────
  0   │ 收到写请求        │                     │                 │
  1   │ 更新内存: v=100   │                     │                 │
  2   │ 调用 send()      │                     │                 │
  3   │                  │ 写入 leader         │                 │
  4   │                  │ 复制到 follower1    │                 │
  5   │                  │ 复制到 follower2    │                 │
  6   │                  │ 所有 ISR 确认       │                 │
  7   │ ← ACK 返回       │                     │                 │
  8   │ future.complete()│                     │ ← 收到"成功"    │ ← 太早！
  9   │                  │                     │                 │
 10   │                  │                     │ 发起读请求       │
 11   │ 返回 v=100       │                     │ ← 读到 100      │ ← Dirty Read!
      │                  │ HW = 99             │                 │    HW 还是 99
 12   │                  │ ↓                   │                 │
 13   │                  │ Follower Fetch请求  │                 │
 14   │                  │ 更新 replica offset │                 │
 15   │                  │ HW 推进到 100       │                 │ ← 太晚！
 16   │                  │                     │                 │
```

**问题**:
- T8: 客户端收到成功，但 HW = 99
- T11: 读操作读到了 uncommitted 数据（offset 100）
- 如果 T12 时 leader crash，offset 100 可能丢失，但客户端已经看到了！

---

### 方式 2: 使用 Deferred Event Queue (正确)

```
Time  │ Coordinator      │ Broker              │ Deferred Queue  │ Client
──────┼──────────────────┼─────────────────────┼─────────────────┼─────────────────
  0   │ 收到写请求        │                     │                 │
  1   │ 更新内存: v=100   │                     │                 │
  2   │ 调用 send()      │                     │                 │
  3   │                  │ 写入 leader         │                 │
  4   │                  │ 复制到 follower1    │                 │
  5   │                  │ 复制到 follower2    │                 │
  6   │                  │ 所有 ISR 确认       │                 │
  7   │ ← ACK 返回       │                     │                 │
  8   │                  │                     │ ← add(100, fut) │ ← 加入队列
  9   │                  │                     │ [waiting...]    │ [waiting...]
 10   │ 收到读请求        │                     │                 │
 11   │ 返回 null        │                     │                 │ ← 读到 null
      │ (HW=99, v=100)   │ HW = 99             │                 │    正确！
 12   │                  │ ↓                   │                 │
 13   │                  │ Follower Fetch请求  │                 │
 14   │                  │ 更新 replica offset │                 │
 15   │                  │ HW 推进到 100       │                 │
 16   │ ← HW 更新事件    │                     │                 │
 17   │                  │                     │ completeUpTo(100)│
 18   │                  │                     │ → future.complete│ ← 收到"成功"
 19   │ 收到读请求        │                     │                 │
 20   │ 返回 v=100       │                     │                 │ ← 读到 100
      │ (HW=100, v=100)  │ HW = 100            │                 │    正确！
```

**正确**:
- T8: ACK 返回，但不立即返回成功，而是加入 deferred queue
- T11: 读操作读到 null（因为 HW=99，offset 100 还未 committed）
- T18: HW 推进到 100 后，才返回成功给客户端
- T20: 读操作可以读到 100（因为 HW=100）

---

## 数据状态对比

### 场景：Leader Crash

#### 只用 Producer ACK (错误)

```
初始状态 (T8):
  Leader:    [0][1][2]...[99][100*] ← ACK 已返回，客户端收到"成功"
  Follower1: [0][1][2]...[99][100]
  Follower2: [0][1][2]...[99]       ← 网络延迟，还没复制 100
  HW = 99

客户端读取 (T11):
  读到: offset 100 ✓

Leader crash (T12):
  新 Leader = Follower2
  [0][1][2]...[99]                  ← 没有 offset 100！

客户端再次读取:
  读到: offset 99 ✗ ← 数据回退！不一致！
```

#### 使用 Deferred Event Queue (正确)

```
初始状态 (T8):
  Leader:    [0][1][2]...[99][100*] ← ACK 已返回，但客户端还在等待
  Follower1: [0][1][2]...[99][100]
  Follower2: [0][1][2]...[99]
  HW = 99
  DeferredQueue: [offset 100 → future (pending)]

客户端读取 (T11):
  读到: null (因为 HW=99，offset 100 还未 committed) ✓

等待 HW 推进 (T15):
  Follower2 也复制到 100
  HW 推进到 100
  DeferredQueue 触发: future.complete()

客户端收到成功 (T18):
  现在即使 leader crash，所有副本都有 offset 100

客户端读取 (T20):
  读到: offset 100 ✓ ← 一致！
```

---

## Producer ACK 和 HW 的语义差异

### Producer ACK

```
┌─────────────────────────────────────┐
│  Producer ACK (acks=all)            │
│                                     │
│  保证：数据已复制到所有 ISR          │
│                                     │
│  [Leader] ─┬─> [Follower1]         │
│            │                        │
│            └─> [Follower2]         │
│                                     │
│  所有 ISR 都确认 → ACK 返回         │
└─────────────────────────────────────┘

时间点: T7
保证: 数据不会丢失
但是: Leader 还不知道 Follower 的进度
```

### High Watermark

```
┌─────────────────────────────────────┐
│  High Watermark                     │
│                                     │
│  定义：所有 ISR 都有的最小 offset   │
│                                     │
│  Leader 维护：                      │
│    replicaOffsets = {              │
│      follower1 -> 100,             │
│      follower2 -> 99,              │
│    }                               │
│                                     │
│  HW = min(100, 99) = 99            │
└─────────────────────────────────────┘

时间点: T15 (在 Follower Fetch 时更新)
保证: 读操作可以安全读取 HW 之前的数据
依赖: Follower 的 Fetch 请求
```

---

## 为什么 HW 不能立即更新？

### Leader 的困境

```
问题：Leader 收到 ACK 后，能立即推进 HW 吗？

ACK 返回时 Leader 知道：
  ✓ 数据已写入所有 ISR
  ✗ 但不知道每个 Follower 的 offset 是多少

为什么不知道？
  因为 ACK 不携带 Follower 的 offset 信息！

  Producer ACK:
    ✓ 成功/失败
    ✓ offset (写入的位置)
    ✗ Follower 状态

  Follower Fetch:
    ✓ fetchOffset (Follower 当前的 offset)
    ✓ Leader 可以根据这个更新 replicaOffsets
    ✓ Leader 可以计算新的 HW
```

### HW 更新的时机

```java
// HW 更新不是写入时触发，而是 Fetch 时触发

// 1. Producer 写入
producer.send(record);  // offset 100
  → Leader 写入成功
  → 复制到 Followers
  → ACK 返回
  → HW 还是 99 (还没更新)

// 2. Follower 发送 Fetch
Follower: FetchRequest(fetchOffset=101)  // "我已经有 0-100 了"
  → Leader 收到 Fetch
  → Leader 更新 replicaOffsets[follower] = 100
  → Leader 重新计算 HW = min(所有 replicaOffsets)
  → HW 推进到 100
  → 触发 HighWatermarkListener
```

---

## Deferred Event Queue 的工作原理

### 数据结构

```java
class DeferredEventQueue {
    // offset -> List<Future>
    TreeMap<Long, List<CompletableFuture<Void>>> pending;

    void add(long offset, CompletableFuture<Void> future) {
        pending.computeIfAbsent(offset, k -> new ArrayList<>()).add(future);
        // 加入队列，等待 HW 到达 offset
    }

    void completeUpTo(long highWatermark) {
        // Complete 所有 offset <= highWatermark 的 futures
        for (Entry<Long, List<Future>> entry : pending.entrySet()) {
            if (entry.getKey() <= highWatermark) {
                entry.getValue().forEach(f -> f.complete(null));
                pending.remove(entry.getKey());
            }
        }
    }
}
```

### 工作流程

```
写入流程:
  1. 客户端: writeOffset(key, value)
     → 返回 future (pending)

  2. Coordinator: 更新内存
     offsets.put(key, value)

  3. Coordinator: 写入 log
     offset = producer.send(record).get()  // 等待 ACK

  4. Coordinator: 加入 deferred queue
     deferredQueue.add(offset, future)
     // future 还是 pending 状态

  5. 等待 HW 推进...

HW 推进:
  6. Broker: Follower Fetch
     → Leader 更新 replicaOffsets
     → Leader 计算 HW
     → HW 推进

  7. Broker: 触发 Listener
     HighWatermarkListener.onHighWatermarkUpdated(newHW)

  8. Coordinator: 处理 HW 事件
     deferredQueue.completeUpTo(newHW)

  9. Coordinator: Complete futures
     future.complete(null)

  10. 客户端: future 完成
      → 收到"成功"响应
```

---

## 总结

### Producer ACK 的局限

| 问题 | 解释 |
|------|------|
| **时间差** | ACK 返回 ≠ HW 推进 (通常相差几十毫秒) |
| **信息不足** | ACK 不包含 Follower 的进度信息 |
| **异步更新** | HW 由 Follower Fetch 触发，不是写入时触发 |
| **语义鸿沟** | ACK = "数据安全"，HW = "数据可见" |

### Deferred Event Queue 的价值

| 价值 | 解释 |
|------|------|
| **保证一致性** | 只在 HW 推进后返回成功 |
| **避免 Dirty Read** | 读操作只看到 HW 之前的数据 |
| **容错性** | 即使 leader crash，数据也不会丢失 |
| **正确的语义** | 客户端收到成功 = 数据已 committed = 对读操作可见 |

### 核心思想

```
Producer ACK:  "数据已写入，不会丢失"
                 ↓
              (时间差)
                 ↓
High Watermark: "数据已提交，对读操作可见"

Deferred Event Queue: 弥补这个时间差，确保正确的语义
```

---

## 类比

### 银行转账

```
只用 Producer ACK:
  你: "我已经转账了" (ACK 返回)
  对方: "我还没收到钱" (HW 还没推进)
  → 不一致！

使用 Deferred Event Queue:
  你: "我发起了转账" (写入 log)
  银行: "转账处理中..." (deferred queue)
  对方: "我收到钱了" (HW 推进)
  银行: "转账成功" (future complete)
  → 一致！
```

### 包裹快递

```
只用 Producer ACK:
  快递: "已发货" (ACK 返回)
  你: "查询物流：在路上"
  你: "包裹在哪？" → 可能还在仓库！
  → 不一致！

使用 Deferred Event Queue:
  快递: "已发货" (写入 log)
  你: "等待中..." (deferred queue)
  快递: "已签收" (HW 推进)
  快递: "送达成功" (future complete)
  → 一致！
```

---

**结论**: Deferred Event Queue 是 Kafka Coordinator 实现强一致性的关键机制，它确保了客户端只在数据真正安全且对读操作可见后才收到成功响应。
