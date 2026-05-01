# SubscriptionState 和 Fetcher 从简单到复杂的演化分析

本文档通过逐步演进的方式，展示 Consumer Group 中 SubscriptionState 和 Fetcher 的设计如何从简单到复杂。

---

## Step 1: 最简单版本 - 单个 Partition 的顺序消费

### 需求
Consumer 从一个固定的 partition 顺序消费消息。

### 实现
```java
class SimpleConsumer {
    TopicPartition partition;
    long currentOffset;  // 当前消费到哪里了

    List<Record> fetch() {
        FetchRequest request = new FetchRequest(partition, currentOffset);
        FetchResponse response = broker.fetch(request);

        currentOffset += response.records.size();  // 更新 offset
        return response.records;
    }
}
```

### 数据结构
```
┌─────────────────────────────────────────┐
│  SimpleConsumer                         │
├─────────────────────────────────────────┤
│  partition: "orders-0"                  │
│  currentOffset: 1000                    │
│                                         │
│  fetch() {                              │
│    从 offset 1000 开始读取              │
│    currentOffset = 1000 + 10 = 1010    │
│  }                                      │
└─────────────────────────────────────────┘
```

### 特点
- ✅ 简单直接
- ✅ 顺序消费
- ❌ 只能消费一个 partition
- ❌ offset 没有持久化（重启后丢失）
- ❌ 没有错误处理

**引入的复杂度**：无

---

## Step 2: 引入 SubscriptionState - 管理多个 Partition

### 新需求
**Consumer 需要同时消费多个 partition，每个 partition 独立跟踪消费进度。**

**为什么？**
- 一个 topic 通常有多个 partition
- 需要并行消费多个 partition 提高吞吐量
- 每个 partition 的消费进度独立

### 新增复杂度
1. **需要为每个 partition 维护独立的 offset**
2. **需要管理 partition 的分配和取消分配**

### 实现
```java
class SubscriptionState {
    // 核心：每个 partition 一个状态
    Map<TopicPartition, TopicPartitionState> assignment;

    void assign(Set<TopicPartition> partitions) {
        assignment.clear();
        for (TopicPartition partition : partitions) {
            assignment.put(partition, new TopicPartitionState());
        }
    }

    void seek(TopicPartition partition, long offset) {
        TopicPartitionState state = assignment.get(partition);
        state.position = new FetchPosition(offset);
    }

    long position(TopicPartition partition) {
        TopicPartitionState state = assignment.get(partition);
        return state.position.offset;
    }
}

class TopicPartitionState {
    FetchPosition position;  // 当前消费到的位置
}

class FetchPosition {
    long offset;  // 消费到的 offset
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  SubscriptionState                                           │
├──────────────────────────────────────────────────────────────┤
│  assignment: Map<TopicPartition, TopicPartitionState>       │
│    ├─ "orders-0" -> TopicPartitionState{position: 1000}    │
│    ├─ "orders-1" -> TopicPartitionState{position: 2000}    │
│    └─ "orders-2" -> TopicPartitionState{position: 3000}    │
└──────────────────────────────────────────────────────────────┘

每个 partition 独立跟踪消费进度 ✅
```

### 引入的复杂度
1. **Map 结构**：管理多个 partition
2. **TopicPartitionState**：封装单个 partition 的状态
3. **FetchPosition**：封装 offset
4. **为什么？** 支持多 partition 并行消费

---

## Step 3: 引入 Fetcher - 统一 Fetch 逻辑

### 新需求
**需要从多个 partition 批量 fetch 数据，而不是一个一个 fetch。**

**为什么？**
```
场景：
Consumer 分配到 3 个 partitions: [orders-0, orders-1, orders-2]

如果逐个 fetch：
  fetch(orders-0) -> 网络往返 1 次
  fetch(orders-1) -> 网络往返 1 次
  fetch(orders-2) -> 网络往返 1 次
  总共：3 次网络往返

批量 fetch：
  fetch([orders-0, orders-1, orders-2]) -> 网络往返 1 次
  总共：1 次网络往返 ✅
```

### 新增复杂度
1. **批量构建 FetchRequest**
2. **按 Broker 分组（不同 partition 可能在不同 Broker）**
3. **异步处理 FetchResponse**

### 实现
```java
class Fetcher {
    SubscriptionState subscriptions;  // 引用 SubscriptionState

    // 核心方法：发送 fetch 请求
    void sendFetches() {
        // 1. 遍历所有分配的 partitions
        Map<Node, List<TopicPartition>> partitionsByNode =
            groupPartitionsByLeader();

        // 2. 为每个 Node 构建 FetchRequest
        for (Map.Entry<Node, List<TopicPartition>> entry : partitionsByNode) {
            Node node = entry.getKey();
            List<TopicPartition> partitions = entry.getValue();

            FetchRequest.Builder builder = FetchRequest.Builder.forConsumer();
            for (TopicPartition partition : partitions) {
                // 从 SubscriptionState 读取 position
                long offset = subscriptions.position(partition);
                builder.add(partition, offset, MAX_BYTES);
            }

            // 3. 发送请求
            client.send(node, builder.build())
                .addListener(response -> handleFetchResponse(response));
        }
    }

    // 处理 FetchResponse
    void handleFetchResponse(FetchResponse response) {
        for (Map.Entry<TopicPartition, FetchData> entry : response.responseData()) {
            TopicPartition partition = entry.getKey();
            FetchData data = entry.getValue();

            // 更新 SubscriptionState 的 position
            long nextOffset = data.records.lastOffset() + 1;
            subscriptions.position(partition, new FetchPosition(nextOffset));

            // 存储 records 到本地 buffer
            fetchBuffer.add(partition, data.records);
        }
    }

    // 返回给应用层
    Map<TopicPartition, List<Record>> fetchedRecords() {
        return fetchBuffer.drain();
    }
}
```

### 数据结构演化
```
┌──────────────────────────────────────────────────────────────┐
│  Fetcher                                                     │
├──────────────────────────────────────────────────────────────┤
│  subscriptions: SubscriptionState  // 引用                   │
│  fetchBuffer: Map<TopicPartition, Queue<Record>>            │
│                                                               │
│  sendFetches() {                                             │
│    按 Broker 分组 partitions                                 │
│    构建 FetchRequest                                         │
│    异步发送                                                   │
│  }                                                            │
│                                                               │
│  handleFetchResponse(response) {                             │
│    更新 subscriptions.position                               │
│    存储 records 到 fetchBuffer                               │
│  }                                                            │
└──────────────────────────────────────────────────────────────┘
         │
         ▼ 读写
┌──────────────────────────────────────────────────────────────┐
│  SubscriptionState                                           │
│  assignment:                                                 │
│    orders-0 -> position: 1000                               │
│    orders-1 -> position: 2000                               │
│    orders-2 -> position: 3000                               │
└──────────────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **Fetcher 类**：封装 fetch 逻辑
2. **按 Node 分组**：不同 partition 可能在不同 Broker
3. **FetchBuffer**：本地缓存 fetched records
4. **异步处理**：fetch 请求是异步的
5. **为什么？** 批量 fetch 提升性能，减少网络往返

---

## Step 4: 引入 FetchState - 管理 Partition 的 Fetch 状态

### 新需求
**Partition 不总是可以 fetch，需要区分不同的状态。**

**为什么？**
```
场景 1：Partition 刚分配
  -> 还不知道从哪个 offset 开始 fetch
  -> 需要先初始化 offset（从 Coordinator 获取 committed offset）
  -> 状态：INITIALIZING

场景 2：需要重置 offset
  -> 遇到 OffsetOutOfRangeException
  -> 需要根据 auto.offset.reset 策略重置
  -> 状态：AWAIT_RESET

场景 3：正常 fetch
  -> Offset 已初始化
  -> 可以正常 fetch
  -> 状态：FETCHING
```

### 新增复杂度
1. **FetchState 状态机**
2. **状态转换逻辑**
3. **根据状态决定是否可 fetchable**

### 实现
```java
enum FetchState {
    INITIALIZING,  // 初始化中，需要获取 committed offset
    FETCHING,      // 正常 fetch
    AWAIT_RESET,   // 等待 offset reset
    AWAIT_VALIDATION;  // 等待 offset validation

    boolean requiresPosition() {
        return this == FETCHING;
    }
}

class TopicPartitionState {
    FetchPosition position;
    FetchState fetchState;  // 新增：状态

    boolean isFetchable() {
        // 只有 FETCHING 状态才可以 fetch
        return fetchState == FetchState.FETCHING && position != null;
    }

    void seekValidated(FetchPosition newPosition) {
        this.position = newPosition;
        // 状态转换：任何状态 -> FETCHING
        this.fetchState = FetchState.FETCHING;
    }

    void awaitReset(AutoOffsetResetStrategy strategy) {
        this.resetStrategy = strategy;
        // 状态转换：FETCHING -> AWAIT_RESET
        this.fetchState = FetchState.AWAIT_RESET;
    }
}

class Fetcher {
    void sendFetches() {
        for (TopicPartition partition : subscriptions.assignedPartitions()) {
            TopicPartitionState state = subscriptions.stateOf(partition);

            // 只 fetch 可 fetchable 的 partition
            if (!state.isFetchable()) {
                continue;  // 跳过 INITIALIZING、AWAIT_RESET 等状态
            }

            // 构建 FetchRequest
            // ...
        }
    }
}
```

### 状态转换图
```
状态机：
                     ┌─────────────────┐
                     │  INITIALIZING   │ (初始状态)
                     └────────┬────────┘
                              │ seekValidated()
                              ▼
                     ┌─────────────────┐
              ┌─────>│    FETCHING     │<──────┐
              │      └────────┬────────┘       │
              │               │ OffsetOutOfRange│
              │               ▼                 │
              │      ┌─────────────────┐       │
              │      │  AWAIT_RESET    │       │
              │      └────────┬────────┘       │
              │               │ reset 完成      │
              └───────────────┴────────────────┘

可 fetchable 的状态：只有 FETCHING ✅
```

### 引入的复杂度
1. **FetchState 枚举**：定义状态
2. **状态转换逻辑**：`transitionTo()` 方法
3. **isFetchable() 检查**：只 fetch 特定状态的 partition
4. **为什么？** 精确控制什么时候可以 fetch，避免错误

---

## Step 5: 引入 Leader Epoch - 处理 Leader 变更

### 新需求
**检测 Leader 变更，避免消费到被 truncate 的数据。**

**为什么？**
```
场景：Log Truncation
Time 0:
  Leader: Broker1, offset 100-110 (10 条消息)
  Consumer: 消费到 offset 105

Time 1: Leader 切换
  New Leader: Broker2, offset 100-103 (只有 4 条消息)
  -> Broker2 的 log 比 Broker1 短（部分数据未复制）

Time 2: Consumer 继续 fetch
  Consumer: 请求 offset 105
  Broker2: 没有 offset 105！(已被 truncate)

问题：
  如果直接从 offset 105 fetch -> 数据丢失
  需要检测到 truncation，回退到安全的 offset
```

### 新增复杂度
1. **Leader Epoch 追踪**
2. **Offset Validation**
3. **Truncation 检测和恢复**

### 实现
```java
class FetchPosition {
    long offset;
    Optional<Integer> offsetEpoch;  // 新增：offset 对应的 epoch
    Metadata.LeaderAndEpoch currentLeader;  // 新增：当前 leader + epoch
}

class TopicPartitionState {
    FetchPosition position;

    void seekValidated(FetchPosition newPosition) {
        this.position = newPosition;
        this.fetchState = FetchState.FETCHING;
    }

    // 新增：检测 leader epoch 变化
    boolean maybeValidatePositionForCurrentLeader(LeaderAndEpoch currentLeader) {
        if (position.currentLeader.equals(currentLeader)) {
            return true;  // Leader 未变化，无需 validation
        }

        // Leader 变化了，需要 validation
        this.fetchState = FetchState.AWAIT_VALIDATION;
        return false;
    }
}

class Fetcher {
    // 新增：发送 OffsetForLeaderEpoch 请求来 validate offset
    void validatePositions() {
        for (TopicPartition partition : needsValidation) {
            FetchPosition position = subscriptions.position(partition);

            // 构建 OffsetForLeaderEpochRequest
            OffsetForLeaderEpochRequest request =
                new OffsetForLeaderEpochRequest(partition, position.offsetEpoch);

            client.send(request).addListener(response -> {
                long endOffset = response.endOffset;

                if (position.offset > endOffset) {
                    // Truncation 检测到！
                    // 回退到安全的 offset
                    subscriptions.seekValidated(partition,
                        new FetchPosition(endOffset));
                } else {
                    // 安全，可以继续
                    subscriptions.completeValidation(partition);
                }
            });
        }
    }
}
```

### 数据流图
```
检测 Leader 变化：
┌──────────────────────────────────────────────────────┐
│ 1. Metadata 更新                                     │
│    发现 partition 的 leader epoch 从 5 变为 6       │
└─────────────────┬────────────────────────────────────┘
                  │
                  ▼
┌──────────────────────────────────────────────────────┐
│ 2. 标记需要 validation                               │
│    fetchState: FETCHING -> AWAIT_VALIDATION         │
└─────────────────┬────────────────────────────────────┘
                  │
                  ▼
┌──────────────────────────────────────────────────────┐
│ 3. 发送 OffsetForLeaderEpoch 请求                    │
│    "我的 offset 105 在新 leader 上还安全吗？"        │
└─────────────────┬────────────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼ 安全              ▼ Truncated
┌─────────────────┐  ┌─────────────────┐
│ 4a. 继续 fetch  │  │ 4b. 回退 offset │
│ state->FETCHING │  │ seek(endOffset) │
└─────────────────┘  └─────────────────┘
```

### 引入的复杂度
1. **Leader Epoch 字段**：FetchPosition 包含 epoch 信息
2. **AWAIT_VALIDATION 状态**：新的 FetchState
3. **OffsetForLeaderEpoch 请求**：验证 offset 是否安全
4. **Truncation 处理**：检测并回退 offset
5. **为什么？** 保证数据一致性，避免读到被 truncate 的数据

---

## Step 6: 引入 Pause/Resume - 流控

### 新需求
**应用层需要暂停某些 partition 的消费（流控）。**

**为什么？**
```
场景 1：背压（Backpressure）
  下游处理慢 -> 内存积压 -> 需要暂停消费

场景 2：优先级处理
  优先处理 partition 0 的数据，暂停其他 partition

场景 3：限流
  单个 partition 消费太快，需要限速
```

### 新增复杂度
1. **pause/resume 标志**
2. **isFetchable() 检查时考虑 pause 状态**

### 实现
```java
class TopicPartitionState {
    FetchPosition position;
    FetchState fetchState;
    boolean paused;  // 新增：是否暂停

    void pause() {
        this.paused = true;
    }

    void resume() {
        this.paused = false;
    }

    boolean isFetchable() {
        // 新增检查：paused 的 partition 不可 fetch
        return !paused &&
               fetchState == FetchState.FETCHING &&
               position != null;
    }
}

class SubscriptionState {
    public void pause(TopicPartition partition) {
        assignedState(partition).pause();
    }

    public void resume(TopicPartition partition) {
        assignedState(partition).resume();
    }

    public Set<TopicPartition> pausedPartitions() {
        return assignment.entrySet().stream()
            .filter(e -> e.getValue().paused)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }
}
```

### 使用示例
```java
// 应用层代码
consumer.pause(Collections.singleton(partition0));  // 暂停 p0

// 下次 poll() 时
consumer.poll(Duration.ofMillis(100));
  -> Fetcher.sendFetches()
    -> 只 fetch partition1, partition2 (跳过 partition0)

// 恢复
consumer.resume(Collections.singleton(partition0));
```

### 引入的复杂度
1. **paused 标志**：TopicPartitionState 新增字段
2. **pause/resume API**：公开给用户
3. **isFetchable() 逻辑增强**：检查 paused 状态
4. **为什么？** 给应用层流控能力，灵活控制消费速率

---

## Step 7: 引入 FetchSession - 优化重复请求

### 新需求
**Consumer 频繁发送相同的 FetchRequest，浪费网络带宽。**

**为什么？**
```
场景：
Consumer poll() 每 100ms 一次
每次都请求相同的 partitions: [p0, p1, p2]

传统方式（Full Fetch）：
  FetchRequest: [p0(offset=1000), p1(offset=2000), p2(offset=3000)]
  FetchResponse: [p0: records, p1: empty, p2: records]

  FetchRequest: [p0(offset=1010), p1(offset=2000), p2(offset=3010)]
  FetchResponse: [p0: records, p1: empty, p2: records]

  -> 每次都发送完整的 partition 列表，浪费！

优化方式（Incremental Fetch）：
  第一次 FetchRequest: [p0(offset=1000), p1(offset=2000), p2(offset=3000)]
  FetchResponse: sessionId=123, [p0: records, p1: empty, p2: records]

  第二次 FetchRequest: sessionId=123, [p0(offset=1010), p2(offset=3010)]
                        -> 只发送变化的！
  FetchResponse: [p0: records, p2: records]

  -> 节省网络带宽 ✅
```

### 新增复杂度
1. **FetchSessionHandler**：管理 session 状态
2. **Session ID 和 Epoch**：追踪 session
3. **增量请求构建**：只发送变化的 partition

### 实现
```java
class FetchSessionHandler {
    int sessionId;  // Broker 分配的 session ID
    int epoch;      // Session 的版本号

    // 缓存上次的请求
    Map<TopicPartition, FetchRequest.PartitionData> lastRequest;

    FetchRequestData build() {
        FetchRequestData data = new FetchRequestData();
        data.sessionId = sessionId;
        data.sessionEpoch = epoch;

        // 计算增量
        for (TopicPartition partition : currentPartitions) {
            FetchRequest.PartitionData current = currentData(partition);
            FetchRequest.PartitionData last = lastRequest.get(partition);

            if (!current.equals(last)) {
                // 只添加变化的 partition
                data.toSend.put(partition, current);
            }
        }

        // 更新缓存
        lastRequest = currentPartitions;
        epoch++;

        return data;
    }
}

class Fetcher {
    // 每个 Node 一个 FetchSessionHandler
    Map<Node, FetchSessionHandler> sessionHandlers;

    void sendFetches() {
        for (Map.Entry<Node, List<TopicPartition>> entry : partitionsByNode) {
            Node node = entry.getKey();
            List<TopicPartition> partitions = entry.getValue();

            FetchSessionHandler handler = sessionHandlers.computeIfAbsent(
                node, n -> new FetchSessionHandler()
            );

            // 使用 session handler 构建增量请求
            FetchRequestData data = handler.build(partitions);

            client.send(node, data.toSend());
        }
    }
}
```

### Session 工作流程
```
第一次请求（FULL）：
┌─────────────────────────────────────────────────────┐
│ FetchRequest (sessionId=0, epoch=0)                 │
│   - p0: offset=1000                                 │
│   - p1: offset=2000                                 │
│   - p2: offset=3000                                 │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│ FetchResponse                                        │
│   - sessionId=123 (新分配)                          │
│   - epoch=1                                         │
│   - p0: [records]                                   │
│   - p1: []                                          │
│   - p2: [records]                                   │
└─────────────────────────────────────────────────────┘

第二次请求（INCREMENTAL）：
┌─────────────────────────────────────────────────────┐
│ FetchRequest (sessionId=123, epoch=1)               │
│   - p0: offset=1010  // 变化了                     │
│   - p2: offset=3010  // 变化了                     │
│   (p1 没变，不发送)                                 │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│ FetchResponse (epoch=2)                             │
│   - p0: [records]                                   │
│   - p2: [records]                                   │
│   (p1 没有数据，不返回)                             │
└─────────────────────────────────────────────────────┘
```

### 引入的复杂度
1. **FetchSessionHandler 类**：管理 session
2. **Session ID + Epoch**：追踪 session 版本
3. **增量计算**：比较上次和本次的差异
4. **缓存上次请求**：需要记住上次发送了什么
5. **为什么？** 优化网络带宽，减少重复数据传输

---

## Step 8: 引入 Preferred Read Replica - 优化读取

### 新需求
**支持从最近的 follower 读取，而不总是从 leader 读取。**

**为什么？**
```
场景：跨数据中心部署
  Leader: us-west
  Follower: us-east
  Consumer: us-east

传统方式：
  Consumer (us-east) -> Leader (us-west)
  网络延迟：100ms

优化方式：
  Consumer (us-east) -> Follower (us-east)
  网络延迟：5ms ✅
```

### 新增复杂度
1. **Preferred Replica 追踪**
2. **动态切换读取的 replica**
3. **过期时间管理**

### 实现
```java
class TopicPartitionState {
    FetchPosition position;
    Integer preferredReadReplica;  // 新增：优先读取的 replica
    Long preferredReadReplicaExpireTimeMs;  // 新增：过期时间

    void updatePreferredReadReplica(int replica, long expireTimeMs) {
        this.preferredReadReplica = replica;
        this.preferredReadReplicaExpireTimeMs = expireTimeMs;
    }

    boolean hasPreferredReadReplica() {
        return preferredReadReplica != null &&
               System.currentTimeMillis() < preferredReadReplicaExpireTimeMs;
    }
}

class Fetcher {
    void sendFetches() {
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = subscriptions.stateOf(partition);

            Node fetchTarget;
            if (state.hasPreferredReadReplica()) {
                // 从 preferred replica 读取
                fetchTarget = metadata.nodeById(state.preferredReadReplica);
            } else {
                // 从 leader 读取
                fetchTarget = metadata.leaderFor(partition);
            }

            // 发送到对应的 Node
            // ...
        }
    }

    void handleFetchResponse(FetchResponse response) {
        for (Map.Entry<TopicPartition, FetchData> entry : response) {
            FetchData data = entry.getValue();

            if (data.preferredReadReplica != null) {
                // Broker 建议使用这个 replica
                subscriptions.updatePreferredReadReplica(
                    partition,
                    data.preferredReadReplica,
                    data.preferredReadReplicaExpireTime
                );
            }
        }
    }
}
```

### 引入的复杂度
1. **preferredReadReplica 字段**：追踪优先 replica
2. **过期时间管理**：preferred replica 有有效期
3. **动态路由**：根据 preferred replica 选择 fetch target
4. **为什么？** 优化跨数据中心场景，降低延迟

---

## Step 9: 引入 Transaction 支持 - Isolation Level

### 新需求
**支持事务消费，Consumer 可以选择只读取 committed 消息。**

**为什么？**
```
场景：
Producer 发送事务消息：
  beginTransaction()
  send(record1)  // offset 100
  send(record2)  // offset 101
  commitTransaction()  // 提交

如果 Consumer 设置 isolation.level=read_committed:
  在事务提交前，offset 100-101 不应该被消费
  只消费到 Last Stable Offset (LSO)
```

### 新增复杂度
1. **Last Stable Offset 追踪**
2. **过滤未提交的消息**
3. **Control Records 处理**

### 实现
```java
class TopicPartitionState {
    FetchPosition position;
    Long highWatermark;      // HWM
    Long logStartOffset;     // LSO
    Long lastStableOffset;   // 新增：Last Stable Offset (for transactions)

    void updateLastStableOffset(long lso) {
        this.lastStableOffset = lso;
    }
}

class Fetcher {
    IsolationLevel isolationLevel;  // READ_UNCOMMITTED or READ_COMMITTED

    void handleFetchResponse(FetchResponse response) {
        for (Map.Entry<TopicPartition, FetchData> entry : response) {
            FetchData data = entry.getValue();

            // 更新 LSO
            subscriptions.updateLastStableOffset(
                partition,
                data.lastStableOffset
            );

            List<Record> records = data.records;

            if (isolationLevel == IsolationLevel.READ_COMMITTED) {
                // 过滤掉未提交的消息
                records = filterAbortedTransactions(records, data.abortedTransactions);
            }

            fetchBuffer.add(partition, records);
        }
    }

    List<Record> filterAbortedTransactions(
        List<Record> records,
        List<AbortedTransaction> aborted
    ) {
        // 过滤掉 aborted transactions 中的消息
        // ...
    }
}
```

### 引入的复杂度
1. **lastStableOffset 字段**：追踪 LSO
2. **IsolationLevel 配置**：READ_UNCOMMITTED vs READ_COMMITTED
3. **AbortedTransaction 处理**：过滤被 abort 的消息
4. **Control Records**：处理事务控制消息（COMMIT/ABORT markers）
5. **为什么？** 支持事务语义，保证读取一致性

---

## 完整架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    KafkaConsumer                                 │
│  (应用层 API)                                                    │
├─────────────────────────────────────────────────────────────────┤
│  - subscribe(topics)                                            │
│  - assign(partitions)                                           │
│  - poll(Duration timeout)                                       │
│  - commitSync() / commitAsync()                                 │
│  - pause(partitions) / resume(partitions)                       │
│  - seek(partition, offset)                                      │
└─────────────────────┬───────────────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
         ▼                         ▼
┌──────────────────────┐  ┌──────────────────────┐
│ SubscriptionState    │  │ Fetcher              │
├──────────────────────┤  ├──────────────────────┤
│ (消费状态管理)       │  │ (Fetch 逻辑)          │
└──────────────────────┘  └──────────────────────┘
         │                         │
         │                         │
┌────────┴─────────────────────────┴────────┐
│                                            │
│  SubscriptionState                         │
│  ┌──────────────────────────────────────┐ │
│  │ assignment: Map<TP, TPState>         │ │
│  │   ├─ orders-0 -> TopicPartitionState│ │
│  │   ├─ orders-1 -> TopicPartitionState│ │
│  │   └─ orders-2 -> TopicPartitionState│ │
│  └──────────────────────────────────────┘ │
│                                            │
│  TopicPartitionState:                     │
│    ├─ position: FetchPosition             │
│    │    - offset: 1000                    │
│    │    - offsetEpoch: 5                  │
│    │    - currentLeader: (broker1, epoch=5)│
│    ├─ fetchState: FETCHING                │
│    ├─ paused: false                       │
│    ├─ highWatermark: 2000                 │
│    ├─ lastStableOffset: 1500              │
│    ├─ preferredReadReplica: broker2       │
│    └─ preferredReadReplicaExpireTimeMs    │
└────────────────────────────────────────────┘

┌────────────────────────────────────────────┐
│  Fetcher                                   │
│  ┌──────────────────────────────────────┐ │
│  │ sessionHandlers: Map<Node, Handler>  │ │
│  │   ├─ broker1 -> FetchSessionHandler  │ │
│  │   └─ broker2 -> FetchSessionHandler  │ │
│  └──────────────────────────────────────┘ │
│                                            │
│  ┌──────────────────────────────────────┐ │
│  │ fetchBuffer: Map<TP, Queue<Records>> │ │
│  └──────────────────────────────────────┘ │
│                                            │
│  核心方法:                                 │
│  ├─ sendFetches()                         │
│  │    - 按 Node 分组                      │
│  │    - 使用 FetchSession 优化            │
│  │    - 考虑 preferredReadReplica         │
│  ├─ handleFetchResponse()                 │
│  │    - 更新 SubscriptionState.position   │
│  │    - 处理 transactions                 │
│  │    - 存储到 fetchBuffer                │
│  └─ collectFetch()                        │
│       - 从 fetchBuffer 取出 records       │
│       - 返回给应用层                       │
└────────────────────────────────────────────┘
```

---

## 复杂度总结表

| Step | 新增组件 | 引入原因 | 复杂度类型 |
|------|---------|---------|-----------|
| 1 | 基础消费 | 基本功能 | - |
| 2 | SubscriptionState, TopicPartitionState | 支持多 partition | 数据结构 |
| 3 | Fetcher | 批量 fetch，提升性能 | 网络优化 |
| 4 | FetchState 状态机 | 精确控制 fetch 时机 | 状态管理 |
| 5 | Leader Epoch, Offset Validation | 处理 leader 变更，保证一致性 | 一致性保证 |
| 6 | Pause/Resume | 流控能力 | 用户控制 |
| 7 | FetchSession | 优化重复请求，节省带宽 | 性能优化 |
| 8 | Preferred Read Replica | 降低跨数据中心延迟 | 性能优化 |
| 9 | Transaction Support (LSO) | 支持事务消费 | 一致性保证 |

---

## 关键设计决策

### 1. 为什么状态在 Consumer 端而不是 Broker 端？

**Consumer Group 设计**：
- ✅ **Partition 独占** → Consumer 自己管理就够了
- ✅ **Broker 无状态** → 简单，易扩展
- ✅ **灵活 seek** → Consumer 可以随时 seek 到任意 offset

**对比 Share Group**：
- ❌ **Partition 共享** → 需要 Broker 协调
- ❌ **Broker 有状态** → 复杂，但必须

### 2. 为什么需要 FetchState 状态机？

**问题**：
```
场景 1：Partition 刚分配
  -> position = null
  -> 如果直接 fetch，会报错

场景 2：OffsetOutOfRange
  -> 需要 reset offset
  -> 如果直接 fetch，会陷入循环
```

**解决方案**：
- 用状态机明确区分不同阶段
- 只有 FETCHING 状态才可以 fetch
- 其他状态需要先完成初始化/reset/validation

### 3. 为什么需要 FetchSession？

**没有 FetchSession**：
```
每次 FetchRequest 都发送完整的 partition 列表
  -> 如果订阅 100 个 partitions
  -> 每次都传输 100 个 partition 的信息
  -> 浪费带宽
```

**有 FetchSession**：
```
第一次：发送完整列表（100 个 partitions）
后续：只发送变化的（可能只有 5 个 partition 的 offset 变化）
  -> 节省 95% 的数据传输 ✅
```

### 4. 为什么需要 Leader Epoch Validation？

**问题**：
```
Leader 切换后，新 Leader 的 log 可能更短
  -> Consumer 的 offset 可能已经被 truncate
  -> 如果不检测，会丢失数据
```

**解决方案**：
```
每次检测到 leader 变化：
  1. 发送 OffsetForLeaderEpoch 请求
  2. 检查我的 offset 是否还在新 leader 的 log 中
  3. 如果被 truncate，回退到安全的 offset
```

---

## 与 SharePartition 的对比总结

| 维度 | SubscriptionState + Fetcher | SharePartition |
|------|---------------------------|----------------|
| **位置** | Consumer 端 | Broker 端 |
| **状态粒度** | Partition 级别 | 消息级别 |
| **状态内容** | FetchPosition (offset + epoch) | RecordState (AVAILABLE/ACQUIRED/...) |
| **持久化** | 按需 (commit 时) | 实时 |
| **复杂度** | 中等（状态机 + session + validation） | 高（状态机 + 锁 + 持久化） |
| **优化重点** | 网络传输（FetchSession） | 消息分配（锁机制） |
| **一致性保证** | Leader Epoch Validation | Persister + StateEpoch |

---

## 总结

SubscriptionState 和 Fetcher 从简单到复杂的演化：

1. **Step 1-2**：基础功能 - 管理多 partition 的消费进度
2. **Step 3**：性能优化 - 批量 fetch
3. **Step 4**：状态管理 - FetchState 状态机
4. **Step 5**：一致性 - Leader Epoch Validation
5. **Step 6**：用户控制 - Pause/Resume
6. **Step 7**：网络优化 - FetchSession
7. **Step 8**：延迟优化 - Preferred Read Replica
8. **Step 9**：事务支持 - Isolation Level

每一步都解决了实际的问题，逐步构建出一个完整的、高性能的、可靠的消费系统！
