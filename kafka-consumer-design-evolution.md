# Kafka Consumer 设计演进：从简单到复杂

## 目录
1. [MVP 1: 最简单的消息拉取](#mvp-1-最简单的消息拉取)
2. [MVP 2: 添加 Offset 管理](#mvp-2-添加-offset-管理)
3. [MVP 3: 添加 Consumer Group 协调](#mvp-3-添加-consumer-group-协调)
4. [MVP 4: 添加 Rebalance 机制](#mvp-4-添加-rebalance-机制)
5. [MVP 5: 添加容错和优化](#mvp-5-添加容错和优化)
6. [最终设计总结](#最终设计总结)

---

## MVP 1: 最简单的消息拉取

### 问题定义
最基础的需求：从 Kafka 某个 topic 的某个 partition 读取消息。

### 核心设计决策

**决策1：Pull vs Push 模型**

为什么选择 Pull（消费者主动拉取）而不是 Push（broker 推送）？

1. **消费速度控制**：不同消费者处理能力不同
   - 数据库写入慢 → 需要慢速消费
   - 内存处理快 → 需要快速消费
   - Push 模型容易压垮慢消费者

2. **批量处理优化**：消费者可以决定一次拉多少
   - 处理能力强 → 一次拉 1000 条
   - 处理能力弱 → 一次拉 10 条

3. **简化 Broker**：Broker 不需要追踪每个消费者的状态

### MVP 1 代码

```java
/**
 * 最简单的 Consumer：只能从指定 partition 拉取消息
 */
public class SimpleConsumerV1 {
    private final String brokerAddress;
    private final String topic;
    private final int partition;
    private Socket socket;

    public SimpleConsumerV1(String brokerAddress, String topic, int partition) {
        this.brokerAddress = brokerAddress;
        this.topic = topic;
        this.partition = partition;
    }

    public void connect() throws IOException {
        // 连接到指定的 broker
        String[] parts = brokerAddress.split(":");
        socket = new Socket(parts[0], Integer.parseInt(parts[1]));
    }

    /**
     * 拉取消息的核心方法
     * @param offset 从哪个 offset 开始读
     * @param maxBytes 最多读多少字节
     * @return 消息列表
     */
    public List<Message> fetch(long offset, int maxBytes) throws IOException {
        // 构造 Fetch 请求
        FetchRequest request = new FetchRequest(topic, partition, offset, maxBytes);

        // 发送请求
        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        // 接收响应
        InputStream in = socket.getInputStream();
        FetchResponse response = FetchResponse.deserialize(in);

        return response.getMessages();
    }

    public void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }
}

// 使用示例
public class ConsumerExample1 {
    public static void main(String[] args) throws IOException {
        SimpleConsumerV1 consumer = new SimpleConsumerV1(
            "localhost:9092",
            "my-topic",
            0  // partition 0
        );

        consumer.connect();

        long offset = 0;  // 从头开始读
        while (true) {
            List<Message> messages = consumer.fetch(offset, 1024 * 1024); // 每次最多 1MB

            for (Message msg : messages) {
                System.out.println("Received: " + msg.getValue());
                offset = msg.getOffset() + 1;  // 手动更新 offset
            }

            Thread.sleep(100);  // 避免空轮询
        }
    }
}
```

### MVP 1 的问题

1. ❌ **Offset 管理全靠用户**：用户需要自己记录 offset，重启后不知道从哪读
2. ❌ **只能读一个 partition**：无法并行消费多个 partition
3. ❌ **无容错机制**：broker 挂了就崩溃
4. ❌ **无协作机制**：多个消费者无法协同工作

---

## MVP 2: 添加 Offset 管理

### 新问题
消费者重启后，如何知道上次消费到哪里了？

### 核心设计决策

**决策2：Position vs Committed Offset 分离**

为什么需要两个 offset？

1. **Position（当前位置）**
   - 内存中的状态
   - 表示"下一条要读的消息"
   - 随着 `fetch()` 自动前进
   - 进程崩溃会丢失

2. **Committed Offset（已提交位置）**
   - 持久化到 Kafka（特殊 topic `__consumer_offsets`）
   - 表示"已经成功处理的消息"
   - 需要显式 commit
   - 进程崩溃后恢复的依据

**为什么分离？**
- 允许用户控制"何时算消费成功"
- 支持不同的消息语义保证

### 消息语义保证

```
场景1：先 commit 再处理（at-most-once）
┌─────────┐     ┌─────────┐     ┌─────────┐
│ fetch   │────▶│ commit  │────▶│ process │
└─────────┘     └─────────┘     └─────────┘
                                      ↓ (crash)
                                   丢失消息

场景2：先处理再 commit（at-least-once）
┌─────────┐     ┌─────────┐     ┌─────────┐
│ fetch   │────▶│ process │────▶│ commit  │
└─────────┘     └─────────┘     └─────────┘
                      ↓ (crash)
                   重复消费

场景3：事务性 commit（exactly-once）
┌─────────┐     ┌──────────────────────┐     ┌─────────┐
│ fetch   │────▶│ process + commit 原子 │────▶│ success │
└─────────┘     └──────────────────────┘     └─────────┘
```

### MVP 2 代码

```java
/**
 * 添加 Offset 管理的 Consumer
 */
public class SimpleConsumerV2 {
    private final String brokerAddress;
    private final String topic;
    private final int partition;
    private final String consumerGroup;  // 新增：消费者组 ID

    private Socket socket;
    private long currentPosition;  // 当前读取位置（内存）

    public SimpleConsumerV2(String brokerAddress, String topic, int partition, String consumerGroup) {
        this.brokerAddress = brokerAddress;
        this.topic = topic;
        this.partition = partition;
        this.consumerGroup = consumerGroup;
    }

    public void connect() throws IOException {
        String[] parts = brokerAddress.split(":");
        socket = new Socket(parts[0], Integer.parseInt(parts[1]));

        // 连接后，从 Kafka 读取上次提交的 offset
        currentPosition = fetchCommittedOffset();

        if (currentPosition < 0) {
            // 如果没有提交过 offset，从头开始
            currentPosition = 0;
        }
    }

    /**
     * 从 Kafka 获取上次提交的 offset
     */
    private long fetchCommittedOffset() throws IOException {
        OffsetFetchRequest request = new OffsetFetchRequest(
            consumerGroup,
            topic,
            partition
        );

        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        OffsetFetchResponse response = OffsetFetchResponse.deserialize(in);

        return response.getOffset();
    }

    /**
     * 拉取消息（自动更新 position）
     */
    public List<Message> poll(int maxRecords) throws IOException {
        List<Message> messages = fetch(currentPosition, maxRecords * 1024);

        if (!messages.isEmpty()) {
            // 自动前进 position
            currentPosition = messages.get(messages.size() - 1).getOffset() + 1;
        }

        return messages;
    }

    /**
     * 提交 offset 到 Kafka
     */
    public void commitSync() throws IOException {
        OffsetCommitRequest request = new OffsetCommitRequest(
            consumerGroup,
            topic,
            partition,
            currentPosition  // 提交当前 position
        );

        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        OffsetCommitResponse response = OffsetCommitResponse.deserialize(in);

        if (!response.isSuccess()) {
            throw new IOException("Failed to commit offset");
        }
    }

    /**
     * 异步提交（不等待响应）
     */
    public void commitAsync() throws IOException {
        OffsetCommitRequest request = new OffsetCommitRequest(
            consumerGroup,
            topic,
            partition,
            currentPosition
        );

        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        // 不等待响应，直接返回
    }

    private List<Message> fetch(long offset, int maxBytes) throws IOException {
        FetchRequest request = new FetchRequest(topic, partition, offset, maxBytes);
        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        FetchResponse response = FetchResponse.deserialize(in);
        return response.getMessages();
    }

    public void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }
}

// 使用示例：At-least-once 语义
public class ConsumerExample2 {
    public static void main(String[] args) throws Exception {
        SimpleConsumerV2 consumer = new SimpleConsumerV2(
            "localhost:9092",
            "my-topic",
            0,
            "my-consumer-group"
        );

        consumer.connect();

        while (true) {
            List<Message> messages = consumer.poll(100);

            // 先处理消息
            for (Message msg : messages) {
                processMessage(msg);  // 可能失败
            }

            // 处理成功后才 commit
            if (!messages.isEmpty()) {
                consumer.commitSync();  // 阻塞等待 commit 成功
            }

            Thread.sleep(100);
        }
    }

    private static void processMessage(Message msg) {
        // 写入数据库、发送邮件等
        System.out.println("Processed: " + msg.getValue());
    }
}

// 使用示例：批量 commit 优化
public class ConsumerExample2Batch {
    public static void main(String[] args) throws Exception {
        SimpleConsumerV2 consumer = new SimpleConsumerV2(
            "localhost:9092",
            "my-topic",
            0,
            "my-consumer-group"
        );

        consumer.connect();

        List<Message> buffer = new ArrayList<>();
        int batchSize = 100;

        while (true) {
            List<Message> messages = consumer.poll(10);
            buffer.addAll(messages);

            // 累积到一定数量后批量处理
            if (buffer.size() >= batchSize) {
                processMessagesBatch(buffer);
                consumer.commitSync();  // 批量 commit
                buffer.clear();
            }
        }
    }

    private static void processMessagesBatch(List<Message> messages) {
        // 批量插入数据库
    }
}
```

### 为什么需要 commitSync 和 commitAsync？

**commitSync（同步提交）**
- 优点：保证 commit 成功
- 缺点：阻塞，影响吞吐量
- 适用：关键业务，不能容忍重复

**commitAsync（异步提交）**
- 优点：不阻塞，高吞吐量
- 缺点：可能失败（网络问题）
- 适用：高性能场景，可容忍少量重复

**最佳实践：异步为主 + 同步兜底**
```java
while (true) {
    List<Message> messages = consumer.poll(100);
    process(messages);
    consumer.commitAsync();  // 日常用异步
}

// 关闭前用同步保证最后一次 commit 成功
consumer.commitSync();
consumer.close();
```

### MVP 2 的问题

1. ✅ Offset 管理：已解决，支持持久化和恢复
2. ❌ **仍然只能读一个 partition**：无法扩展
3. ❌ **多个消费者无法协作**：两个消费者会重复消费
4. ❌ **无法动态调整负载**：partition 数量变化时无法适应

---

## MVP 3: 添加 Consumer Group 协调

### 新问题
如何让多个消费者协同工作，共同消费一个 topic 的所有 partition？

### 业务场景

假设有一个 topic 有 4 个 partition，希望：
- 单个消费者 → 处理所有 4 个 partition（慢）
- 2 个消费者 → 每个处理 2 个 partition（快一倍）
- 4 个消费者 → 每个处理 1 个 partition（快四倍）

```
Topic: orders (4 partitions)

场景1：1 个 consumer
┌─────────────────────────────────────┐
│         Consumer A                  │
│  P0, P1, P2, P3                    │
└─────────────────────────────────────┘

场景2：2 个 consumers
┌──────────────────┐  ┌──────────────────┐
│   Consumer A     │  │   Consumer B     │
│   P0, P1         │  │   P2, P3         │
└──────────────────┘  └──────────────────┘

场景3：4 个 consumers
┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐
│  A  │  │  B  │  │  C  │  │  D  │
│ P0  │  │ P1  │  │ P2  │  │ P3  │
└─────┘  └─────┘  └─────┘  └─────┘
```

### 核心设计决策

**决策3：引入 Consumer Group Coordinator**

为什么需要协调者？

1. **分区分配**：决定哪个 consumer 负责哪些 partition
2. **成员管理**：追踪哪些 consumer 还活着
3. **故障检测**：consumer 挂了要把它的 partition 分给别人

**协调者放在哪？**
- ❌ 客户端选举：复杂，需要处理脑裂
- ✅ Broker 端服务：利用 Kafka 自身的高可用

### Consumer Group 核心概念

**Group ID**：同一个 group 的 consumer 共同消费所有 partition
```java
// 这两个 consumer 属于同一个 group，会分摊 partition
Consumer c1 = new Consumer("group-1");
Consumer c2 = new Consumer("group-1");

// 这个 consumer 属于不同 group，会收到所有消息（类似 pub-sub）
Consumer c3 = new Consumer("group-2");
```

**分区分配策略**
1. **Range**：按 partition 编号连续分配
2. **RoundRobin**：轮询分配
3. **Sticky**：尽量保持之前的分配（减少数据迁移）

### MVP 3 代码

```java
/**
 * 支持 Consumer Group 的 Consumer
 */
public class SimpleConsumerV3 {
    private final String brokerAddress;
    private final Set<String> topics;  // 订阅的 topic 列表
    private final String consumerGroup;
    private final String consumerId;

    private Socket socket;
    private Map<TopicPartition, Long> positions;  // 每个 partition 的 position
    private Set<TopicPartition> assignedPartitions;  // 分配给我的 partition

    public SimpleConsumerV3(String brokerAddress, Set<String> topics, String consumerGroup) {
        this.brokerAddress = brokerAddress;
        this.topics = topics;
        this.consumerGroup = consumerGroup;
        this.consumerId = generateConsumerId();  // 唯一 ID
        this.positions = new HashMap<>();
        this.assignedPartitions = new HashSet<>();
    }

    private String generateConsumerId() {
        return consumerGroup + "-" + UUID.randomUUID().toString();
    }

    /**
     * 订阅 topics（核心 API）
     */
    public void subscribe(Collection<String> topics) {
        this.topics.clear();
        this.topics.addAll(topics);
    }

    public void connect() throws IOException {
        String[] parts = brokerAddress.split(":");
        socket = new Socket(parts[0], Integer.parseInt(parts[1]));

        // 加入 consumer group
        joinGroup();
    }

    /**
     * 加入 Consumer Group 并获取分区分配
     */
    private void joinGroup() throws IOException {
        // 1. 发送 JoinGroup 请求
        JoinGroupRequest joinRequest = new JoinGroupRequest(
            consumerGroup,
            consumerId,
            topics,
            "range"  // 分区分配策略
        );

        OutputStream out = socket.getOutputStream();
        out.write(joinRequest.serialize());

        // 2. 接收分配结果
        InputStream in = socket.getInputStream();
        JoinGroupResponse joinResponse = JoinGroupResponse.deserialize(in);

        // 3. 保存分配的 partition
        assignedPartitions = joinResponse.getAssignedPartitions();

        // 4. 为每个 partition 获取 committed offset
        for (TopicPartition tp : assignedPartitions) {
            long offset = fetchCommittedOffset(tp);
            if (offset < 0) {
                offset = 0;  // 如果没有 committed offset，从头开始
            }
            positions.put(tp, offset);
        }

        System.out.println("Assigned partitions: " + assignedPartitions);
    }

    private long fetchCommittedOffset(TopicPartition tp) throws IOException {
        // 类似 MVP 2，但支持多个 partition
        OffsetFetchRequest request = new OffsetFetchRequest(
            consumerGroup,
            tp.topic(),
            tp.partition()
        );

        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        OffsetFetchResponse response = OffsetFetchResponse.deserialize(in);
        return response.getOffset();
    }

    /**
     * 拉取消息（从所有分配的 partition）
     */
    public Map<TopicPartition, List<Message>> poll(Duration timeout) throws IOException {
        Map<TopicPartition, List<Message>> result = new HashMap<>();

        // 为每个分配的 partition 发起 fetch
        for (TopicPartition tp : assignedPartitions) {
            long offset = positions.get(tp);
            List<Message> messages = fetch(tp, offset, 1024 * 1024);

            if (!messages.isEmpty()) {
                result.put(tp, messages);
                // 更新 position
                positions.put(tp, messages.get(messages.size() - 1).getOffset() + 1);
            }
        }

        return result;
    }

    /**
     * 提交所有 partition 的 offset
     */
    public void commitSync() throws IOException {
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offset = entry.getValue();

            OffsetCommitRequest request = new OffsetCommitRequest(
                consumerGroup,
                tp.topic(),
                tp.partition(),
                offset
            );

            OutputStream out = socket.getOutputStream();
            out.write(request.serialize());

            InputStream in = socket.getInputStream();
            OffsetCommitResponse response = OffsetCommitResponse.deserialize(in);

            if (!response.isSuccess()) {
                throw new IOException("Failed to commit offset for " + tp);
            }
        }
    }

    private List<Message> fetch(TopicPartition tp, long offset, int maxBytes) throws IOException {
        FetchRequest request = new FetchRequest(tp.topic(), tp.partition(), offset, maxBytes);
        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        FetchResponse response = FetchResponse.deserialize(in);
        return response.getMessages();
    }

    public void close() throws IOException {
        // 离开 consumer group
        leaveGroup();

        if (socket != null) {
            socket.close();
        }
    }

    private void leaveGroup() throws IOException {
        LeaveGroupRequest request = new LeaveGroupRequest(consumerGroup, consumerId);
        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());
    }
}

// 使用示例：多个 consumer 协同工作
public class ConsumerExample3 {
    public static void main(String[] args) throws Exception {
        // Consumer 1
        new Thread(() -> {
            try {
                SimpleConsumerV3 consumer = new SimpleConsumerV3(
                    "localhost:9092",
                    Set.of("orders"),
                    "order-processors"
                );
                consumer.connect();

                while (true) {
                    Map<TopicPartition, List<Message>> records = consumer.poll(Duration.ofMillis(100));

                    for (Map.Entry<TopicPartition, List<Message>> entry : records.entrySet()) {
                        System.out.println("Consumer 1 - Partition: " + entry.getKey());
                        for (Message msg : entry.getValue()) {
                            processMessage(msg);
                        }
                    }

                    if (!records.isEmpty()) {
                        consumer.commitSync();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Consumer 2（自动会分配不同的 partition）
        new Thread(() -> {
            try {
                SimpleConsumerV3 consumer = new SimpleConsumerV3(
                    "localhost:9092",
                    Set.of("orders"),
                    "order-processors"  // 同一个 group
                );
                consumer.connect();

                while (true) {
                    Map<TopicPartition, List<Message>> records = consumer.poll(Duration.ofMillis(100));

                    for (Map.Entry<TopicPartition, List<Message>> entry : records.entrySet()) {
                        System.out.println("Consumer 2 - Partition: " + entry.getKey());
                        for (Message msg : entry.getValue()) {
                            processMessage(msg);
                        }
                    }

                    if (!records.isEmpty()) {
                        consumer.commitSync();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static void processMessage(Message msg) {
        System.out.println("Processed: " + msg.getValue());
    }
}
```

### 分区分配示例

假设 topic `orders` 有 4 个 partition：

```
初始状态：只有 Consumer A
┌─────────────────────────────────────┐
│         Consumer A                  │
│  P0, P1, P2, P3                    │
└─────────────────────────────────────┘

Consumer B 加入后：重新分配
┌──────────────────┐  ┌──────────────────┐
│   Consumer A     │  │   Consumer B     │
│   P0, P1         │  │   P2, P3         │
└──────────────────┘  └──────────────────┘

Consumer C 加入后：再次重新分配
┌──────────┐  ┌──────────┐  ┌──────────┐
│    A     │  │    B     │  │    C     │
│  P0, P1  │  │    P2    │  │    P3    │
└──────────┘  └──────────┘  └──────────┘

Consumer A 挂了：P0 和 P1 重新分配给 B 和 C
              ┌──────────┐  ┌──────────┐
              │    B     │  │    C     │
              │ P0, P2   │  │ P1, P3   │
              └──────────┘  └──────────┘
```

### MVP 3 的问题

1. ✅ 多 partition 消费：已解决
2. ✅ 多消费者协作：已解决
3. ❌ **Rebalance 过程中的消息丢失/重复**：上面的分配切换不是原子的
4. ❌ **无心跳机制**：无法检测 consumer 是否还活着
5. ❌ **无法感知 partition 数量变化**：topic 扩容后感知不到

---

## MVP 4: 添加 Rebalance 机制

### 新问题
Consumer 加入/离开时，如何安全地重新分配 partition？

### 问题场景

```
场景：Consumer A 挂了，partition 重新分配

时间线：
T1: Consumer A 处理 P0，position = 100
T2: Consumer A 挂了（未 commit）
T3: Coordinator 检测到 A 挂了
T4: 重新分配 P0 给 Consumer B
T5: Consumer B 从 committed offset = 50 开始读（因为 A 未 commit）
T6: 重复消费 offset 50-99 的消息 ❌
```

### 核心设计决策

**决策4：Rebalance 协议**

Rebalance 需要保证：
1. **停止旧的消费**：避免两个 consumer 同时消费同一个 partition
2. **保存状态**：旧 consumer 有机会 commit offset
3. **分配新 partition**：新 consumer 接管
4. **恢复消费**：从正确的 offset 开始

**Rebalance 两阶段协议**
```
Phase 1: Revoke（撤销）
- 停止 fetch
- commit offset
- 释放 partition 所有权

Phase 2: Assign（分配）
- 接收新的 partition 分配
- 获取 committed offset
- 开始 fetch
```

**决策5：心跳机制**

为什么需要心跳？
1. **活性检测**：判断 consumer 是否还活着
2. **Rebalance 触发**：coordinator 通过心跳响应通知 rebalance

**两个超时参数**
```java
session.timeout.ms = 10000  // 心跳超时
max.poll.interval.ms = 300000  // poll 调用间隔超时
```

为什么需要两个？
- `session.timeout.ms`：检测进程挂了（网络断、进程死）
- `max.poll.interval.ms`：检测 livelock（进程活着但卡死了）

```
场景1：进程挂了
Consumer -> (heartbeat) -> Coordinator
        10s 没心跳
        ↓
Coordinator 认为 consumer 挂了，触发 rebalance

场景2：处理卡死（livelock）
Consumer 在处理一个很大的消息，5 分钟还没处理完
- 心跳线程还在发送心跳 ✓
- 但是 poll() 超过 5 分钟没调用 ✗
- Coordinator 认为 consumer 卡死了，触发 rebalance
```

### MVP 4 代码

```java
/**
 * 支持 Rebalance 的 Consumer
 */
public class SimpleConsumerV4 {
    private final String brokerAddress;
    private final Set<String> topics;
    private final String consumerGroup;
    private final String consumerId;
    private final RebalanceListener rebalanceListener;  // 新增

    private Socket socket;
    private Map<TopicPartition, Long> positions;
    private Set<TopicPartition> assignedPartitions;

    // 心跳相关
    private final HeartbeatThread heartbeatThread;
    private final int sessionTimeoutMs;
    private final int maxPollIntervalMs;
    private volatile long lastPollTime;

    public SimpleConsumerV4(String brokerAddress,
                           Set<String> topics,
                           String consumerGroup,
                           RebalanceListener listener) {
        this.brokerAddress = brokerAddress;
        this.topics = topics;
        this.consumerGroup = consumerGroup;
        this.consumerId = generateConsumerId();
        this.rebalanceListener = listener;
        this.positions = new HashMap<>();
        this.assignedPartitions = new HashSet<>();

        // 配置超时参数
        this.sessionTimeoutMs = 10000;  // 10s
        this.maxPollIntervalMs = 300000;  // 5min
        this.lastPollTime = System.currentTimeMillis();

        // 启动心跳线程
        this.heartbeatThread = new HeartbeatThread(this);
    }

    private String generateConsumerId() {
        return consumerGroup + "-" + UUID.randomUUID().toString();
    }

    public void connect() throws IOException {
        String[] parts = brokerAddress.split(":");
        socket = new Socket(parts[0], Integer.parseInt(parts[1]));

        // 加入 group 并获取分配
        joinGroup();

        // 启动心跳线程
        heartbeatThread.start();
    }

    private void joinGroup() throws IOException {
        // 1. 发送 JoinGroup 请求
        JoinGroupRequest joinRequest = new JoinGroupRequest(
            consumerGroup,
            consumerId,
            sessionTimeoutMs,
            topics,
            "range"
        );

        OutputStream out = socket.getOutputStream();
        out.write(joinRequest.serialize());

        InputStream in = socket.getInputStream();
        JoinGroupResponse joinResponse = JoinGroupResponse.deserialize(in);

        // 2. 如果有旧的分配，先撤销（Revoke）
        if (!assignedPartitions.isEmpty()) {
            Set<TopicPartition> revokedPartitions = new HashSet<>(assignedPartitions);

            // 调用 rebalance listener
            rebalanceListener.onPartitionsRevoked(revokedPartitions);

            // 提交 offset
            commitSync();

            // 清空旧分配
            assignedPartitions.clear();
            positions.clear();
        }

        // 3. 接收新分配（Assign）
        assignedPartitions = joinResponse.getAssignedPartitions();

        // 4. 为每个 partition 获取 offset
        for (TopicPartition tp : assignedPartitions) {
            long offset = fetchCommittedOffset(tp);
            if (offset < 0) {
                offset = 0;
            }
            positions.put(tp, offset);
        }

        // 5. 调用 rebalance listener
        rebalanceListener.onPartitionsAssigned(assignedPartitions);

        System.out.println("Rebalance completed. Assigned: " + assignedPartitions);
    }

    /**
     * 发送心跳
     */
    void sendHeartbeat() throws IOException {
        HeartbeatRequest request = new HeartbeatRequest(
            consumerGroup,
            consumerId
        );

        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        HeartbeatResponse response = HeartbeatResponse.deserialize(in);

        // 检查是否需要 rebalance
        if (response.shouldRebalance()) {
            System.out.println("Rebalance triggered by coordinator");
            joinGroup();  // 重新 join group
        }
    }

    /**
     * 拉取消息
     */
    public Map<TopicPartition, List<Message>> poll(Duration timeout) throws IOException {
        // 检查 poll 间隔
        long now = System.currentTimeMillis();
        if (now - lastPollTime > maxPollIntervalMs) {
            throw new IllegalStateException(
                "poll() was not called within max.poll.interval.ms"
            );
        }
        lastPollTime = now;

        Map<TopicPartition, List<Message>> result = new HashMap<>();

        for (TopicPartition tp : assignedPartitions) {
            long offset = positions.get(tp);
            List<Message> messages = fetch(tp, offset, 1024 * 1024);

            if (!messages.isEmpty()) {
                result.put(tp, messages);
                positions.put(tp, messages.get(messages.size() - 1).getOffset() + 1);
            }
        }

        return result;
    }

    public void commitSync() throws IOException {
        for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
            TopicPartition tp = entry.getKey();
            long offset = entry.getValue();

            OffsetCommitRequest request = new OffsetCommitRequest(
                consumerGroup,
                tp.topic(),
                tp.partition(),
                offset
            );

            OutputStream out = socket.getOutputStream();
            out.write(request.serialize());

            InputStream in = socket.getInputStream();
            OffsetCommitResponse response = OffsetCommitResponse.deserialize(in);

            if (!response.isSuccess()) {
                throw new IOException("Failed to commit offset for " + tp);
            }
        }
    }

    private long fetchCommittedOffset(TopicPartition tp) throws IOException {
        OffsetFetchRequest request = new OffsetFetchRequest(
            consumerGroup,
            tp.topic(),
            tp.partition()
        );

        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        OffsetFetchResponse response = OffsetFetchResponse.deserialize(in);
        return response.getOffset();
    }

    private List<Message> fetch(TopicPartition tp, long offset, int maxBytes) throws IOException {
        FetchRequest request = new FetchRequest(tp.topic(), tp.partition(), offset, maxBytes);
        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());

        InputStream in = socket.getInputStream();
        FetchResponse response = FetchResponse.deserialize(in);
        return response.getMessages();
    }

    public void close() throws IOException {
        // 停止心跳
        heartbeatThread.shutdown();

        // 最后一次 commit
        commitSync();

        // 离开 group
        leaveGroup();

        if (socket != null) {
            socket.close();
        }
    }

    private void leaveGroup() throws IOException {
        LeaveGroupRequest request = new LeaveGroupRequest(consumerGroup, consumerId);
        OutputStream out = socket.getOutputStream();
        out.write(request.serialize());
    }
}

/**
 * Rebalance 监听器
 */
interface RebalanceListener {
    /**
     * Partition 被撤销前调用
     * 通常在这里 commit offset、保存状态
     */
    void onPartitionsRevoked(Set<TopicPartition> partitions);

    /**
     * Partition 被分配后调用
     * 通常在这里初始化状态
     */
    void onPartitionsAssigned(Set<TopicPartition> partitions);
}

/**
 * 心跳线程
 */
class HeartbeatThread extends Thread {
    private final SimpleConsumerV4 consumer;
    private volatile boolean running = true;
    private final int heartbeatIntervalMs = 3000;  // 3s 发一次心跳

    public HeartbeatThread(SimpleConsumerV4 consumer) {
        this.consumer = consumer;
        this.setDaemon(true);
        this.setName("heartbeat-thread");
    }

    @Override
    public void run() {
        while (running) {
            try {
                consumer.sendHeartbeat();
                Thread.sleep(heartbeatIntervalMs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        running = false;
    }
}

// 使用示例：处理 Rebalance
public class ConsumerExample4 {
    public static void main(String[] args) throws Exception {
        // 本地状态（例如：每个 partition 的处理计数）
        Map<TopicPartition, Integer> partitionCounts = new ConcurrentHashMap<>();

        SimpleConsumerV4 consumer = new SimpleConsumerV4(
            "localhost:9092",
            Set.of("orders"),
            "order-processors",
            new RebalanceListener() {
                @Override
                public void onPartitionsRevoked(Set<TopicPartition> partitions) {
                    System.out.println("Revoking partitions: " + partitions);

                    // 保存本地状态到外部存储
                    for (TopicPartition tp : partitions) {
                        Integer count = partitionCounts.remove(tp);
                        if (count != null) {
                            saveToDatabase(tp, count);
                        }
                    }
                }

                @Override
                public void onPartitionsAssigned(Set<TopicPartition> partitions) {
                    System.out.println("Assigned partitions: " + partitions);

                    // 恢复本地状态
                    for (TopicPartition tp : partitions) {
                        Integer count = loadFromDatabase(tp);
                        partitionCounts.put(tp, count != null ? count : 0);
                    }
                }
            }
        );

        consumer.connect();

        while (true) {
            Map<TopicPartition, List<Message>> records = consumer.poll(Duration.ofMillis(100));

            for (Map.Entry<TopicPartition, List<Message>> entry : records.entrySet()) {
                TopicPartition tp = entry.getKey();

                for (Message msg : entry.getValue()) {
                    processMessage(msg);

                    // 更新本地状态
                    partitionCounts.merge(tp, 1, Integer::sum);
                }
            }

            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }

    private static void processMessage(Message msg) {
        System.out.println("Processed: " + msg.getValue());
    }

    private static void saveToDatabase(TopicPartition tp, Integer count) {
        // 保存到 Redis/MySQL 等
        System.out.println("Saved state for " + tp + ": " + count);
    }

    private static Integer loadFromDatabase(TopicPartition tp) {
        // 从 Redis/MySQL 加载
        System.out.println("Loaded state for " + tp);
        return 0;
    }
}
```

### Rebalance 时序图

```
Consumer A          Coordinator         Consumer B
    |                    |                    |
    | --- poll() ------> |                    |
    |                    |                    |
    | <-- should rebalance                    |
    |                    |                    |
    | onPartitionsRevoked(P0, P1)             |
    | commitSync()       |                    |
    |                    |                    |
    | --- JoinGroup ---> |                    |
    |                    | <--- JoinGroup --- |
    |                    |                    |
    |                    | (决定新分配)        |
    |                    | A: P0              |
    |                    | B: P1              |
    |                    |                    |
    | <-- Assigned(P0) --|                    |
    |                    | --- Assigned(P1) ->|
    |                    |                    |
    | onPartitionsAssigned(P0)                |
    |                    |   onPartitionsAssigned(P1)
    |                    |                    |
    | --- poll() ------> |                    |
    |                    | <--- poll() ------ |
```

### MVP 4 的问题

1. ✅ Rebalance 协议：已解决
2. ✅ 心跳机制：已解决
3. ❌ **Broker 故障时的容错**：连接断了怎么办？
4. ❌ **多 Broker 支持**：只能连一个 broker
5. ❌ **性能优化**：每次 poll 都要发多次网络请求

---

## MVP 5: 添加容错和优化

### 新问题
1. Broker 挂了怎么办？
2. 如何提高性能？
3. 如何优雅处理异常？

### 核心设计决策

**决策6：多 Broker 支持 + 故障转移**

```java
// 配置多个 broker
bootstrap.servers = "broker1:9092,broker2:9092,broker3:9092"
```

Coordinator 在某个 broker 上，如果这个 broker 挂了：
1. 重新发现新的 coordinator
2. 重新加入 group

**决策7：批量 Fetch 优化**

```java
// 一次 fetch 请求拉取多个 partition
FetchRequest {
    topics: [
        {topic: "orders", partitions: [0, 1, 2]},
        {topic: "users", partitions: [0]}
    ]
}
```

**决策8：Wakeup 机制**

为什么需要 wakeup？
- Consumer 不是线程安全的
- 但需要从另一个线程中断 `poll()`（例如优雅关闭）

```java
// 主线程
while (running) {
    consumer.poll(...);
}

// 关闭线程
consumer.wakeup();  // 唯一线程安全的方法
```

**决策9：pause/resume 机制**

为什么需要暂停？
- 异步处理时，某些 partition 处理慢
- 暂停这些 partition，避免消息堆积

```java
// 处理线程池满了
if (threadPool.isOverloaded()) {
    consumer.pause(slowPartitions);
}

// 处理完成后恢复
consumer.resume(slowPartitions);
```

### MVP 5 代码

```java
/**
 * 完整的 Consumer 实现
 */
public class KafkaConsumer<K, V> {
    private final String consumerGroup;
    private final String consumerId;
    private final List<String> bootstrapServers;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final RebalanceListener rebalanceListener;

    // 网络层
    private final NetworkClient networkClient;
    private Node coordinator;

    // 状态
    private Map<TopicPartition, Long> positions;
    private Set<TopicPartition> assignedPartitions;
    private Set<TopicPartition> pausedPartitions;  // 暂停的 partition

    // 心跳
    private final HeartbeatThread heartbeatThread;
    private final int sessionTimeoutMs;
    private final int maxPollIntervalMs;
    private volatile long lastPollTime;

    // Wakeup
    private final AtomicBoolean wakeupTriggered;

    // 配置
    private final int maxPollRecords;
    private final int fetchMinBytes;
    private final int fetchMaxWaitMs;

    public KafkaConsumer(Properties config) {
        this.consumerGroup = config.getProperty("group.id");
        this.consumerId = generateConsumerId();
        this.bootstrapServers = Arrays.asList(
            config.getProperty("bootstrap.servers").split(",")
        );

        // 反序列化器
        this.keyDeserializer = createDeserializer(
            config.getProperty("key.deserializer")
        );
        this.valueDeserializer = createDeserializer(
            config.getProperty("value.deserializer")
        );

        this.rebalanceListener = null;  // 可选

        // 初始化网络客户端
        this.networkClient = new NetworkClient(bootstrapServers);

        // 状态
        this.positions = new HashMap<>();
        this.assignedPartitions = new HashSet<>();
        this.pausedPartitions = new HashSet<>();

        // 配置
        this.sessionTimeoutMs = Integer.parseInt(
            config.getProperty("session.timeout.ms", "10000")
        );
        this.maxPollIntervalMs = Integer.parseInt(
            config.getProperty("max.poll.interval.ms", "300000")
        );
        this.maxPollRecords = Integer.parseInt(
            config.getProperty("max.poll.records", "500")
        );
        this.fetchMinBytes = Integer.parseInt(
            config.getProperty("fetch.min.bytes", "1")
        );
        this.fetchMaxWaitMs = Integer.parseInt(
            config.getProperty("fetch.max.wait.ms", "500")
        );

        this.lastPollTime = System.currentTimeMillis();
        this.wakeupTriggered = new AtomicBoolean(false);

        // 启动心跳线程
        this.heartbeatThread = new HeartbeatThread(this);
        this.heartbeatThread.start();
    }

    /**
     * 订阅 topics
     */
    public void subscribe(Collection<String> topics) {
        subscribe(topics, null);
    }

    public void subscribe(Collection<String> topics, RebalanceListener listener) {
        // 查找 coordinator
        findCoordinator();

        // 加入 group
        joinGroup(topics, listener);
    }

    /**
     * 查找 Group Coordinator
     */
    private void findCoordinator() {
        for (String server : bootstrapServers) {
            try {
                FindCoordinatorRequest request = new FindCoordinatorRequest(consumerGroup);
                FindCoordinatorResponse response = networkClient.send(server, request);

                if (response.isSuccess()) {
                    coordinator = response.getCoordinator();
                    System.out.println("Found coordinator: " + coordinator);
                    return;
                }
            } catch (IOException e) {
                // 尝试下一个 broker
                continue;
            }
        }

        throw new RuntimeException("Failed to find coordinator");
    }

    private void joinGroup(Collection<String> topics, RebalanceListener listener) {
        while (true) {
            try {
                // 发送 JoinGroup 请求
                JoinGroupRequest request = new JoinGroupRequest(
                    consumerGroup,
                    consumerId,
                    sessionTimeoutMs,
                    new ArrayList<>(topics),
                    "range"
                );

                JoinGroupResponse response = networkClient.send(coordinator, request);

                // Revoke 旧分配
                if (!assignedPartitions.isEmpty() && listener != null) {
                    listener.onPartitionsRevoked(assignedPartitions);
                    commitSync();
                }

                // 接收新分配
                assignedPartitions = response.getAssignedPartitions();

                // 获取 offset
                for (TopicPartition tp : assignedPartitions) {
                    long offset = fetchCommittedOffset(tp);
                    positions.put(tp, offset >= 0 ? offset : 0);
                }

                // Assign 回调
                if (listener != null) {
                    listener.onPartitionsAssigned(assignedPartitions);
                }

                System.out.println("Joined group. Assigned: " + assignedPartitions);
                return;

            } catch (IOException e) {
                // Coordinator 挂了，重新查找
                System.out.println("Coordinator failed, finding new coordinator...");
                findCoordinator();
            }
        }
    }

    /**
     * 拉取消息（核心 API）
     */
    public ConsumerRecords<K, V> poll(Duration timeout) {
        // 检查 wakeup
        if (wakeupTriggered.get()) {
            wakeupTriggered.set(false);
            throw new WakeupException();
        }

        // 检查 poll 间隔
        long now = System.currentTimeMillis();
        if (now - lastPollTime > maxPollIntervalMs) {
            throw new IllegalStateException("max.poll.interval.ms exceeded");
        }
        lastPollTime = now;

        // 构造批量 fetch 请求（性能优化）
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<>();

        for (TopicPartition tp : assignedPartitions) {
            // 跳过暂停的 partition
            if (pausedPartitions.contains(tp)) {
                continue;
            }

            long offset = positions.get(tp);
            fetchData.put(tp, new FetchRequest.PartitionData(offset, 1024 * 1024));
        }

        if (fetchData.isEmpty()) {
            return ConsumerRecords.empty();
        }

        // 发送 fetch 请求
        try {
            FetchRequest request = new FetchRequest(
                fetchData,
                fetchMinBytes,
                fetchMaxWaitMs,
                maxPollRecords
            );

            FetchResponse response = networkClient.send(coordinator, request);

            // 解析响应
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();

            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry :
                 response.getData().entrySet()) {
                TopicPartition tp = entry.getKey();
                List<Message> messages = entry.getValue().getMessages();

                List<ConsumerRecord<K, V>> deserializedRecords = new ArrayList<>();
                for (Message msg : messages) {
                    K key = keyDeserializer.deserialize(tp.topic(), msg.getKey());
                    V value = valueDeserializer.deserialize(tp.topic(), msg.getValue());

                    deserializedRecords.add(new ConsumerRecord<>(
                        tp.topic(),
                        tp.partition(),
                        msg.getOffset(),
                        key,
                        value
                    ));
                }

                if (!deserializedRecords.isEmpty()) {
                    records.put(tp, deserializedRecords);

                    // 更新 position
                    long lastOffset = deserializedRecords.get(
                        deserializedRecords.size() - 1
                    ).offset();
                    positions.put(tp, lastOffset + 1);
                }
            }

            return new ConsumerRecords<>(records);

        } catch (IOException e) {
            // 网络异常，下次 poll 重试
            System.err.println("Fetch failed: " + e.getMessage());
            return ConsumerRecords.empty();
        }
    }

    /**
     * 同步提交 offset
     */
    public void commitSync() {
        commitSync(null);
    }

    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets == null) {
            // 提交所有 partition 的 position
            offsets = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }
        }

        try {
            OffsetCommitRequest request = new OffsetCommitRequest(
                consumerGroup,
                offsets
            );

            OffsetCommitResponse response = networkClient.send(coordinator, request);

            if (!response.isSuccess()) {
                throw new CommitFailedException("Failed to commit offsets");
            }
        } catch (IOException e) {
            throw new CommitFailedException("Failed to commit offsets", e);
        }
    }

    /**
     * 异步提交 offset
     */
    public void commitAsync() {
        commitAsync(null, null);
    }

    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(null, callback);
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                           OffsetCommitCallback callback) {
        if (offsets == null) {
            offsets = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }
        }

        // 异步发送，不等待响应
        Map<TopicPartition, OffsetAndMetadata> finalOffsets = offsets;
        new Thread(() -> {
            try {
                OffsetCommitRequest request = new OffsetCommitRequest(
                    consumerGroup,
                    finalOffsets
                );

                OffsetCommitResponse response = networkClient.send(coordinator, request);

                if (callback != null) {
                    if (response.isSuccess()) {
                        callback.onComplete(finalOffsets, null);
                    } else {
                        callback.onComplete(finalOffsets,
                            new Exception("Commit failed"));
                    }
                }
            } catch (Exception e) {
                if (callback != null) {
                    callback.onComplete(finalOffsets, e);
                }
            }
        }).start();
    }

    /**
     * Seek 到指定 offset
     */
    public void seek(TopicPartition partition, long offset) {
        if (!assignedPartitions.contains(partition)) {
            throw new IllegalStateException("Not assigned to partition: " + partition);
        }
        positions.put(partition, offset);
    }

    public void seekToBeginning(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            positions.put(tp, 0L);
        }
    }

    public void seekToEnd(Collection<TopicPartition> partitions) {
        try {
            EndOffsetRequest request = new EndOffsetRequest(partitions);
            EndOffsetResponse response = networkClient.send(coordinator, request);

            for (Map.Entry<TopicPartition, Long> entry :
                 response.getOffsets().entrySet()) {
                positions.put(entry.getKey(), entry.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to seek to end", e);
        }
    }

    /**
     * 暂停 partition
     */
    public void pause(Collection<TopicPartition> partitions) {
        pausedPartitions.addAll(partitions);
    }

    /**
     * 恢复 partition
     */
    public void resume(Collection<TopicPartition> partitions) {
        pausedPartitions.removeAll(partitions);
    }

    public Set<TopicPartition> paused() {
        return new HashSet<>(pausedPartitions);
    }

    /**
     * 中断 poll（线程安全）
     */
    public void wakeup() {
        wakeupTriggered.set(true);
    }

    /**
     * 关闭 consumer
     */
    public void close() {
        // 停止心跳
        heartbeatThread.shutdown();

        try {
            // 最后一次同步 commit
            commitSync();

            // 离开 group
            LeaveGroupRequest request = new LeaveGroupRequest(consumerGroup, consumerId);
            networkClient.send(coordinator, request);
        } catch (Exception e) {
            System.err.println("Error during close: " + e.getMessage());
        } finally {
            networkClient.close();
        }
    }

    private long fetchCommittedOffset(TopicPartition tp) {
        try {
            OffsetFetchRequest request = new OffsetFetchRequest(
                consumerGroup,
                Set.of(tp)
            );

            OffsetFetchResponse response = networkClient.send(coordinator, request);
            return response.getOffset(tp);
        } catch (IOException e) {
            return -1;
        }
    }

    private String generateConsumerId() {
        return consumerGroup + "-" + UUID.randomUUID().toString();
    }

    private <T> Deserializer<T> createDeserializer(String className) {
        try {
            return (Deserializer<T>) Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create deserializer", e);
        }
    }
}

// 完整使用示例
public class ConsumerExample5 {
    private static volatile boolean running = true;

    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092");
        props.put("group.id", "order-processors");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("session.timeout.ms", "10000");
        props.put("max.poll.interval.ms", "300000");
        props.put("max.poll.records", "500");
        props.put("enable.auto.commit", "false");  // 手动 commit

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅
        consumer.subscribe(Arrays.asList("orders"), new RebalanceListener() {
            @Override
            public void onPartitionsRevoked(Set<TopicPartition> partitions) {
                System.out.println("Revoking: " + partitions);
                // 保存状态
            }

            @Override
            public void onPartitionsAssigned(Set<TopicPartition> partitions) {
                System.out.println("Assigned: " + partitions);
                // 恢复状态
            }
        });

        // 注册 shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            running = false;
            consumer.wakeup();  // 中断 poll
        }));

        // 线程池（异步处理）
        ExecutorService executor = Executors.newFixedThreadPool(10);

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    continue;
                }

                // 提交处理任务
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords =
                        records.records(partition);

                    executor.submit(() -> {
                        for (ConsumerRecord<String, String> record : partitionRecords) {
                            processRecord(record);
                        }
                    });
                }

                // 同步 commit
                consumer.commitSync();

                // 如果线程池满了，暂停消费
                if (executor.getQueue().size() > 1000) {
                    consumer.pause(consumer.assignment());
                    System.out.println("Paused consumption due to backlog");
                } else {
                    consumer.resume(consumer.assignment());
                }
            }
        } catch (WakeupException e) {
            // 正常关闭
        } finally {
            consumer.close();
            executor.shutdown();
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        // 处理逻辑
        System.out.println("Processed: " + record.value());
    }
}

// 高级用法：手动分配 partition
public class ConsumerExample5ManualAssign {
    public static void main(String[] args) {
        Properties props = new Properties();
        // ... 配置

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 手动分配（不使用 consumer group）
        TopicPartition partition0 = new TopicPartition("orders", 0);
        TopicPartition partition1 = new TopicPartition("orders", 1);
        consumer.assign(Arrays.asList(partition0, partition1));

        // Seek 到指定位置
        consumer.seek(partition0, 100);  // 从 offset 100 开始
        consumer.seekToEnd(Arrays.asList(partition1));  // 从最新开始

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            // 处理消息
        }
    }
}
```

### 性能优化总结

**1. 批量 Fetch**
```java
// 一次请求拉取多个 partition
FetchRequest {
    partitions: [P0, P1, P2, P3],
    minBytes: 1024,        // 最少等到 1KB 再返回
    maxWaitMs: 500         // 最多等 500ms
}
```

**2. 异步 Commit**
```java
// 不阻塞主线程
consumer.commitAsync();
```

**3. 多线程处理**
```java
while (true) {
    records = consumer.poll(100);
    executor.submit(() -> process(records));  // 异步处理
}
```

**4. Pause/Resume 流控**
```java
if (backlogTooLarge) {
    consumer.pause(partitions);  // 暂停拉取
}
```

---

## 最终设计总结

### 复杂度演进路径

```
MVP 1: 简单 Pull 模型
  ↓ 问题：无 offset 管理

MVP 2: Position/Committed 分离
  ↓ 问题：无协作能力

MVP 3: Consumer Group 协调
  ↓ 问题：rebalance 不安全

MVP 4: Rebalance 协议 + 心跳
  ↓ 问题：无容错、性能不足

MVP 5: 多 Broker + 批量优化 + Wakeup/Pause
```

### 核心 API 设计理念

| API | 设计目的 | 复杂度来源 |
|-----|---------|-----------|
| `poll()` | Pull 模型核心，消费者控制速度 | 需要整合 heartbeat、rebalance、fetch |
| `commitSync/Async()` | 精确控制消费语义 | 区分 position 和 committed |
| `subscribe()` | 动态负载均衡 | 需要 coordinator 协调 |
| `assign()` | 手动控制（特殊场景） | 绕过 group 协调 |
| `seek()` | 重新消费（replay） | 需要解耦 position 和 committed |
| `pause/resume()` | 流量控制 | 支持异步处理模式 |
| `wakeup()` | 优雅关闭 | 非线程安全下的唯一线程安全方法 |

### 为什么这么设计？

**1. Pull vs Push**
- 消费者能力不同 → Pull 让消费者控制速度
- 简化 Broker → Broker 不需要追踪消费状态

**2. Position vs Committed**
- 需要控制"何时算消费成功" → 分离内存位置和持久化位置
- 支持不同语义（at-least-once/at-most-once）

**3. Consumer Group**
- 需要水平扩展 → 多个消费者协同工作
- 自动负载均衡 → Coordinator 统一管理

**4. Rebalance 协议**
- Consumer 加入/离开 → 需要重新分配 partition
- 避免数据丢失/重复 → 两阶段协议（revoke + assign）

**5. 心跳机制**
- 检测 consumer 存活 → session.timeout.ms
- 检测 consumer 卡死 → max.poll.interval.ms

**6. 容错和优化**
- Broker 故障 → 多 Broker 支持 + 自动切换
- 性能瓶颈 → 批量 fetch、异步 commit
- 流量控制 → pause/resume
- 优雅关闭 → wakeup

---

## 关键设计权衡

### 1. 简单 vs 灵活

**简单模式**：
```java
consumer.subscribe("orders");
while (true) {
    records = consumer.poll(100);
    process(records);
    // 自动 commit
}
```

**灵活模式**：
```java
consumer.assign(partitions);  // 手动分配
consumer.seek(partition, offset);  // 手动定位
consumer.commitSync(offsets);  // 手动 commit
```

### 2. 同步 vs 异步

**同步 commit**：可靠但慢
**异步 commit**：快但可能失败

**最佳实践**：
```java
while (true) {
    consumer.commitAsync();  // 日常
}
consumer.commitSync();  // 关闭前
```

### 3. 自动 vs 手动

**自动 commit**：简单，但语义弱
**手动 commit**：复杂，但可控

### 4. 线程模型

**单线程**：简单，但吞吐量低
**多线程**：复杂（需要 pause/resume），但高性能

---

## 实际案例

### 案例1：高吞吐量日志收集
```java
props.put("max.poll.records", "5000");  // 大批量
props.put("enable.auto.commit", "true");  // 自动 commit
// 允许少量重复
```

### 案例2：金融交易（exactly-once）
```java
props.put("isolation.level", "read_committed");
props.put("enable.auto.commit", "false");

// 事务性处理
consumer.poll();
processToDatabase();
consumer.commitSync();  // 手动 commit
```

### 案例3：实时流处理
```java
executor = Executors.newFixedThreadPool(10);

while (true) {
    records = consumer.poll(100);
    executor.submit(() -> process(records));

    // 流控
    if (executor.getQueue().size() > 1000) {
        consumer.pause(partitions);
    }
}
```

---

## 总结：从简单到复杂的必然性

每一层复杂度都是为了解决实际问题：

1. **Pull 模型** → 消费者控制速度
2. **Offset 分离** → 控制消费语义
3. **Consumer Group** → 水平扩展
4. **Rebalance** → 动态负载均衡
5. **心跳机制** → 故障检测
6. **容错优化** → 生产可用

Kafka Consumer 的设计是**渐进式复杂度**的典范：
- 简单场景可以用简单 API
- 复杂场景有足够的控制能力
- 每层抽象都有明确的职责

这就是为什么 Kafka Consumer API 看起来复杂，但每个复杂度都有其存在的必要性。
