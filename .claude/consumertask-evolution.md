# ConsumerTask 从简单到复杂的演化分析

本文档展示 Remote Log Metadata 管理中 ConsumerTask 的设计如何从简单到复杂演化。

---

## 背景：ConsumerTask 是什么？

**ConsumerTask** 是 Kafka Tiered Storage（分层存储）功能中负责消费 **Remote Log Metadata Topic** 的组件。

### 核心职责
```
User Topic (orders)                Remote Log Metadata Topic
  ├─ partition 0  ────────────┐      (__remote_log_metadata)
  ├─ partition 1              │         ├─ partition 0
  └─ partition 2              │         ├─ partition 1
                              │         └─ partition 2
                              │              │
                              │              │ metadata events
                              │              ▼
                              └────> ConsumerTask
                                       - 消费 metadata events
                                       - 构建 partition 的 remote log metadata
                                       - 通知 handler 更新内存状态
```

**为什么需要？**
- Tiered Storage 将旧数据上传到 S3/HDFS 等远程存储
- 需要知道哪些 segment 在远程存储、它们的 offset 范围等
- 这些元数据存储在 `__remote_log_metadata` topic 中
- ConsumerTask 负责消费这个 topic，维护本地的元数据缓存

---

## Step 1: 最简单版本 - 单个 Partition 的元数据消费

### 需求
从 `__remote_log_metadata` topic 的一个 partition 消费元数据事件。

### 实现
```java
class SimpleConsumerTask implements Runnable {
    Consumer<byte[], byte[]> consumer;
    RemoteLogMetadataSerde serde;

    void run() {
        // 订阅 metadata topic
        consumer.subscribe(Collections.singleton("__remote_log_metadata"));

        while (true) {
            // poll records
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            // 处理每条 record
            for (ConsumerRecord<byte[], byte[]> record : records) {
                RemoteLogMetadata metadata = serde.deserialize(record.value());
                System.out.println("Received metadata: " + metadata);
            }
        }
    }
}
```

### 数据结构
```
┌─────────────────────────────────────────┐
│  SimpleConsumerTask                     │
├─────────────────────────────────────────┤
│  consumer: Consumer                     │
│  serde: RemoteLogMetadataSerde         │
│                                         │
│  run() {                                │
│    while (true) {                       │
│      poll()                             │
│      deserialize()                      │
│      print()                            │
│    }                                    │
│  }                                      │
└─────────────────────────────────────────┘
```

### 特点
- ✅ 简单直接
- ✅ 能消费 metadata
- ❌ 没有状态管理
- ❌ 无法停止
- ❌ 不知道处理到哪里了

**引入的复杂度**：无

---

## Step 2: 引入 Event Handler - 处理元数据

### 新需求
**消费到元数据后，需要更新内存中的状态（哪些 segment 在远程存储）。**

**为什么？**
```
场景：
Consumer 读取 offset 1000，但 local log 只有 offset 500-600
  -> 需要查询：offset 1000 在哪个 remote segment？
  -> 需要维护：remote segment 的索引

解决方案：
  消费 metadata event -> 更新内存索引
```

### 新增复杂度
1. **RemotePartitionMetadataEventHandler**：处理 metadata 更新
2. **状态维护**：内存中的 remote log segment 索引

### 实现
```java
class ConsumerTask implements Runnable {
    Consumer<byte[], byte[]> consumer;
    RemoteLogMetadataSerde serde;
    RemotePartitionMetadataEventHandler handler;  // 新增

    void run() {
        consumer.subscribe(Collections.singleton("__remote_log_metadata"));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> record : records) {
                RemoteLogMetadata metadata = serde.deserialize(record.value());

                // 新增：交给 handler 处理
                handler.handleRemoteLogMetadata(metadata);
            }
        }
    }
}

interface RemotePartitionMetadataEventHandler {
    void handleRemoteLogMetadata(RemoteLogMetadata metadata);
}

// Handler 的实现
class RemoteLogMetadataManager implements RemotePartitionMetadataEventHandler {
    // 内存索引：partition -> remote segments
    Map<TopicIdPartition, List<RemoteLogSegmentMetadata>> remoteSegments;

    void handleRemoteLogMetadata(RemoteLogMetadata metadata) {
        if (metadata instanceof RemoteLogSegmentMetadata) {
            RemoteLogSegmentMetadata segment = (RemoteLogSegmentMetadata) metadata;
            // 更新内存索引
            remoteSegments.computeIfAbsent(segment.topicIdPartition(), k -> new ArrayList<>())
                .add(segment);
        }
    }
}
```

### 数据流图
```
__remote_log_metadata topic
        │
        │ metadata events
        ▼
┌──────────────────────────┐
│  ConsumerTask            │
│  - poll()                │
│  - deserialize()         │
└───────┬──────────────────┘
        │
        │ handleRemoteLogMetadata()
        ▼
┌──────────────────────────────────────┐
│  RemotePartitionMetadataEventHandler │
│  - 更新内存索引                       │
│  - remoteSegments Map                │
└──────────────────────────────────────┘
```

### 引入的复杂度
1. **RemotePartitionMetadataEventHandler 接口**：解耦消费和处理
2. **内存索引维护**：handler 负责维护状态
3. **为什么？** 分离关注点，ConsumerTask 只负责消费，handler 负责业务逻辑

---

## Step 3: 引入 Partition Assignment - 动态分配

### 新需求
**Broker 只需要消费它作为 leader/follower 的 partition 的元数据，不需要消费所有元数据。**

**为什么？**
```
场景：
Broker1 是 orders-0 的 leader
Broker2 是 orders-1 的 leader

问题：
  如果两个 Broker 都消费所有 metadata
  -> 浪费内存（每个 Broker 都存储所有 partition 的 metadata）
  -> 浪费 CPU（处理不相关的 metadata）

解决方案：
  只消费自己负责的 partition 的 metadata
  -> Broker1 只消费 orders-0 的 metadata
  -> Broker2 只消费 orders-1 的 metadata
```

### 新增复杂度
1. **Partition Assignment 管理**
2. **动态添加/删除分配**
3. **过滤不相关的 metadata**

### 实现
```java
class ConsumerTask implements Runnable {
    Consumer<byte[], byte[]> consumer;
    RemotePartitionMetadataEventHandler handler;

    // 新增：我负责的 user partitions
    Set<TopicIdPartition> assignedUserPartitions = new HashSet<>();

    void run() {
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> record : records) {
                RemoteLogMetadata metadata = serde.deserialize(record.value());

                // 新增：只处理我负责的 partition 的 metadata
                if (assignedUserPartitions.contains(metadata.topicIdPartition())) {
                    handler.handleRemoteLogMetadata(metadata);
                } else {
                    // 跳过不相关的 metadata
                    log.debug("Skipping metadata for unassigned partition: {}",
                        metadata.topicIdPartition());
                }
            }
        }
    }

    // 新增：动态添加 partition 分配
    public void addAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        assignedUserPartitions.addAll(partitions);
    }

    // 新增：动态删除 partition 分配
    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        assignedUserPartitions.removeAll(partitions);
    }
}
```

### 分配模型
```
Broker1:
  assignedUserPartitions = {orders-0, orders-1}
    │
    ├─ 只处理 orders-0 和 orders-1 的 metadata
    └─ 跳过 orders-2 的 metadata

Broker2:
  assignedUserPartitions = {orders-2}
    │
    ├─ 只处理 orders-2 的 metadata
    └─ 跳过 orders-0 和 orders-1 的 metadata
```

### 引入的复杂度
1. **assignedUserPartitions Set**：跟踪分配的 partitions
2. **addAssignmentsForPartitions/removeAssignmentsForPartitions API**：动态管理
3. **过滤逻辑**：shouldProcess() 检查
4. **为什么？** 减少不必要的内存和 CPU 开销

---

## Step 4: 引入 Metadata Partitioner - 映射到 Metadata Partition

### 新需求
**`__remote_log_metadata` topic 也是分区的，需要知道某个 user partition 的 metadata 在哪个 metadata partition。**

**为什么？**
```
问题：
  __remote_log_metadata 有 50 个 partitions
  orders topic 有 100 个 partitions
  orders-0 的 metadata 在哪个 metadata partition？

解决方案：
  使用 hash 映射：
    metadataPartition = hash(topicId, partition) % 50
    orders-0 -> metadata partition 5
    orders-1 -> metadata partition 23
    ...

优势：
  - 确定性映射（同一个 user partition 总是映射到同一个 metadata partition）
  - 负载均衡（均匀分布）
```

### 新增复杂度
1. **RemoteLogMetadataTopicPartitioner**：计算映射
2. **metadata partition 到 user partition 的反向映射**
3. **Consumer 只订阅相关的 metadata partitions**

### 实现
```java
interface RemoteLogMetadataTopicPartitioner {
    int metadataPartition(TopicIdPartition topicIdPartition);
}

class ConsumerTask implements Runnable {
    Consumer<byte[], byte[]> consumer;
    RemoteLogMetadataTopicPartitioner partitioner;  // 新增

    // 新增：user partition -> metadata partition 的映射
    Map<TopicIdPartition, Integer> userPartitionToMetadataPartition = new HashMap<>();

    // 新增：我需要消费的 metadata partitions
    Set<Integer> assignedMetadataPartitions = new HashSet<>();

    void addAssignmentsForPartitions(Set<TopicIdPartition> userPartitions) {
        for (TopicIdPartition tp : userPartitions) {
            // 计算 metadata partition
            int metadataPartition = partitioner.metadataPartition(tp);
            userPartitionToMetadataPartition.put(tp, metadataPartition);
            assignedMetadataPartitions.add(metadataPartition);
        }

        // 更新 Consumer 的订阅
        Set<TopicPartition> metadataTopicPartitions = assignedMetadataPartitions.stream()
            .map(p -> new TopicPartition("__remote_log_metadata", p))
            .collect(Collectors.toSet());

        consumer.assign(metadataTopicPartitions);
    }
}
```

### 映射关系图
```
User Partitions        Metadata Partitions
  orders-0  ──hash──>  metadata-5
  orders-1  ──hash──>  metadata-23
  orders-2  ──hash──>  metadata-5
  orders-3  ──hash──>  metadata-12
  ...

Broker1 负责 orders-0, orders-2:
  -> 需要消费 metadata-5 (因为 orders-0 和 orders-2 都映射到这里)
  -> consumer.assign([metadata-5])

Broker2 负责 orders-1:
  -> 需要消费 metadata-23
  -> consumer.assign([metadata-23])
```

### 引入的复杂度
1. **RemoteLogMetadataTopicPartitioner**：hash 映射逻辑
2. **assignedMetadataPartitions**：跟踪需要消费的 metadata partitions
3. **consumer.assign()**：显式分配，而不是 subscribe
4. **为什么？** 精确控制消费哪些 metadata partitions，避免浪费

---

## Step 5: 引入 Offset 追踪 - 知道读到哪里了

### 新需求
**需要记住每个 metadata partition 消费到哪个 offset，以便重启后从上次位置继续。**

**为什么？**
```
场景：
  Broker 消费到 metadata-5 的 offset 1000
  Broker 重启
  如果不记住 offset -> 从头消费（重复处理）
  如果记住 offset -> 从 1001 继续（避免重复）
```

### 新增复杂度
1. **readOffsetsByMetadataPartition**：记录每个 metadata partition 的读取进度
2. **consumer.seek()**：恢复到上次的 offset

### 实现
```java
class ConsumerTask implements Runnable {
    Consumer<byte[], byte[]> consumer;

    // 新增：记录每个 metadata partition 的消费进度
    Map<Integer, Long> readOffsetsByMetadataPartition = new ConcurrentHashMap<>();

    void run() {
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> record : records) {
                // 处理 metadata
                processRecord(record);

                // 新增：更新消费进度
                readOffsetsByMetadataPartition.put(record.partition(), record.offset());
            }
        }
    }

    void addAssignmentsForPartitions(Set<TopicIdPartition> userPartitions) {
        // ... 计算 metadata partitions ...

        consumer.assign(metadataTopicPartitions);

        // 新增：恢复到上次的 offset
        for (TopicPartition tp : metadataTopicPartitions) {
            Long lastReadOffset = readOffsetsByMetadataPartition.get(tp.partition());
            if (lastReadOffset != null) {
                consumer.seek(tp, lastReadOffset + 1);  // 从下一个 offset 开始
            } else {
                consumer.seekToBeginning(Collections.singleton(tp));  // 从头开始
            }
        }
    }
}
```

### Offset 追踪图
```
Metadata Partition 5:
  ┌──────────────────────────────────────┐
  │ offset 0   offset 1   ...  offset 999│
  │ [已读]     [已读]          [已读]    │
  └──────────────────────────────────────┘
                                  ▲
                                  │
                        readOffsetsByMetadataPartition
                        {5: 999}

重启后：
  consumer.seek(partition=5, offset=1000)  ✅
```

### 引入的复杂度
1. **readOffsetsByMetadataPartition Map**：持久化追踪
2. **consumer.seek() 逻辑**：恢复 offset
3. **seekToBeginning() 逻辑**：首次消费
4. **为什么？** 避免重复处理，支持故障恢复

---

## Step 6: 引入 Initialization 状态 - 等待追赶

### 新需求
**新分配的 partition 需要先读取完历史 metadata，才能标记为 "initialized"。**

**为什么？**
```
场景：
  orders-0 新分配给 Broker1
  metadata-5 已经有 10000 条历史 metadata

问题：
  如果立即标记为 "ready"
  -> 查询时可能查不到历史 segment 的信息
  -> 数据不完整

解决方案：
  1. 读取 metadata-5 的 endOffset (10000)
  2. 消费到 offset 10000
  3. 标记 orders-0 为 "initialized"
  4. 现在可以安全地提供查询服务
```

### 新增复杂度
1. **UserTopicIdPartition 状态类**：跟踪 initialization 状态
2. **fetchStartAndEndOffsets()**：获取 metadata partition 的范围
3. **maybeMarkUserPartitionsAsReady()**：检查是否追赶完成

### 实现
```java
class UserTopicIdPartition {
    TopicIdPartition topicIdPartition;
    Integer metadataPartition;
    boolean isAssigned;       // 新增：是否已分配
    boolean isInitialized;    // 新增：是否已初始化（追赶完历史数据）
}

class ConsumerTask implements Runnable {
    // 新增：user partition 的状态
    Map<TopicIdPartition, UserTopicIdPartition> assignedUserTopicIdPartitions;

    // 新增：metadata partition 的起止 offset
    Map<TopicPartition, StartAndEndOffsetHolder> offsetHolderByMetadataPartition;

    void run() {
        while (true) {
            // Poll and process records
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            processRecords(records);

            // 新增：检查是否可以标记为 initialized
            maybeMarkUserPartitionsAsReady();
        }
    }

    void maybeMarkUserPartitionsAsReady() {
        for (UserTopicIdPartition utp : assignedUserTopicIdPartitions.values()) {
            if (utp.isAssigned && !utp.isInitialized) {
                Integer metadataPartition = utp.metadataPartition;
                StartAndEndOffsetHolder holder = offsetHolderByMetadataPartition.get(metadataPartition);

                if (holder != null) {
                    Long readOffset = readOffsetsByMetadataPartition.get(metadataPartition);

                    // 检查：是否已经读到 endOffset
                    if (readOffset + 1 >= holder.endOffset) {
                        // 标记为 initialized
                        utp.isInitialized = true;
                        handler.markInitialized(utp.topicIdPartition);
                        log.info("Partition {} is now initialized", utp.topicIdPartition);
                    }
                }
            }
        }
    }

    void addAssignmentsForPartitions(Set<TopicIdPartition> userPartitions) {
        // ... assign consumer ...

        // 新增：获取 metadata partition 的 endOffset
        Set<TopicPartition> metadataPartitions = ...;
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(metadataPartitions);
        Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(metadataPartitions);

        offsetHolderByMetadataPartition = endOffsets.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new StartAndEndOffsetHolder(
                    startOffsets.get(e.getKey()),
                    e.getValue()
                )
            ));
    }
}

class StartAndEndOffsetHolder {
    Long startOffset;
    Long endOffset;
}
```

### Initialization 流程图
```
Time 0: 分配 orders-0
  ├─ 计算 metadataPartition = 5
  ├─ fetchEndOffsets(metadata-5) -> endOffset = 10000
  ├─ consumer.seekToBeginning(metadata-5)
  └─ isInitialized = false

Time 1-N: 消费历史数据
  ├─ poll() -> offset 0-100
  ├─ poll() -> offset 101-200
  ├─ ...
  ├─ poll() -> offset 9901-10000
  └─ readOffset = 10000

Time N+1: 检查 initialization
  ├─ readOffset (10000) >= endOffset (10000) ✅
  ├─ markInitialized(orders-0)
  └─ isInitialized = true

Time N+2: 可以提供服务
  ├─ 查询 orders-0 的 remote segments
  └─ 返回完整的历史数据 ✅
```

### 引入的复杂度
1. **UserTopicIdPartition 类**：封装状态
2. **isAssigned / isInitialized 标志**：状态机
3. **StartAndEndOffsetHolder**：记录 offset 范围
4. **maybeMarkUserPartitionsAsReady() 逻辑**：检查追赶进度
5. **为什么？** 保证数据完整性，避免提供不完整的查询结果

---

## Step 7: 引入并发控制 - 线程安全

### 新需求
**partition 分配的 add/remove 来自其他线程（request handler），需要线程安全。**

**为什么？**
```
场景：
  Thread 1 (Request Handler): 收到 LeaderAndIsr 请求
    -> addAssignmentsForPartitions(orders-0)

  Thread 2 (ConsumerTask): 正在 poll()
    -> 读取 assignedUserTopicIdPartitions

问题：
  并发修改 assignedUserTopicIdPartitions
  -> ConcurrentModificationException
  -> 数据竞争

解决方案：
  使用锁保护共享状态
```

### 新增复杂度
1. **assignPartitionsLock**：同步锁
2. **volatile 字段**：确保可见性
3. **wait/notify 机制**：等待分配

### 实现
```java
class ConsumerTask implements Runnable {
    // 新增：锁
    private final Object assignPartitionsLock = new Object();

    // 新增：volatile 确保可见性
    private volatile Map<TopicIdPartition, UserTopicIdPartition> assignedUserTopicIdPartitions;
    private volatile Set<Integer> assignedMetadataPartitions;
    private volatile boolean hasAssignmentChanged = true;

    void run() {
        while (!isClosed) {
            // 新增：如果分配变化了，需要更新 consumer
            if (hasAssignmentChanged) {
                maybeWaitForPartitionAssignments();
            }

            // Poll and process
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            processRecords(records);
            maybeMarkUserPartitionsAsReady();
        }
    }

    void maybeWaitForPartitionAssignments() throws InterruptedException {
        // 新增：拍摄快照，减少锁的持有时间
        Set<Integer> metadataPartitionSnapshot;
        Set<UserTopicIdPartition> userPartitionsSnapshot;

        synchronized (assignPartitionsLock) {
            // 等待分配
            while (!isClosed && assignedUserTopicIdPartitions.isEmpty()) {
                log.debug("Waiting for partition assignments");
                assignPartitionsLock.wait();  // 释放锁，等待
            }

            if (!isClosed && hasAssignmentChanged) {
                // 拍摄快照
                metadataPartitionSnapshot = new HashSet<>(assignedMetadataPartitions);
                userPartitionsSnapshot = new HashSet<>(assignedUserTopicIdPartitions.values());
                hasAssignmentChanged = false;
            }
        }
        // 锁外执行耗时操作（consumer API 调用）

        if (!metadataPartitionSnapshot.isEmpty()) {
            // 更新 consumer assignment
            consumer.assign(toRemoteLogPartitions(metadataPartitionSnapshot));
            // ... seek, fetch offsets, etc ...
        }
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        synchronized (assignPartitionsLock) {
            // 更新分配
            Map<TopicIdPartition, UserTopicIdPartition> updated =
                new HashMap<>(assignedUserTopicIdPartitions);
            partitions.forEach(tp -> updated.putIfAbsent(tp, newUserTopicIdPartition(tp)));

            if (!updated.equals(assignedUserTopicIdPartitions)) {
                assignedUserTopicIdPartitions = Collections.unmodifiableMap(updated);
                hasAssignmentChanged = true;
                assignPartitionsLock.notifyAll();  // 唤醒等待的线程
            }
        }
    }
}
```

### 线程交互图
```
Thread 1 (Request Handler)          Thread 2 (ConsumerTask)
        │                                   │
        │                                   ├─ wait for assignment
        │                                   │  (assignPartitionsLock.wait())
        │                                   │
        ├─ addAssignmentsForPartitions()    │
        │  synchronized (lock) {            │
        │    update assignments             │
        │    hasAssignmentChanged = true    │
        │    notifyAll()  ─────────────────>├─ wake up!
        │  }                                 │
        │                                   ├─ snapshot assignments
        │                                   ├─ release lock
        │                                   ├─ consumer.assign()
        │                                   └─ continue polling
```

### 引入的复杂度
1. **assignPartitionsLock**：保护共享状态
2. **volatile 字段**：内存可见性
3. **wait/notify**：线程通信
4. **快照模式**：减少锁持有时间（在锁外执行 consumer API）
5. **为什么？** 线程安全，避免数据竞争

---

## Step 8: 引入重复检查 - 避免重复处理

### 新需求
**同一个 metadata event 可能被多次消费（rebalance、重启），需要去重。**

**为什么？**
```
场景：
  offset 100: RemoteLogSegmentMetadata(segment-1, offsets 0-999)
  ConsumerTask 处理了 offset 100
  Broker 重启
  ConsumerTask 从 offset 100 再次消费

问题：
  如果重复处理 -> segment-1 被添加两次
  -> 内存浪费，查询结果重复

解决方案：
  记录每个 user partition 的最后处理的 offset
  只处理更新的 offset
```

### 新增复杂度
1. **readOffsetsByUserTopicPartition**：记录每个 user partition 的处理进度
2. **shouldProcess() 检查**：去重逻辑

### 实现
```java
class ConsumerTask implements Runnable {
    // 新增：记录每个 user partition 消费到的 metadata offset
    Map<TopicIdPartition, Long> readOffsetsByUserTopicPartition = new HashMap<>();

    void processConsumerRecord(ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null) {
            // Tombstone (删除标记)
            readOffsetsByMetadataPartition.put(record.partition(), record.offset());
            return;
        }

        RemoteLogMetadata metadata = serde.deserialize(record.value());

        // 新增：检查是否应该处理
        if (shouldProcess(metadata, record.offset())) {
            handler.handleRemoteLogMetadata(metadata);
            // 新增：更新 user partition 的处理进度
            readOffsetsByUserTopicPartition.put(metadata.topicIdPartition(), record.offset());
        } else {
            log.trace("Skipping already processed event: {}", metadata);
        }

        // 总是更新 metadata partition 的消费进度
        readOffsetsByMetadataPartition.put(record.partition(), record.offset());
    }

    boolean shouldProcess(RemoteLogMetadata metadata, long recordOffset) {
        TopicIdPartition tpId = metadata.topicIdPartition();

        // 检查 1: 是否分配给我
        if (!processedAssignmentOfUserTopicIdPartitions.contains(tpId)) {
            return false;
        }

        // 检查 2: 是否已经处理过
        Long readOffset = readOffsetsByUserTopicPartition.get(tpId);
        if (readOffset != null && readOffset >= recordOffset) {
            return false;  // 已经处理过更新的 offset
        }

        return true;
    }
}
```

### 去重逻辑图
```
User Partition: orders-0

readOffsetsByUserTopicPartition:
  orders-0 -> 999 (上次处理到 metadata offset 999)

新消费的 record:
  offset 998: RemoteLogSegmentMetadata(segment-1)
    -> shouldProcess() -> readOffset (999) >= recordOffset (998)
    -> false ❌ 跳过（已经处理过更新的）

  offset 1000: RemoteLogSegmentMetadata(segment-2)
    -> shouldProcess() -> readOffset (999) < recordOffset (1000)
    -> true ✅ 处理
    -> update readOffsetsByUserTopicPartition[orders-0] = 1000
```

### 引入的复杂度
1. **readOffsetsByUserTopicPartition Map**：user partition 级别的去重
2. **shouldProcess() 逻辑**：两层检查（分配 + 去重）
3. **为什么？** 避免重复处理，保证幂等性

---

## Step 9: 引入 Tombstone 处理 - 支持删除

### 新需求
**Metadata topic 使用 compaction，tombstone (value=null) 用于删除，需要正确处理。**

**为什么？**
```
__remote_log_metadata topic 配置：
  cleanup.policy=compact

场景：
  offset 100: RemoteLogSegmentMetadata(segment-1) [添加]
  offset 200: null [删除 segment-1]

如果不处理 tombstone:
  -> segment-1 永远不会被删除
  -> 内存泄漏

正确处理：
  -> 跳过 tombstone（用于 compaction 提示）
  -> 但仍然更新 readOffset（避免重复消费）
```

### 实现
```java
void processConsumerRecord(ConsumerRecord<byte[], byte[]> record) {
    // 新增：tombstone 处理
    if (record.value() == null) {
        log.debug("Skipping tombstone at offset {} partition {}",
            record.offset(), record.partition());
        // 更新 offset，但不处理
        readOffsetsByMetadataPartition.put(record.partition(), record.offset());
        return;
    }

    // 正常处理
    RemoteLogMetadata metadata = serde.deserialize(record.value());
    if (shouldProcess(metadata, record.offset())) {
        handler.handleRemoteLogMetadata(metadata);
        readOffsetsByUserTopicPartition.put(metadata.topicIdPartition(), record.offset());
    }
    readOffsetsByMetadataPartition.put(record.partition(), record.offset());
}
```

### 引入的复杂度
1. **null 值检查**：区分 tombstone 和正常 record
2. **跳过 tombstone 但更新 offset**：保证消费进度正确
3. **为什么？** 支持 compaction，正确处理删除

---

## Step 10: 引入 Retry 机制 - 处理错误

### 新需求
**获取 endOffsets 可能失败（leader 不可用），需要重试机制。**

**为什么？**
```
场景：
  调用 consumer.endOffsets(metadata-5)
  Leader 还未选举出来
  -> 抛出 LeaderNotAvailableException

问题：
  如果直接失败 -> partition 永远无法 initialized
  如果立即重试 -> 频繁调用，浪费资源

解决方案：
  记录失败时间，延迟重试
```

### 新增复杂度
1. **hasLastOffsetsFetchFailed 标志**
2. **lastFailedFetchOffsetsTimestamp**：记录失败时间
3. **offsetFetchRetryIntervalMs**：重试间隔
4. **maybeFetchStartAndEndOffsets() 逻辑**：延迟重试

### 实现
```java
class ConsumerTask implements Runnable {
    boolean hasLastOffsetsFetchFailed = false;
    long lastFailedFetchOffsetsTimestamp;
    long offsetFetchRetryIntervalMs;  // 配置：重试间隔

    void fetchStartAndEndOffsets() {
        try {
            Set<TopicPartition> uninitializedPartitions = ...;
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(uninitializedPartitions);
            Map<TopicPartition, Long> startOffsets = consumer.beginningOffsets(uninitializedPartitions);

            offsetHolderByMetadataPartition = endOffsets.entrySet().stream()
                .collect(Collectors.toMap(...));

            hasLastOffsetsFetchFailed = false;  // 成功
        } catch (RetriableException ex) {
            // Leader 不可用等可重试错误
            hasLastOffsetsFetchFailed = true;
            lastFailedFetchOffsetsTimestamp = time.milliseconds();
            log.warn("Failed to fetch offsets, will retry", ex);
        }
    }

    void maybeFetchStartAndEndOffsets() {
        // 检查：是否需要重试 && 是否到了重试时间
        if (hasLastOffsetsFetchFailed &&
            lastFailedFetchOffsetsTimestamp + offsetFetchRetryIntervalMs < time.milliseconds()) {
            fetchStartAndEndOffsets();
        }
    }

    void maybeMarkUserPartitionsAsReady() {
        if (isAllUserTopicPartitionsInitialized) {
            return;
        }

        // 新增：可能需要重试获取 offsets
        maybeFetchStartAndEndOffsets();

        // ... 检查 initialization ...
    }
}
```

### Retry 时间线
```
Time 0: addAssignmentsForPartitions(orders-0)
  ├─ fetchStartAndEndOffsets()
  ├─ consumer.endOffsets() -> LeaderNotAvailableException ❌
  ├─ hasLastOffsetsFetchFailed = true
  └─ lastFailedFetchOffsetsTimestamp = 0

Time 100ms: poll()
  ├─ maybeFetchStartAndEndOffsets()
  ├─ 检查：0 + 5000 > 100 -> 不重试（太早）
  └─ continue

Time 5100ms: poll()
  ├─ maybeFetchStartAndEndOffsets()
  ├─ 检查：0 + 5000 < 5100 -> 重试 ✅
  ├─ fetchStartAndEndOffsets()
  ├─ consumer.endOffsets() -> success
  └─ hasLastOffsetsFetchFailed = false
```

### 引入的复杂度
1. **hasLastOffsetsFetchFailed 标志**：记录失败状态
2. **lastFailedFetchOffsetsTimestamp**：记录失败时间
3. **maybeFetchStartAndEndOffsets() 逻辑**：延迟重试
4. **RetriableException 捕获**：区分可重试和不可重试错误
5. **为什么？** 优雅处理瞬时错误，避免频繁重试

---

## 完整架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                      ConsumerTask                                │
│  (运行在独立线程，消费 __remote_log_metadata topic)              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ 核心组件                                               │    │
│  │  - consumer: Consumer<byte[], byte[]>                 │    │
│  │  - serde: RemoteLogMetadataSerde                      │    │
│  │  - handler: RemotePartitionMetadataEventHandler       │    │
│  │  - partitioner: RemoteLogMetadataTopicPartitioner     │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ 状态管理                                               │    │
│  │  - assignedUserTopicIdPartitions:                     │    │
│  │      Map<TopicIdPartition, UserTopicIdPartition>      │    │
│  │      orders-0 -> UserTopicIdPartition{               │    │
│  │        metadataPartition: 5,                          │    │
│  │        isAssigned: true,                              │    │
│  │        isInitialized: true                            │    │
│  │      }                                                 │    │
│  │                                                        │    │
│  │  - assignedMetadataPartitions: Set<Integer>           │    │
│  │      {5, 23, 12}  // 需要消费的 metadata partitions   │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Offset 追踪                                            │    │
│  │  - readOffsetsByMetadataPartition:                    │    │
│  │      Map<Integer, Long>                               │    │
│  │      {5: 9999, 23: 5432}  // metadata partition offset│    │
│  │                                                        │    │
│  │  - readOffsetsByUserTopicPartition:                   │    │
│  │      Map<TopicIdPartition, Long>                      │    │
│  │      {orders-0: 1234}  // user partition 的 metadata  │    │
│  │                         offset (用于去重)              │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Initialization 追踪                                    │    │
│  │  - offsetHolderByMetadataPartition:                   │    │
│  │      Map<TopicPartition, StartAndEndOffsetHolder>     │    │
│  │      metadata-5 -> {startOffset: 0, endOffset: 10000} │    │
│  │                                                        │    │
│  │  - isAllUserTopicPartitionsInitialized: boolean       │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ 并发控制                                               │    │
│  │  - assignPartitionsLock: Object (锁)                  │    │
│  │  - volatile hasAssignmentChanged: boolean             │    │
│  │  - volatile isClosed: boolean                         │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Retry 管理                                             │    │
│  │  - hasLastOffsetsFetchFailed: boolean                 │    │
│  │  - lastFailedFetchOffsetsTimestamp: long              │    │
│  │  - offsetFetchRetryIntervalMs: long                   │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                  │
│  核心方法:                                                       │
│  ├─ run()  // 主循环                                            │
│  ├─ ingestRecords()  // poll + process                         │
│  ├─ processConsumerRecord()  // 处理单条 record                │
│  ├─ maybeWaitForPartitionAssignments()  // 等待/更新分配       │
│  ├─ maybeMarkUserPartitionsAsReady()  // 检查 initialization   │
│  ├─ addAssignmentsForPartitions()  // 添加分配                 │
│  ├─ removeAssignmentsForPartitions()  // 删除分配              │
│  └─ close()  // 关闭                                            │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼ 调用
┌─────────────────────────────────────────────────────────────────┐
│         RemotePartitionMetadataEventHandler                      │
│  (处理 metadata 事件，更新内存索引)                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 复杂度总结表

| Step | 新增组件 | 引入原因 | 复杂度类型 |
|------|---------|---------|-----------|
| 1 | 基础消费 | 消费 metadata topic | - |
| 2 | RemotePartitionMetadataEventHandler | 处理 metadata，更新内存状态 | 关注点分离 |
| 3 | Partition Assignment | 只消费相关 partition 的 metadata | 资源优化 |
| 4 | RemoteLogMetadataTopicPartitioner | 映射 user partition -> metadata partition | 分布式映射 |
| 5 | Offset 追踪 | 记住消费进度，支持重启 | 故障恢复 |
| 6 | Initialization 状态 | 等待追赶历史数据 | 数据完整性 |
| 7 | 并发控制 (锁) | 线程安全（assignment 来自其他线程） | 并发编程 |
| 8 | 去重检查 | 避免重复处理同一个 metadata | 幂等性 |
| 9 | Tombstone 处理 | 支持 compaction 的删除 | 数据模型 |
| 10 | Retry 机制 | 优雅处理瞬时错误 | 容错性 |

---

## 关键设计决策

### 1. 为什么需要 metadata partition 映射？

**问题**：
```
__remote_log_metadata 有 50 个 partitions
orders 有 100 个 partitions

如果每个 user partition 对应一个 metadata partition
  -> 需要 100 个 metadata partitions
  -> 太多了

如果所有 user partition 共享一个 metadata partition
  -> 单点瓶颈
```

**解决方案**：
```
使用 hash 映射：
  metadataPartition = hash(topicId, partition) % 50

优势：
  - 确定性（同一个 partition 总是映射到同一个 metadata partition）
  - 负载均衡（均匀分布）
  - 可扩展（增加 metadata partitions 数量）
```

### 2. 为什么需要 initialization 状态？

**问题**：
```
新分配的 partition 的 metadata partition 可能有大量历史数据
如果立即提供查询服务 -> 数据不完整
```

**解决方案**：
```
1. 读取 endOffset
2. 消费到 endOffset
3. 标记为 initialized
4. 开始提供查询服务

保证：查询时一定能看到所有历史 remote segments
```

### 3. 为什么需要两级 offset 追踪？

**Metadata Partition Offset**：
- `readOffsetsByMetadataPartition`
- 用途：记住 metadata partition 的消费进度
- 场景：重启后恢复消费位置

**User Partition Offset**：
- `readOffsetsByUserTopicPartition`
- 用途：去重（避免重复处理）
- 场景：同一个 metadata partition 可能有多个 user partition 的事件

### 4. 为什么使用快照模式（snapshot）？

**问题**：
```
在锁内调用 consumer API（endOffsets, assign, seek）
  -> 这些 API 可能很慢（网络调用）
  -> 锁持有时间长
  -> 阻塞 request handler 线程
```

**解决方案**：
```
synchronized (lock) {
    // 快速拍摄快照
    snapshot = new HashSet<>(assignments);
}
// 锁外执行耗时操作
consumer.assign(snapshot);
```

### 5. 为什么需要 retry 延迟？

**问题**：
```
获取 endOffsets 失败
如果立即重试 -> 频繁调用，浪费资源
如果不重试 -> partition 永远无法 initialized
```

**解决方案**：
```
记录失败时间
延迟 5 秒后重试
避免频繁重试，但保证最终会成功
```

---

## 总结

ConsumerTask 从简单到复杂的演化：

1. **Step 1-2**：基础功能 - 消费 metadata + 处理 handler
2. **Step 3-4**：分布式映射 - partition assignment + metadata partitioner
3. **Step 5**：故障恢复 - offset 追踪
4. **Step 6**：数据完整性 - initialization 状态
5. **Step 7**：并发控制 - 线程安全
6. **Step 8**：幂等性 - 去重检查
7. **Step 9**：数据模型 - tombstone 处理
8. **Step 10**：容错性 - retry 机制

每一步都解决了实际的问题，逐步构建出一个完整的、分布式的、容错的 metadata 消费系统！

**核心价值**：
- 让每个 Broker 只消费它需要的 metadata
- 保证数据完整性（initialization）
- 支持故障恢复（offset 追踪）
- 线程安全（并发控制）
- 幂等性（去重）
- 容错性（retry）
