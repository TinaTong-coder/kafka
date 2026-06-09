# Kafka Consumer 代码复用设计：以 subscribe 和 poll 为例

## 目录
1. [设计概览](#设计概览)
2. [API 层次设计](#api-层次设计)
3. [subscribe 的多态设计](#subscribe-的多态设计)
4. [poll 的统一处理](#poll-的统一处理)
5. [内部组件职责分离](#内部组件职责分离)
6. [设计模式总结](#设计模式总结)

---

## 设计概览

Kafka Consumer 面临的核心挑战：
- **需求多样性**：用户有不同的使用场景（简单/复杂、同步/异步、单线程/多线程）
- **实现复杂性**：底层有多种协议实现（Classic Protocol / New Consumer Protocol）
- **代码维护性**：避免代码重复，便于扩展新功能

**核心设计理念**：
> 通过**分层抽象 + 委托模式 + 模板方法**，在保持 API 简洁的同时最大化代码复用

### 架构分层

```
┌─────────────────────────────────────────────────────────┐
│                  KafkaConsumer (Facade)                 │  ← 用户接口层
│  subscribe(topics)                                      │
│  subscribe(topics, listener)                            │
│  subscribe(pattern)                                     │
│  poll(timeout)                                          │
└────────────────────┬────────────────────────────────────┘
                     │ 委托 (Delegation)
                     ▼
┌─────────────────────────────────────────────────────────┐
│              ConsumerDelegate (Interface)               │  ← 抽象层
└────────────────────┬───────────────────┬────────────────┘
                     │                   │
          ┌──────────┴────────┐   ┌──────┴──────────┐
          │                   │   │                 │
┌─────────▼─────────┐  ┌──────▼──────┐   ┌────────▼─────────┐
│ AsyncKafkaConsumer│  │ClassicKafka │   │ ShareConsumer    │  ← 实现层
│ (New Protocol)    │  │Consumer     │   │ (Share Group)    │
│                   │  │(Legacy)     │   │                  │
└─────────┬─────────┘  └──────┬──────┘   └────────┬─────────┘
          │                   │                   │
          └───────────────────┴───────────────────┘
                              │
          ┌───────────────────┴─────────────────────┐
          │                                         │
┌─────────▼──────────┐              ┌───────────────▼──────────┐
│  Fetcher           │              │  ConsumerCoordinator     │  ← 组件层
│  (数据拉取)         │              │  (组协调)                 │
└────────────────────┘              └──────────────────────────┘
```

---

## API 层次设计

### 1. 用户 API 层（KafkaConsumer）

**设计目标**：提供统一、简洁的外观（Facade Pattern）

```java
public class KafkaConsumer<K, V> implements Consumer<K, V> {
    // 核心：所有方法都委托给 delegate
    private final ConsumerDelegate<K, V> delegate;

    // 构造函数：根据配置创建不同的实现
    public KafkaConsumer(Map<String, Object> configs) {
        this.delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    // API 方法：简单委托
    @Override
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);  // 委托给具体实现
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        delegate.subscribe(topics, listener);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return delegate.poll(timeout);  // 委托给具体实现
    }
}
```

**关键点1：用户只看到一个类 `KafkaConsumer`**
- 无论底层是 AsyncKafkaConsumer 还是 ClassicKafkaConsumer
- 用户代码不需要改变

**关键点2：通过工厂模式选择实现**

```java
public class ConsumerDelegateCreator {
    public <K, V> ConsumerDelegate<K, V> create(ConsumerConfig config, ...) {
        GroupProtocol protocol = config.getString("group.protocol");

        if (protocol == GroupProtocol.CONSUMER) {
            return new AsyncKafkaConsumer<>(...);  // 新协议
        } else {
            return new ClassicKafkaConsumer<>(...); // 旧协议
        }
    }
}
```

**配置驱动的多态**：
```java
// 用户只需改配置，无需改代码
Properties props = new Properties();
props.put("group.protocol", "consumer");  // 选择新协议
// props.put("group.protocol", "classic");  // 选择旧协议

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
```

---

## subscribe 的多态设计

### 问题：如何支持多种订阅方式？

用户需求多样：
1. 简单订阅：`subscribe(["topic1", "topic2"])`
2. 带监听器：`subscribe(topics, rebalanceListener)`
3. 正则订阅：`subscribe(Pattern.compile("log-.*"))`
4. 正则+监听器：`subscribe(pattern, listener)`

### 设计方案：重载 + 默认参数

#### Consumer 接口（公共契约）

```java
public interface Consumer<K, V> {
    // 基础方法
    void subscribe(Collection<String> topics);

    // 扩展方法：添加监听器
    void subscribe(Collection<String> topics, ConsumerRebalanceListener listener);

    // 正则订阅
    void subscribe(Pattern pattern);
    void subscribe(Pattern pattern, ConsumerRebalanceListener listener);
}
```

**设计理念**：
- 简单场景用简单API（无参listener）
- 复杂场景有完整控制（带listener）
- 通过**方法重载**避免参数爆炸

#### 实现层：统一内部处理

以 AsyncKafkaConsumer 为例：

```java
public class AsyncKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    // 公共 API：简单版本（委托给带 listener 的版本）
    @Override
    public void subscribe(Collection<String> topics) {
        subscribeInternal(topics, Optional.empty());  // 无 listener
    }

    // 公共 API：完整版本
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribeInternal(topics, Optional.of(listener));
    }

    // 正则订阅：简单版本
    @Override
    public void subscribe(Pattern pattern) {
        subscribeInternal(pattern, Optional.empty());
    }

    // 正则订阅：完整版本
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        subscribeInternal(pattern, Optional.of(listener));
    }

    // ========================================
    // 核心：所有公共方法都汇聚到私有方法
    // ========================================

    /**
     * 统一的内部实现（Topic 订阅）
     * 复用点1：参数验证
     * 复用点2：锁管理
     * 复用点3：事件生成
     */
    private void subscribeInternal(Collection<String> topics,
                                   Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();  // 复用：线程安全检查
        try {
            throwIfGroupIdNotDefined();  // 复用：参数校验

            if (topics == null)
                throw new IllegalArgumentException("Topic collection cannot be null");

            if (topics.isEmpty()) {
                unsubscribe();  // 复用：空订阅等同于取消订阅
                return;
            }

            // 复用：统一的topic验证逻辑
            for (String topic : topics) {
                if (isBlank(topic))
                    throw new IllegalArgumentException("Topic cannot be null or empty");
            }

            // 复用：清理不相关的缓存数据
            final Set<TopicPartition> currentTopicPartitions = new HashSet<>();
            for (TopicPartition tp : subscriptions.assignedPartitions()) {
                if (topics.contains(tp.topic()))
                    currentTopicPartitions.add(tp);
            }
            fetchBuffer.retainAll(currentTopicPartitions);

            log.info("Subscribed to topic(s): {}", String.join(", ", topics));

            // 复用：统一的事件处理机制
            applicationEventHandler.addAndGet(new TopicSubscriptionChangeEvent(
                new HashSet<>(topics),
                listener,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            release();  // 复用：确保锁释放
        }
    }

    /**
     * 统一的内部实现（Pattern 订阅）
     * 注意：正则订阅和Topic订阅的内部逻辑略有不同，但结构相同
     */
    private void subscribeInternal(Pattern pattern,
                                   Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();  // 复用：相同的锁管理
        try {
            throwIfGroupIdNotDefined();  // 复用：相同的参数校验

            if (pattern == null || pattern.toString().isEmpty())
                throw new IllegalArgumentException("Topic pattern cannot be null or empty");

            log.info("Subscribed to pattern: '{}'", pattern);

            // 不同点：生成不同的事件类型（但结构相同）
            applicationEventHandler.addAndGet(new TopicPatternSubscriptionChangeEvent(
                pattern,
                listener,
                defaultApiTimeoutDeadlineMs()
            ));
        } finally {
            release();  // 复用：相同的清理逻辑
        }
    }
}
```

### 代码复用技巧总结

#### 技巧1：Optional 统一处理有无 listener

```java
// 不好的设计：两份逻辑
void subscribe(Collection<String> topics) {
    // 大量逻辑A
}

void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    // 大量逻辑A的重复代码
    // 加上listener处理
}

// 好的设计：使用 Optional 统一
void subscribe(Collection<String> topics) {
    subscribeInternal(topics, Optional.empty());
}

void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    subscribeInternal(topics, Optional.of(listener));
}

private void subscribeInternal(Collection<String> topics,
                               Optional<ConsumerRebalanceListener> listener) {
    // 统一逻辑
    // listener 在需要时从 Optional 中取出
}
```

#### 技巧2：模板方法复用流程

所有 subscribe 方法都遵循相同的模板：

```java
private void subscribeInternal(...) {
    // 1. 加锁 + 检查状态（复用）
    acquireAndEnsureOpen();

    try {
        // 2. 参数校验（复用）
        throwIfGroupIdNotDefined();
        validateParameters(...);

        // 3. 业务逻辑（部分复用，部分差异化）
        doSubscribe(...);

        // 4. 生成事件（复用事件机制，差异化事件类型）
        applicationEventHandler.addAndGet(createEvent(...));

    } finally {
        // 5. 释放锁（复用）
        release();
    }
}
```

**复用的部分**：
- 锁管理（`acquireAndEnsureOpen` / `release`）
- 参数校验（`throwIfGroupIdNotDefined`）
- 事件机制（`applicationEventHandler`）

**差异化的部分**：
- 具体的验证逻辑（topic vs pattern）
- 事件类型（`TopicSubscriptionChangeEvent` vs `TopicPatternSubscriptionChangeEvent`）

#### 技巧3：层次化的 API 设计

```
用户层 API（多个重载）
    │
    ├─ subscribe(topics)  ──────┐
    ├─ subscribe(topics, listener) ─┤
    ├─ subscribe(pattern)  ────────┤
    └─ subscribe(pattern, listener)┤
                                   │
                                   ▼
            内部层 API（2个核心方法）
                │
                ├─ subscribeInternal(topics, Optional<listener>)
                └─ subscribeInternal(pattern, Optional<listener>)
                                   │
                                   ▼
                    组件层（共享的基础能力）
                │
                ├─ acquireAndEnsureOpen()
                ├─ throwIfGroupIdNotDefined()
                ├─ applicationEventHandler
                └─ fetchBuffer
```

**好处**：
- 用户层：提供便利性（4个重载方法）
- 内部层：保持简洁（2个核心方法）
- 组件层：高度复用（多个订阅方式共用）

---

## poll 的统一处理

### 问题：poll 需要整合多个复杂流程

`poll()` 需要处理：
1. 心跳发送
2. Rebalance 检测和执行
3. 数据拉取
4. Offset 更新
5. Wakeup 中断
6. 异常处理

如何在不同实现（AsyncKafkaConsumer、ClassicKafkaConsumer）中复用这些逻辑？

### 设计方案：分层委托 + 组件化

#### 顶层：统一的 poll 入口

```java
public class AsyncKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        Timer timer = time.timer(timeout);  // 复用：时间管理

        acquireAndEnsureOpen();  // 复用：锁 + 状态检查
        try {
            kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());  // 复用：指标

            // 复用：统一的订阅检查
            if (subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException(
                    "Consumer is not subscribed to any topics or assigned any partitions"
                );
            }

            boolean firstPass = true;

            do {
                // 复用：Wakeup 检查
                wakeupTrigger.maybeTriggerWakeup();

                // 复用：检查是否有未完成的 poll（防止并发 poll）
                checkInflightPoll(timer, firstPass);
                firstPass = false;

                // 核心：拉取数据
                final Fetch<K, V> fetch = pollForFetches(timer);

                if (!fetch.isEmpty()) {
                    // 有数据，返回前触发下一轮 fetch（流水线优化）
                    // 这是性能优化：避免用户处理数据时网络闲置
                    return processFetchResults(fetch, timer);
                }

                // 没数据，继续循环直到超时
            } while (timer.notExpired());

            // 超时，返回空结果
            return ConsumerRecords.empty();

        } finally {
            kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
            release();  // 复用：释放锁
        }
    }
}
```

**复用点分析**：

| 复用组件 | 作用 | 复用方式 |
|---------|------|---------|
| `acquireAndEnsureOpen()` | 线程安全 + 状态检查 | 所有public方法复用 |
| `wakeupTrigger` | 中断机制 | 组件化复用 |
| `subscriptions` | 订阅状态管理 | 组件化复用 |
| `kafkaConsumerMetrics` | 指标收集 | 组件化复用 |
| `timer` | 超时控制 | 工具类复用 |

#### 核心逻辑：pollForFetches

```java
private Fetch<K, V> pollForFetches(Timer timer) {
    // 1. 处理后台线程的事件（如 rebalance 通知）
    processPendingEvents();  // 复用：事件处理框架

    // 2. 发送心跳（如果需要）
    // 注意：心跳在后台线程处理，这里只是触发

    // 3. 更新 fetch positions
    updateFetchPositions(timer);  // 复用：position 管理

    // 4. 从缓冲区获取已拉取的数据
    final Fetch<K, V> fetch = fetchBuffer.collectFetch();

    // 5. 如果缓冲区没数据，触发新的 fetch 请求
    if (fetch.isEmpty()) {
        fetchBuffer.awaitNotEmpty(timer.remainingMs());
    }

    return fetch;
}
```

**关键设计**：
- 所有复杂逻辑都委托给**专门的组件**
- `poll()` 本身只是**协调者**，不包含具体实现

### 组件化复用示例

#### FetchBuffer（复用数据缓冲）

```java
public class FetchBuffer<K, V> {
    private final Queue<CompletedFetch<K, V>> completedFetches;

    // 所有实现都复用这个缓冲区
    public Fetch<K, V> collectFetch() {
        // 从缓冲区提取数据
    }

    public void add(CompletedFetch<K, V> fetch) {
        // 添加新拉取的数据
    }

    public void retainAll(Set<TopicPartition> partitions) {
        // 清理不需要的分区数据
    }
}
```

#### SubscriptionState（复用订阅状态）

```java
public class SubscriptionState {
    // 所有实现都复用这个状态管理

    public boolean hasNoSubscriptionOrUserAssignment() {
        return !hasAutoAssignedPartitions() && !hasPatternSubscription() && assignedPartitions.isEmpty();
    }

    public Set<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    public void assignFromUser(Set<TopicPartition> partitions) {
        // 手动分配
    }

    public void assignFromSubscribed(Collection<TopicPartition> partitions) {
        // 自动分配（subscribe）
    }
}
```

#### WakeupTrigger（复用中断机制）

```java
public class WakeupTrigger {
    private final AtomicBoolean wakeupOccurred = new AtomicBoolean(false);

    // 线程安全的 wakeup
    public void wakeup() {
        wakeupOccurred.set(true);
    }

    // 检查并抛出异常
    public void maybeTriggerWakeup() throws WakeupException {
        if (wakeupOccurred.getAndSet(false)) {
            throw new WakeupException();
        }
    }
}
```

**复用优势**：
- AsyncKafkaConsumer 和 ClassicKafkaConsumer **共享同一套组件**
- 新增功能只需修改组件，不需要改每个实现
- 测试可以单独测试组件

---

## 内部组件职责分离

### 单一职责原则（SRP）

Kafka Consumer 将功能拆分为多个组件，每个组件只负责一件事：

```
KafkaConsumer (协调者)
    │
    ├─ SubscriptionState          (订阅状态管理)
    ├─ Fetcher                     (数据拉取)
    ├─ ConsumerCoordinator         (组协调)
    ├─ ConsumerNetworkClient       (网络通信)
    ├─ OffsetCommitCallback        (Offset 提交)
    ├─ ConsumerInterceptors        (拦截器链)
    ├─ Deserializer                (反序列化)
    └─ ConsumerMetrics             (指标收集)
```

### 组件接口设计示例

#### Fetcher（数据拉取组件）

```java
public class Fetcher<K, V> {
    private final ConsumerNetworkClient client;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;

    /**
     * 发起 fetch 请求
     * 复用点：所有需要拉取数据的场景都调用这个方法
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        // 1. 检查哪些 partition 需要拉取
        // 2. 构造 FetchRequest
        // 3. 发送请求
        // 4. 解析响应
        // 5. 反序列化
        return records;
    }

    /**
     * 更新 fetch position
     * 复用点：subscribe 和 assign 都需要更新 position
     */
    public void resetOffsetIfNeeded(TopicPartition partition,
                                    OffsetResetStrategy strategy) {
        // 根据策略重置 offset（earliest/latest/none）
    }
}
```

#### ConsumerCoordinator（组协调组件）

```java
public class ConsumerCoordinator {
    private final GroupCoordinatorClient client;
    private final SubscriptionState subscriptions;

    /**
     * 加入 consumer group
     * 复用点：subscribe 触发，rebalance 时重新加入
     */
    public void joinGroupIfNeeded(Timer timer) {
        if (needsJoinGroup()) {
            // 1. 发送 JoinGroup 请求
            // 2. 等待分配结果
            // 3. 更新 subscriptions
            // 4. 触发 rebalance listener
        }
    }

    /**
     * 发送心跳
     * 复用点：poll 和后台线程都调用
     */
    public void heartbeat() {
        // 发送心跳，接收 rebalance 通知
    }
}
```

### 组件间交互：依赖注入

```java
public class AsyncKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {

    // 依赖的组件（通过构造函数注入）
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final FetchBuffer<K, V> fetchBuffer;
    private final ApplicationEventHandler applicationEventHandler;
    private final WakeupTrigger wakeupTrigger;

    // 构造函数：组装所有组件
    AsyncKafkaConsumer(ConsumerConfig config,
                      Deserializer<K> keyDeserializer,
                      Deserializer<V> valueDeserializer,
                      Optional<ApplicationEventHandler> eventHandler) {

        // 创建共享的状态对象
        this.subscriptions = new SubscriptionState(config, logContext);
        this.metadata = new ConsumerMetadata(config, subscriptions, logContext);

        // 创建缓冲区
        this.fetchBuffer = new FetchBuffer<>(logContext);

        // 创建事件处理器
        this.applicationEventHandler = eventHandler.orElseGet(
            () -> new ApplicationEventHandler(...)
        );

        // 创建 wakeup 触发器
        this.wakeupTrigger = new WakeupTrigger();

        // 所有组件共享这些状态对象
    }
}
```

**复用优势**：
- 组件可以独立测试（Mock 其他组件）
- 组件可以独立演进（修改 Fetcher 不影响 Coordinator）
- 组件可以在不同实现间共享

---

## 设计模式总结

### 1. 外观模式（Facade Pattern）

**应用场景**：`KafkaConsumer` 隐藏底层实现复杂性

```java
// 用户只看到简单接口
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("topic"));
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    // 处理数据
}

// 背后是复杂的协调
// - ConsumerDelegate 选择
// - 网络线程管理
// - 心跳发送
// - Rebalance 协调
// - 数据拉取和缓冲
```

### 2. 委托模式（Delegation Pattern）

**应用场景**：`KafkaConsumer` 委托给 `ConsumerDelegate`

```java
public class KafkaConsumer<K, V> {
    private final ConsumerDelegate<K, V> delegate;

    public ConsumerRecords<K, V> poll(Duration timeout) {
        return delegate.poll(timeout);  // 完全委托
    }
}
```

**好处**：
- 运行时选择实现（AsyncKafkaConsumer vs ClassicKafkaConsumer）
- KafkaConsumer 本身不包含业务逻辑，只是路由

### 3. 策略模式（Strategy Pattern）

**应用场景**：不同的 consumer 实现

```java
// 策略接口
interface ConsumerDelegate<K, V> extends Consumer<K, V> {
    ConsumerRecords<K, V> poll(Duration timeout);
}

// 策略1：异步实现
class AsyncKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {
    public ConsumerRecords<K, V> poll(Duration timeout) {
        // 基于事件驱动的 poll 实现
    }
}

// 策略2：经典实现
class ClassicKafkaConsumer<K, V> implements ConsumerDelegate<K, V> {
    public ConsumerRecords<K, V> poll(Duration timeout) {
        // 基于同步阻塞的 poll 实现
    }
}

// 策略选择器
class ConsumerDelegateCreator {
    public <K, V> ConsumerDelegate<K, V> create(ConsumerConfig config) {
        if (config.useNewProtocol()) {
            return new AsyncKafkaConsumer<>();
        } else {
            return new ClassicKafkaConsumer<>();
        }
    }
}
```

### 4. 模板方法模式（Template Method Pattern）

**应用场景**：subscribe 的统一流程

```java
// 模板方法
private void subscribeInternal(...) {
    acquireAndEnsureOpen();      // 步骤1：加锁（固定）
    try {
        validate();              // 步骤2：校验（固定）
        doSubscribe();           // 步骤3：核心逻辑（可变）
        generateEvent();         // 步骤4：生成事件（固定）
    } finally {
        release();               // 步骤5：释放锁（固定）
    }
}

// 具体方法1：Topic 订阅
void subscribe(Collection<String> topics) {
    subscribeInternal(topics, Optional.empty());
}

// 具体方法2：Pattern 订阅
void subscribe(Pattern pattern) {
    subscribeInternal(pattern, Optional.empty());
}
```

### 5. 组合模式（Composite Pattern）

**应用场景**：组件组合

```java
class AsyncKafkaConsumer {
    // 多个组件组合成完整功能
    private final SubscriptionState subscriptions;
    private final FetchBuffer fetchBuffer;
    private final ApplicationEventHandler eventHandler;
    private final WakeupTrigger wakeupTrigger;

    public ConsumerRecords<K, V> poll(Duration timeout) {
        // 协调所有组件完成 poll 操作
        eventHandler.process();
        subscriptions.validate();
        Fetch fetch = fetchBuffer.collectFetch();
        wakeupTrigger.maybeTriggerWakeup();
        return convertToRecords(fetch);
    }
}
```

### 6. 依赖注入模式（Dependency Injection）

**应用场景**：组件解耦

```java
// 不好的设计：硬编码依赖
class BadConsumer {
    private final Fetcher fetcher;

    public BadConsumer() {
        this.fetcher = new Fetcher();  // 紧耦合
    }
}

// 好的设计：依赖注入
class GoodConsumer {
    private final Fetcher fetcher;

    public GoodConsumer(Fetcher fetcher) {  // 通过构造函数注入
        this.fetcher = fetcher;
    }
}

// 测试时可以注入 Mock
GoodConsumer consumer = new GoodConsumer(mockFetcher);
```

---

## 代码复用的层次

### Level 1：方法级复用

同一个类内的方法复用：

```java
// 公共方法调用私有方法
public void subscribe(Collection<String> topics) {
    subscribeInternal(topics, Optional.empty());
}

public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    subscribeInternal(topics, Optional.of(listener));
}

// 私有方法实现核心逻辑
private void subscribeInternal(Collection<String> topics,
                               Optional<ConsumerRebalanceListener> listener) {
    // 核心逻辑
}
```

### Level 2：组件级复用

不同类共享组件：

```java
// AsyncKafkaConsumer 使用
class AsyncKafkaConsumer {
    private final SubscriptionState subscriptions;  // 共享组件
    private final FetchBuffer fetchBuffer;          // 共享组件
}

// ClassicKafkaConsumer 也使用同样的组件
class ClassicKafkaConsumer {
    private final SubscriptionState subscriptions;  // 同一个组件
    private final FetchBuffer fetchBuffer;          // 同一个组件
}
```

### Level 3：接口级复用

通过接口定义通用行为：

```java
// 接口定义契约
interface Consumer<K, V> {
    void subscribe(Collection<String> topics);
    ConsumerRecords<K, V> poll(Duration timeout);
}

// 多个实现共享接口
class AsyncKafkaConsumer implements Consumer { ... }
class ClassicKafkaConsumer implements Consumer { ... }
class ShareConsumer implements Consumer { ... }

// 用户代码只依赖接口
public void processMessages(Consumer<String, String> consumer) {
    consumer.subscribe(...);
    consumer.poll(...);
}
```

### Level 4：框架级复用

通过抽象基类或工具类复用：

```java
// 工具类复用
class ConsumerUtils {
    static void acquireAndEnsureOpen(Lock lock, AtomicBoolean closed) {
        lock.lock();
        if (closed.get()) {
            lock.unlock();
            throw new IllegalStateException("Consumer closed");
        }
    }
}

// 所有实现都可以使用
class AsyncKafkaConsumer {
    public void poll(...) {
        ConsumerUtils.acquireAndEnsureOpen(lock, closed);
        // ...
    }
}
```

---

## 实际代码示例：完整的复用链路

### 场景：用户调用 subscribe

```java
// 1. 用户代码（调用外观）
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("orders"));

// ▼ 外观层委托

// 2. KafkaConsumer（外观）
public class KafkaConsumer<K, V> {
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);  // 委托给实现
    }
}

// ▼ 实现层路由

// 3. AsyncKafkaConsumer（具体实现）
public class AsyncKafkaConsumer<K, V> {
    public void subscribe(Collection<String> topics) {
        subscribeInternal(topics, Optional.empty());  // 方法级复用
    }

    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        subscribeInternal(topics, Optional.of(listener));  // 同一个内部方法
    }

    private void subscribeInternal(Collection<String> topics,
                                   Optional<ConsumerRebalanceListener> listener) {
        acquireAndEnsureOpen();  // 复用锁管理
        try {
            throwIfGroupIdNotDefined();  // 复用参数校验
            validateTopics(topics);      // 复用topic验证

            // 复用缓冲区清理
            fetchBuffer.retainAll(relevantPartitions(topics));

            // 复用事件机制
            applicationEventHandler.addAndGet(
                new TopicSubscriptionChangeEvent(topics, listener, timeout)
            );
        } finally {
            release();  // 复用锁释放
        }
    }
}

// ▼ 组件层执行

// 4. ApplicationEventHandler（事件处理组件）
public class ApplicationEventHandler {
    public void addAndGet(ApplicationEvent event) {
        eventQueue.add(event);  // 所有事件都通过这个队列
        backgroundThread.wakeup();  // 通知后台线程处理
    }
}

// 5. BackgroundThread（后台线程）
class ConsumerNetworkThread extends Thread {
    public void run() {
        while (running) {
            ApplicationEvent event = eventQueue.poll();
            if (event instanceof TopicSubscriptionChangeEvent) {
                handleSubscriptionChange((TopicSubscriptionChangeEvent) event);
            }
        }
    }

    private void handleSubscriptionChange(TopicSubscriptionChangeEvent event) {
        // 更新订阅状态（复用 SubscriptionState 组件）
        subscriptions.subscribe(event.topics(), event.listener());

        // 触发元数据更新（复用 Metadata 组件）
        metadata.requestUpdate();

        // 可能触发 rebalance（复用 Coordinator 组件）
        coordinator.joinGroupIfNeeded();
    }
}
```

**复用链路分析**：
- **外观层**：KafkaConsumer 复用委托模式
- **实现层**：AsyncKafkaConsumer 复用方法重载 + Optional
- **组件层**：复用锁、校验、缓冲区、事件队列
- **线程层**：复用后台线程、订阅状态、元数据、协调器

---

## 总结：Kafka Consumer 的代码复用哲学

### 核心原则

1. **接口隔离**：用户只看到 `Consumer` 接口，不知道底层实现
2. **实现多态**：通过配置选择不同实现（AsyncKafkaConsumer / ClassicKafkaConsumer）
3. **组件化**：功能拆分到独立组件（Fetcher、Coordinator、SubscriptionState）
4. **方法复用**：公共方法委托私有方法，避免代码重复
5. **工具抽象**：通用逻辑抽取为工具类或基类

### API 设计的权衡

| 维度 | 简单 API | 复杂 API | Kafka 的选择 |
|-----|---------|---------|-------------|
| **方法数量** | 少（1-2个） | 多（10+个） | **中等**（4-6个重载） |
| **参数灵活性** | 固定参数 | 可选参数 | **Optional参数** |
| **用户体验** | 简单易用 | 功能强大 | **分层设计**（简单场景简单用，复杂场景有能力） |
| **代码复用** | 难（逻辑分散） | 易（内部统一） | **模板方法**（内部统一，外部多样） |

### subscribe 的设计总结

```java
// 简单用法：最少参数
consumer.subscribe(Arrays.asList("topic"));

// 高级用法：添加监听器
consumer.subscribe(Arrays.asList("topic"), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // 自定义逻辑
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 自定义逻辑
    }
});

// 正则订阅：动态匹配 topic
consumer.subscribe(Pattern.compile("log-.*"));

// 手动分配：完全控制
consumer.assign(Arrays.asList(new TopicPartition("topic", 0)));
```

**设计精髓**：
- **渐进式复杂度**：简单场景用简单API，复杂场景有完整控制
- **内部统一**：4个公共方法最终汇聚到2个私有方法
- **组件复用**：所有方法共享锁、校验、事件处理等组件

### poll 的设计总结

```java
// 用户只需调用一个方法
ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));

// 背后整合了：
// - 心跳发送 (复用 Coordinator)
// - Rebalance 检测 (复用 EventHandler)
// - 数据拉取 (复用 Fetcher)
// - 缓冲管理 (复用 FetchBuffer)
// - Wakeup 处理 (复用 WakeupTrigger)
// - 指标收集 (复用 Metrics)
```

**设计精髓**：
- **单一入口**：用户只调用一个 `poll()` 方法
- **协调者模式**：poll 本身不实现具体逻辑，只协调各组件
- **组件隔离**：每个组件独立演进，互不影响

---

## 给其他系统设计的启示

### 1. 如何设计多层次 API？

```
简单层：subscribe(topics)
    ↓ 内部调用
中间层：subscribe(topics, Optional<listener>)
    ↓ 内部调用
复杂层：subscribeInternal(topics, listener, options)
```

**关键**：让简单的场景简单用，复杂的场景有能力

### 2. 如何避免代码重复？

```
公共逻辑 → 提取到私有方法
通用组件 → 提取到独立类
固定流程 → 使用模板方法
多个实现 → 提取到接口
```

### 3. 如何支持多种实现？

```
定义接口 Consumer
    ↓
实现1: AsyncKafkaConsumer
实现2: ClassicKafkaConsumer
实现3: ShareConsumer
    ↓
用户只依赖接口，运行时选择实现
```

### 4. 如何做到高内聚低耦合？

```
KafkaConsumer (低内聚：只做路由)
    ↓ 委托
AsyncKafkaConsumer (高内聚：完整实现)
    ↓ 组合
Fetcher + Coordinator + SubscriptionState (各自高内聚)
```

---

## 最终代码示意图

```
用户代码:
    consumer.subscribe(topics)
    consumer.poll(timeout)

         ↓ (外观模式)

KafkaConsumer:
    delegate.subscribe(topics)
    delegate.poll(timeout)

         ↓ (策略模式)

AsyncKafkaConsumer:
    subscribeInternal(topics, Optional.empty())  ← 方法复用
    subscribeInternal(topics, Optional.of(listener))

    pollInternal(timeout) {
        wakeupTrigger.check()      ← 组件复用
        eventHandler.process()     ← 组件复用
        fetchBuffer.collect()      ← 组件复用
    }

         ↓ (组合模式)

Components:
    ├─ SubscriptionState (订阅状态)
    ├─ FetchBuffer (数据缓冲)
    ├─ ApplicationEventHandler (事件处理)
    ├─ WakeupTrigger (中断控制)
    ├─ Fetcher (数据拉取)
    └─ ConsumerCoordinator (组协调)
```

这种设计让 Kafka Consumer 既保持了 API 的简洁性，又具备了极高的代码复用率和扩展性。
