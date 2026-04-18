# Mini HTTP Client - 详细答案与讲解

## 📚 目录

1. [核心概念答案](#1-核心概念答案)
2. [代码实现详解](#2-代码实现详解)
3. [设计决策解释](#3-设计决策解释)
4. [常见问题答案](#4-常见问题答案)
5. [扩展练习答案](#5-扩展练习答案)

---

## 1. 核心概念答案

### Q1: 为什么 Kafka 不使用独立的网络线程？

**答案**：

Kafka Consumer 选择单线程模型而不是多线程，主要基于以下考虑：

#### ✅ 优势

1. **无需线程同步**
   ```java
   // ❌ 多线程模型 - 需要大量锁
   class MultiThreaded {
       private final Object lock = new Object();

       void networkThread() {
           Response r = socket.read();
           synchronized(lock) {  // 必须加锁
               callback.onComplete(r);
           }
       }

       void userThread() {
           synchronized(lock) {  // 访问共享状态必须加锁
               consumer.poll();
           }
       }
   }

   // ✅ 单线程模型 - 无需锁
   class SingleThreaded {
       void poll() {
           // 所有操作在同一线程，无需同步
           selector.select();
           handleResponses();
           invokeCallbacks();
       }
   }
   ```

2. **无上下文切换开销**
   - 多线程：线程切换需要保存/恢复寄存器、切换栈等，开销约 1-10 微秒
   - 单线程：无切换开销，所有代码在同一栈上执行

3. **简化并发模型**
   ```java
   // ❌ 多线程 - 复杂的状态管理
   class State {
       volatile boolean ready;  // 需要 volatile
       AtomicInteger count;     // 需要 Atomic
       ConcurrentHashMap map;   // 需要并发集合
   }

   // ✅ 单线程 - 简单的状态管理
   class State {
       boolean ready;           // 普通变量即可
       int count;
       HashMap map;             // 普通集合即可
   }
   ```

4. **可预测的执行顺序**
   ```java
   // 单线程：顺序清晰
   send(request1);    // 1. 发送请求1
   send(request2);    // 2. 发送请求2
   poll();            // 3. 处理响应（按顺序）
   ```

#### ❌ 劣势

1. **无法利用多核**
   - 但 Kafka Consumer 通常不是 CPU 密集型，而是 I/O 密集型
   - 一个 Consumer 处理多个 partition 已经足够高效

2. **长时间操作会阻塞**
   - 解决方案：用户 callback 必须快速返回
   - 重操作应该异步提交到线程池

#### 🎯 结论

对于 Kafka Consumer 这种：
- I/O 密集型
- 需要保证消息顺序
- 用户 API 简单易用

单线程模型是最优选择。

---

### Q2: compose() 相比直接转换有什么优势？

**答案**：

#### 对比场景

```java
// ❌ 方案 A：直接转换（不使用 compose）
public RequestFuture<HttpResult> send(Request request) {
    RequestFuture<HttpResult> future = new RequestFuture<>();

    // 网络层直接处理业务逻辑
    selector.select();
    byte[] bytes = readSocket();
    Response response = new Response(bytes);

    // 网络层混入了 HTTP 解析逻辑 - 违反单一职责
    HttpResult result = parseHttp(response);
    future.complete(result);

    return future;
}

// ✅ 方案 B：使用 compose 分层
public RequestFuture<Response> send(Request request) {
    RequestFuture<Response> future = new RequestFuture<>();
    // 网络层只关心字节流
    selector.select();
    byte[] bytes = readSocket();
    future.complete(new Response(bytes));
    return future;
}

// 协议层通过 compose 处理
RequestFuture<HttpResult> businessFuture =
    networkFuture.compose(new HttpResponseHandler());
```

#### ✅ compose() 的优势

**1. 关注点分离（Separation of Concerns）**

```
┌─────────────────────────────────────┐
│ 网络层 (NetworkClient)               │
│ - 职责：TCP 连接、字节流收发          │
│ - 不关心：HTTP、业务逻辑              │
│ - 可复用：支持任何协议                │
└─────────────────────────────────────┘
            ↓ compose
┌─────────────────────────────────────┐
│ 协议层 (HttpResponseHandler)         │
│ - 职责：解析 HTTP 响应                │
│ - 不关心：网络细节、业务逻辑          │
│ - 可复用：任何 HTTP 客户端都可用      │
└─────────────────────────────────────┘
            ↓ addListener
┌─────────────────────────────────────┐
│ 业务层 (用户 Callback)               │
│ - 职责：处理业务逻辑                  │
│ - 不关心：网络、协议                  │
└─────────────────────────────────────┘
```

**2. 可测试性**

```java
// ✅ 使用 compose：可以单独测试每一层
@Test
public void testHttpParser() {
    // 不需要启动网络，直接测试解析逻辑
    Response mockResponse = new Response("HTTP/1.1 200 OK\r\n\r\n");
    RequestFuture<HttpResult> future = RequestFuture.success(mockResponse)
        .compose(new HttpResponseHandler());

    assertEquals(200, future.value().getStatusCode());
}

// ❌ 不使用 compose：必须启动网络才能测试
@Test
public void testWithoutCompose() {
    // 必须启动真实的网络连接
    NetworkClient client = new NetworkClient();
    // 依赖外部服务，测试不稳定
}
```

**3. 可复用性**

```java
// 同一个网络层，支持多种协议
RequestFuture<Response> networkFuture = network.send(request);

// HTTP 协议
RequestFuture<HttpResult> httpFuture =
    networkFuture.compose(new HttpResponseHandler());

// 自定义协议
RequestFuture<CustomResult> customFuture =
    networkFuture.compose(new CustomProtocolHandler());

// Kafka 协议
RequestFuture<OffsetCommitResult> kafkaFuture =
    networkFuture.compose(new OffsetCommitResponseHandler());
```

**4. 类型安全**

```java
// compose 提供编译时类型检查
RequestFuture<Response> f1 = network.send(request);
RequestFuture<HttpResult> f2 = f1.compose(handler);  // 类型转换清晰

// 不能将错误的类型传递
// f2.compose(new WrongTypeHandler());  // ❌ 编译错误
```

**5. 易于扩展**

```java
// 添加新功能：缓存层
RequestFuture<Response> networkFuture = network.send(request);
RequestFuture<Response> cachedFuture = networkFuture.compose(new CacheHandler());
RequestFuture<HttpResult> httpFuture = cachedFuture.compose(new HttpResponseHandler());

// 添加新功能：重试层
RequestFuture<Response> retriedFuture = networkFuture.compose(new RetryHandler());
RequestFuture<HttpResult> httpFuture = retriedFuture.compose(new HttpResponseHandler());
```

#### 🎯 结论

`compose()` 不仅仅是类型转换，而是实现了：
- **职责分离**：每层只做一件事
- **可测试**：每层可以独立测试
- **可复用**：每层可以在不同场景复用
- **可扩展**：容易添加新功能

这就是为什么 Kafka 要用 `compose()`！

---

### Q3: 如果在用户 callback 中再次调用 getAsync()，会发生什么？

**答案**：

这是一个非常重要的问题，涉及到递归和调用栈管理。

#### 场景演示

```java
// 用户代码：在 callback 中再次发起请求
client.getAsync("example.com", 80, "/api1", new HttpCallback() {
    @Override
    public void onComplete(HttpResult result, Exception e) {
        System.out.println("第一个请求完成");

        // 在 callback 中发起第二个请求
        client.getAsync("example.com", 80, "/api2", new HttpCallback() {
            @Override
            public void onComplete(HttpResult result2, Exception e2) {
                System.out.println("第二个请求完成");
            }
        });
    }
});
```

#### ❌ 如果不延迟执行 Callback（错误设计）

```java
// 错误设计：直接在 Future 完成时调用 callback
businessFuture.addListener(new RequestFutureListener<HttpResult>() {
    void onSuccess(HttpResult result) {
        callback.onComplete(result, null);  // ❌ 直接调用
    }
});
```

**调用栈会变成**：

```
poll()
  → firePendingCompletions()
    → future.complete()
      → fireSuccess()
        → listener.onSuccess()
          → callback.onComplete()        ← 用户代码
            → client.getAsync()           ← 用户在 callback 中调用
              → future2.addListener()
              → pollNoWait()
                → poll()                   ← 递归！
                  → firePendingCompletions()
                    → future2.complete()
                      → listener2.onSuccess()
                        → callback2.onComplete()
                          → ... (栈越来越深)
```

**问题**：
1. 调用栈深度不可控（可能栈溢出）
2. 执行顺序混乱
3. 难以调试

#### ✅ 延迟执行 Callback（正确设计）

```java
// 正确设计：不直接调用，而是加入队列
businessFuture.addListener(new RequestFutureListener<HttpResult>() {
    void onSuccess(HttpResult result) {
        completedCallbacks.add(new Completion(callback, result));  // ✅ 加入队列
    }
});

// 在明确的时机调用
void invokeCompletedCallbacks() {
    while ((c = queue.poll()) != null) {
        c.callback.onComplete(c.result);
    }
}
```

**调用栈变成**：

```
第一次 poll():
  → invokeCompletedCallbacks()
    → callback1.onComplete()      ← 用户代码
      → client.getAsync()          ← 发起第二个请求
        → future2.addListener()    ← 只是注册监听器
        → completedCallbacks.add() ← 加入队列
    返回到 poll()
  → poll() 完成

第二次 poll():
  → invokeCompletedCallbacks()
    → callback2.onComplete()      ← 处理第二个请求
    返回到 poll()
  → poll() 完成
```

**优势**：
1. 调用栈深度可控（每次 poll 最多一层）
2. 执行顺序清晰（FIFO）
3. 容易调试

#### 🔍 实际运行示例

```java
// 执行流程
client.getAsync("/api1", callback1);
// 输出：
// [1] Request queued: /api1
// [2] Listener added

client.poll(1000);
// 输出：
// [3] Poll started
// [4] Invoking callbacks
// [5] Callback #1: /api1 完成
//     [6] Inside callback: calling getAsync(/api2)
//     [7] Request queued: /api2
//     [8] Listener added
// [9] Poll completed

client.poll(1000);
// 输出：
// [10] Poll started
// [11] Invoking callbacks
// [12] Callback #2: /api2 完成
// [13] Poll completed
```

#### 🎯 结论

延迟执行 Callback 的设计：
- ✅ 避免了递归调用
- ✅ 控制了调用栈深度
- ✅ 保证了执行顺序
- ✅ 用户可以在 callback 中安全地调用任何 API

这就是为什么 Kafka 要用队列延迟执行！

---

### Q4: 如何保证多个请求的回调顺序？

**答案**：

通过 **队列的 FIFO 特性** 保证顺序。

#### 实现原理

```java
public class HttpClient {
    // 使用 ConcurrentLinkedQueue 保证顺序
    private final ConcurrentLinkedQueue<CallbackCompletion> completedCallbacks =
        new ConcurrentLinkedQueue<>();

    public void getAsync(String url, HttpCallback callback) {
        // ...
        businessFuture.addListener(result -> {
            completedCallbacks.add(new Completion(callback, result));  // 按顺序入队
        });
    }

    private void invokeCompletedCallbacks() {
        while (true) {
            CallbackCompletion c = completedCallbacks.poll();  // 按顺序出队
            if (c == null) break;
            c.callback.onComplete(c.result);  // 按顺序调用
        }
    }
}
```

#### 顺序保证示例

```java
// 发起 3 个请求
client.getAsync("/api1", callback1);  // T1: 入队位置 1
client.getAsync("/api2", callback2);  // T2: 入队位置 2
client.getAsync("/api3", callback3);  // T3: 入队位置 3

// 假设响应顺序是：api3 → api1 → api2
// 网络层收到响应的顺序：3 → 1 → 2

// Future 完成顺序：
// T10: api3 完成 → completedCallbacks.add(callback3)  // 队列：[callback3]
// T15: api1 完成 → completedCallbacks.add(callback1)  // 队列：[callback3, callback1]
// T20: api2 完成 → completedCallbacks.add(callback2)  // 队列：[callback3, callback1, callback2]

// invokeCompletedCallbacks() 按队列顺序调用：
// 第 1 个：callback3.onComplete()
// 第 2 个：callback1.onComplete()
// 第 3 个：callback2.onComplete()
```

#### 🎯 关键点

1. **入队顺序 = Future 完成顺序**（不是请求发起顺序）
2. **出队顺序 = 入队顺序**（FIFO）
3. **调用顺序 = 出队顺序**

如果要保证按请求发起顺序调用，需要额外的机制：

```java
// 扩展：按请求序号排序
class OrderedCompletion {
    long sequence;  // 请求序号
    Callback callback;
}

// 使用 PriorityQueue 按序号排序
PriorityQueue<OrderedCompletion> queue =
    new PriorityQueue<>(Comparator.comparing(c -> c.sequence));
```

---

## 2. 代码实现详解

### 2.1 RequestFuture.compose() 深度解析

#### 完整代码

```java
public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
    // 1. 创建新的 Future（目标类型）
    final RequestFuture<S> adapted = new RequestFuture<>();

    // 2. 给当前 Future（源类型）添加监听器
    addListener(new RequestFutureListener<T>() {
        @Override
        public void onSuccess(T value) {
            // 3. 当前 Future 成功时，调用 adapter 转换
            adapter.onSuccess(value, adapted);
        }

        @Override
        public void onFailure(RuntimeException e) {
            // 4. 当前 Future 失败时，调用 adapter 处理
            adapter.onFailure(e, adapted);
        }
    });

    // 5. 返回新 Future
    return adapted;
}
```

#### 执行流程图

```
时刻 T0: 调用 compose
  │
  ├─ 创建 adapted Future (RequestFuture<S>)
  │
  ├─ 给当前 Future (RequestFuture<T>) 添加 listener
  │  └─ listener 内部持有 adapted 的引用
  │
  └─ 返回 adapted

时刻 T1: 当前 Future 完成 (complete(T value))
  │
  ├─ fireSuccess()
  │  │
  │  └─ 遍历 listeners
  │     │
  │     └─ 调用 listener.onSuccess(value)
  │        │
  │        └─ adapter.onSuccess(value, adapted)
  │           │
  │           ├─ 转换 T → S
  │           │
  │           └─ adapted.complete(s)  ← 触发 adapted Future
  │
  └─ 完成

时刻 T2: adapted Future 完成
  │
  └─ 触发 adapted 的 listeners
     └─ 用户的 callback
```

#### 为什么这样设计？

**1. 延迟转换**
```java
// 不是立即转换
S result = adapter.convert(T value);  // ❌ 立即转换
RequestFuture<S> future = RequestFuture.success(result);

// 而是延迟转换（等 Future 完成）
RequestFuture<S> future = originalFuture.compose(adapter);  // ✅ 延迟转换
```

**2. 错误传播**
```java
// adapter 可以决定如何处理错误
public void onFailure(RuntimeException e, RequestFuture<S> future) {
    if (e instanceof RetriableException) {
        // 可重试错误：转换为特定异常
        future.raise(new CustomRetryException(e));
    } else {
        // 不可重试：直接传播
        future.raise(e);
    }
}
```

**3. 链式调用**
```java
RequestFuture<Response> f1 = network.send(request);
RequestFuture<HttpResult> f2 = f1.compose(httpHandler);
RequestFuture<BusinessData> f3 = f2.compose(businessHandler);
RequestFuture<CachedData> f4 = f3.compose(cacheHandler);
// 无限扩展
```

---

### 2.2 NetworkClient 事件循环详解

#### poll() 方法剖析

```java
public void poll(long timeoutMs) throws IOException {
    // ============ 阶段 1: 触发已完成的回调 ============
    // 为什么在开始？因为可能有上次 poll 遗留的完成事件
    firePendingCompletions();

    // ============ 阶段 2: 发送待发送的请求 ============
    trySend();
    // 为什么在这里？因为要尽快发送请求，减少延迟

    // ============ 阶段 3: 等待网络事件 ============
    int readyCount = selector.select(timeoutMs);
    // 为什么用 Selector？非阻塞 I/O，一个线程处理多个连接

    // ============ 阶段 4: 处理可读事件 ============
    if (readyCount > 0) {
        handleReadableChannels();
        // 读取响应，加入 pendingCompletion 队列
    }

    // ============ 阶段 5: 再次触发回调 ============
    firePendingCompletions();
    // 为什么再次调用？因为阶段 4 可能产生了新的完成事件
}
```

#### 为什么要两次 firePendingCompletions()？

```java
// 场景：快速响应的请求
client.send(request);  // T0: 加入 unsent 队列

poll(100);  // T1: 第一次 poll
  // 阶段 1: firePendingCompletions() - 空的，没事做
  // 阶段 2: trySend() - 发送请求
  // 阶段 3: selector.select(100) - 假设立即收到响应
  // 阶段 4: handleReadableChannels() - 加入 pendingCompletion
  // 阶段 5: firePendingCompletions() - 🔥 触发回调！

// 如果没有阶段 5，callback 要等到下次 poll 才会被调用
// 延迟增加了 100ms！
```

#### trySend() 详解

```java
private void trySend() throws IOException {
    // 为什么用循环？一次 poll 可以发送多个请求
    while (!unsent.isEmpty()) {
        PendingRequest pending = unsent.poll();
        if (pending == null) break;

        // 打开连接（非阻塞）
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);  // 🔥 关键：非阻塞模式
        channel.connect(new InetSocketAddress(host, port));

        // 注册到 Selector
        SelectionKey key = channel.register(selector,
            SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        key.attach(pending);  // 附加请求信息

        // 加入 inFlight
        inFlight.put(key, pending);
    }
}
```

**为什么用非阻塞模式？**

```java
// ❌ 阻塞模式
channel.configureBlocking(true);
channel.connect(address);  // 阻塞！可能等待数秒
// 期间无法处理其他请求

// ✅ 非阻塞模式
channel.configureBlocking(false);
channel.connect(address);  // 立即返回，连接在后台进行
// 可以继续处理其他请求
selector.select();  // 等连接完成后会通知
```

---

### 2.3 HttpClient 延迟执行详解

#### 为什么需要 CallbackCompletion 包装？

```java
private static class CallbackCompletion {
    final HttpCallback callback;   // 用户回调
    final HttpResult result;       // 结果（成功时）
    final Exception exception;     // 异常（失败时）

    CallbackCompletion(HttpCallback callback, HttpResult result, Exception exception) {
        this.callback = callback;
        this.result = result;
        this.exception = exception;
    }
}
```

**设计原因**：

1. **解耦 Future 和 Callback**
   ```java
   // Future 完成时，不直接调用 callback
   future.complete(result);
     → listener.onSuccess(result)
       → completedCallbacks.add(new Completion(...))  // 只是入队

   // 稍后在安全的上下文中调用
   invokeCompletedCallbacks();
     → completion.callback.onComplete(...)  // 真正调用
   ```

2. **异常隔离**
   ```java
   private void invokeCompletedCallbacks() {
       while ((c = queue.poll()) != null) {
           try {
               c.callback.onComplete(c.result, c.exception);
           } catch (Exception e) {
               // 用户 callback 抛异常，不影响其他 callback
               System.err.println("Callback threw exception: " + e);
           }
       }
   }
   ```

3. **批量处理**
   ```java
   // 一次 poll 可以处理多个完成的 callback
   void invokeCompletedCallbacks() {
       int count = 0;
       while (...) {
           count++;
           completion.invoke();
       }
       System.out.println("Invoked " + count + " callbacks");
   }
   ```

---

## 3. 设计决策解释

### 3.1 为什么使用 ConcurrentLinkedQueue 而不是 ArrayList？

```java
// 实际代码
private final ConcurrentLinkedQueue<CallbackCompletion> completedCallbacks =
    new ConcurrentLinkedQueue<>();
```

**原因**：

1. **线程安全（虽然是单线程，但为了扩展性）**
   ```java
   // 如果将来需要支持多线程
   networkThread: pendingCompletion.add(completion);
   userThread: completion = pendingCompletion.poll();
   // ConcurrentLinkedQueue 无需额外同步
   ```

2. **无锁实现，高性能**
   ```java
   // ConcurrentLinkedQueue 使用 CAS，比 synchronized 快
   public boolean add(E e) {
       // 使用 CAS 操作，无需锁
       compareAndSet(tail, newNode);
   }
   ```

3. **FIFO 保证**
   ```java
   queue.add(callback1);  // 先进
   queue.add(callback2);
   queue.add(callback3);

   queue.poll();  // callback1 先出
   queue.poll();  // callback2
   queue.poll();  // callback3
   ```

---

### 3.2 为什么 Response 是不可变的？

```java
public class Response {
    private final Request request;        // final
    private final String rawResponse;     // final
    private final long receivedTime;      // final
    private final boolean disconnected;   // final

    // 只有构造函数，没有 setter
    public Response(Request request, String rawResponse, boolean disconnected) {
        this.request = request;
        this.rawResponse = rawResponse;
        this.receivedTime = System.currentTimeMillis();
        this.disconnected = disconnected;
    }
}
```

**原因**：

1. **线程安全**
   ```java
   // Response 对象可以在多个 listener 之间共享
   future.addListener(listener1);
   future.addListener(listener2);
   // 两个 listener 收到同一个 Response，不会互相干扰
   ```

2. **防止意外修改**
   ```java
   // ❌ 如果可变
   void onSuccess(Response r) {
       r.setRawResponse("hacked");  // 破坏了其他 listener 的数据
   }

   // ✅ 不可变
   void onSuccess(Response r) {
       // r.setRawResponse(...);  // 编译错误
       // 只能读取，不能修改
   }
   ```

3. **简化推理**
   ```java
   // Response 创建后永不改变
   Response r = new Response(...);
   // 之后任何时候访问 r，数据都是一致的
   ```

---

### 3.3 为什么 HttpResponseHandler 继承 RequestFutureAdapter？

```java
public class HttpResponseHandler extends RequestFutureAdapter<Response, HttpResult> {
    @Override
    public void onSuccess(Response response, RequestFuture<HttpResult> future) {
        // 转换逻辑
        HttpResult result = parse(response);
        future.complete(result);
    }

    @Override
    public void onFailure(RuntimeException e, RequestFuture<HttpResult> future) {
        // 错误处理
        future.raise(e);
    }
}
```

**而不是**：

```java
// ❌ 直接实现 RequestFutureListener
public class HttpResponseHandler implements RequestFutureListener<Response> {
    @Override
    public void onSuccess(Response response) {
        // 问题：怎么传递给下一个 Future？
        // 没有 RequestFuture<HttpResult> 参数！
    }
}
```

**原因**：

`RequestFutureAdapter` 的设计是为了 **类型转换**：

```java
public abstract class RequestFutureAdapter<F, T> {
    // 注意：有两个参数
    //   F value - 源类型的值
    //   RequestFuture<T> future - 目标类型的 Future
    public abstract void onSuccess(F value, RequestFuture<T> future);
}
```

这样 `compose()` 就可以：

```java
public <S> RequestFuture<S> compose(RequestFutureAdapter<T, S> adapter) {
    RequestFuture<S> adapted = new RequestFuture<>();  // 创建目标 Future

    addListener(new RequestFutureListener<T>() {
        void onSuccess(T value) {
            adapter.onSuccess(value, adapted);  // 传递目标 Future
        }
    });

    return adapted;
}
```

---

## 4. 常见问题答案

### Q: 为什么不用 CompletableFuture？

**答案**：

Java 8 的 `CompletableFuture` 确实功能强大，但 Kafka 有特殊需求：

1. **精确控制回调执行时机**
   ```java
   // CompletableFuture：回调立即在完成线程执行
   future.thenApply(result -> {
       // 在哪个线程执行？不确定！
       // 可能是网络线程，也可能是调用 complete() 的线程
   });

   // Kafka RequestFuture：明确在 poll() 中执行
   future.addListener(result -> {
       queue.add(completion);  // 加入队列
   });
   invokeCompletedCallbacks();  // 明确的执行点
   ```

2. **更简单的实现**
   ```java
   // Kafka RequestFuture：300 行代码
   // CompletableFuture：3000+ 行代码
   // Kafka 只需要基本功能，不需要复杂的组合操作
   ```

3. **兼容性**
   ```java
   // Kafka 支持 Java 7+
   // CompletableFuture 需要 Java 8+
   ```

---

### Q: selector.select() 的 timeout 如何选择？

**答案**：

```java
public void poll(long timeoutMs) {
    selector.select(timeoutMs);  // timeout 怎么定？
}
```

**策略**：

1. **有待发送请求：timeout = 0**
   ```java
   if (!unsent.isEmpty()) {
       selector.select(0);  // 不等待，立即返回
       // 尽快发送请求
   }
   ```

2. **有进行中的请求：timeout = 请求超时时间**
   ```java
   long minTimeout = Long.MAX_VALUE;
   for (Request req : inFlight) {
       long remaining = req.timeoutMs - (now - req.createdTime);
       minTimeout = Math.min(minTimeout, remaining);
   }
   selector.select(minTimeout);
   ```

3. **无事可做：timeout = 用户指定**
   ```java
   if (unsent.isEmpty() && inFlight.isEmpty()) {
       selector.select(timeoutMs);  // 用户指定的超时
   }
   ```

---

### Q: 如何处理慢请求？

**答案**：

```java
// 方案 1：超时机制
public class Request {
    long createdTime;
    long timeoutMs;

    boolean isExpired(long now) {
        return (now - createdTime) > timeoutMs;
    }
}

// 在 poll() 中检查
void poll() {
    long now = System.currentTimeMillis();
    for (Request req : inFlight) {
        if (req.isExpired(now)) {
            // 超时处理
            req.future.raise(new TimeoutException());
            inFlight.remove(req);
            closeChannel(req.channel);
        }
    }
}

// 方案 2：异步超时检测
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

void sendWithTimeout(Request req, long timeout) {
    ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
        if (!req.future.isDone()) {
            req.future.raise(new TimeoutException());
        }
    }, timeout, TimeUnit.MILLISECONDS);

    req.future.addListener(result -> {
        timeoutTask.cancel(false);  // 完成后取消超时任务
    });
}
```

---

## 5. 扩展练习答案

### 练习 1: 添加 POST 请求支持

**完整实现**见下一个文件 `EXERCISE_SOLUTIONS.md`

**关键点**：

1. 扩展 Request 类
2. 添加 body 和 headers
3. 修改 HTTP 请求格式

---

### 练习 2: 添加超时处理

**关键点**：

1. 在 Request 中添加 timeout 字段
2. 在 poll() 中检查超时
3. 触发 TimeoutException

---

### 练习 3: 添加重试机制

**关键点**：

1. 在 Future 失败时检查是否可重试
2. 重新加入 unsent 队列
3. 限制重试次数

---

## 📚 总结

这个 Mini HTTP Client 项目展示了：

1. ✅ **单线程异步模型** - 高效且简单
2. ✅ **三层 Future 架构** - 关注点分离
3. ✅ **延迟执行 Callback** - 调用栈控制
4. ✅ **Reactor 模式** - 事件驱动
5. ✅ **NIO 非阻塞 I/O** - 高并发

**与 Kafka 源码几乎一模一样的设计！**

通过这个项目，你不仅学会了如何实现异步客户端，更重要的是理解了：
- **为什么** 要这样设计
- **什么时候** 该用这种模式
- **如何** 应用到自己的项目

---

**继续阅读 `EXERCISE_SOLUTIONS.md` 查看扩展练习的完整答案！**
