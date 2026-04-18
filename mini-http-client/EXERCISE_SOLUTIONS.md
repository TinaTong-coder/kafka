# 扩展练习答案

本文档提供了所有扩展练习的完整答案和详细讲解。

## 📚 目录

1. [初级练习](#初级练习)
   - [练习 1: 添加 POST 请求支持](#练习-1-添加-post-请求支持)
   - [练习 2: 添加请求超时处理](#练习-2-添加请求超时处理)
   - [练习 3: 添加重试机制](#练习-3-添加重试机制)

2. [中级练习](#中级练习)
   - [练习 4: 支持连接池](#练习-4-支持连接池)
   - [练习 5: 添加请求取消功能](#练习-5-添加请求取消功能)
   - [练习 6: 实现同步版本 API](#练习-6-实现同步版本-api)

3. [高级练习](#高级练习)
   - [练习 7: 添加 HTTPS 支持](#练习-7-添加-https-支持)
   - [练习 8: 实现请求优先级队列](#练习-8-实现请求优先级队列)

---

## 初级练习

### 练习 1: 添加 POST 请求支持

#### 目标
支持 POST 请求，包括：
- 自定义 HTTP method
- 添加 request body
- 自定义 headers

#### 完整实现

**1. 扩展 Request 类**

```java
package com.example.network;

import java.util.HashMap;
import java.util.Map;

/**
 * 扩展的 HTTP 请求（支持 POST）
 */
public class Request {
    private final String host;
    private final int port;
    private final String path;
    private final String method;  // 新增：HTTP 方法
    private final Map<String, String> headers;  // 新增：请求头
    private final String body;  // 新增：请求体
    private final long createdTime;

    // 构造函数（GET 请求，向后兼容）
    public Request(String host, int port, String path) {
        this(host, port, path, "GET", new HashMap<>(), null);
    }

    // 完整构造函数
    public Request(String host, int port, String path, String method,
                   Map<String, String> headers, String body) {
        this.host = host;
        this.port = port;
        this.path = path;
        this.method = method;
        this.headers = new HashMap<>(headers);  // 防御性复制
        this.body = body;
        this.createdTime = System.currentTimeMillis();

        // 自动添加必需的 headers
        if (!this.headers.containsKey("Host")) {
            this.headers.put("Host", host + ":" + port);
        }
        if (body != null && !this.headers.containsKey("Content-Length")) {
            this.headers.put("Content-Length", String.valueOf(body.getBytes().length));
        }
        if (!this.headers.containsKey("Connection")) {
            this.headers.put("Connection", "close");
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    public String getMethod() {
        return method;
    }

    public Map<String, String> getHeaders() {
        return new HashMap<>(headers);  // 返回副本
    }

    public String getBody() {
        return body;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * 构建 HTTP 请求字符串
     */
    public String toHttpString() {
        StringBuilder sb = new StringBuilder();

        // 请求行
        sb.append(method).append(" ").append(path).append(" HTTP/1.1\r\n");

        // 请求头
        for (Map.Entry<String, String> header : headers.entrySet()) {
            sb.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
        }

        // 空行（分隔 headers 和 body）
        sb.append("\r\n");

        // 请求体（如果有）
        if (body != null) {
            sb.append(body);
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("Request{%s %s:%d%s}", method, host, port, path);
    }

    /**
     * Builder 模式，方便构建复杂请求
     */
    public static class Builder {
        private String host;
        private int port = 80;
        private String path = "/";
        private String method = "GET";
        private Map<String, String> headers = new HashMap<>();
        private String body;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder method(String method) {
            this.method = method.toUpperCase();
            return this;
        }

        public Builder header(String name, String value) {
            this.headers.put(name, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public Builder body(String body) {
            this.body = body;
            return this;
        }

        public Builder get() {
            this.method = "GET";
            return this;
        }

        public Builder post() {
            this.method = "POST";
            return this;
        }

        public Builder put() {
            this.method = "PUT";
            return this;
        }

        public Builder delete() {
            this.method = "DELETE";
            return this;
        }

        public Request build() {
            if (host == null) {
                throw new IllegalStateException("Host is required");
            }
            return new Request(host, port, path, method, headers, body);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
```

**2. 扩展 HttpClient**

```java
package com.example.client;

// ... 导入省略 ...

public class HttpClient {
    // ... 现有代码 ...

    /**
     * 异步 POST 请求
     */
    public void postAsync(String host, int port, String path, String body,
                         Map<String, String> headers, HttpCallback callback) {
        System.out.println("\n========== postAsync START ==========");
        System.out.println("[HttpClient] POST Request: " + host + ":" + port + path);

        // 构建 POST 请求
        Map<String, String> allHeaders = new HashMap<>();
        if (headers != null) {
            allHeaders.putAll(headers);
        }
        // 添加 Content-Type（如果未指定）
        if (!allHeaders.containsKey("Content-Type")) {
            allHeaders.put("Content-Type", "application/x-www-form-urlencoded");
        }

        Request request = new Request(host, port, path, "POST", allHeaders, body);

        // 后续逻辑与 getAsync 相同
        sendRequest(request, callback);
    }

    /**
     * 通用的发送方法（内部使用）
     */
    private void sendRequest(Request request, HttpCallback callback) {
        RequestFuture<Response> networkFuture = networkClient.send(request);
        RequestFuture<HttpResult> businessFuture = networkFuture.compose(new HttpResponseHandler());

        businessFuture.addListener(new RequestFutureListener<HttpResult>() {
            @Override
            public void onSuccess(HttpResult result) {
                completedCallbacks.add(new CallbackCompletion(callback, result, null));
            }

            @Override
            public void onFailure(RuntimeException exception) {
                completedCallbacks.add(new CallbackCompletion(callback, null, exception));
            }
        });

        pollNoWait();
    }

    /**
     * 使用 Builder 的方式
     */
    public void sendAsync(Request request, HttpCallback callback) {
        sendRequest(request, callback);
    }

    // ... 其他代码不变 ...
}
```

**3. 使用示例**

```java
package com.example.demo;

import com.example.api.HttpCallback;
import com.example.api.HttpResult;
import com.example.client.HttpClient;
import com.example.network.Request;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PostDemo {
    public static void main(String[] args) throws IOException {
        HttpClient client = new HttpClient();

        // 方式 1: 使用 postAsync
        String jsonBody = "{\"name\":\"test\",\"value\":123}";
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        client.postAsync("httpbin.org", 80, "/post", jsonBody, headers, new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                if (exception != null) {
                    System.err.println("❌ POST failed: " + exception.getMessage());
                } else {
                    System.out.println("✅ POST succeeded!");
                    System.out.println("   Status: " + result.getStatusCode());
                    System.out.println("   Body: " + result.getBody());
                }
            }
        });

        // 方式 2: 使用 Builder
        Request request = Request.builder()
            .host("httpbin.org")
            .port(80)
            .path("/post")
            .post()
            .header("Content-Type", "application/json")
            .header("User-Agent", "MiniHttpClient/1.0")
            .body(jsonBody)
            .build();

        client.sendAsync(request, new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                System.out.println("✅ Request with Builder succeeded!");
            }
        });

        // 等待完成
        client.waitForCompletion(10000);
        client.close();
    }
}
```

#### 为什么这样设计？

1. **Builder 模式**
   ```java
   // ✅ 清晰易读
   Request request = Request.builder()
       .host("example.com")
       .post()
       .header("Content-Type", "application/json")
       .body("{\"key\":\"value\"}")
       .build();

   // ❌ 构造函数参数太多
   Request request = new Request("example.com", 80, "/api", "POST",
       Map.of("Content-Type", "application/json"), "{\"key\":\"value\"}");
   ```

2. **向后兼容**
   ```java
   // 旧代码继续工作
   Request request = new Request("example.com", 80, "/api");  // GET

   // 新功能
   Request request = Request.builder().host("example.com").post().build();
   ```

3. **自动处理 headers**
   ```java
   // 自动添加 Content-Length
   if (body != null) {
       headers.put("Content-Length", String.valueOf(body.length()));
   }
   ```

---

### 练习 2: 添加请求超时处理

#### 目标
- 为每个请求设置超时时间
- 超时后自动取消请求
- 触发 TimeoutException

#### 完整实现

**1. 扩展 Request 类**

```java
public class Request {
    // ... 现有字段 ...
    private final long timeoutMs;  // 新增：超时时间

    public static class Builder {
        // ... 现有字段 ...
        private long timeoutMs = 30000;  // 默认 30 秒

        public Builder timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Request build() {
            // ... 现有代码 ...
            return new Request(host, port, path, method, headers, body, timeoutMs);
        }
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public boolean isExpired(long now) {
        return (now - createdTime) > timeoutMs;
    }
}
```

**2. 修改 NetworkClient**

```java
package com.example.network;

public class NetworkClient {
    // ... 现有代码 ...

    public void poll(long timeoutMs) throws IOException {
        System.out.println("[NetworkClient] Poll started");

        // 🔥 新增：检查超时
        checkTimeouts(System.currentTimeMillis());

        firePendingCompletions();
        trySend();

        // 计算实际等待时间（考虑最近的超时）
        long actualTimeout = calculateTimeout(timeoutMs);
        int readyCount = selector.select(actualTimeout);

        if (readyCount > 0) {
            handleReadableChannels();
        }

        // 再次检查超时
        checkTimeouts(System.currentTimeMillis());

        firePendingCompletions();
    }

    /**
     * 🔥 检查并处理超时的请求
     */
    private void checkTimeouts(long now) {
        Iterator<Map.Entry<String, PendingRequest>> iterator = inFlight.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, PendingRequest> entry = iterator.next();
            PendingRequest pending = entry.getValue();

            if (pending.request.isExpired(now)) {
                System.err.println("[NetworkClient] Request timeout: " + pending.request);

                // 创建超时异常
                TimeoutException timeoutException = new TimeoutException(
                    "Request timed out after " + pending.request.getTimeoutMs() + "ms"
                );

                // 加入完成队列
                pendingCompletion.add(new CompletedRequest(
                    pending.handler,
                    null,
                    new RuntimeException(timeoutException)
                ));

                // 清理资源
                try {
                    SelectionKey key = findKeyForRequest(pending);
                    if (key != null) {
                        key.cancel();
                        key.channel().close();
                    }
                } catch (IOException e) {
                    System.err.println("[NetworkClient] Error closing timed out channel: " + e);
                }

                // 从 inFlight 移除
                iterator.remove();
            }
        }
    }

    /**
     * 计算实际的超时时间（取最小值）
     */
    private long calculateTimeout(long defaultTimeout) {
        long now = System.currentTimeMillis();
        long minTimeout = defaultTimeout;

        for (PendingRequest pending : inFlight.values()) {
            long remaining = pending.request.getTimeoutMs() - (now - pending.request.getCreatedTime());
            if (remaining > 0) {
                minTimeout = Math.min(minTimeout, remaining);
            }
        }

        return Math.max(0, minTimeout);
    }

    /**
     * 查找请求对应的 SelectionKey
     */
    private SelectionKey findKeyForRequest(PendingRequest pending) {
        for (SelectionKey key : selector.keys()) {
            if (key.attachment() == pending) {
                return key;
            }
        }
        return null;
    }

    // TimeoutException 定义
    public static class TimeoutException extends Exception {
        public TimeoutException(String message) {
            super(message);
        }
    }
}
```

**3. 使用示例**

```java
public class TimeoutDemo {
    public static void main(String[] args) throws IOException {
        HttpClient client = new HttpClient();

        // 发起一个会超时的请求
        Request request = Request.builder()
            .host("httpbin.org")
            .path("/delay/5")  // 服务器延迟 5 秒响应
            .timeout(2000)      // 客户端超时 2 秒
            .build();

        client.sendAsync(request, new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                if (exception != null) {
                    System.err.println("❌ Request failed: " + exception.getMessage());
                    // 输出：Request timed out after 2000ms
                } else {
                    System.out.println("✅ Request succeeded");
                }
            }
        });

        // 等待（会超时）
        client.waitForCompletion(5000);
        client.close();
    }
}
```

#### 为什么这样设计？

1. **双重检查**
   ```java
   poll() {
       checkTimeouts();  // poll 开始时检查
       selector.select();
       checkTimeouts();  // poll 结束时检查
   }
   ```
   确保及时检测到超时

2. **动态计算超时**
   ```java
   // 不是固定等待 timeoutMs
   long actualTimeout = calculateTimeout(timeoutMs);
   selector.select(actualTimeout);
   ```
   确保最近的超时能够被及时触发

3. **资源清理**
   ```java
   // 超时后关闭连接
   key.cancel();
   channel.close();
   inFlight.remove(request);
   ```
   防止资源泄漏

---

### 练习 3: 添加重试机制

#### 目标
- 自动重试失败的请求
- 支持可配置的重试次数
- 指数退避策略

#### 完整实现

**1. 定义重试策略**

```java
package com.example.client;

/**
 * 重试策略
 */
public class RetryPolicy {
    private final int maxRetries;           // 最大重试次数
    private final long initialBackoffMs;    // 初始退避时间
    private final long maxBackoffMs;        // 最大退避时间
    private final double backoffMultiplier; // 退避倍数

    public RetryPolicy(int maxRetries, long initialBackoffMs, long maxBackoffMs, double backoffMultiplier) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
    }

    public static RetryPolicy none() {
        return new RetryPolicy(0, 0, 0, 1.0);
    }

    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(3, 100, 5000, 2.0);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * 计算第 n 次重试的退避时间
     */
    public long getBackoffMs(int retryCount) {
        if (retryCount <= 0) {
            return 0;
        }

        // 指数退避：100ms, 200ms, 400ms, 800ms, ...
        long backoff = (long) (initialBackoffMs * Math.pow(backoffMultiplier, retryCount - 1));
        return Math.min(backoff, maxBackoffMs);
    }

    /**
     * 判断异常是否可重试
     */
    public boolean isRetriable(Exception e) {
        // 网络错误、超时 → 可重试
        if (e instanceof IOException || e instanceof NetworkClient.TimeoutException) {
            return true;
        }

        // HTTP 5xx 错误 → 可重试
        // HTTP 4xx 错误 → 不可重试（客户端错误）
        // 这里简化处理
        return false;
    }
}
```

**2. 扩展 Request 添加重试信息**

```java
public class Request {
    // ... 现有字段 ...
    private final RetryPolicy retryPolicy;  // 重试策略
    private int retryCount;                 // 已重试次数

    public static class Builder {
        // ... 现有字段 ...
        private RetryPolicy retryPolicy = RetryPolicy.defaultPolicy();

        public Builder retryPolicy(RetryPolicy policy) {
            this.retryPolicy = policy;
            return this;
        }

        public Builder noRetry() {
            this.retryPolicy = RetryPolicy.none();
            return this;
        }

        public Request build() {
            return new Request(host, port, path, method, headers, body, timeoutMs, retryPolicy);
        }
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public boolean canRetry() {
        return retryCount < retryPolicy.getMaxRetries();
    }
}
```

**3. 修改 HttpClient 添加重试逻辑**

```java
package com.example.client;

public class HttpClient {
    // ... 现有代码 ...

    /**
     * 发送请求（带重试）
     */
    private void sendRequest(Request request, HttpCallback callback) {
        RequestFuture<Response> networkFuture = networkClient.send(request);
        RequestFuture<HttpResult> businessFuture = networkFuture.compose(new HttpResponseHandler());

        businessFuture.addListener(new RequestFutureListener<HttpResult>() {
            @Override
            public void onSuccess(HttpResult result) {
                // 成功：直接调用 callback
                completedCallbacks.add(new CallbackCompletion(callback, result, null));
            }

            @Override
            public void onFailure(RuntimeException exception) {
                // 失败：检查是否需要重试
                handleFailure(request, callback, exception);
            }
        });

        pollNoWait();
    }

    /**
     * 🔥 处理失败（重试逻辑）
     */
    private void handleFailure(Request request, HttpCallback callback, RuntimeException exception) {
        RetryPolicy policy = request.getRetryPolicy();

        // 检查是否可以重试
        if (request.canRetry() && policy.isRetriable(exception)) {
            // 递增重试次数
            request.incrementRetryCount();

            // 计算退避时间
            long backoffMs = policy.getBackoffMs(request.getRetryCount());

            System.out.println("[HttpClient] Retrying request (attempt " +
                (request.getRetryCount() + 1) + "/" + (policy.getMaxRetries() + 1) +
                ") after " + backoffMs + "ms");

            // 调度重试
            scheduleRetry(request, callback, backoffMs);
        } else {
            // 不能重试，调用 callback
            System.err.println("[HttpClient] Request failed after " + request.getRetryCount() + " retries");
            completedCallbacks.add(new CallbackCompletion(callback, null, exception));
        }
    }

    /**
     * 调度重试
     */
    private void scheduleRetry(Request request, HttpCallback callback, long delayMs) {
        // 简单实现：使用 sleep
        // 生产环境应该使用 ScheduledExecutorService

        new Thread(() -> {
            try {
                Thread.sleep(delayMs);
                // 重新发送请求
                sendRequest(request, callback);
            } catch (InterruptedException e) {
                completedCallbacks.add(new CallbackCompletion(callback, null, e));
            }
        }).start();
    }

    // ... 其他代码 ...
}
```

**4. 使用示例**

```java
public class RetryDemo {
    public static void main(String[] args) throws IOException {
        HttpClient client = new HttpClient();

        // 自定义重试策略
        RetryPolicy customPolicy = new RetryPolicy(
            5,      // 最多重试 5 次
            100,    // 初始退避 100ms
            10000,  // 最大退避 10 秒
            2.0     // 每次翻倍
        );

        Request request = Request.builder()
            .host("httpbin.org")
            .path("/status/500")  // 总是返回 500 错误
            .retryPolicy(customPolicy)
            .build();

        client.sendAsync(request, new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                if (exception != null) {
                    System.err.println("❌ Failed after all retries: " + exception.getMessage());
                } else {
                    System.out.println("✅ Succeeded!");
                }
            }
        });

        client.waitForCompletion(60000);  // 等待足够长的时间
        client.close();
    }
}
```

**输出示例**：

```
[HttpClient] Retrying request (attempt 2/6) after 100ms
[HttpClient] Retrying request (attempt 3/6) after 200ms
[HttpClient] Retrying request (attempt 4/6) after 400ms
[HttpClient] Retrying request (attempt 5/6) after 800ms
[HttpClient] Retrying request (attempt 6/6) after 1600ms
[HttpClient] Request failed after 5 retries
❌ Failed after all retries: HTTP 500
```

#### 为什么这样设计？

1. **指数退避**
   ```java
   // 避免雪崩效应
   backoff = 100ms * 2^(retryCount-1)
   // 100ms, 200ms, 400ms, 800ms, 1600ms, ...
   ```

2. **可配置性**
   ```java
   // 不同场景使用不同策略
   RetryPolicy.none();           // 不重试
   RetryPolicy.defaultPolicy();  // 默认策略
   new RetryPolicy(10, ...);     // 自定义
   ```

3. **智能判断**
   ```java
   // 只重试可恢复的错误
   if (exception instanceof IOException) {
       return true;  // 网络错误，可重试
   }
   if (statusCode == 500) {
       return true;  // 服务器错误，可重试
   }
   if (statusCode == 404) {
       return false;  // 客户端错误，不重试
   }
   ```

---

## 中级练习

### 练习 4: 支持连接池

#### 目标
- 复用 TCP 连接（HTTP Keep-Alive）
- 限制最大连接数
- 连接空闲超时

#### 实现思路

```java
public class ConnectionPool {
    private final Map<String, Queue<SocketChannel>> availableConnections;
    private final Map<String, Set<SocketChannel>> busyConnections;
    private final int maxConnectionsPerHost;
    private final long idleTimeoutMs;

    public SocketChannel acquire(String host, int port) {
        String key = host + ":" + port;
        Queue<SocketChannel> available = availableConnections.get(key);

        // 尝试复用现有连接
        if (available != null) {
            SocketChannel channel = available.poll();
            if (channel != null && channel.isConnected()) {
                busyConnections.get(key).add(channel);
                return channel;
            }
        }

        // 创建新连接
        if (getTotalConnections(key) < maxConnectionsPerHost) {
            SocketChannel newChannel = createConnection(host, port);
            busyConnections.computeIfAbsent(key, k -> new HashSet<>()).add(newChannel);
            return newChannel;
        }

        // 连接池满，等待或失败
        return null;
    }

    public void release(String host, int port, SocketChannel channel) {
        String key = host + ":" + port;
        busyConnections.get(key).remove(channel);
        availableConnections.computeIfAbsent(key, k -> new LinkedList<>()).add(channel);
    }
}
```

（完整实现留给读者练习）

---

### 练习 5: 添加请求取消功能

#### 目标
- 用户可以取消正在进行的请求
- 清理相关资源

#### 实现思路

```java
public class CancellableRequest {
    private final Request request;
    private final RequestFuture<HttpResult> future;
    private volatile boolean cancelled = false;

    public void cancel() {
        if (cancelled) return;
        cancelled = true;

        // 触发 Future 失败
        future.raise(new CancellationException());

        // 关闭网络连接
        closeConnection();
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
```

---

### 练习 6: 实现同步版本 API

#### 完整实现

```java
public class HttpClient {
    // ... 现有异步代码 ...

    /**
     * 🔥 同步 GET 请求
     */
    public HttpResult getSync(String host, int port, String path, long timeoutMs)
        throws IOException, InterruptedException {

        // 使用 CountDownLatch 等待异步操作完成
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<HttpResult> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        // 发起异步请求
        getAsync(host, port, path, new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                resultRef.set(result);
                exceptionRef.set(exception);
                latch.countDown();  // 释放等待
            }
        });

        // 阻塞等待完成
        long startTime = System.currentTimeMillis();
        while (latch.getCount() > 0 && (System.currentTimeMillis() - startTime) < timeoutMs) {
            poll(100);  // 驱动事件循环
        }

        // 检查结果
        Exception exception = exceptionRef.get();
        if (exception != null) {
            throw new IOException("Request failed", exception);
        }

        HttpResult result = resultRef.get();
        if (result == null) {
            throw new IOException("Request timeout");
        }

        return result;
    }
}
```

**使用示例**：

```java
// 同步方式
try {
    HttpResult result = client.getSync("httpbin.org", 80, "/get", 5000);
    System.out.println("Status: " + result.getStatusCode());
} catch (IOException e) {
    System.err.println("Request failed: " + e);
}

// 异步方式
client.getAsync("httpbin.org", 80, "/get", (result, exception) -> {
    if (exception == null) {
        System.out.println("Status: " + result.getStatusCode());
    }
});
```

#### 为什么这样设计？

1. **复用异步实现**
   ```java
   // 同步 API 内部调用异步 API
   // 避免代码重复
   getSync() {
       getAsync(..., callback);
       wait();
   }
   ```

2. **手动驱动事件循环**
   ```java
   // 不能简单地 latch.await()，因为没有独立的网络线程
   while (!done) {
       poll(100);  // 必须手动驱动
   }
   ```

---

## 高级练习

### 练习 7: 添加 HTTPS 支持

#### 实现思路

```java
public class HttpsNetworkClient extends NetworkClient {
    private final SSLContext sslContext;

    @Override
    protected SocketChannel createChannel(String host, int port) throws IOException {
        if (port == 443) {
            // 创建 SSL Socket
            SSLSocketFactory factory = sslContext.getSocketFactory();
            SSLSocket sslSocket = (SSLSocket) factory.createSocket(host, port);

            // 配置 SSL
            sslSocket.setUseClientMode(true);
            sslSocket.startHandshake();

            // 转换为 SocketChannel
            return sslSocket.getChannel();
        } else {
            return super.createChannel(host, port);
        }
    }
}
```

---

### 练习 8: 实现请求优先级队列

#### 完整实现

```java
public class PriorityNetworkClient extends NetworkClient {
    // 使用优先级队列替代普通队列
    private final PriorityQueue<PriorityRequest> unsent = new PriorityQueue<>(
        Comparator.comparingInt(PriorityRequest::getPriority).reversed()
    );

    public static class PriorityRequest {
        final Request request;
        final int priority;  // 数字越大优先级越高

        public PriorityRequest(Request request, int priority) {
            this.request = request;
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

    @Override
    protected void trySend() {
        // 按优先级发送
        while (!unsent.isEmpty()) {
            PriorityRequest pr = unsent.poll();
            send(pr.request);
        }
    }
}
```

**使用示例**：

```java
// 高优先级请求
client.sendAsync(request1, 10, callback);  // 优先级 10

// 低优先级请求
client.sendAsync(request2, 1, callback);   // 优先级 1

// request1 会先被发送
```

---

## 📚 总结

通过这些扩展练习，你学会了：

1. ✅ **POST 请求** - Builder 模式构建复杂对象
2. ✅ **超时处理** - 动态计算超时，及时清理资源
3. ✅ **重试机制** - 指数退避，智能判断可重试错误
4. ✅ **连接池** - 复用连接，提高性能
5. ✅ **请求取消** - 资源管理
6. ✅ **同步 API** - 复用异步实现，手动驱动事件循环
7. ✅ **HTTPS** - SSL/TLS 支持
8. ✅ **优先级队列** - 公平调度

这些都是**生产级 HTTP 客户端**必备的功能！

---

**继续探索 Kafka 源码，你会发现类似的设计无处不在！**
