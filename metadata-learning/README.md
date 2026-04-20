# Metadata Learning - Kafka版本化缓存设计

## 学习目标

理解Kafka Metadata的版本化缓存设计：
- 双版本号（updateVersion vs requestVersion）
- 跨线程协作（User Thread等待Sender Thread更新）
- Copy-On-Write缓存（MetadataCache不可变）
- 惰性更新 + 强制更新策略

## 项目结构

```
src/main/java/com/example/
├── cache/
│   ├── MetadataCache.java       # 不可变缓存（COW模式）
│   └── Cluster.java             # 集群信息（简化版）
├── coordinator/
│   └── Metadata.java            # 核心元数据管理器
├── producer/
│   ├── MockProducer.java        # 模拟Producer
│   └── MockSender.java          # 模拟Sender线程
└── test/
    ├── VersionControlTest.java  # 版本号测试
    └── ConcurrentUpdateTest.java # 并发更新测试
```

## 核心设计

### 1. 双版本号

```java
class Metadata {
    private int updateVersion;   // 每次收到响应时++
    private int requestVersion;  // 每次添加新topic时++
}
```

**为什么需要两个版本号？**

| 版本号 | 何时递增 | 用途 |
|-------|---------|------|
| updateVersion | 收到MetadataResponse | User Thread等待metadata更新 |
| requestVersion | 添加新topic | Sender判断是否有新topic需要拉取 |

### 2. 调用栈

```
[Producer Thread]                      [Sender Thread]
send("topic-A", record)
├─ cluster = metadata.fetch()
├─ partition = null?  (topic-A不存在)
│
├─ oldVersion = metadata.requestUpdate()
│  └─ synchronized {
│      needFullUpdate = true;
│      return updateVersion;  // 5
│     }
│                                      poll()
├─ metadata.awaitUpdate(oldVersion=5)  ├─ timeToNextUpdate() <= 0?
│  └─ synchronized {                   │  └─ needFullUpdate == true
│      while (updateVersion == 5) {    │
│        wait(); ──────────────────┐   ├─ sendMetadataRequest()
│      }         ←─────────────────┼───┤
│     }          notifyAll()       │   │
│                                  │   └─ handleMetadataResponse()
└─ cluster = metadata.fetch()      │      └─ synchronized {
   └─ partition found!             │          updateVersion++;  // 5 → 6
                                   │          cache = new MetadataCache(resp);
                         (退出循环) ←┘          notifyAll();
                         updateVersion=6       needFullUpdate = false;
                                              }
```

### 3. Copy-On-Write缓存

**问题**: Producer读取metadata时，Sender正在更新metadata，如何避免加锁？

**方案**: 不可变的MetadataCache

```java
class Metadata {
    private MetadataCache cache;  // 不可变对象

    public synchronized Cluster fetch() {
        return cache.cluster();  // 返回当前快照
    }

    public synchronized void update(MetadataResponse resp) {
        // 创建新对象，不修改旧的
        this.cache = new MetadataCache(resp);
        this.updateVersion++;
        notifyAll();
    }
}
```

**好处**:
- Producer的`fetch()`只需要短时间持有锁（获取引用）
- 释放锁后可以安全读取cache（不会被修改）
- Sender更新时创建新对象，不影响旧对象

### 4. 更新策略

| 场景 | 触发条件 | 更新类型 |
|------|---------|---------|
| 惰性更新 | `now - lastSuccessfulRefreshMs > metadataExpireMs` | Full Update |
| 强制更新 | `metadata.requestUpdate()` | Full Update |
| 增量更新 | `metadata.requestUpdateForNewTopics()` | Partial Update |

```
[Sender Thread]
poll()
└─ timeToNextUpdate(now)
   ├─ if (needFullUpdate) return 0;  // 立即更新
   │
   ├─ timeToExpire = lastSuccessfulRefreshMs + metadataExpireMs - now
   │  └─ if (timeToExpire <= 0) return 0;  // 过期，需要更新
   │
   └─ timeToBackoff = lastRefreshMs + refreshBackoffMs - now
      └─ return max(timeToExpire, timeToBackoff);  // 距离下次更新的时间
```

## 关键问题

### Q1: 为什么User Thread不直接发送MetadataRequest？

**答**: 避免阻塞用户代码，提高吞吐量。

**对比**:
```
// 同步设计（不好）:
send(record) [User Thread]
├─ sendMetadataRequest()  // 阻塞等待网络IO
├─ wait for response      // 浪费CPU
└─ append to accumulator

// 异步设计（Kafka实现）:
send(record) [User Thread]
├─ metadata.requestUpdate()  // 只设置flag
├─ metadata.awaitUpdate()    // wait()释放CPU
└─ append to accumulator

                                [Sender Thread]
                                poll()
                                └─ sendMetadataRequest()  // 专门的线程处理IO
```

---

### Q2: requestVersion有什么用？

**答**: 检测是否有新的topic请求，决定是否发送Partial Update。

**场景**:
```
[User Thread 1]                    [User Thread 2]
metadata.add("topic-A")
└─ requestVersion++ (5 → 6)        metadata.add("topic-B")
                                   └─ requestVersion++ (6 → 7)

                                        [Sender Thread]
                                        poll()
                                        ├─ snapshot = requestVersion;  // 7
                                        ├─ sendMetadataRequest(snapshot=7)
                                        │
                                        └─ handleMetadataResponse()
                                           └─ if (requestVersion > snapshot) {
                                               // 有新的topic添加，需要再次更新
                                               needPartialUpdate = true;
                                              }
```

---

### Q3: 为什么需要backoff时间？

**答**: 避免频繁请求broker，减轻broker压力。

```
[Sender Thread]
poll()
├─ sendMetadataRequest()
│  └─ lastRefreshMs = now;  // 记录发送时间
│
└─ timeToAllowUpdate(now)
   └─ return max(0, lastRefreshMs + refreshBackoffMs - now);
      // 至少等待refreshBackoffMs才能再次请求
```

## 练习任务

### 基础版（分支：2.6）
1. 实现Metadata的双版本号机制
2. 实现fetch()和update()的同步
3. 实现awaitUpdate()的wait/notify
4. 思考题：
   - Q1: 为什么awaitUpdate传入oldVersion而不是直接判断needFullUpdate？
   - Q2: 如果多个User Thread同时requestUpdate，会发生什么？
   - Q3: MetadataCache为什么是不可变的？

### 进阶版（分支：answer）
1. 实现requestVersion的增量更新逻辑
2. 实现timeToNextUpdate()的过期和backoff判断
3. 模拟Producer和Sender的并发交互
4. 测试用例：
   - 正常更新：User等待Sender更新完成
   - 超时场景：awaitUpdate超时抛出TimeoutException
   - 并发场景：多个User同时等待更新

## 与statemachine的区别

| 维度 | statemachine-learning | metadata-learning |
|------|-------------------|-------------------|
| **核心问题** | 异步协议状态转换 | 跨线程缓存更新 |
| **并发模型** | HeartbeatThread修改状态 | Sender更新缓存，User等待 |
| **同步机制** | Generation snapshot | Version + wait/notify |
| **设计模式** | 状态机 + Future链 | 版本化 + COW缓存 |

## 如何使用

```bash
# 编译
javac src/main/java/com/example/*/*.java src/main/java/com/example/test/*.java

# 运行测试
java com.example.test.VersionControlTest
java com.example.test.ConcurrentUpdateTest
```
