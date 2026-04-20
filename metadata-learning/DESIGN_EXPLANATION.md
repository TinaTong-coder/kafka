# Kafka Metadata版本化缓存设计深度解析

## 核心问题：为什么需要版本号？

Producer发送消息时需要知道topic的partition信息，但这个信息可能不存在（新topic）或过期（partition扩容）。

如果每次send都同步请求metadata → 太慢
如果每次send都用缓存 → 可能过期

**Kafka的解决方案**：异步更新 + 版本号等待

## 设计对比

### 错误设计1：同步更新

```
[User Thread]
send(record)
├─ cluster = metadata.fetch()
├─ if (topic不存在) {
│   response = sendMetadataRequest();  // 阻塞网络IO
│   metadata.update(response);
│  }
└─ append to accumulator
```

**问题**：
- 阻塞用户线程
- 每个producer线程都发送请求 → 重复请求

---

### 错误设计2：轮询缓存

```
[User Thread]
send(record)
├─ while (metadata.fetch().partitionCountForTopic(topic) == null) {
│   sleep(10);  // 自旋等待
│  }
└─ append to accumulator
```

**问题**：
- CPU空转
- 无法保证Sender何时更新

---

### 正确设计：版本号 + wait/notify

```
[User Thread]                           [Sender Thread]
send(record)
├─ cluster = metadata.fetch()
├─ if (topic不存在) {
│   oldVersion = metadata.requestUpdate();  // 获取版本号
│   metadata.awaitUpdate(oldVersion);       // wait(释放CPU)
│  }                ←──────────────────────── notifyAll()
└─ append                                    │
                                             └─ metadata.update()
                                                └─ updateVersion++
```

**好处**：
- User Thread释放CPU（不自旋）
- 多个User Thread等待同一次更新（不重复请求）
- Sender完成后精准唤醒所有等待者

---

## 双版本号设计

### updateVersion：更新完成版本号

**用途**：User Thread等待metadata更新完成

```
[User Thread]
oldVersion = metadata.requestUpdate();  // oldVersion = 5
metadata.awaitUpdate(oldVersion=5)
└─ while (updateVersion == 5) {  // 版本没变，继续等待
    wait();
   }
   // updateVersion变成6，说明更新完成，退出循环
```

### requestVersion：请求版本号

**用途**：Sender检测是否有新的topic请求

```
[User Thread 1]               [User Thread 2]
metadata.add("topic-A")       metadata.add("topic-B")
└─ requestVersion++ (5→6)     └─ requestVersion++ (6→7)

                                   [Sender Thread]
                                   poll()
                                   ├─ snapshot = requestVersion;  // 7
                                   ├─ sendMetadataRequest(snapshot=7)
                                   │
                                   └─ handleResponse()
                                      └─ if (requestVersion > snapshot) {
                                          // 7 == 7, 没有新请求
                                         }

                                   [User Thread 3] (此时)
                                   metadata.add("topic-C")
                                   └─ requestVersion++ (7→8)

                                   [Sender Thread] (下次poll)
                                   └─ if (requestVersion > 7) {
                                       // 8 > 7, 有新请求，需要再次更新
                                       needPartialUpdate = true;
                                      }
```

---

## 完整调用栈

```
[Producer Thread 1]                     [Producer Thread 2]
send("topic-new", record)               send("topic-new", record)
├─ cluster = metadata.fetch()           ├─ cluster = metadata.fetch()
│  └─ synchronized { return cache.cluster(); }
│     (快速获取引用，释放锁)
│
├─ partition = cluster.partitionCountForTopic("topic-new")
│  └─ null (topic不存在)
│
├─ metadata.add("topic-new")            ├─ metadata.add("topic-new")
│  └─ synchronized {                    │  └─ synchronized {
│      if (topics.add("topic-new")) {   │      if (topics.add("topic-new")) {  // 已存在，不递增
│        requestVersion++; (5→6)        │        // 不执行
│      }                                │      }
│     }                                 │     }
│                                       │
├─ oldVersion = metadata.requestUpdate()├─ oldVersion = metadata.requestUpdate()
│  └─ synchronized {                    │  └─ synchronized {
│      needFullUpdate = true;           │      needFullUpdate = true;  // 已经是true
│      return updateVersion;  // 6      │      return updateVersion;  // 6 (相同)
│     }                                 │     }
│                                       │
├─ metadata.awaitUpdate(6, 60000)       ├─ metadata.awaitUpdate(6, 60000)
│  └─ synchronized {                    │  └─ synchronized {
│      while (updateVersion == 6) {     │      while (updateVersion == 6) {
│        wait(); ──────────────────┐    │        wait(); ──────────────────┐
│      }          ←────────────────┼────┼───┐   }          ←────────────────┼────┐
│     }           notifyAll()      │    │   │              notifyAll()      │    │
│                                  │    │   │                               │    │
└─ cluster = metadata.fetch()      │    │   └─ cluster = metadata.fetch()  │    │
   └─ partition = 10               │    │      └─ partition = 10            │    │
                                   │    │                                   │    │
                    [Sender Thread]│    │                                   │    │
                    run()          │    │                                   │    │
                    └─ poll()      │    │                                   │    │
                       ├─ timeToNextUpdate(now)                             │    │
                       │  └─ if (needFullUpdate) return 0;  // 立即更新      │    │
                       │                                                     │    │
                       ├─ requestVersionSnapshot = requestVersion();  // 6  │    │
                       ├─ sendMetadataRequest()                             │    │
                       │  └─ sleep(100);  // 模拟网络延迟                     │    │
                       │                                                     │    │
                       └─ handleMetadataResponse()                          │    │
                          └─ metadata.update(snapshot=6, response, false, now)  │
                             └─ synchronized {                              │    │
                                 updateVersion++;  // 6 → 7                 │    │
                                 cache = new MetadataCache(response);  // COW  │
                                 notifyAll(); ──────────────────────────────┼────┘
                                }
```

---

## COW (Copy-On-Write) 缓存

### 为什么需要不可变缓存？

**问题**：Producer读取metadata时，Sender正在更新，如何避免长时间持有锁？

**方案1 - 可变对象 + 长锁**（不好）:
```
class Metadata {
    private Map<String, Integer> topicPartitions;  // 可变

    public synchronized Map<String, Integer> fetch() {
        return topicPartitions;  // 不能释放锁，否则可能被修改
    }

    public synchronized void update(Map<String, Integer> newData) {
        topicPartitions.clear();
        topicPartitions.putAll(newData);  // 修改同一对象
    }
}
```
问题：fetch()必须持有锁直到使用完毕 → 阻塞update()

---

**方案2 - COW不可变对象**（Kafka实现）:
```
class Metadata {
    private MetadataCache cache;  // 不可变对象

    public synchronized Cluster fetch() {
        return cache.cluster();  // 获取引用后立即释放锁
    }  // 释放锁，但旧cache对象不会被修改

    public synchronized void update(Map<String, Integer> newData) {
        this.cache = new MetadataCache(newData);  // 创建新对象
        // 旧对象仍然被User Thread安全持有
    }
}
```

**调用栈**:
```
[User Thread]                          [Sender Thread]
fetch()
├─ synchronized {
│   ref = cache;  ───────┐ (引用旧对象)
│  }                     │             update()
│  (释放锁)              │             └─ synchronized {
│                        │                 cache = new MetadataCache();
├─ cluster = ref.cluster(); (读取旧对象)    }
└─ ... (继续使用旧对象)   │
                         │
                  (旧对象不会被修改)
```

**好处**：
- User Thread只需短暂持有锁（获取引用）
- 释放锁后安全读取（对象不可变）
- Sender更新不影响正在读取的线程

---

## 更新策略

### 1. 强制更新（requestUpdate）

```
metadata.requestUpdate()
└─ needFullUpdate = true;

[Sender]
poll()
└─ if (needFullUpdate) {
    sendMetadataRequest();  // 立即更新
   }
```

### 2. 惰性更新（过期时间）

```
[Sender]
poll()
└─ timeSinceLastSuccess = now - lastSuccessfulRefreshMs
   └─ if (timeSinceLastSuccess > metadataExpireMs) {
       sendMetadataRequest();  // 过期了，需要更新
      }
```

### 3. Backoff机制（避免频繁请求）

```
[Sender]
poll()
└─ timeSinceLastRefresh = now - lastRefreshMs
   └─ if (timeSinceLastRefresh < refreshBackoffMs) {
       return;  // 间隔太短，跳过本次更新
      }
```

### 综合决策：timeToNextUpdate()

```java
public long timeToNextUpdate(long now) {
    // 1. 强制更新：立即执行
    if (needFullUpdate || needPartialUpdate) {
        return 0;
    }

    // 2. 计算过期时间
    long timeToExpire = (lastSuccessfulRefreshMs + metadataExpireMs) - now;
    if (timeToExpire <= 0) {
        return 0;  // 已过期
    }

    // 3. 计算backoff时间
    long timeToBackoff = (lastRefreshMs + refreshBackoffMs) - now;

    // 4. 返回max(过期时间, backoff时间)
    return Math.max(timeToExpire, timeToBackoff);
}
```

---

## 关键问题

### Q1: 如果多个Producer同时requestUpdate，会发送多次请求吗？

**答**：不会。Sender只检查flag，不关心有多少个Producer在等待。

```
[Producer 1]                [Producer 2]
requestUpdate()             requestUpdate()
└─ needFullUpdate = true;   └─ needFullUpdate = true;  // 已经是true

                                [Sender]
                                poll()
                                └─ if (needFullUpdate) {
                                    sendMetadataRequest();  // 只发送一次
                                    needFullUpdate = false;
                                   }
```

### Q2: awaitUpdate为什么要传入oldVersion，而不是直接判断needFullUpdate？

**答**：因为needFullUpdate会被Sender重置为false，但updateVersion不会回退。

**错误设计**:
```
awaitUpdate()
└─ while (needFullUpdate) {  // 错误！
    wait();
   }
```
问题：Sender更新后，needFullUpdate=false，但User Thread还没醒来，再次检查时直接退出 → 可能拿到旧数据。

**正确设计**:
```
awaitUpdate(oldVersion)
└─ while (updateVersion <= oldVersion) {  // 只要版本号变化就退出
    wait();
   }
```
保证：updateVersion单调递增，退出循环时一定是新数据。

### Q3: 为什么Sender需要snapshot requestVersion？

**答**：检测更新期间是否有新的topic添加。

```
[Sender]
poll()
├─ snapshot = requestVersion;  // 7
├─ sendMetadataRequest()
│  └─ sleep(100);  // 网络延迟
│                      [User Thread] (此时)
│                      metadata.add("new-topic")
│                      └─ requestVersion++ (7→8)
│
└─ handleResponse()
   └─ metadata.update(snapshot=7, ...)
      └─ if (requestVersion > snapshot) {  // 8 > 7
          needPartialUpdate = true;  // 需要再次更新
         }
```

---

## 学习价值总结

1. **版本号等待模式**：比自旋等待更高效
2. **双版本号**：updateVersion（完成）vs requestVersion（请求）
3. **COW缓存**：不可变对象 → 短锁 + 无竞争读取
4. **wait/notify协作**：跨线程同步的经典模式
5. **更新策略**：强制 + 惰性 + backoff

掌握这个设计后，你就理解了Kafka Producer的核心性能优化之一！
