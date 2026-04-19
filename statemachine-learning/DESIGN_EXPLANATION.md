# Kafka异步状态机设计深度解析

## 核心问题：为什么需要异步状态机？

Consumer Group Rebalance是一个**两阶段协议**：
1. **JoinGroup**: 所有member向coordinator报到，coordinator选出leader
2. **SyncGroup**: Leader计算分配方案，coordinator分发给所有member

如果用同步方式实现会很简单，但Kafka选择异步实现，为什么？

## 三个关键设计点

### 1. 状态转换由ResponseHandler触发

**错误设计**:
```
sendJoinGroupRequest() [User Thread]
├─ state = PREPARING_REBALANCE    // 立即改状态
└─ client.send(request)            // 请求可能失败！
```

**问题**: 如果网络失败，状态已改但请求未发出，状态不一致。

**正确设计**:
```
sendJoinGroupRequest() [User Thread]
└─ client.send(request).compose(JoinGroupResponseHandler)
                                      └─ handle(response)
                                         └─ if (success) state = PREPARING_REBALANCE
```

**调用栈**:
```
[User Thread]
initiateJoinGroup()
├─ state = PREPARING_REBALANCE                          // 预设状态（乐观）
└─ sendJoinGroupRequest()
   └─ client.send()
      └─ RequestFutureCompletionHandler.onComplete()
         └─ JoinGroupResponseAdapter.onSuccess()
            ├─ if (state != PREPARING_REBALANCE) FAIL   // 守卫检查
            └─ state = COMPLETING_REBALANCE             // 转换成功
```

**好处**:
- 网络失败时状态不改变
- Handler可以检查state守卫（防止并发修改）

---

### 2. compose()串联两阶段协议

**问题**: JoinGroup成功后需要立即发SyncGroup，如何串联？

**方案对比**:

**方案A - 用户代码手动串联**（不好）:
```
RequestFuture<JoinGroupResponse> joinFuture = sendJoinGroupRequest();
joinFuture.addListener(response -> {
    if (response.success) {
        RequestFuture<SyncGroupResponse> syncFuture = sendSyncGroupRequest();
        syncFuture.addListener(syncResponse -> {
            // 最终结果
        });
    }
});
```
问题：嵌套回调，且外层Future无法直接拿到最终结果。

**方案B - compose()自动串联**（Kafka实现）:
```
RequestFuture<String> sendJoinGroupRequest() {
    RequestFuture<JoinGroupResponse> joinFuture = client.send(joinRequest);

    return joinFuture.compose(new RequestFutureAdapter<JoinGroupResponse, String>() {
        @Override
        public void onSuccess(JoinGroupResponse resp, RequestFuture<String> outerFuture) {
            // 处理JoinGroup响应
            state = COMPLETING_REBALANCE;

            // 发起SyncGroup
            RequestFuture<String> syncFuture = sendSyncGroupRequest();

            // 关键：把syncFuture的结果chain到outerFuture
            syncFuture.addListener(new RequestFutureListener<String>() {
                public void onSuccess(String assignment) {
                    outerFuture.complete(assignment);  // 转发到外层
                }
                public void onFailure(RuntimeException e) {
                    outerFuture.raise(e);
                }
            });
        }
    });
}
```

**调用栈**:
```
[User Thread]
sendJoinGroupRequest()
└─ returns RequestFuture<String>  ←─────────────────────────┐
                                                              │
[Network Events]                                              │
JoinGroupResponse arrives                                     │
└─ JoinGroupResponseAdapter.onSuccess()                       │
   ├─ state = COMPLETING_REBALANCE                            │
   ├─ syncFuture = sendSyncGroupRequest()                     │
   │                                                           │
   └─ chainFuture(syncFuture, outerFuture) ──────────────────┘
      └─ SyncGroupResponse arrives
         └─ outerFuture.complete(assignment)
```

**好处**:
- 外层代码无需关心中间步骤，直接等待最终结果
- 类型转换：`RequestFuture<JoinGroupResponse>` → `RequestFuture<String>`

---

### 3. Generation守卫防止并发修改

**场景**: HeartbeatThread和User Thread并发访问

```
[User Thread]                          [Heartbeat Thread]
joinGroupIfNeeded()
├─ synchronized {
│   generationSnapshot = generation;   // gen1
│   stateSnapshot = state;             // STABLE
│  }
├─ onJoinComplete(assignment)
│  ├─ commitOffsets()                        [Heartbeat超时]
│  └─ ... (10秒)                             └─ synchronized {
│                                                 generation = NO_GENERATION;
│                                                 state = UNJOINED;
│                                                }
└─ synchronized {
    if (generation == generationSnapshot) {
        // gen1 == NO_GENERATION? NO!
        // 检测到不一致，重新rebalance
        resetStateAndRejoin();
    }
   }
```

**调用栈**:
```
[User Thread]
joinGroupIfNeeded()
├─ initiateJoinGroup()
│  └─ joinFuture = sendJoinGroupRequest()
│
├─ wait for joinFuture                    [Heartbeat Thread]
│                                          pollHeartbeat()
│                                          └─ heartbeatFailed()
│                                             └─ synchronized {
│                                                 generation = NO_GENERATION;
│                                                }
├─ if (joinFuture.succeeded())
│  ├─ synchronized {
│  │   generationSnapshot = generation;    // Snapshot!
│  │   stateSnapshot = state;
│  │  }
│  │
│  ├─ onJoinComplete()  // 不在锁内，可能很慢
│  │
│  └─ synchronized {
│      if (generation != generationSnapshot) {  // 检测到修改
│          resetStateAndRejoin();               // 重新rebalance
│      }
│     }
```

**为什么不在整个rebalance期间持有锁？**
- `onJoinComplete()`可能执行用户回调（commitOffsets），很慢
- 持有锁会阻塞HeartbeatThread发送心跳 → session超时 → 被踢出group

**好处**:
- 允许HeartbeatThread并发修改
- 通过snapshot检测修改
- 失败后优雅地重试

---

## 完整调用栈（带线程标注）

```
[User Thread]
ensureActiveGroup()
└─ joinGroupIfNeeded()
   └─ while(rejoinNeeded || joinFuture != null)
      ├─ initiateJoinGroup()
      │  ├─ synchronized { state = PREPARING_REBALANCE; }
      │  └─ joinFuture = sendJoinGroupRequest()
      │     └─ client.send(JoinGroupRequest)
      │        └─ .compose(JoinGroupResponseAdapter)
      │
      ├─ client.poll(joinFuture)  // 等待网络事件
      │  └─ [Network Event]
      │     └─ JoinGroupResponse arrives
      │        └─ JoinGroupResponseAdapter.onSuccess()
      │           ├─ synchronized {
      │           │   state = COMPLETING_REBALANCE;
      │           │   generation = new Generation(...);
      │           │  }
      │           ├─ syncFuture = sendSyncGroupRequest()
      │           │  └─ client.send(SyncGroupRequest)
      │           │     └─ .compose(SyncGroupResponseAdapter)
      │           │        └─ [Network Event]
      │           │           └─ SyncGroupResponse arrives
      │           │              └─ SyncGroupResponseAdapter.onSuccess()
      │           │                 └─ synchronized {
      │           │                     state = STABLE;
      │           │                     rejoinNeeded = false;
      │           │                    }
      │           └─ chainFuture(syncFuture, outerFuture)
      │
      ├─ synchronized {
      │   generationSnapshot = generation;
      │   stateSnapshot = state;
      │  }
      │
      ├─ onJoinComplete(assignment)  // 不在锁内
      │                                    [Heartbeat Thread - 并发]
      │                                    pollHeartbeat()
      │                                    └─ if (sessionExpired)
      │                                       └─ synchronized {
      │                                           generation = NO_GENERATION;
      │                                           state = UNJOINED;
      │                                          }
      └─ synchronized {
          if (generation == generationSnapshot && state == STABLE) {
              // Success
          } else {
              // Generation被清空，重新rebalance
              resetStateAndRejoin();
          }
         }
```

---

## 与mini-http-client的对比

| 维度 | mini-http-client | statemachine-learning |
|------|------------------|-------------------|
| **状态** | 无状态 | 四状态机（UNJOINED→PREPARING→COMPLETING→STABLE） |
| **Future层数** | 3层（Network→Protocol→Business） | 2层（JoinGroup→SyncGroup） |
| **compose用途** | 类型转换（Response→HttpResult→Business） | **协议串联**（JoinGroup→SyncGroup） |
| **并发** | HeartbeatThread**读**队列（ConcurrentLinkedQueue） | HeartbeatThread**写**状态（需要Generation守卫） |
| **核心难点** | 回调延迟执行（invokeCompletedCallbacks） | 状态守卫 + 两阶段串联 |

---

## 学习价值总结

1. **状态机驱动**：ResponseHandler改状态，不在发送时改
2. **compose()串联协议**：不只是类型转换，还能串联多阶段异步操作
3. **Generation守卫**：snapshot模式检测并发修改
4. **锁粒度控制**：用户回调不在锁内执行

这种设计比mini-http-client更复杂，因为：
- 状态转换有严格顺序要求
- 两阶段协议需要串联
- HeartbeatThread会并发修改状态

掌握这个模式后，你就理解了Kafka Consumer Group的核心设计！
