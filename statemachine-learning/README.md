# State Machine Learning - Kafka异步状态机学习项目

## 学习目标

理解Kafka Consumer Group Rebalance的异步状态机设计：
- 四状态转换（UNJOINED → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE）
- ResponseHandler驱动状态转换
- compose()串联多阶段协议（JoinGroup → SyncGroup）
- 状态守卫防止并发问题

## 项目结构

```
src/main/java/com/example/
├── client/
│   ├── RequestFuture.java           # 已学习（来自mini-http-client）
│   ├── RequestFutureAdapter.java    # 已学习
│   └── RequestFutureListener.java   # 已学习
├── protocol/
│   ├── JoinGroupRequest.java        # Join请求
│   ├── JoinGroupResponse.java       # Join响应
│   ├── SyncGroupRequest.java        # Sync请求
│   └── SyncGroupResponse.java       # Sync响应
├── state/
│   ├── MemberState.java             # 状态枚举
│   ├── Generation.java              # Generation信息
│   ├── GroupCoordinator.java        # 核心状态机
│   └── MockNetworkClient.java       # 模拟网络层
└── test/
    ├── NormalFlowTest.java          # 正常流程测试
    └── ConcurrentTest.java          # 并发场景测试
```

## 核心流程

### 1. 状态定义
```java
enum MemberState {
    UNJOINED,              // 未加入
    PREPARING_REBALANCE,   // 已发送JoinGroup，等待响应
    COMPLETING_REBALANCE,  // 已收到JoinGroup响应，等待SyncGroup
    STABLE                 // 已完成，可以正常工作
}
```

### 2. 状态转换调用栈

```
[User Thread]
ensureActiveGroup()
├─ joinGroupIfNeeded()
│  └─ while(rejoinNeeded || joinFuture != null)
│     ├─ initiateJoinGroup()
│     │  ├─ state = PREPARING_REBALANCE        // Transition 1
│     │  ├─ joinFuture = sendJoinGroupRequest()
│     │  └─ joinFuture.compose(new JoinGroupResponseHandler())
│     │                         │
│     │                         └─ handle(response, future)
│     │                            ├─ state = COMPLETING_REBALANCE   // Transition 2
│     │                            └─ return onJoinLeader/Follower()
│     │                                   └─ sendSyncGroupRequest().compose(SyncGroupResponseHandler)
│     │                                                                │
│     │                                                                └─ handle(response, future)
│     │                                                                   ├─ state = STABLE    // Transition 3
│     │                                                                   └─ future.complete(assignment)
│     │
│     ├─ client.poll(joinFuture)  // 等待整个链路完成
│     └─ onJoinComplete(joinFuture.value())  // 用户回调
```

### 3. compose()串联两阶段协议

```java
RequestFuture<ByteBuffer> sendJoinGroupRequest() {
    return client.send(joinRequest)
        .compose(new JoinGroupResponseHandler());
        // JoinGroupResponseHandler.onSuccess() 内部调用 sendSyncGroupRequest()
}

// 在JoinGroupResponseHandler里：
public void onSuccess(JoinGroupResponse response, RequestFuture<ByteBuffer> future) {
    state = COMPLETING_REBALANCE;

    RequestFuture<ByteBuffer> syncFuture;
    if (response.isLeader()) {
        syncFuture = onJoinLeader(response);  // Leader分配partition
    } else {
        syncFuture = onJoinFollower();        // Follower等待分配
    }

    // 关键：把SyncGroup的结果传递给外层Future
    syncFuture.chain(future);  // 或用其他方式链接
}
```

## 练习任务

### 基础版（分支：2.6）
1. 实现MemberState枚举和Generation类
2. 实现JoinGroupRequest/Response（简化版，只包含memberId/generationId）
3. 实现GroupCoordinator的joinGroupIfNeeded()骨架（TODO标记核心逻辑）
4. 思考题：
   - Q1: 为什么需要generation守卫？
   - Q2: 如果JoinGroup成功但SyncGroup失败，状态如何恢复？
   - Q3: compose()如何实现两阶段future串联？

### 进阶版（分支：answer）
1. 完整实现JoinGroupResponseHandler和SyncGroupResponseHandler
2. 添加HeartbeatThread并发场景（清空generation）
3. 实现错误重试逻辑（UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS）
4. 测试用例：
   - 正常流程：UNJOINED → STABLE
   - 中断场景：SyncGroup失败后重新JoinGroup
   - 并发场景：Heartbeat清空generation，状态回退

## 关键问题

### Q1: 为什么状态转换在Handler里，而不是在发送请求后立即转换？

**答**: 防止网络失败导致状态不一致。只有收到成功响应才转换状态。

调用栈对比：
```
// 错误设计：
sendJoinGroupRequest()
├─ state = PREPARING_REBALANCE    // 请求可能失败！
└─ client.send(request)

// 正确设计：
sendJoinGroupRequest()
└─ client.send(request).compose(handler)
                              └─ handle(response)
                                 └─ if (success) state = PREPARING_REBALANCE
```

### Q2: compose()如何链接两阶段Future？

**方案1 - chain()**: 把子Future结果转发到父Future
```java
syncFuture.addListener(new RequestFutureListener<ByteBuffer>() {
    public void onSuccess(ByteBuffer value) {
        parentFuture.complete(value);  // 转发结果
    }
    public void onFailure(RuntimeException e) {
        parentFuture.raise(e);         // 转发错误
    }
});
```

**方案2 - 直接返回新Future**: 不用compose，直接替换
```java
// Kafka实际做法：onJoinLeader返回SyncFuture
RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse resp) {
    return sendSyncGroupRequest();  // 直接返回新future
}
```

### Q3: 如何防止HeartbeatThread并发修改generation？

**答**: synchronized + generation检查
```
[User Thread]                    [Heartbeat Thread]
joinGroupIfNeeded()
├─ synchronized(this) {
│   generationSnapshot = generation;
│   stateSnapshot = state;
│  }
├─ onJoinComplete()              // 不在锁内！可能被中断
│                                     resetGenerationOnHeartbeatExpire()
│                                     └─ synchronized(this) {
│                                         generation = NO_GENERATION;
│                                         state = UNJOINED;
│                                        }
└─ synchronized(this) {
    if (generation == generationSnapshot && state == STABLE) {
        // 检查通过，提交成功
    } else {
        // generation被清空，重新rebalance
        resetStateAndRejoin();
    }
   }
```

## 与mini-http-client的区别

| 特性 | mini-http-client | statemachine-learning |
|------|------------------|-------------------|
| 状态 | 无状态 | 四状态机 |
| Future层数 | 3层（Network → Protocol → Business） | 2层（JoinGroup → SyncGroup串联） |
| 并发 | HeartbeatThread读队列 | HeartbeatThread写状态（generation） |
| 设计核心 | 回调延迟执行 | 状态守卫 + 重入保护 |

## 分支说明

- **2.6**: 学习版，包含框架和TODO，供你实现
- **answer**: 完整答案 + 测试用例

## 如何使用

```bash
# 切换到学习分支
git checkout 2.6

# 查看TODO任务
grep -r "TODO" src/

# 实现后运行测试（答案分支提供）
git checkout answer
# 运行测试
javac src/main/java/com/example/*/*.java src/main/java/com/example/test/*.java
java com.example.test.NormalFlowTest
```
