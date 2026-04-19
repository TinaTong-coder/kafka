package com.example.state;

import com.example.client.RequestFuture;
import com.example.client.RequestFutureAdapter;
import com.example.client.RequestFutureListener;
import com.example.protocol.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 核心状态机 - 管理Consumer Group成员的rebalance流程
 *
 * 关键设计：
 * 1. 状态转换由ResponseHandler触发（不在send时改状态）
 * 2. compose()串联JoinGroup和SyncGroup两阶段
 * 3. Generation守卫防止HeartbeatThread并发修改
 * 4. joinFuture重入保护（避免重复发送请求）
 */
public class GroupCoordinator {
    private final String groupId;
    private final MockNetworkClient client;

    // 状态机核心字段
    private MemberState state = MemberState.UNJOINED;
    private Generation generation = Generation.NO_GENERATION;
    private boolean rejoinNeeded = true;
    private RequestFuture<String> joinFuture = null;  // String表示assignment

    public GroupCoordinator(String groupId, MockNetworkClient client) {
        this.groupId = groupId;
        this.client = client;
    }

    /**
     * 确保group处于active状态（STABLE）
     *
     * 调用栈：
     * ensureActiveGroup()
     * └─ joinGroupIfNeeded()
     *    ├─ initiateJoinGroup()  [state: UNJOINED → PREPARING_REBALANCE]
     *    │  └─ sendJoinGroupRequest().compose(JoinGroupResponseHandler)
     *    │                                        └─ [state: PREPARING_REBALANCE → COMPLETING_REBALANCE]
     *    │                                           └─ sendSyncGroupRequest().compose(SyncGroupResponseHandler)
     *    │                                                                              └─ [state: COMPLETING_REBALANCE → STABLE]
     *    ├─ waitForFuture(joinFuture)  // 同步等待完成
     *    └─ onJoinComplete(assignment)
     */
    public synchronized void ensureActiveGroup() {
        joinGroupIfNeeded();
    }

    /**
     * 如果需要rejoin，则执行join流程
     */
    private void joinGroupIfNeeded() {
        while (rejoinNeeded || joinFuture != null) {
            // 1. 发起join（或复用已有的joinFuture）
            RequestFuture<String> future = initiateJoinGroup();

            // 2. 等待完成（简化版，真实Kafka用client.poll()）
            if (!future.isDone()) {
                System.out.println("[WARN] Future not done yet");
                return;
            }

            // 3. 检查结果
            if (future.succeeded()) {
                // 成功：执行用户回调
                Generation generationSnapshot;
                MemberState stateSnapshot;

                synchronized (this) {
                    generationSnapshot = this.generation;
                    stateSnapshot = this.state;
                }

                // 重要：onJoinComplete不在锁内执行（可能很慢）
                // 期间HeartbeatThread可能清空generation
                if (generationSnapshot != Generation.NO_GENERATION && stateSnapshot == MemberState.STABLE) {
                    onJoinComplete(generationSnapshot, future.value());
                    resetJoinGroupFuture();
                } else {
                    // Generation被HeartbeatThread清空了，需要重新rebalance
                    System.out.println("[WARN] Generation cleared by heartbeat, retry rebalance");
                    resetStateAndRejoin();
                    resetJoinGroupFuture();
                }
            } else {
                // 失败：根据错误类型决定是否重试
                RuntimeException exception = future.exception();
                System.out.println("[ERROR] Rebalance failed: " + exception.getMessage());
                resetJoinGroupFuture();

                // 某些错误直接重试，某些需要等待
                // 这里简化为直接重试
                resetStateAndRejoin();
            }
        }
    }

    /**
     * 发起JoinGroup流程（如果已有joinFuture则复用）
     *
     * 关键：状态转换发生在Handler里，不在这里
     */
    private synchronized RequestFuture<String> initiateJoinGroup() {
        if (joinFuture == null) {
            // Transition 1: UNJOINED → PREPARING_REBALANCE
            state = MemberState.PREPARING_REBALANCE;
            System.out.println("[STATE] " + MemberState.UNJOINED + " → " + MemberState.PREPARING_REBALANCE);

            joinFuture = sendJoinGroupRequest();
        }
        return joinFuture;
    }

    /**
     * 发送JoinGroup请求，返回最终的assignment (String)
     *
     * 关键设计：compose()串联两阶段
     * JoinGroupResponse → SyncGroupResponse → String (assignment)
     */
    private RequestFuture<String> sendJoinGroupRequest() {
        System.out.println("[SEND] JoinGroupRequest");

        JoinGroupRequest request = new JoinGroupRequest(groupId, generation.memberId);
        RequestFuture<JoinGroupResponse> joinResponseFuture = client.sendJoinGroupRequest(request);

        // compose: JoinGroupResponse → String
        // Adapter的onSuccess会调用sendSyncGroupRequest，并把结果传递给外层Future
        return joinResponseFuture.compose(new JoinGroupResponseAdapter());
    }

    /**
     * JoinGroupResponse处理器
     *
     * 职责：
     * 1. 检查JoinGroup是否成功
     * 2. 更新状态：PREPARING_REBALANCE → COMPLETING_REBALANCE
     * 3. 发起SyncGroup请求
     * 4. 把SyncGroup的结果传递给外层Future
     */
    private class JoinGroupResponseAdapter extends RequestFutureAdapter<JoinGroupResponse, String> {
        @Override
        public void onSuccess(JoinGroupResponse response, RequestFuture<String> future) {
            synchronized (GroupCoordinator.this) {
                if (!response.isSuccess()) {
                    future.raise(new RuntimeException("JoinGroup failed: " + response.error));
                    return;
                }

                // Transition 2: PREPARING_REBALANCE → COMPLETING_REBALANCE
                if (state != MemberState.PREPARING_REBALANCE) {
                    // 状态不匹配，可能被heartbeat中断了
                    future.raise(new RuntimeException("State mismatch, expected PREPARING_REBALANCE but got " + state));
                    return;
                }

                state = MemberState.COMPLETING_REBALANCE;
                System.out.println("[STATE] " + MemberState.PREPARING_REBALANCE + " → " + MemberState.COMPLETING_REBALANCE);

                // 更新generation
                generation = new Generation(
                    response.generationId,
                    response.memberId,
                    response.protocolName
                );
                System.out.println("[RECV] JoinGroupResponse: " + response);
                System.out.println("[INFO] Generation updated to: " + generation);

                // 发起SyncGroup（Leader需要执行分配逻辑）
                RequestFuture<String> syncFuture;
                if (response.isLeader()) {
                    syncFuture = onJoinLeader(response);
                } else {
                    syncFuture = onJoinFollower();
                }

                // 关键：把syncFuture的结果chain到外层future
                // 这样外层等待的future会在SyncGroup完成后得到结果
                chainFuture(syncFuture, future);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<String> future) {
            future.raise(e);
        }
    }

    /**
     * Leader处理JoinGroup响应：需要执行partition分配
     */
    private RequestFuture<String> onJoinLeader(JoinGroupResponse joinResponse) {
        System.out.println("[INFO] I am the leader, performing assignment");

        // 简化版分配逻辑：固定分配
        Map<String, String> assignment = new HashMap<>();
        assignment.put(generation.memberId, "partition-0,partition-1,partition-2");

        return sendSyncGroupRequest(assignment);
    }

    /**
     * Follower处理JoinGroup响应：等待leader分配
     */
    private RequestFuture<String> onJoinFollower() {
        System.out.println("[INFO] I am a follower, waiting for assignment");
        return sendSyncGroupRequest(new HashMap<>());  // 空分配
    }

    /**
     * 发送SyncGroup请求
     */
    private RequestFuture<String> sendSyncGroupRequest(Map<String, String> assignment) {
        System.out.println("[SEND] SyncGroupRequest");

        SyncGroupRequest request = new SyncGroupRequest(
            groupId,
            generation.generationId,
            generation.memberId,
            assignment
        );

        RequestFuture<SyncGroupResponse> syncResponseFuture = client.sendSyncGroupRequest(request);

        // compose: SyncGroupResponse → String
        return syncResponseFuture.compose(new SyncGroupResponseAdapter());
    }

    /**
     * SyncGroupResponse处理器
     *
     * 职责：
     * 1. 检查SyncGroup是否成功
     * 2. 更新状态：COMPLETING_REBALANCE → STABLE
     * 3. 标记rejoinNeeded = false
     */
    private class SyncGroupResponseAdapter extends RequestFutureAdapter<SyncGroupResponse, String> {
        @Override
        public void onSuccess(SyncGroupResponse response, RequestFuture<String> future) {
            synchronized (GroupCoordinator.this) {
                if (!response.isSuccess()) {
                    future.raise(new RuntimeException("SyncGroup failed: " + response.error));
                    return;
                }

                // Transition 3: COMPLETING_REBALANCE → STABLE
                if (generation == Generation.NO_GENERATION || state != MemberState.COMPLETING_REBALANCE) {
                    // Generation被清空了（HeartbeatThread），需要重新rebalance
                    future.raise(new RuntimeException("Generation cleared before SyncGroup response"));
                    return;
                }

                state = MemberState.STABLE;
                System.out.println("[STATE] " + MemberState.COMPLETING_REBALANCE + " → " + MemberState.STABLE);
                System.out.println("[RECV] SyncGroupResponse: " + response);

                rejoinNeeded = false;

                // 完成Future，返回assignment
                future.complete(response.memberAssignment);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<String> future) {
            requestRejoin();
            future.raise(e);
        }
    }

    /**
     * Chain两个Future：把source的结果转发到target
     */
    private <T> void chainFuture(RequestFuture<T> source, RequestFuture<T> target) {
        source.addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                target.complete(value);
            }

            @Override
            public void onFailure(RuntimeException e) {
                target.raise(e);
            }
        });
    }

    /**
     * Rebalance完成后的用户回调
     */
    private void onJoinComplete(Generation generation, String assignment) {
        System.out.println("[SUCCESS] Rebalance complete!");
        System.out.println("  Generation: " + generation);
        System.out.println("  Assignment: " + assignment);
    }

    /**
     * 请求rejoin（由错误处理或HeartbeatThread调用）
     */
    public synchronized void requestRejoin() {
        rejoinNeeded = true;
    }

    /**
     * 重置状态并请求rejoin
     */
    private synchronized void resetStateAndRejoin() {
        state = MemberState.UNJOINED;
        generation = Generation.NO_GENERATION;
        rejoinNeeded = true;
    }

    /**
     * 清空joinFuture（允许下次重新发起）
     */
    private synchronized void resetJoinGroupFuture() {
        joinFuture = null;
    }

    /**
     * 模拟HeartbeatThread清空generation（用于测试并发场景）
     */
    public synchronized void simulateHeartbeatFailure() {
        System.out.println("[HEARTBEAT] Heartbeat failed, clearing generation");
        generation = Generation.NO_GENERATION;
        state = MemberState.UNJOINED;
        rejoinNeeded = true;
    }

    // Getters for testing
    public synchronized MemberState getState() {
        return state;
    }

    public synchronized Generation getGeneration() {
        return generation;
    }
}
