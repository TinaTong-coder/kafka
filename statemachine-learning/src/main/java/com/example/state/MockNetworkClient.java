package com.example.state;

import com.example.client.RequestFuture;
import com.example.protocol.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模拟网络客户端，用于测试状态机逻辑
 *
 * 简化了真实的NetworkClient，只模拟核心行为：
 * - 异步发送请求
 * - 模拟coordinator响应
 * - 支持成功/失败场景
 */
public class MockNetworkClient {
    private final AtomicInteger generationIdCounter = new AtomicInteger(0);
    private final Map<String, String> memberIds = new HashMap<>();  // groupId -> memberId
    private String currentLeaderId = null;

    /**
     * 发送JoinGroup请求
     * 模拟coordinator行为：
     * - 首次加入：分配新memberId
     * - 重新加入：复用memberId
     * - 第一个member成为leader
     */
    public RequestFuture<JoinGroupResponse> sendJoinGroupRequest(JoinGroupRequest request) {
        RequestFuture<JoinGroupResponse> future = new RequestFuture<>();

        // 模拟异步处理
        String memberId = request.memberId;
        if (memberId == null || memberId.isEmpty()) {
            // 首次加入，分配新ID
            memberId = "member-" + System.currentTimeMillis();
            memberIds.put(request.groupId, memberId);
        }

        // 如果是第一个member，成为leader
        if (currentLeaderId == null) {
            currentLeaderId = memberId;
        }

        int generationId = generationIdCounter.incrementAndGet();

        JoinGroupResponse response = new JoinGroupResponse(
            JoinGroupResponse.ErrorCode.NONE,
            generationId,
            memberId,
            currentLeaderId,
            "range"
        );

        // 模拟网络延迟后完成
        future.complete(response);
        return future;
    }

    /**
     * 发送SyncGroup请求
     */
    public RequestFuture<SyncGroupResponse> sendSyncGroupRequest(SyncGroupRequest request) {
        RequestFuture<SyncGroupResponse> future = new RequestFuture<>();

        // Leader会携带分配结果，直接返回
        // Follower需要等待leader的分配（这里简化直接返回）
        String assignment;
        if (request.groupAssignment.isEmpty()) {
            // Follower
            assignment = "partition-0,partition-1";  // 模拟分配
        } else {
            // Leader
            assignment = request.groupAssignment.get(request.memberId);
        }

        SyncGroupResponse response = new SyncGroupResponse(
            SyncGroupResponse.ErrorCode.NONE,
            assignment
        );

        future.complete(response);
        return future;
    }

    /**
     * 模拟coordinator错误响应
     */
    public RequestFuture<JoinGroupResponse> sendJoinGroupRequestWithError(
            JoinGroupRequest request,
            JoinGroupResponse.ErrorCode errorCode) {
        RequestFuture<JoinGroupResponse> future = new RequestFuture<>();
        JoinGroupResponse response = new JoinGroupResponse(
            errorCode, -1, "", "", ""
        );
        future.complete(response);
        return future;
    }

    /**
     * 重置coordinator状态（模拟coordinator重启）
     */
    public void reset() {
        generationIdCounter.set(0);
        memberIds.clear();
        currentLeaderId = null;
    }
}
