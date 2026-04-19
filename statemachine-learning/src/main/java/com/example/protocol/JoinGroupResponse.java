package com.example.protocol;

/**
 * JoinGroup响应 - Coordinator返回的加入结果
 *
 * 包含：
 * - generationId: 新的世代ID
 * - memberId: 分配给这个member的ID
 * - leaderId: 当前generation的leader（负责分配partition）
 * - protocolName: 使用的分配协议
 * - error: 错误码（NONE表示成功）
 */
public class JoinGroupResponse {
    public enum ErrorCode {
        NONE,
        UNKNOWN_MEMBER_ID,      // memberId不存在，需要重新加入
        REBALANCE_IN_PROGRESS,  // 另一个rebalance正在进行
        ILLEGAL_GENERATION      // generation已过期
    }

    public final ErrorCode error;
    public final int generationId;
    public final String memberId;
    public final String leaderId;
    public final String protocolName;

    public JoinGroupResponse(ErrorCode error, int generationId, String memberId,
                            String leaderId, String protocolName) {
        this.error = error;
        this.generationId = generationId;
        this.memberId = memberId;
        this.leaderId = leaderId;
        this.protocolName = protocolName;
    }

    public boolean isLeader() {
        return memberId.equals(leaderId);
    }

    public boolean isSuccess() {
        return error == ErrorCode.NONE;
    }

    @Override
    public String toString() {
        return "JoinGroupResponse{" +
                "error=" + error +
                ", generationId=" + generationId +
                ", memberId='" + memberId + '\'' +
                ", isLeader=" + isLeader() +
                ", protocolName='" + protocolName + '\'' +
                '}';
    }
}
