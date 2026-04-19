package com.example.protocol;

/**
 * SyncGroup响应
 *
 * 包含：
 * - memberAssignment: leader分配给当前member的partition列表
 * - error: 错误码
 */
public class SyncGroupResponse {
    public enum ErrorCode {
        NONE,
        UNKNOWN_MEMBER_ID,
        ILLEGAL_GENERATION,
        REBALANCE_IN_PROGRESS
    }

    public final ErrorCode error;
    public final String memberAssignment;  // 简化为String，真实是ByteBuffer

    public SyncGroupResponse(ErrorCode error, String memberAssignment) {
        this.error = error;
        this.memberAssignment = memberAssignment;
    }

    public boolean isSuccess() {
        return error == ErrorCode.NONE;
    }

    @Override
    public String toString() {
        return "SyncGroupResponse{" +
                "error=" + error +
                ", assignment='" + memberAssignment + '\'' +
                '}';
    }
}
