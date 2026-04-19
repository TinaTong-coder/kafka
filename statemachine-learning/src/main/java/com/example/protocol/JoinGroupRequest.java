package com.example.protocol;

/**
 * JoinGroup请求 - Consumer向Coordinator发起加入group的请求
 *
 * 真实Kafka包含更多字段（sessionTimeout, rebalanceTimeout, protocols等）
 * 这里简化只保留核心字段
 */
public class JoinGroupRequest {
    public final String groupId;
    public final String memberId;  // 首次加入时为空，重新加入时使用之前分配的ID

    public JoinGroupRequest(String groupId, String memberId) {
        this.groupId = groupId;
        this.memberId = memberId;
    }

    @Override
    public String toString() {
        return "JoinGroupRequest{" +
                "groupId='" + groupId + '\'' +
                ", memberId='" + memberId + '\'' +
                '}';
    }
}
