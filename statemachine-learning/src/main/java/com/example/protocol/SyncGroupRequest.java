package com.example.protocol;

import java.util.Map;

/**
 * SyncGroup请求
 *
 * Leader: 携带所有member的partition分配结果
 * Follower: 携带空的分配结果，只是为了获取leader的分配
 */
public class SyncGroupRequest {
    public final String groupId;
    public final int generationId;
    public final String memberId;

    // Leader填充：memberId -> partition assignment
    // Follower传空Map
    public final Map<String, String> groupAssignment;

    public SyncGroupRequest(String groupId, int generationId, String memberId,
                           Map<String, String> groupAssignment) {
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.groupAssignment = groupAssignment;
    }

    @Override
    public String toString() {
        return "SyncGroupRequest{" +
                "groupId='" + groupId + '\'' +
                ", generationId=" + generationId +
                ", memberId='" + memberId + '\'' +
                ", assignmentSize=" + groupAssignment.size() +
                '}';
    }
}
