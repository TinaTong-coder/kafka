package com.example.state;

/**
 * Consumer group member状态枚举
 *
 * 状态转换流程:
 * UNJOINED → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE
 *    ↑                                                        │
 *    └────────────────────────────────────────────────────────┘
 *              (heartbeat失败 / 手动离组)
 */
public enum MemberState {
    /**
     * 未加入任何group，或已离开group
     */
    UNJOINED,

    /**
     * 已发送JoinGroup请求，等待coordinator响应
     * 在此状态下：
     * - 已发送JoinGroupRequest
     * - 还未收到JoinGroupResponse
     * - heartbeat disabled
     */
    PREPARING_REBALANCE,

    /**
     * 已收到JoinGroup响应，正在等待leader分配完成
     * 在此状态下：
     * - 已收到JoinGroupResponse（知道自己的generation和memberId）
     * - 已发送SyncGroupRequest
     * - 还未收到SyncGroupResponse（leader的partition分配结果）
     * - heartbeat enabled（从此状态开始发心跳）
     */
    COMPLETING_REBALANCE,

    /**
     * 已完成rebalance，可以正常工作
     * 在此状态下：
     * - 已收到SyncGroupResponse（知道自己负责哪些partition）
     * - 可以开始fetch数据
     * - heartbeat enabled
     */
    STABLE;

    /**
     * 是否还未加入group（UNJOINED或PREPARING_REBALANCE）
     * 这两种状态下不发送heartbeat
     */
    public boolean hasNotJoinedGroup() {
        return this == UNJOINED || this == PREPARING_REBALANCE;
    }
}
