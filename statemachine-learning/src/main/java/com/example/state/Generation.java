package com.example.state;

/**
 * Generation表示一次rebalance的世代信息
 *
 * 每次rebalance成功后，coordinator会分配新的generation:
 * - generationId: 单调递增的世代ID
 * - memberId: 当前member在这个generation里的唯一ID
 * - protocolName: 使用的分配协议（如"range", "roundrobin"）
 *
 * 为什么需要Generation？
 * 1. 防止过期请求：HeartbeatThread可能在rebalance时清空generation
 * 2. 并发安全：通过snapshot比对检测generation是否被修改
 * 3. 协议版本：确保所有member使用相同的分配协议
 */
public class Generation {
    /**
     * 特殊值：表示没有generation（未加入group或已离开）
     */
    public static final Generation NO_GENERATION = new Generation(-1, "", "");

    public final int generationId;
    public final String memberId;
    public final String protocolName;

    public Generation(int generationId, String memberId, String protocolName) {
        this.generationId = generationId;
        this.memberId = memberId;
        this.protocolName = protocolName;
    }

    @Override
    public String toString() {
        return "Generation{" +
                "generationId=" + generationId +
                ", memberId='" + memberId + '\'' +
                ", protocolName='" + protocolName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Generation that = (Generation) o;
        return generationId == that.generationId &&
               memberId.equals(that.memberId) &&
               protocolName.equals(that.protocolName);
    }

    @Override
    public int hashCode() {
        int result = generationId;
        result = 31 * result + memberId.hashCode();
        result = 31 * result + protocolName.hashCode();
        return result;
    }
}
