package com.example.cache;

import java.util.*;

/**
 * 集群信息（简化版）
 *
 * 真实Kafka的Cluster包含:
 * - nodes: broker列表
 * - partitions: topic partition映射
 * - controller: controller broker
 *
 * 这里简化为只包含topic列表和partition数量
 */
public class Cluster {
    private final Map<String, Integer> topicPartitions;  // topic -> partition count

    public Cluster(Map<String, Integer> topicPartitions) {
        this.topicPartitions = new HashMap<>(topicPartitions);
    }

    /**
     * 获取topic的partition数量
     * @return partition数量，如果topic不存在返回null
     */
    public Integer partitionCountForTopic(String topic) {
        return topicPartitions.get(topic);
    }

    /**
     * 获取所有topic列表
     */
    public Set<String> topics() {
        return topicPartitions.keySet();
    }

    /**
     * topic是否存在
     */
    public boolean containsTopic(String topic) {
        return topicPartitions.containsKey(topic);
    }

    @Override
    public String toString() {
        return "Cluster{topics=" + topicPartitions + "}";
    }

    /**
     * 空集群
     */
    public static Cluster empty() {
        return new Cluster(Collections.emptyMap());
    }
}
