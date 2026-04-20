package com.example.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * 不可变的Metadata缓存（Copy-On-Write模式）
 *
 * 为什么不可变？
 * 1. Producer读取时无需长时间持有锁
 * 2. Sender更新时创建新对象，不影响正在读取的旧对象
 * 3. 线程安全，无需额外同步
 *
 * 真实Kafka的MetadataCache包含:
 * - cluster: 集群信息
 * - metadataByPartition: partition级别的详细信息
 * - unauthorizedTopics: 未授权的topic
 *
 * 这里简化为只包含Cluster对象
 */
public class MetadataCache {
    private final Cluster cluster;

    public MetadataCache(Map<String, Integer> topicPartitions) {
        this.cluster = new Cluster(topicPartitions);
    }

    private MetadataCache(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * 获取集群信息（不可变）
     */
    public Cluster cluster() {
        return cluster;
    }

    /**
     * 创建空缓存
     */
    public static MetadataCache empty() {
        return new MetadataCache(Cluster.empty());
    }

    /**
     * 合并新的metadata（返回新对象）
     * 用于Partial Update场景
     */
    public MetadataCache mergeWith(Map<String, Integer> newTopicPartitions) {
        Map<String, Integer> merged = new HashMap<>();

        // 保留旧数据
        for (String topic : cluster.topics()) {
            Integer count = cluster.partitionCountForTopic(topic);
            if (count != null) {
                merged.put(topic, count);
            }
        }

        // 添加/覆盖新数据
        merged.putAll(newTopicPartitions);

        return new MetadataCache(merged);
    }

    @Override
    public String toString() {
        return "MetadataCache{" + cluster + "}";
    }
}
