package com.example.producer;

import com.example.cache.Cluster;
import com.example.coordinator.Metadata;
import com.example.coordinator.TimeoutException;

/**
 * 模拟Producer
 *
 * 真实KafkaProducer的send流程:
 * 1. 获取metadata
 * 2. 如果topic不存在，请求更新并等待
 * 3. 计算partition
 * 4. 追加到RecordAccumulator
 * 5. 返回Future
 *
 * 这里简化为只模拟metadata获取和等待流程
 */
public class MockProducer {
    private final Metadata metadata;
    private final long maxBlockMs;  // 最大阻塞时间

    public MockProducer(Metadata metadata, long maxBlockMs) {
        this.metadata = metadata;
        this.maxBlockMs = maxBlockMs;
    }

    /**
     * 发送消息（简化版）
     *
     * @param topic 目标topic
     * @return partition数量
     * @throws InterruptedException 被中断
     * @throws TimeoutException metadata更新超时
     *
     * 调用栈:
     * [User Thread]
     * producer.send(topic, record)
     * └─ waitOnMetadata(topic)
     *    ├─ cluster = metadata.fetch()
     *    ├─ partition = cluster.partitionCountForTopic(topic)
     *    │
     *    ├─ if (partition == null) {  // topic不存在，需要更新metadata
     *    │   oldVersion = metadata.requestUpdate();
     *    │   metadata.awaitUpdate(oldVersion, maxBlockMs);
     *    │   cluster = metadata.fetch();  // 重新获取
     *    │  }
     *    │
     *    └─ return partition;
     */
    public int send(String topic) throws InterruptedException, TimeoutException {
        System.out.println("[Producer] Sending to topic: " + topic);

        // 1. 获取当前metadata
        Cluster cluster = metadata.fetch();
        Integer partitionCount = cluster.partitionCountForTopic(topic);

        // 2. 如果topic不存在，等待metadata更新
        if (partitionCount == null) {
            System.out.println("[Producer] Topic " + topic + " not found in metadata, requesting update");

            // 添加topic到关注列表
            metadata.add(topic);

            // 请求更新并获取旧版本号
            int oldVersion = metadata.requestUpdate();
            System.out.println("[Producer] Requested update, old version: " + oldVersion);

            // 等待更新完成
            System.out.println("[Producer] Waiting for metadata update...");
            metadata.awaitUpdate(oldVersion, maxBlockMs);

            // 重新获取
            cluster = metadata.fetch();
            partitionCount = cluster.partitionCountForTopic(topic);

            if (partitionCount == null) {
                throw new RuntimeException("Topic " + topic + " still not found after metadata update");
            }
        }

        System.out.println("[Producer] Topic " + topic + " has " + partitionCount + " partitions");
        return partitionCount;
    }
}
