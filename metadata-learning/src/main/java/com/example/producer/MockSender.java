package com.example.producer;

import com.example.coordinator.Metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * 模拟Sender线程
 *
 * 真实Kafka的Sender职责:
 * 1. 检查metadata是否需要更新
 * 2. 发送MetadataRequest
 * 3. 处理MetadataResponse，更新Metadata
 * 4. 批量发送ProducerRecord
 *
 * 这里简化为只模拟metadata更新流程
 */
public class MockSender extends Thread {
    private final Metadata metadata;
    private volatile boolean running = true;
    private final long pollIntervalMs;

    // 模拟broker的metadata存储
    private final Map<String, Integer> brokerMetadata = new HashMap<>();

    public MockSender(Metadata metadata, long pollIntervalMs) {
        super("MockSender");
        this.metadata = metadata;
        this.pollIntervalMs = pollIntervalMs;
        setDaemon(true);

        // 预设一些topic
        brokerMetadata.put("topic-1", 3);
        brokerMetadata.put("topic-2", 5);
    }

    /**
     * 添加broker上的topic（模拟topic创建）
     */
    public void addTopicToBroker(String topic, int partitions) {
        brokerMetadata.put(topic, partitions);
    }

    @Override
    public void run() {
        System.out.println("[Sender] Started");

        while (running) {
            try {
                poll();
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                break;
            }
        }

        System.out.println("[Sender] Stopped");
    }

    /**
     * 核心poll循环
     *
     * 调用栈:
     * [Sender Thread]
     * run()
     * └─ poll()
     *    ├─ timeToNextUpdate = metadata.timeToNextUpdate(now)
     *    ├─ if (timeToNextUpdate <= 0) {
     *    │   requestVersionSnapshot = metadata.requestVersion()
     *    │   sendMetadataRequest(requestVersionSnapshot)
     *    │  }
     *    └─ handleOtherTasks()
     */
    private void poll() {
        long now = System.currentTimeMillis();

        // 1. 检查是否需要更新metadata
        long timeToNextUpdate = metadata.timeToNextUpdate(now);

        if (timeToNextUpdate <= 0) {
            // 需要更新
            int requestVersionSnapshot = metadata.requestVersion();
            System.out.println("[Sender] Metadata update needed (requestVersion=" + requestVersionSnapshot + ")");

            // 2. 发送MetadataRequest（模拟网络延迟）
            Map<String, Integer> response = sendMetadataRequest();

            // 3. 处理MetadataResponse
            handleMetadataResponse(requestVersionSnapshot, response, now);
        }
    }

    /**
     * 发送MetadataRequest（模拟网络IO）
     */
    private Map<String, Integer> sendMetadataRequest() {
        System.out.println("[Sender] Sending MetadataRequest...");

        // 模拟网络延迟
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 返回broker的metadata
        return new HashMap<>(brokerMetadata);
    }

    /**
     * 处理MetadataResponse
     *
     * @param requestVersion 发送请求时的requestVersion快照
     * @param response broker返回的metadata
     * @param nowMs 当前时间
     */
    private void handleMetadataResponse(int requestVersion, Map<String, Integer> response, long nowMs) {
        System.out.println("[Sender] Received MetadataResponse: " + response);

        // 更新Metadata（会递增updateVersion并notifyAll）
        metadata.update(requestVersion, response, false, nowMs);
    }

    public void shutdown() {
        this.running = false;
        this.interrupt();
    }
}
