package com.example.test;

import com.example.cache.Cluster;
import com.example.coordinator.Metadata;
import com.example.producer.MockProducer;
import com.example.producer.MockSender;

/**
 * 测试版本号控制机制
 *
 * 场景：
 * 1. Producer尝试发送到不存在的topic
 * 2. Producer请求更新（获取oldVersion）
 * 3. Sender检测到需要更新，发送请求并更新metadata（updateVersion++）
 * 4. Producer的awaitUpdate检测到版本变化，退出等待
 */
public class VersionControlTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Test: Version Control Mechanism ===\n");

        // 1. 创建Metadata（backoff=1s, expire=60s）
        Metadata metadata = new Metadata(1000, 60000);

        // 2. 启动Sender线程（每100ms poll一次）
        MockSender sender = new MockSender(metadata, 100);
        sender.start();

        // 3. 创建Producer
        MockProducer producer = new MockProducer(metadata, 5000);

        // 等待Sender初始化
        Thread.sleep(200);

        System.out.println("=== Initial State ===");
        System.out.println("updateVersion: " + metadata.updateVersion());
        System.out.println("requestVersion: " + metadata.requestVersion());
        Cluster cluster = metadata.fetch();
        System.out.println("Cluster: " + cluster);
        System.out.println();

        // 4. 发送到已存在的topic（不需要等待）
        System.out.println("=== Scenario 1: Send to existing topic ===");
        int partitions = producer.send("topic-1");
        assert partitions == 3 : "Expected 3 partitions";
        System.out.println("✅ Send to topic-1 succeeded (no wait needed)\n");

        // 5. 添加新topic到broker
        System.out.println("=== Scenario 2: Send to new topic ===");
        sender.addTopicToBroker("topic-new", 7);

        // 6. 发送到不存在的topic（需要等待metadata更新）
        long startMs = System.currentTimeMillis();
        partitions = producer.send("topic-new");
        long elapsedMs = System.currentTimeMillis() - startMs;

        assert partitions == 7 : "Expected 7 partitions";
        System.out.println("✅ Send to topic-new succeeded after " + elapsedMs + "ms");
        System.out.println();

        // 7. 验证版本号递增
        System.out.println("=== Final State ===");
        System.out.println("updateVersion: " + metadata.updateVersion());
        System.out.println("requestVersion: " + metadata.requestVersion());
        cluster = metadata.fetch();
        System.out.println("Cluster: " + cluster);

        assert metadata.updateVersion() >= 2 : "updateVersion should be at least 2";
        assert metadata.requestVersion() >= 1 : "requestVersion should be at least 1";

        // 8. 清理
        sender.shutdown();
        sender.join();

        System.out.println("\n✅ Test PASSED");
    }
}
