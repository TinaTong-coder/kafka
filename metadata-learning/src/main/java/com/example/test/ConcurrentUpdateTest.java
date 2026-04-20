package com.example.test;

import com.example.coordinator.Metadata;
import com.example.producer.MockProducer;
import com.example.producer.MockSender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 测试并发场景：多个Producer同时等待metadata更新
 *
 * 场景：
 * 1. 3个Producer线程同时发送到不存在的topic
 * 2. 所有Producer都调用requestUpdate()和awaitUpdate()
 * 3. Sender只需要发送一次MetadataRequest
 * 4. Sender更新后，notifyAll()唤醒所有等待的Producer
 */
public class ConcurrentUpdateTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Test: Concurrent Metadata Update ===\n");

        // 1. 创建Metadata
        Metadata metadata = new Metadata(1000, 60000);

        // 2. 启动Sender
        MockSender sender = new MockSender(metadata, 100);
        sender.start();

        Thread.sleep(200);  // 等待初始化

        // 3. 添加新topic到broker
        sender.addTopicToBroker("topic-concurrent", 10);

        // 4. 启动3个Producer线程，同时发送到新topic
        int numProducers = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numProducers);
        List<Thread> producerThreads = new ArrayList<>();
        List<Long> waitTimes = new ArrayList<>();

        for (int i = 0; i < numProducers; i++) {
            final int id = i;
            Thread t = new Thread(() -> {
                try {
                    MockProducer producer = new MockProducer(metadata, 5000);

                    // 等待所有线程就绪
                    startLatch.await();

                    System.out.println("[Producer-" + id + "] Starting send");
                    long startMs = System.currentTimeMillis();

                    int partitions = producer.send("topic-concurrent");

                    long elapsedMs = System.currentTimeMillis() - startMs;
                    synchronized (waitTimes) {
                        waitTimes.add(elapsedMs);
                    }

                    System.out.println("[Producer-" + id + "] Got " + partitions + " partitions after " + elapsedMs + "ms");
                    assert partitions == 10 : "Expected 10 partitions";

                    doneLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, "Producer-" + i);

            producerThreads.add(t);
            t.start();
        }

        // 5. 让所有Producer同时开始
        System.out.println("=== Starting " + numProducers + " producers concurrently ===");
        startLatch.countDown();

        // 6. 等待所有Producer完成
        doneLatch.await();

        // 7. 验证结果
        System.out.println("\n=== Results ===");
        System.out.println("All producers completed successfully");
        System.out.println("Wait times: " + waitTimes);
        System.out.println("updateVersion: " + metadata.updateVersion());

        // 所有Producer应该在相近的时间内完成（都等待同一次更新）
        long maxWait = waitTimes.stream().max(Long::compareTo).orElse(0L);
        long minWait = waitTimes.stream().min(Long::compareTo).orElse(0L);
        long diff = maxWait - minWait;

        System.out.println("Max wait: " + maxWait + "ms, Min wait: " + minWait + "ms, Diff: " + diff + "ms");
        assert diff < 500 : "Wait times should be similar (diff < 500ms), but got " + diff + "ms";

        // 8. 清理
        sender.shutdown();
        sender.join();

        for (Thread t : producerThreads) {
            t.join();
        }

        System.out.println("\n✅ Test PASSED");
    }
}
