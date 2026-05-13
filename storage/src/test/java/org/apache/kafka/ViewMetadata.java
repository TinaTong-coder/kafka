package org.apache.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ViewMetadata {

    // 内部类用于存储 key 的历史记录
    static class KeyRecord {
        long offset;
        int partition;
        long timestamp;
        RemoteLogMetadata metadata;
        boolean isNull;

        KeyRecord(long offset, int partition, long timestamp, RemoteLogMetadata metadata, boolean isNull) {
            this.offset = offset;
            this.partition = partition;
            this.timestamp = timestamp;
            this.metadata = metadata;
            this.isNull = isNull;
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metadata-viewer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // 存储每个 key 的历史记录
        Map<String, List<KeyRecord>> keyHistory = new HashMap<>();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("__remote_log_metadata"));

            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            int totalMessages = 0;
            int maxMessages = 100; // 可以调整这个值

            System.out.println("===== Reading from __remote_log_metadata topic =====");
            System.out.println("Tracking key changes and value updates...\n");

            long startTime = System.currentTimeMillis();

            while (totalMessages < maxMessages) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));

                // 如果 5 秒内没有新消息，退出
                if (records.isEmpty()) {
                    if (System.currentTimeMillis() - startTime > 5000) {
                        System.out.println("\nNo more messages found. Stopping...\n");
                        break;
                    }
                    continue;
                }

                startTime = System.currentTimeMillis(); // 重置计时器

                for (ConsumerRecord<String, byte[]> record : records) {
                    totalMessages++;

                    String key = record.key();
                    byte[] value = record.value();
                    long offset = record.offset();
                    int partition = record.partition();
                    long timestamp = record.timestamp();

                    // 处理 null value
                    RemoteLogMetadata metadata = null;
                    boolean isNull = (value == null);

                    if (!isNull) {
                        try {
                            metadata = serde.deserialize(value);
                        } catch (Exception e) {
                            System.out.println("ERROR deserializing at offset " + offset + ": " + e.getMessage());
                            continue;
                        }
                    }

                    // 创建记录
                    KeyRecord currentRecord = new KeyRecord(offset, partition, timestamp, metadata, isNull);

                    // 获取这个 key 的历史
                    List<KeyRecord> history = keyHistory.computeIfAbsent(key, k -> new ArrayList<>());

                    // 如果是第一次出现
                    if (history.isEmpty()) {
                        System.out.println("========== NEW KEY ==========");
                        System.out.println("Key: " + key);
                        System.out.println("First seen at:");
                        System.out.println("  Partition: " + partition);
                        System.out.println("  Offset: " + offset);
                        System.out.println("  Timestamp: " + Instant.ofEpochMilli(timestamp));

                        if (isNull) {
                            System.out.println("  Value: <NULL>");
                        } else {
                            printMetadata(metadata, "  ");
                        }
                        System.out.println();
                    } else {
                        // 比较与上一次的变化
                        KeyRecord previous = history.get(history.size() - 1);

                        // 检测变化
                        boolean hasChanged = detectChange(previous, currentRecord);

                        if (hasChanged) {
                            System.out.println("========== KEY CHANGED ==========");
                            System.out.println("Key: " + key);
                            System.out.println("Change detected:");
                            System.out.println("  Offset: " + previous.offset + " => " + offset);
                            System.out.println("  Timestamp: " + Instant.ofEpochMilli(timestamp));
                            System.out.println();

                            System.out.println("  PREVIOUS (offset " + previous.offset + "):");
                            if (previous.isNull) {
                                System.out.println("    Value: <NULL>");
                            } else {
                                printMetadata(previous.metadata, "    ");
                            }

                            System.out.println();
                            System.out.println("  CURRENT (offset " + offset + "):");
                            if (isNull) {
                                System.out.println("    Value: <NULL>");
                            } else {
                                printMetadata(metadata, "    ");
                            }
                            System.out.println();
                        }
                    }

                    // 添加到历史记录
                    history.add(currentRecord);

                    if (totalMessages >= maxMessages) break;
                }
            }

            // 打印摘要
            printSummary(keyHistory, totalMessages);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean detectChange(KeyRecord prev, KeyRecord curr) {
        // 如果一个是 null，另一个不是，肯定有变化
        if (prev.isNull != curr.isNull) {
            return true;
        }

        // 如果都是 null，没有变化
        if (prev.isNull && curr.isNull) {
            return false;
        }

        // 比较元数据
        RemoteLogMetadata prevMeta = prev.metadata;
        RemoteLogMetadata currMeta = curr.metadata;

        // 比较类型
        if (!prevMeta.getClass().equals(currMeta.getClass())) {
            return true;
        }

        // 如果是 RemoteLogSegmentMetadata，比较状态
        if (prevMeta instanceof RemoteLogSegmentMetadata) {
            RemoteLogSegmentMetadata prevSeg = (RemoteLogSegmentMetadata) prevMeta;
            RemoteLogSegmentMetadata currSeg = (RemoteLogSegmentMetadata) currMeta;
            return !prevSeg.state().equals(currSeg.state());
        }

        // 如果是 RemoteLogSegmentMetadataUpdate，比较状态
        if (prevMeta instanceof RemoteLogSegmentMetadataUpdate) {
            RemoteLogSegmentMetadataUpdate prevUpd = (RemoteLogSegmentMetadataUpdate) prevMeta;
            RemoteLogSegmentMetadataUpdate currUpd = (RemoteLogSegmentMetadataUpdate) currMeta;
            return !prevUpd.state().equals(currUpd.state());
        }

        // 默认认为有变化
        return true;
    }

    private static void printMetadata(RemoteLogMetadata metadata, String indent) {
        System.out.println(indent + "Type: " + metadata.getClass().getSimpleName());
        System.out.println(indent + "Topic-Partition: " + metadata.topicIdPartition());
        System.out.println(indent + "Broker ID: " + metadata.brokerId());
        System.out.println(indent + "Event Timestamp: " + Instant.ofEpochMilli(metadata.eventTimestampMs()));

        if (metadata instanceof RemoteLogSegmentMetadata) {
            RemoteLogSegmentMetadata segMeta = (RemoteLogSegmentMetadata) metadata;
            System.out.println(indent + "State: " + segMeta.state());
            System.out.println(indent + "Segment ID: " + segMeta.remoteLogSegmentId().id());
            System.out.println(indent + "Start Offset: " + segMeta.startOffset());
            System.out.println(indent + "End Offset: " + segMeta.endOffset());
            System.out.println(indent + "Segment Size: " + segMeta.segmentSizeInBytes() + " bytes");
            System.out.println(indent + "Leader Epochs: " + segMeta.segmentLeaderEpochs());
        } else if (metadata instanceof RemoteLogSegmentMetadataUpdate) {
            RemoteLogSegmentMetadataUpdate update = (RemoteLogSegmentMetadataUpdate) metadata;
            System.out.println(indent + "State: " + update.state());
            System.out.println(indent + "Segment ID: " + update.remoteLogSegmentId().id());
        } else if (metadata instanceof RemotePartitionDeleteMetadata) {
            RemotePartitionDeleteMetadata deleteMeta = (RemotePartitionDeleteMetadata) metadata;
            System.out.println(indent + "State: " + deleteMeta.state());
        }
    }

    private static void printSummary(Map<String, List<KeyRecord>> keyHistory, int totalMessages) {
        System.out.println("\n========== SUMMARY ==========");
        System.out.println("Total messages read: " + totalMessages);
        System.out.println("Total unique keys: " + keyHistory.size());
        System.out.println();

        System.out.println("Key Statistics:");
        for (Map.Entry<String, List<KeyRecord>> entry : keyHistory.entrySet()) {
            String key = entry.getKey();
            List<KeyRecord> records = entry.getValue();

            System.out.println("\n  Key: " + key);
            System.out.println("    Occurrences: " + records.size());
            System.out.println("    First offset: " + records.get(0).offset);
            System.out.println("    Last offset: " + records.get(records.size() - 1).offset);

            // 统计状态变化
            if (records.size() > 1) {
                System.out.println("    State transitions:");
                for (int i = 1; i < records.size(); i++) {
                    KeyRecord prev = records.get(i - 1);
                    KeyRecord curr = records.get(i);

                    String prevState = getState(prev);
                    String currState = getState(curr);

                    if (!prevState.equals(currState)) {
                        System.out.println("      Offset " + prev.offset + " => " + curr.offset + ": " +
                                prevState + " => " + currState);
                    }
                }
            }
        }
    }

    private static String getState(KeyRecord record) {
        if (record.isNull) {
            return "<NULL>";
        }

        RemoteLogMetadata meta = record.metadata;
        if (meta instanceof RemoteLogSegmentMetadata) {
            return ((RemoteLogSegmentMetadata) meta).state().toString();
        } else if (meta instanceof RemoteLogSegmentMetadataUpdate) {
            return ((RemoteLogSegmentMetadataUpdate) meta).state().toString();
        } else if (meta instanceof RemotePartitionDeleteMetadata) {
            return ((RemotePartitionDeleteMetadata) meta).state().toString();
        }

        return meta.getClass().getSimpleName();
    }
}