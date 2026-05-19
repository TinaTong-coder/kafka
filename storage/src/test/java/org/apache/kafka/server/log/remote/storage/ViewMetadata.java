/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ViewMetadata {
    static class StateChange {
        long offset;
        String state;
        StateChange(long offset, String state) {
            this.offset = offset;
            this.state = state;
        }
    }
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metadata-viewer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // 存储每个 key 的状态变化历史
        Map<String, List<StateChange>> keyStateChanges = new LinkedHashMap<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("__remote_log_metadata"));
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            long startTime = System.currentTimeMillis();
            System.out.println("Reading from __remote_log_metadata topic...\n");
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    if (System.currentTimeMillis() - startTime > 5000) {
                        break;
                    }
                    continue;
                }
                startTime = System.currentTimeMillis();
                for (ConsumerRecord<String, byte[]> record : records) {
                    String key = record.key();
                    byte[] value = record.value();
                    long offset = record.offset();
                    String state;
                    if (value == null) {
                        state = "TOMBSTONE";
                    } else {
                        try {
                            RemoteLogMetadata metadata = serde.deserialize(value);
                            state = extractState(metadata);
                        } catch (Exception e) {
                            state = "ERROR";
                        }
                    }
                    keyStateChanges
                        .computeIfAbsent(key, k -> new ArrayList<>())
                        .add(new StateChange(offset, state));
                }
            }
            // 打印紧凑格式
            System.out.println("Key -> State Changes [offset:state, ...]");
            System.out.println("=".repeat(100));
            for (Map.Entry<String, List<StateChange>> entry : keyStateChanges.entrySet()) {
                String key = entry.getKey();
                List<StateChange> changes = entry.getValue();
                StringBuilder sb = new StringBuilder();
                sb.append(key).append(" -> ");
                for (int i = 0; i < changes.size(); i++) {
                    StateChange change = changes.get(i);
                    sb.append(change.offset).append(":").append(change.state);
                    if (i < changes.size() - 1) {
                        sb.append(", ");
                    }
                }
                System.out.println(sb.toString());
            }
            System.out.println("\nTotal keys: " + keyStateChanges.size());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private static String extractState(RemoteLogMetadata metadata) {
        if (metadata instanceof RemoteLogSegmentMetadata) {
            return ((RemoteLogSegmentMetadata) metadata).state().toString();
        } else if (metadata instanceof RemoteLogSegmentMetadataUpdate) {
            return ((RemoteLogSegmentMetadataUpdate) metadata).state().toString();
        } else if (metadata instanceof RemotePartitionDeleteMetadata) {
            return ((RemotePartitionDeleteMetadata) metadata).state().toString();
        }
        return metadata.getClass().getSimpleName();
    }
}
