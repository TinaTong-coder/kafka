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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataManagerTestUtils;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify backward compatibility: new code can handle old message format (messages without keys).
 *
 * Background:
 * - Old code (before commit b92795741b) wrote messages with null keys
 * - New code writes messages with keys in format: "topicId:partition:endOffset:brokerLeaderEpoch"
 * - Consumer (ConsumerTask) only reads the value field, not the key
 * - Therefore, new code can read old messages without keys
 *
 * Upgrade strategy:
 * - Existing clusters: topic starts with delete policy, old messages (null keys) exist
 * - After upgrade: new messages written with keys
 * - Old messages expire via time-based retention (24h default)
 * - After retention period: topic can be safely changed to compacted policy
 */
@ClusterTestDefaults(brokers = 3)
public class RemoteLogMetadataOldFormatCompatibilityTest {
    private static final String METADATA_TOPIC = "__remote_log_metadata";
    private static final int SEG_SIZE = 1048576;

    private final ClusterInstance clusterInstance;
    private final Time time = Time.SYSTEM;
    private TopicBasedRemoteLogMetadataManager remoteLogMetadataManager;

    RemoteLogMetadataOldFormatCompatibilityTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private TopicBasedRemoteLogMetadataManager createManager() {
        if (remoteLogMetadataManager == null) {
            remoteLogMetadataManager = RemoteLogMetadataManagerTestUtils.builder()
                    .bootstrapServers(clusterInstance.bootstrapServers())
                    .build();
        }
        return remoteLogMetadataManager;
    }

    @AfterEach
    public void teardown() throws IOException {
        if (remoteLogMetadataManager != null) {
            remoteLogMetadataManager.close();
        }
    }

    /**
     * Test that consumer logic can deserialize messages regardless of key format.
     * This test verifies that the deserialization depends only on the value field.
     */
    @Test
    public void testConsumerCanDeserializeMessagesWithAnyKeyFormat() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-key-format", 0)
        );

        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long endOffset = 500L;
        int brokerLeaderEpoch = 1;

        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
                segmentId,
                0L,
                endOffset,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch
        );

        RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
        byte[] serializedValue = serde.serialize(metadata);

        // Test 1: Simulate old format (null key)
        ConsumerRecord<byte[], byte[]> oldFormatRecord = new ConsumerRecord<>(
                METADATA_TOPIC,
                0,
                100L,
                null,  // Old format: null key
                serializedValue
        );

        RemoteLogMetadata deserializedOld = assertDoesNotThrow(() -> serde.deserialize(oldFormatRecord.value()),
                "Should be able to deserialize message with null key");
        assertNotNull(deserializedOld);
        assertEquals(segmentId, ((RemoteLogSegmentMetadata) deserializedOld).remoteLogSegmentId());
        assertNull(oldFormatRecord.key(), "Old format should have null key");

        // Test 2: Simulate new format (with key)
        String newFormatKey = metadata.metadataKey();
        ConsumerRecord<byte[], byte[]> newFormatRecord = new ConsumerRecord<>(
                METADATA_TOPIC,
                0,
                101L,
                newFormatKey.getBytes(),  // New format: has key
                serializedValue
        );

        RemoteLogMetadata deserializedNew = assertDoesNotThrow(() -> serde.deserialize(newFormatRecord.value()),
                "Should be able to deserialize message with key");
        assertNotNull(deserializedNew);
        assertEquals(segmentId, ((RemoteLogSegmentMetadata) deserializedNew).remoteLogSegmentId());
        assertNotNull(newFormatRecord.key(), "New format should have key");
        assertEquals(newFormatKey, new String(newFormatRecord.key()));

        // Test 3: Verify both deserializations produce the same metadata
        assertEquals(
                ((RemoteLogSegmentMetadata) deserializedOld).remoteLogSegmentId(),
                ((RemoteLogSegmentMetadata) deserializedNew).remoteLogSegmentId(),
                "Deserialized metadata should be the same regardless of key format"
        );
    }

    /**
     * Test upgrade scenario: topic starts with delete policy (old messages with null keys),
     * then new messages with keys are written, and finally topic is changed to compacted.
     *
     * This simulates the actual upgrade path where:
     * 1. Old cluster has messages with null keys (delete policy)
     * 2. Upgrade to new code: new messages have keys
     * 3. Both old and new messages coexist temporarily
     * 4. After old messages expire, change to compacted policy
     */
    @ClusterTest
    public void testUpgradeScenarioWithMixedMessageFormats() throws Exception {
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-upgrade-scenario", 0)
        );

        // Step 1: Initialize RLMM with delete policy (simulating old cluster)
        System.out.println("Step 1: Initializing RLMM with delete policy...");

        // First create the topic with delete policy by modifying the config
        TopicBasedRemoteLogMetadataManager rlmm = createManager();
        rlmm.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );
        waitForInitialization(rlmm, topicIdPartition);
        System.out.println("RLMM initialized and metadata topic created.");

        // Change topic to delete policy to allow null keys
        changeTopicToDeletePolicy();
        System.out.println("Changed metadata topic to delete cleanup policy.");

        // Close RLMM before writing old format messages
        rlmm.close();
        remoteLogMetadataManager = null;
        Thread.sleep(2000);

        // Step 2: Write old format message (null key) - simulating old code
        System.out.println("Step 2: Writing old format message (null key)...");
        RemoteLogSegmentId oldSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long oldEndOffset = 500L;
        int brokerLeaderEpoch = 1;

        RemoteLogSegmentMetadata oldMetadata = new RemoteLogSegmentMetadata(
                oldSegmentId,
                0L,
                oldEndOffset,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch
        );

        writeMessageWithNullKey(topicIdPartition, oldMetadata);
        System.out.println("Old format message written successfully.");

        Thread.sleep(2000);

        // Step 3: Re-initialize RLMM (simulating upgrade to new code)
        System.out.println("Step 3: Upgrading to new code (re-initializing RLMM)...");
        final TopicBasedRemoteLogMetadataManager rlmm2 = createManager();
        rlmm2.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );
        waitForInitialization(rlmm2, topicIdPartition);
        System.out.println("RLMM re-initialized with new code.");

        // Step 4: Update old segment to verify it was read correctly
        System.out.println("Step 4: Updating old segment to COPY_SEGMENT_FINISHED...");
        RemoteLogSegmentMetadataUpdate oldUpdate = new RemoteLogSegmentMetadataUpdate(
                oldSegmentId,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0,
                brokerLeaderEpoch,
                oldEndOffset
        );
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(oldUpdate).get(),
                "Should be able to update old segment");

        Thread.sleep(1000);

        // Verify old segment is readable
        Optional<RemoteLogSegmentMetadata> retrievedOld =
                rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 250);
        assertTrue(retrievedOld.isPresent(), "Should be able to read old segment");
        assertEquals(oldSegmentId, retrievedOld.get().remoteLogSegmentId());
        System.out.println("✅ Old segment processed successfully!");

        // Step 5: Write new format message (with key) - new code
        System.out.println("Step 5: Writing new format message (with key)...");
        RemoteLogSegmentId newSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long newEndOffset = 1500L;

        RemoteLogSegmentMetadata newMetadata = new RemoteLogSegmentMetadata(
                newSegmentId,
                501L,
                newEndOffset,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 501L),
                brokerLeaderEpoch
        );

        assertDoesNotThrow(() -> rlmm2.addRemoteLogSegmentMetadata(newMetadata).get(),
                "Should be able to add new segment with key");

        RemoteLogSegmentMetadataUpdate newUpdate = new RemoteLogSegmentMetadataUpdate(
                newSegmentId,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0,
                brokerLeaderEpoch,
                newEndOffset
        );
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(newUpdate).get());

        Thread.sleep(1000);

        // Step 6: Verify both old and new segments coexist
        System.out.println("Step 6: Verifying both old and new segments coexist...");
        Optional<RemoteLogSegmentMetadata> retrievedNew =
                rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 1000);
        assertTrue(retrievedNew.isPresent(), "Should be able to read new segment");
        assertEquals(newSegmentId, retrievedNew.get().remoteLogSegmentId());

        retrievedOld = rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 250);
        assertTrue(retrievedOld.isPresent(), "Old segment should still be accessible");
        assertEquals(oldSegmentId, retrievedOld.get().remoteLogSegmentId());

        System.out.println("✅ Both old and new segments coexist successfully!");

        // Step 7: Change topic to compacted (simulating after retention period)
        System.out.println("Step 7: Changing topic to compacted policy...");
        changeTopicToCompactedPolicy();
        System.out.println("✅ Topic changed to compacted policy.");

        // Step 8: Verify new segments can still be written with compacted policy
        System.out.println("Step 8: Writing another new segment with compacted policy...");
        RemoteLogSegmentId thirdSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long thirdEndOffset = 2500L;

        RemoteLogSegmentMetadata thirdMetadata = new RemoteLogSegmentMetadata(
                thirdSegmentId,
                1501L,
                thirdEndOffset,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 1501L),
                brokerLeaderEpoch
        );

        assertDoesNotThrow(() -> rlmm2.addRemoteLogSegmentMetadata(thirdMetadata).get(),
                "Should be able to add new segment with compacted policy");

        System.out.println("✅ Test passed! Upgrade scenario with mixed message formats works correctly.");
    }

    private void writeMessageWithNullKey(TopicIdPartition topicIdPartition,
                                         RemoteLogSegmentMetadata metadata) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            byte[] value = serde.serialize(metadata);

            int metadataPartition = Math.abs(topicIdPartition.hashCode()) % 3;

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    METADATA_TOPIC,
                    metadataPartition,
                    null,  // Old format: null key
                    value
            );

            producer.send(record).get();
            producer.flush();
        }
    }

    private void changeTopicToDeletePolicy() throws Exception {
        try (Admin admin = Admin.create(Collections.singletonMap(
                "bootstrap.servers", clusterInstance.bootstrapServers()))) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, Collections.singletonList(
                    new AlterConfigOp(
                            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE),
                            AlterConfigOp.OpType.SET
                    )
            ));

            admin.incrementalAlterConfigs(configs).all().get();
            Thread.sleep(2000); // Wait for config change to propagate
        }
    }

    private void changeTopicToCompactedPolicy() throws Exception {
        try (Admin admin = Admin.create(Collections.singletonMap(
                "bootstrap.servers", clusterInstance.bootstrapServers()))) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(resource, Collections.singletonList(
                    new AlterConfigOp(
                            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT),
                            AlterConfigOp.OpType.SET
                    )
            ));

            admin.incrementalAlterConfigs(configs).all().get();
            Thread.sleep(2000); // Wait for config change to propagate
        }
    }

    private void waitForInitialization(TopicBasedRemoteLogMetadataManager rlmm,
                                       TopicIdPartition topicIdPartition) throws InterruptedException {
        int maxWaitMs = 30000;
        int waitedMs = 0;
        while (!rlmm.isReady(topicIdPartition) && waitedMs < maxWaitMs) {
            Thread.sleep(100);
            waitedMs += 100;
        }
        assertTrue(rlmm.isReady(topicIdPartition),
                "RLMM should be initialized within " + maxWaitMs + "ms");
    }
}
