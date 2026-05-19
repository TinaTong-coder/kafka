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
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataTopicPartitioner;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;

import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataOldFormatCompatibilityTest.class);
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
     * Test upgrade scenario: topic starts with delete policy (old messages with null keys),
     * then new messages with keys are written, and finally topic is changed to compacted.
     *
     * This simulates the actual upgrade path where:
     * 1. Old cluster has messages with null keys (delete policy)
     * 2. Upgrade to new code: new messages have keys
     * 3. Both old and new messages coexist temporarily
     * 4. After old messages expire, change to compacted policy
     *
     * Tests both COPY_SEGMENT_STARTED and DELETE_SEGMENT_STARTED states, which are
     * the states that a new broker would encounter when reading old messages.
     */
    @ClusterTest
    public void testUpgradeScenarioWithMixedMessageFormats() throws Exception {
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-upgrade-scenario", 0)
        );

        // Step 1: Initialize RLMM with delete policy (simulating old cluster)
        log.info("Step 1: Initializing RLMM with delete policy...");

        // First create the topic with delete policy by modifying the config
        TopicBasedRemoteLogMetadataManager rlmm = createManager();
        rlmm.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );
        waitForInitialization(rlmm, topicIdPartition);
        log.info("RLMM initialized and metadata topic created.");

        // Change topic to delete policy to allow null keys
        changeTopicToDeletePolicy();
        log.info("Changed metadata topic to delete cleanup policy.");

        // Close RLMM before writing old format messages
        rlmm.close();
        remoteLogMetadataManager = null;
        Thread.sleep(2000);

        // Step 2: Write old format messages (null key) - simulating old code
        // Test both COPY_SEGMENT_STARTED and DELETE_SEGMENT_STARTED states
        log.info("Step 2: Writing old format messages (null key) with COPY_SEGMENT_STARTED state...");
        RemoteLogSegmentId oldSegmentId1 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long oldEndOffset1 = 500L;
        int brokerLeaderEpoch = 1;

        RemoteLogSegmentMetadata oldMetadata1 = new RemoteLogSegmentMetadata(
                oldSegmentId1,
                0L,
                oldEndOffset1,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch
        );

        writeMessageWithNullKey(topicIdPartition, oldMetadata1);
        log.info("Old format message (COPY_SEGMENT_STARTED) written successfully.");

        // Write another old format message going through full lifecycle to DELETE_SEGMENT_STARTED
        log.info("Writing old format message with full lifecycle: COPY_SEGMENT_STARTED -> COPY_SEGMENT_FINISHED -> DELETE_SEGMENT_STARTED...");
        RemoteLogSegmentId oldSegmentId2 = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long oldEndOffset2 = 1000L;

        // 1. Write COPY_SEGMENT_STARTED
        RemoteLogSegmentMetadata oldMetadata2 = new RemoteLogSegmentMetadata(
                oldSegmentId2,
                501L,
                oldEndOffset2,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 501L),
                brokerLeaderEpoch
        );
        writeMessageWithNullKey(topicIdPartition, oldMetadata2);
        log.info("  - COPY_SEGMENT_STARTED written");

        // 2. Write COPY_SEGMENT_FINISHED
        RemoteLogSegmentMetadataUpdate copyFinished = new RemoteLogSegmentMetadataUpdate(
                oldSegmentId2,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0,
                brokerLeaderEpoch,
                oldEndOffset2
        );
        writeUpdateWithNullKey(topicIdPartition, copyFinished);
        log.info("  - COPY_SEGMENT_FINISHED written");

        // 3. Write DELETE_SEGMENT_STARTED
        RemoteLogSegmentMetadataUpdate deleteStarted = new RemoteLogSegmentMetadataUpdate(
                oldSegmentId2,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                0,
                brokerLeaderEpoch,
                oldEndOffset2
        );
        writeUpdateWithNullKey(topicIdPartition, deleteStarted);
        log.info("  - DELETE_SEGMENT_STARTED written");
        log.info("Old format messages written successfully.");

        Thread.sleep(2000);

        // Step 3: Re-initialize RLMM (simulating upgrade to new code)
        log.info("Step 3: Upgrading to new code (re-initializing RLMM)...");
        final TopicBasedRemoteLogMetadataManager rlmm2 = createManager();
        rlmm2.onPartitionLeadershipChanges(
                Collections.singleton(topicIdPartition),
                Collections.emptySet()
        );
        waitForInitialization(rlmm2, topicIdPartition);
        log.info("RLMM re-initialized with new code.");

        // Wait for consumer to catch up and process old messages
        Thread.sleep(3000);

        // Step 4: New broker re-emits states with keys based on what it reads
        // This simulates what happens when a new broker starts up and processes existing segments
        log.info("Step 4: New broker re-emitting states with keys...");

        // First segment: was in COPY_SEGMENT_STARTED, so re-emit COPY_SEGMENT_STARTED with key
        log.info("Processing first segment (was in COPY_SEGMENT_STARTED state)...");
        RemoteLogSegmentMetadata reEmit1 = new RemoteLogSegmentMetadata(
                oldSegmentId1,
                0L,
                oldEndOffset1,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch
        );
        assertDoesNotThrow(() -> rlmm2.addRemoteLogSegmentMetadata(reEmit1).get(),
                "New broker should be able to re-emit COPY_SEGMENT_STARTED with key");
        log.info("  ✅ Re-emitted COPY_SEGMENT_STARTED (now with key)");

        Thread.sleep(1000);

        // Update to COPY_SEGMENT_FINISHED
        RemoteLogSegmentMetadataUpdate update1 = new RemoteLogSegmentMetadataUpdate(
                oldSegmentId1,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                0,
                brokerLeaderEpoch,
                oldEndOffset1
        );
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(update1).get(),
                "Should be able to update to COPY_SEGMENT_FINISHED");

        Thread.sleep(1000);

        // Verify first segment can be read and has correct state
        Optional<RemoteLogSegmentMetadata> retrievedOld1 =
                rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 250);
        assertTrue(retrievedOld1.isPresent(), "Should be able to read first segment");
        assertEquals(oldSegmentId1, retrievedOld1.get().remoteLogSegmentId());
        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED, retrievedOld1.get().state());
        log.info("  ✅ Updated to COPY_SEGMENT_FINISHED");

        // Second segment: was in DELETE_SEGMENT_STARTED, so directly re-emit DELETE_SEGMENT_STARTED with key
        // (no need to go through COPY_SEGMENT_STARTED again)
        log.info("Processing second segment (was in DELETE_SEGMENT_STARTED state)...");
        RemoteLogSegmentMetadataUpdate reEmitDelete = new RemoteLogSegmentMetadataUpdate(
                oldSegmentId2,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED,
                0,
                brokerLeaderEpoch,
                oldEndOffset2
        );
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(reEmitDelete).get(),
                "New broker should be able to re-emit DELETE_SEGMENT_STARTED with key");
        log.info("  ✅ Re-emitted DELETE_SEGMENT_STARTED (now with key) - skipping COPY states");

        Thread.sleep(1000);

        // Update to DELETE_SEGMENT_FINISHED
        RemoteLogSegmentMetadataUpdate update2 = new RemoteLogSegmentMetadataUpdate(
                oldSegmentId2,
                time.milliseconds(),
                Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_FINISHED,
                0,
                brokerLeaderEpoch,
                oldEndOffset2
        );
        assertDoesNotThrow(() -> rlmm2.updateRemoteLogSegmentMetadata(update2).get(),
                "Should be able to update to DELETE_SEGMENT_FINISHED");
        log.info("  ✅ Updated to DELETE_SEGMENT_FINISHED");

        // Step 5: Write new format message (with key) - new code
        log.info("Step 5: Writing new format message (with key)...");
        RemoteLogSegmentId newSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long newEndOffset = 1500L;

        RemoteLogSegmentMetadata newMetadata = new RemoteLogSegmentMetadata(
                newSegmentId,
                1001L,
                newEndOffset,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 1001L),
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

        // Step 6: Verify remaining segments (first old segment + new segment)
        // Note: second old segment is deleted (DELETE_SEGMENT_FINISHED) so it should not be retrievable
        log.info("Step 6: Verifying remaining segments (first old segment + new segment)...");
        Optional<RemoteLogSegmentMetadata> retrievedNew =
                rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 1250);
        assertTrue(retrievedNew.isPresent(), "Should be able to read new segment");
        assertEquals(newSegmentId, retrievedNew.get().remoteLogSegmentId());

        retrievedOld1 = rlmm2.remoteLogSegmentMetadata(topicIdPartition, 0, 250);
        assertTrue(retrievedOld1.isPresent(), "First old segment should still be accessible");
        assertEquals(oldSegmentId1, retrievedOld1.get().remoteLogSegmentId());

        log.info("✅ All accessible segments verified successfully!");

        // Step 7: Change topic to compacted (simulating after retention period)
        log.info("Step 7: Changing topic to compacted policy...");
        changeTopicToCompactedPolicy();
        log.info("✅ Topic changed to compacted policy.");

        // Step 8: Verify new segments can still be written with compacted policy
        log.info("Step 8: Writing another new segment with compacted policy...");
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

        log.info("✅ Test passed! Upgrade scenario with mixed message formats works correctly.");
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

            RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(3);
            int metadataPartition = partitioner.metadataPartition(topicIdPartition);

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

    private void writeUpdateWithNullKey(TopicIdPartition topicIdPartition,
                                        RemoteLogSegmentMetadataUpdate update) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            byte[] value = serde.serialize(update);

            RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(3);
            int metadataPartition = partitioner.metadataPartition(topicIdPartition);

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
