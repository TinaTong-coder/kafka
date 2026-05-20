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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tool to manage remote.log.storage.version upgrades and migrate the __remote_log_metadata topic.
 *
 * This tool supports two upgrade paths:
 *
 * 1. Version 0 to 1: Upgrades feature and configures topic with retention.ms
 *    - Changes topic cleanup.policy to "compact,delete"
 *    - Sets retention.ms to ensure old-format (null-key) messages expire after specified period
 *    - Sets min.compaction.lag.ms to the same value as retention.ms
 *    - CRITICAL: This dual configuration ensures safe migration:
 *      * retention.ms: Old-format (null-key) messages expire naturally after this period
 *      * min.compaction.lag.ms: Log cleaner waits this long before compacting
 *      Without these settings, the log cleaner could immediately delete null-key messages during compaction,
 *      causing data loss. By setting both to the same value, null-key messages expire via retention
 *      BEFORE compaction begins.
 *
 * 2. Version 1 to 2: Validates no null-key messages exist, then upgrades to compact-only cleanup policy
 *    - Scans the entire topic for null-key messages
 *    - Only proceeds if no null-key messages are found
 *    - Changes cleanup.policy to "compact" (removes "delete")
 *    - Removes min.compaction.lag.ms override (no longer needed)
 *    - Removes retention.ms override (compact-only topics retain all data)
 *
 * Usage:
 *   # Upgrade from version 0 to 1
 *   kafka-remote-log-metadata-migration.sh --bootstrap-server localhost:9092 --upgrade-to-v1 --retention-ms 1209600000
 *
 *   # Upgrade from version 1 to 2 (with validation)
 *   kafka-remote-log-metadata-migration.sh --bootstrap-server localhost:9092 --check --auto-upgrade
 *
 * Exit codes:
 *   0 - Success (no null-key messages found, or operation completed successfully)
 *   1 - Failure (null-key messages found, or error occurred)
 */
public class RemoteLogMetadataMigrationTool {
    private static final String METADATA_TOPIC = "__remote_log_metadata";
    private static final String VALIDATION_CONFIG_KEY = "remote.log.metadata.v2.validated";
    private static final String VALIDATION_TIMESTAMP_KEY = "remote.log.metadata.v2.validated.timestamp";

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (HelpScreenException e) {
            return 0;
        } catch (ArgumentParserException e) {
            System.err.println("Command line error: " + e.getMessage() + ". Type --help for help.");
            return 1;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-remote-log-metadata-migration")
            .defaultHelp(true)
            .description("Tool to manage remote.log.storage.version upgrades and migrate the __remote_log_metadata topic.");

        parser.addArgument("--bootstrap-server")
            .required(true)
            .help("REQUIRED: A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.");

        parser.addArgument("--command-config")
            .type(Arguments.fileType())
            .help("Property file containing configs to be passed to Admin/Consumer Client.");

        parser.addArgument("--upgrade-to-v1")
            .action(Arguments.storeTrue())
            .help("Upgrade from remote.log.storage.version=0 to version 1, and configure topic with min.compaction.lag.ms.");

        parser.addArgument("--check")
            .action(Arguments.storeTrue())
            .help("Check if the topic contains any messages with null keys. This is required before upgrading to version 2.");

        parser.addArgument("--auto-upgrade")
            .action(Arguments.storeTrue())
            .help("Automatically upgrade to remote.log.storage.version=2 if validation passes. Requires --check.");

        parser.addArgument("--retention-ms")
            .type(Long.class)
            .setDefault(1209600000L)
            .help("Retention period in milliseconds for the __remote_log_metadata topic when upgrading to version 1 (default: 1209600000, which is 14 days). " +
                  "This parameter is CRITICAL: it serves two purposes: " +
                  "1) retention.ms: Ensures old-format (null-key) messages expire and are deleted after this period. " +
                  "2) min.compaction.lag.ms: Set to the same value to prevent log cleaner from compacting before old messages expire. " +
                  "Once the topic cleanup policy is changed to 'compact,delete', the log cleaner could immediately delete null-key messages during compaction. " +
                  "By setting both retention.ms and min.compaction.lag.ms to the same value, we ensure null-key messages expire naturally via retention " +
                  "before the log cleaner begins compacting. This prevents data loss during the migration period. Used with --upgrade-to-v1.");

        parser.addArgument("--timeout-ms")
            .type(Long.class)
            .setDefault(60000L)
            .help("Maximum time in milliseconds to wait while checking for messages (default: 60000). Used with --check.");

        Namespace namespace = parser.parseArgs(args);

        String bootstrapServers = namespace.getString("bootstrap_server");
        String commandConfig = namespace.getString("command_config");
        boolean upgradeToV1 = namespace.getBoolean("upgrade_to_v1");
        boolean check = namespace.getBoolean("check");
        boolean autoUpgrade = namespace.getBoolean("auto_upgrade");
        long retentionMs = namespace.getLong("retention_ms");
        long timeoutMs = namespace.getLong("timeout_ms");

        Properties props = new Properties();
        if (commandConfig != null) {
            try {
                props = Utils.loadProps(commandConfig);
            } catch (java.io.IOException e) {
                throw new TerseException("Failed to load properties from file: " + commandConfig + ". Error: " + e.getMessage());
            }
        }

        if (autoUpgrade && !check) {
            throw new TerseException("--auto-upgrade requires --check to be specified.");
        }

        if (upgradeToV1 && check) {
            throw new TerseException("Cannot specify both --upgrade-to-v1 and --check. Use --upgrade-to-v1 for 0->1 upgrade, or --check for 1->2 upgrade.");
        }

        if (upgradeToV1) {
            performUpgradeToV1(bootstrapServers, props, retentionMs);
        } else if (check) {
            checkForNullKeyMessages(bootstrapServers, props, timeoutMs, autoUpgrade);
        } else {
            throw new TerseException("No operation specified. Use --upgrade-to-v1 for version 0->1 upgrade, or --check for version 1->2 validation.");
        }
    }

    private static void performUpgradeToV1(String bootstrapServers, Properties baseProps, long retentionMs) throws Exception {
        System.out.println("Initiating upgrade to remote.log.storage.version=1...");
        System.out.println();

        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (Admin admin = Admin.create(adminProps)) {
            // Check current version
            org.apache.kafka.clients.admin.FeatureMetadata featureMetadata =
                admin.describeFeatures().featureMetadata().get();

            org.apache.kafka.clients.admin.FinalizedVersionRange versionRange =
                featureMetadata.finalizedFeatures().get(org.apache.kafka.server.common.RemoteLogStorageVersion.FEATURE_NAME);

            short currentVersion = (versionRange != null) ? versionRange.maxVersionLevel() : 0;

            System.out.println("Current remote.log.storage.version: " + currentVersion);

            if (currentVersion != 0) {
                if (currentVersion == 1) {
                    System.out.println("ℹ️  Already at version 1. No upgrade needed.");
                    return;
                } else {
                    throw new TerseException("Current version is " + currentVersion + ". This command is for upgrading from version 0 to 1.");
                }
            }

            // First, update topic configurations before upgrading feature
            // This ensures the controller doesn't overwrite with hardcoded values
            long retentionDays = retentionMs / (24 * 60 * 60 * 1000L);
            System.out.println("Pre-configuring __remote_log_metadata topic...");
            System.out.println("  - cleanup.policy=compact,delete");
            System.out.println("  - retention.ms=" + retentionMs + " (" + retentionDays + " days)");
            System.out.println("  - min.compaction.lag.ms=" + retentionMs + " (same as retention.ms, " + retentionDays + " days)");
            System.out.println("  - segment.ms=" + (7 * 24 * 60 * 60 * 1000L) + " (7 days)");
            System.out.println();
            System.out.println("IMPORTANT: retention.ms and min.compaction.lag.ms are critical for safe migration.");
            System.out.println("Once cleanup.policy becomes 'compact,delete', the log cleaner can immediately delete null-key messages during compaction.");
            System.out.println("Setting retention.ms=" + retentionMs + "ms ensures old-format (null-key) messages expire after " + retentionDays + " days.");
            System.out.println("Setting min.compaction.lag.ms to the same value ensures the log cleaner waits " + retentionDays + " days before compacting,");
            System.out.println("allowing null-key messages to expire naturally via retention before compaction begins.");
            System.out.println("This prevents data loss during the migration period.");
            System.out.println();

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);

            List<AlterConfigOp> configOps = new java.util.ArrayList<>();
            configOps.add(new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact,delete"), AlterConfigOp.OpType.SET));
            configOps.add(new AlterConfigOp(new ConfigEntry("retention.ms", String.valueOf(retentionMs)), AlterConfigOp.OpType.SET));
            configOps.add(new AlterConfigOp(new ConfigEntry("min.compaction.lag.ms", String.valueOf(retentionMs)), AlterConfigOp.OpType.SET));
            configOps.add(new AlterConfigOp(new ConfigEntry("segment.ms", String.valueOf(7 * 24 * 60 * 60 * 1000L)), AlterConfigOp.OpType.SET));

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(topicResource, configOps);

            admin.incrementalAlterConfigs(configs).all().get();
            System.out.println("✅ Topic configurations updated successfully.");
            System.out.println();

            // Now upgrade feature to version 1
            // The controller will check the topic config and skip updates since it's already correct
            System.out.println("Upgrading feature to version 1...");
            Map<String, FeatureUpdate> updates = new HashMap<>();
            updates.put(
                org.apache.kafka.server.common.RemoteLogStorageVersion.FEATURE_NAME,
                new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE)
            );

            admin.updateFeatures(updates, new UpdateFeaturesOptions()).all().get();
            System.out.println("✅ Feature upgraded to version 1 successfully.");
            System.out.println();
            System.out.println("✅ Upgrade to version 1 completed successfully!");
            System.out.println();
            System.out.println("==================== NEXT STEPS ====================");
            System.out.println();
            System.out.println("CRITICAL: You must wait for the retention period (" + retentionDays + " days) before proceeding to version 2.");
            System.out.println();
            System.out.println("Why this waiting period is necessary:");
            System.out.println("1. During these " + retentionDays + " days:");
            System.out.println("   - Old-format (null-key) messages will naturally expire based on retention.ms=" + retentionMs + "ms");
            System.out.println("   - The log cleaner will NOT compact the topic yet (prevented by min.compaction.lag.ms=" + retentionMs + "ms)");
            System.out.println("   - This ensures null-key messages are deleted via retention, NOT via compaction");
            System.out.println();
            System.out.println("2. After waiting " + retentionDays + " days, run validation and upgrade to version 2:");
            System.out.println("   kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --check --auto-upgrade");
            System.out.println();
            System.out.println("3. The validation check will:");
            System.out.println("   - Scan the entire __remote_log_metadata topic for any remaining null-key messages");
            System.out.println("   - Only proceed with upgrade if NO null-key messages are found");
            System.out.println("   - Change cleanup.policy to 'compact' (removing 'delete')");
            System.out.println("   - Remove retention.ms and min.compaction.lag.ms overrides");
            System.out.println();
            System.out.println("===================================================");
        }
    }

    private static void checkForNullKeyMessages(String bootstrapServers, Properties baseProps, long timeoutMs, boolean autoUpgrade) throws Exception {
        // First, check the current topic configuration to remind users about the retention period
        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (Admin admin = Admin.create(adminProps)) {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);
            Config topicConfig = admin.describeConfigs(Collections.singleton(topicResource))
                .all().get().get(topicResource);

            ConfigEntry retentionMsEntry = topicConfig.get(TopicConfig.RETENTION_MS_CONFIG);
            ConfigEntry minCompactionLagMsEntry = topicConfig.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG);

            if (retentionMsEntry != null && retentionMsEntry.value() != null) {
                long retentionMs = Long.parseLong(retentionMsEntry.value());
                long retentionDays = retentionMs / (24 * 60 * 60 * 1000L);

                System.out.println("========== IMPORTANT REMINDER ==========");
                System.out.println();
                System.out.println("Current __remote_log_metadata topic configuration:");
                System.out.println("  - retention.ms=" + retentionMs + "ms (" + retentionDays + " days)");
                if (minCompactionLagMsEntry != null && minCompactionLagMsEntry.value() != null) {
                    long minCompactionLagMs = Long.parseLong(minCompactionLagMsEntry.value());
                    long minCompactionLagDays = minCompactionLagMs / (24 * 60 * 60 * 1000L);
                    System.out.println("  - min.compaction.lag.ms=" + minCompactionLagMs + "ms (" + minCompactionLagDays + " days)");
                }
                System.out.println();
                System.out.println("Before proceeding with this validation, ensure that:");
                System.out.println("1. At least " + retentionDays + " days have passed since upgrading to version 1");
                System.out.println("2. This allows all old-format (null-key) messages to expire via retention");
                System.out.println("3. The log cleaner has NOT compacted the topic yet (prevented by min.compaction.lag.ms)");
                System.out.println();
                System.out.println("If you upgraded to version 1 recently (less than " + retentionDays + " days ago),");
                System.out.println("you should WAIT before running this validation to avoid false negatives.");
                System.out.println();
                System.out.println("========================================");
                System.out.println();
            }
        } catch (Exception e) {
            // If we can't get the config, just log a warning and continue
            System.out.println("Warning: Could not retrieve topic configuration. Proceeding with validation anyway.");
            System.out.println("Error: " + e.getMessage());
            System.out.println();
        }

        Properties consumerProps = new Properties();
        consumerProps.putAll(baseProps);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "remote-log-metadata-migration-tool-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        System.out.println("Checking __remote_log_metadata topic for messages with null keys...");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Timeout: " + timeoutMs + "ms");
        if (autoUpgrade) {
            System.out.println("Auto-upgrade: ENABLED (will upgrade to version 2 if validation passes)");
        }
        System.out.println();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            // Get all partitions of the metadata topic
            List<TopicPartition> partitions = consumer.partitionsFor(METADATA_TOPIC)
                .stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

            if (partitions.isEmpty()) {
                System.out.println("✅ Topic " + METADATA_TOPIC + " does not exist or has no partitions.");
                System.out.println("✅ No null-key messages found. Safe to upgrade to version 2.");
                return;
            }

            System.out.println("Found " + partitions.size() + " partition(s) in " + METADATA_TOPIC);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            AtomicLong totalMessages = new AtomicLong(0);
            AtomicLong nullKeyMessages = new AtomicLong(0);
            long startTime = System.currentTimeMillis();
            boolean hasMoreRecords = true;

            System.out.println("Scanning messages...");

            while (hasMoreRecords && (System.currentTimeMillis() - startTime) < timeoutMs) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    // Check if we've reached the end of all partitions
                    hasMoreRecords = false;
                    for (TopicPartition partition : partitions) {
                        long position = consumer.position(partition);
                        long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
                        if (position < endOffset) {
                            hasMoreRecords = true;
                            break;
                        }
                    }
                } else {
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        totalMessages.incrementAndGet();

                        if (record.key() == null) {
                            nullKeyMessages.incrementAndGet();
                            System.out.println("⚠️  Found message with null key at partition=" + record.partition() +
                                ", offset=" + record.offset() + ", timestamp=" + record.timestamp());
                        }

                        // Print progress every 10000 messages
                        if (totalMessages.get() % 10000 == 0) {
                            System.out.println("Scanned " + totalMessages.get() + " messages so far...");
                        }
                    }
                }
            }

            System.out.println();
            System.out.println("Scan completed.");
            System.out.println("Total messages scanned: " + totalMessages.get());
            System.out.println("Messages with null keys: " + nullKeyMessages.get());
            System.out.println();

            if (nullKeyMessages.get() > 0) {
                System.out.println("❌ VALIDATION FAILED: Found " + nullKeyMessages.get() + " message(s) with null keys.");
                System.out.println();
                System.out.println("Action required:");
                System.out.println("1. Wait for these messages to expire based on retention.ms setting");
                System.out.println("2. Or increase retention.ms temporarily to allow more time for old messages to expire");
                System.out.println("3. Then run this tool again to verify all null-key messages are gone");
                System.out.println("4. Only then proceed with the upgrade to remote.log.storage.version=2");
                throw new TerseException("Cannot upgrade to version 2: null-key messages found in " + METADATA_TOPIC);
            } else {
                System.out.println("✅ VALIDATION PASSED: No null-key messages found.");
                System.out.println("✅ Safe to upgrade to remote.log.storage.version=2.");
                System.out.println();

                if (autoUpgrade) {
                    performAutoUpgrade(bootstrapServers, baseProps);
                } else {
                    System.out.println("To upgrade, run:");
                    System.out.println("  kafka-features.sh upgrade --bootstrap-server " + bootstrapServers + " --feature remote.log.storage.version=2");
                    System.out.println();
                    System.out.println("Or run this tool with --auto-upgrade to automatically upgrade:");
                    System.out.println("  kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --check --auto-upgrade");
                }
            }
        }
    }

    private static void performAutoUpgrade(String bootstrapServers, Properties baseProps) throws Exception {
        System.out.println("Initiating automatic upgrade to remote.log.storage.version=2...");
        System.out.println();

        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (Admin admin = Admin.create(adminProps)) {
            // First, check current version
            org.apache.kafka.clients.admin.FeatureMetadata featureMetadata =
                admin.describeFeatures().featureMetadata().get();

            org.apache.kafka.clients.admin.FinalizedVersionRange versionRange =
                featureMetadata.finalizedFeatures().get(org.apache.kafka.server.common.RemoteLogStorageVersion.FEATURE_NAME);

            short currentVersion = (versionRange != null) ? versionRange.maxVersionLevel() : 0;

            System.out.println("Current remote.log.storage.version: " + currentVersion);

            if (currentVersion == 0) {
                throw new TerseException(
                    "Cannot upgrade directly from version 0 to version 2. " +
                    "Must upgrade to version 1 first using: " +
                    "kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --upgrade-to-v1");
            }

            if (currentVersion == 2) {
                System.out.println("ℹ️  Already at version 2. No upgrade needed.");
                return;
            }

            if (currentVersion != 1) {
                throw new TerseException("Unexpected current version: " + currentVersion + ". Expected version 1.");
            }

            // Perform the upgrade from 1 to 2
            // The controller will automatically update topic configurations:
            //   - Change cleanup.policy to 'compact' (removing 'delete')
            //   - Remove min.compaction.lag.ms override
            //   - Remove retention.ms override
            System.out.println("Upgrading from version 1 to version 2...");
            Map<String, FeatureUpdate> updates = new HashMap<>();
            updates.put(
                org.apache.kafka.server.common.RemoteLogStorageVersion.FEATURE_NAME,
                new FeatureUpdate((short) 2, FeatureUpdate.UpgradeType.UPGRADE)
            );

            admin.updateFeatures(updates, new UpdateFeaturesOptions()).all().get();

            System.out.println();
            System.out.println("✅ Successfully upgraded to remote.log.storage.version=2!");
            System.out.println();
            System.out.println("The controller has automatically updated __remote_log_metadata topic configuration:");
            System.out.println("  - cleanup.policy changed to 'compact' (compact-only)");
            System.out.println("  - min.compaction.lag.ms override removed");
            System.out.println("  - retention.ms override removed");
            System.out.println();
            System.out.println("All metadata messages now have proper keys and will be retained indefinitely via compaction.");
        }
    }
}
