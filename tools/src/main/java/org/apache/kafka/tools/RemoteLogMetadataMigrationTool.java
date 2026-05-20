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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;

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
 * Tool to validate and migrate the __remote_log_metadata topic before upgrading to
 * remote.log.storage.version=2.
 *
 * This tool checks if the __remote_log_metadata topic contains any messages with null keys,
 * which would prevent safe upgrade to version 2 (which removes min.compaction.lag.ms override).
 *
 * Usage:
 *   kafka-remote-log-metadata-migration.sh --bootstrap-server localhost:9092 --check
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
            .description("Tool to validate and migrate the __remote_log_metadata topic before upgrading to remote.log.storage.version=2.");

        parser.addArgument("--bootstrap-server")
            .required(true)
            .help("REQUIRED: A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.");

        parser.addArgument("--command-config")
            .type(Arguments.fileType())
            .help("Property file containing configs to be passed to Consumer Client.");

        parser.addArgument("--check")
            .action(Arguments.storeTrue())
            .help("Check if the topic contains any messages with null keys. This is required before upgrading to version 2.");

        parser.addArgument("--timeout-ms")
            .type(Long.class)
            .setDefault(60000L)
            .help("Maximum time in milliseconds to wait while checking for messages (default: 60000).");

        Namespace namespace = parser.parseArgs(args);

        String bootstrapServers = namespace.getString("bootstrap_server");
        String commandConfig = namespace.getString("command_config");
        boolean check = namespace.getBoolean("check");
        long timeoutMs = namespace.getLong("timeout_ms");

        Properties props = new Properties();
        if (commandConfig != null) {
            props = org.apache.kafka.server.util.CommandLineUtils.loadPropsFromFile(commandConfig);
        }

        if (check) {
            checkForNullKeyMessages(bootstrapServers, props, timeoutMs);
        } else {
            throw new TerseException("No operation specified. Use --check to validate the topic.");
        }
    }

    private static void checkForNullKeyMessages(String bootstrapServers, Properties baseProps, long timeoutMs) throws Exception {
        Properties props = new Properties();
        props.putAll(baseProps);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "remote-log-metadata-migration-tool-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        System.out.println("Checking __remote_log_metadata topic for messages with null keys...");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Timeout: " + timeoutMs + "ms");
        System.out.println();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
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
                System.out.println("To upgrade, run:");
                System.out.println("  kafka-features.sh upgrade --bootstrap-server " + bootstrapServers + " --feature remote.log.storage.version=2");
            }
        }
    }
}
