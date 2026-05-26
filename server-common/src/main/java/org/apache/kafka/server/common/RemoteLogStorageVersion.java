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
package org.apache.kafka.server.common;

import java.util.Map;

/**
 * Remote log storage feature version controls the cleanup policy for the __remote_log_metadata topic.
 *
 * <p>Note: Starting from the version that introduces keys (when this feature is added), all remote log
 * metadata messages are produced with keys regardless of the feature level. The feature level only
 * controls whether the topic uses compaction cleanup policy.</p>
 *
 * <ul>
 *   <li>Version 0: Topic uses delete cleanup policy. New code still writes keys for forward compatibility.</li>
 *   <li>Version 1: Topic uses compaction cleanup policy. All messages have keys, enabling effective compaction.</li>
 *   <li>Version 2: Removes min.compaction.lag.ms override, reverts to default. Validates no null-key messages remain.</li>
 * </ul>
 */
public enum RemoteLogStorageVersion implements FeatureVersion {

    /**
     * Version 0: Original implementation.
     * - Topic uses delete cleanup policy
     * - Messages are produced with keys (for forward compatibility)
     * - Compatible with clusters that have not yet enabled compaction
     */
    RLS_V0(0, MetadataVersion.IBP_3_5_IV0, Map.of()),

    /**
     * Version 1: Compaction enabled (targeting 4.6 release).
     * - Topic uses compaction cleanup policy
     * - Messages are produced with keys
     * - Enables space savings through log compaction
     * - Sets min.compaction.lag.ms to 14 days for safety during initial rollout
     */
    RLS_V1(1, MetadataVersion.IBP_4_4_IV0, Map.of()),

    /**
     * Version 2: Optimized compaction settings.
     * - Removes min.compaction.lag.ms override (reverts to default)
     * - Validates that no null-key messages remain in the topic before upgrade
     * - Allows more aggressive compaction for storage optimization
     */
    RLS_V2(2, MetadataVersion.IBP_4_4_IV0, Map.of());

    public static final String FEATURE_NAME = "remote.log.metadata.version";

    public static final RemoteLogStorageVersion LATEST_PRODUCTION = RLS_V2;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;
    private final Map<String, Short> dependencies;

    RemoteLogStorageVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion,
        Map<String, Short> dependencies
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.dependencies = dependencies;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return dependencies;
    }

    /**
     * Returns true if the __remote_log_metadata topic should use compaction cleanup policy.
     *
     * @return true if feature level >= 1, false otherwise
     */
    public boolean shouldUseCompaction() {
        return featureLevel >= RLS_V1.featureLevel();
    }

    /**
     * Converts a feature level to its corresponding enum value.
     *
     * @param version the feature level
     * @return the corresponding RemoteLogStorageVersion
     * @throws RuntimeException if the version is unknown
     */
    public static RemoteLogStorageVersion fromFeatureLevel(short version) {
        switch (version) {
            case 0:
                return RLS_V0;
            case 1:
                return RLS_V1;
            case 2:
                return RLS_V2;
            default:
                throw new RuntimeException("Unknown remote log storage feature level: " + (int) version);
        }
    }
}
