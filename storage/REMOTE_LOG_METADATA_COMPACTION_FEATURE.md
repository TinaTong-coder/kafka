# Remote Log Metadata Topic Compaction Feature

## Overview

This feature enables log compaction for the `__remote_log_metadata` topic, reducing storage overhead while maintaining all necessary metadata through Kafka's feature versioning mechanism.

## Design Summary

### Core Principle

- **Messages always include keys** (regardless of feature level)
- **Feature level controls topic cleanup policy only**
  - Level 0: `cleanup.policy=delete` (backward compatible)
  - Level 1: `cleanup.policy=compact` (new behavior)

### Implementation Components

#### 1. Feature Version Definition

**File**: `server-common/src/main/java/org/apache/kafka/server/common/RemoteLogStorageVersion.java`

```java
public enum RemoteLogStorageVersion implements FeatureVersion {
    RLS_V0(0, MetadataVersion.IBP_3_5_IV0, Map.of()),  // delete policy
    RLS_V1(1, MetadataVersion.IBP_4_1_IV0, Map.of());  // compact policy

    public static final String FEATURE_NAME = "remote.log.storage.version";
}
```

**Registered in**: `server-common/src/main/java/org/apache/kafka/server/common/Feature.java`

#### 2. Message Production

**File**: `storage/src/main/java/org/apache/kafka/server/log/remote/metadata/storage/ProducerManager.java`

- **Always produces messages with keys** (already implemented)
- Key format: `{topicId}:{partition}:{endOffset}:{brokerLeaderEpoch}`
- Works correctly with both delete and compact policies

#### 3. Controller Logic

**File**: `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`

When replaying `FeatureLevelRecord`:
```java
if (featureLevelRecord.name().equals(RemoteLogStorageVersion.FEATURE_NAME) &&
    featureLevelRecord.featureLevel() >= 1) {
    configurationControl.maybeUpdateRemoteLogMetadataTopicToCompacted();
}
```

**File**: `metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java`

Method `maybeUpdateRemoteLogMetadataTopicToCompacted()`:
- Checks if topic exists
- Checks if already using compaction
- Updates `cleanup.policy` to `compact` if needed

#### 4. Topic Creation

**File**: `storage/src/main/java/org/apache/kafka/server/log/remote/metadata/storage/TopicBasedRemoteLogMetadataManager.java`

- New clusters: Topic created with `cleanup.policy=compact` (feature V1 by default)
- Existing clusters: Topic already exists, controller updates policy on feature upgrade

## User Operations

### New Cluster

```bash
# Format cluster (remote.log.storage.version=1 automatically)
$ kafka-storage.sh format -t <cluster-id> -c config/kraft/server.properties

# Topic created with cleanup.policy=compact ✅
```

### Existing Cluster Upgrade

```bash
# 1. Upgrade all brokers to new code
# New messages automatically include keys

# 2. Check feature status
$ kafka-features.sh describe --bootstrap-server localhost:9092

# Output:
# Feature: remote.log.storage.version
#   FinalizedLevel: 0
#   MaxVersionLevel: 1     ← All brokers support V1

# 3. Upgrade feature
$ kafka-features.sh upgrade \
    --bootstrap-server localhost:9092 \
    --feature remote.log.storage.version=1

# Kafka automatically:
# ✓ Verifies all brokers support V1
# ✓ Updates feature level
# ✓ Controller updates topic to cleanup.policy=compact

# Controller logs:
# INFO Updating topic __remote_log_metadata cleanup policy to compact
# INFO Topic __remote_log_metadata is now configured for compaction
```

## Safety Guarantees

### 1. Feature Upgrade Validation

Kafka's feature mechanism ensures:
- All active brokers support the new feature level
- Any broker not supporting V1 will block the upgrade
- Prevents mixed-version scenarios automatically

### 2. Message Compatibility

- Old null-key messages (from before upgrade):
  - Expire naturally via `retention.ms`
  - Deleted by log cleaner before compaction
- New messages with keys:
  - Work correctly with delete policy (backward compatible)
  - Work correctly with compact policy (new behavior)

### 3. Rollback Support

```bash
# Downgrade feature (not recommended but supported)
$ kafka-features.sh downgrade \
    --bootstrap-server localhost:9092 \
    --feature remote.log.storage.version=0

# Effect:
# - Topic reverts to cleanup.policy=delete
# - Messages continue to include keys (no code change needed)
# - Already compacted messages remain compacted
```

## Benefits

### 1. Storage Reduction

- Tombstone messages clean up deleted segments
- Only latest metadata per segment kept
- Significant savings for long-running clusters

### 2. Simplified Operations

- Uses standard Kafka feature upgrade mechanism
- No custom migration tools needed
- Standard `kafka-features.sh` commands

### 3. Safe Migration Path

- Old messages expire naturally
- No need to reprocess/republish old messages
- Forward and backward compatible

## Technical Details

### Message Key Format

```
Base record:   {topicId}:{partition}:{endOffset}:{brokerLeaderEpoch}
Update record: {topicId}:{partition}:{endOffset}:{brokerLeaderEpoch}:UPDATE
Tombstone:     Same key with null value
```

### Compaction Behavior

With `cleanup.policy=compact`:
1. Log cleaner first deletes messages older than `retention.ms`
2. Then compacts remaining messages by key
3. Keeps only the latest value for each key
4. Tombstones (null values) trigger deletion of all records for that key

### Interaction with Retention

Old null-key messages are handled safely:
- They have no key → cannot be compacted
- They expire via `retention.ms` → deleted by time-based cleanup
- Once expired, they're removed before compaction runs

## Testing

### Verify Feature Registration

```bash
$ kafka-features.sh describe --bootstrap-server localhost:9092
# Should show remote.log.storage.version
```

### Verify Message Keys

```bash
$ kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic __remote_log_metadata \
    --property print.key=true \
    --from-beginning \
    --max-messages 10

# All recent messages should have keys
```

### Verify Topic Config After Upgrade

```bash
$ kafka-configs.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --entity-type topics \
    --entity-name __remote_log_metadata

# Should show: cleanup.policy=compact
```

## Files Modified

1. **server-common/src/main/java/org/apache/kafka/server/common/RemoteLogStorageVersion.java** (NEW)
   - Feature version definition

2. **server-common/src/main/java/org/apache/kafka/server/common/Feature.java**
   - Register REMOTE_LOG_STORAGE_VERSION feature

3. **metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java**
   - Add `maybeUpdateRemoteLogMetadataTopicToCompacted()` method

4. **metadata/src/main/java/org/apache/kafka/controller/QuorumController.java**
   - Handle feature upgrade in `FEATURE_LEVEL_RECORD` replay

5. **storage/src/main/java/org/apache/kafka/server/log/remote/metadata/storage/TopicBasedRemoteLogMetadataManager.java**
   - Always create topic with `cleanup.policy=compact`

6. **storage/src/main/java/org/apache/kafka/server/log/remote/metadata/storage/ProducerManager.java**
   - Already produces messages with keys (no change needed)

## References

- KIP-405: Kafka Tiered Storage
- Feature versioning: See `Feature.java` and `FeatureVersion.java`
- Log compaction: https://kafka.apache.org/documentation/#compaction
