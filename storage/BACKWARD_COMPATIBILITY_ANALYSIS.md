# Remote Log Metadata Topic Compaction Feature

## Overview

This feature enables safe compaction of the `__remote_log_metadata` topic through a three-version upgrade path controlled by `remote.log.storage.version`.

**Note**: The upgrade path is only required for **existing clusters** that already have the `__remote_log_metadata` topic. New clusters automatically use version 2.

## Message Format

### Old Format (Version 0)
- Key: `null`
- Value: Serialized `RemoteLogMetadata`
- Cannot be compacted (null keys are deleted during compaction)

### New Format (Version 1+)
- Key: `topicId:topicName:partition:endOffset:brokerLeaderEpoch` or `topicId:topicName:partition:endOffset:brokerLeaderEpoch:UPDATE`
- Value: Serialized `RemoteLogMetadata`
- Base record key: `topicId:topicName:partition:endOffset:brokerLeaderEpoch`
- Update record key: `topicId:topicName:partition:endOffset:brokerLeaderEpoch:UPDATE`

## Upgrade Path

### Why Three Versions?

Kafka's log cleaner immediately deletes null-key messages during compaction. Existing clusters may have millions of null-key messages in `__remote_log_metadata`. Direct upgrade to compact-only policy would cause immediate data loss.

The three-version approach:
1. **Version 0**: Current state (delete-only, may contain null-key messages)
2. **Version 1**: Transition state (compact+delete, allows null-key messages to expire via retention while preventing compaction)
3. **Version 2**: Final state (compact-only, all messages have keys)

### Version 0 (Default)
- Topic: `cleanup.policy=delete` or not yet created
- Messages: May have null keys
- Behavior: No compaction, messages expire via retention

### Version 1 (Transition)
- Topic: `cleanup.policy=compact,delete`
- Configuration:
  - `retention.ms`: User-specified (e.g., 14 days)
  - `min.compaction.lag.ms`: Same as retention.ms
  - `segment.ms`: 7 days
- Messages: New messages have keys, old messages expire via retention
- Purpose: Allow null-key messages to expire via retention BEFORE log cleaner begins compacting

### Version 2 (Final)
- Topic: `cleanup.policy=compact`
- Configuration: Uses broker defaults (retention.ms and min.compaction.lag.ms overrides removed)
- Messages: All messages have keys, retained indefinitely via compaction

## Cluster Scenarios

### Scenario 1: New Cluster (Fresh Install)
```bash
kafka-storage.sh format -t <cluster-id> -c server.properties
kafka-server-start.sh server.properties
```
- `remote.log.storage.version` automatically set to 2 (LATEST_PRODUCTION)
- Topic created with `cleanup.policy=compact`
- All messages have keys from the start
- **No migration required**

### Scenario 2: Existing Cluster with Tiered Storage Already Enabled
Cluster already has `__remote_log_metadata` topic with potential null-key messages.

**Upgrade Path** (0 → 1 → 2):

**Step 1: Upgrade to Version 1**
```bash
kafka-remote-log-metadata-migration.sh \
  --bootstrap-server localhost:9092 \
  --upgrade-to-v1 \
  --retention-ms 1209600000  # 14 days
```
Result:
- Topic config: `cleanup.policy=compact,delete`, `retention.ms=1209600000`, `min.compaction.lag.ms=1209600000`, `segment.ms=604800000`
- Feature: `remote.log.storage.version=1`
- New messages have keys, old null-key messages will expire via retention

**Step 2: Wait for Retention Period**
- Wait at least 14 days (or your specified retention period)
- Allows all null-key messages to expire naturally
- Log cleaner won't compact yet (blocked by min.compaction.lag.ms)

**Step 3: Validate and Upgrade to Version 2**
```bash
kafka-remote-log-metadata-migration.sh \
  --bootstrap-server localhost:9092 \
  --check \
  --upgrade-to-v2
```
Result:
- Tool displays retention reminder and checks if enough time has passed
- Scans topic for null-key messages
- Records last null-key message timestamp and suggests retry time if validation fails
- If validation passes:
  - Feature: `remote.log.storage.version=2`
  - Topic config: `cleanup.policy=compact` (min.compaction.lag.ms and retention.ms overrides removed)

**Force Upgrade (Not Recommended)**:
```bash
kafka-remote-log-metadata-migration.sh \
  --bootstrap-server localhost:9092 \
  --check \
  --upgrade-to-v2 \
  --force
```
Use `--force` to upgrade even if null-key messages exist. **Warning**: Null-key messages will be lost during compaction.

### Scenario 3: Existing Cluster Enabling Tiered Storage for First Time
```bash
# Edit server.properties
remote.log.storage.system.enable=true

# Restart broker
kafka-server-start.sh server.properties

# Manually upgrade feature
kafka-features.sh upgrade --feature remote.log.storage.version=2
```
- Feature version starts at 0 (not automatically upgraded)
- Topic will be created on first use
- Can manually upgrade directly to version 2 (no null-key messages exist yet)
- **No migration required** if upgraded before topic creation

## Key Concepts

**Why retention.ms = min.compaction.lag.ms in Version 1**:
- `retention.ms`: Null-key messages expire after this period
- `min.compaction.lag.ms`: Log cleaner waits this long before compacting
- Setting both equal ensures null-key messages expire via retention BEFORE compaction begins
- Prevents data loss from premature compaction

## Feature Gate Enforcement

### FeatureControlManager
Prevents direct upgrade from version 0 to 2:
```java
if (currentVersion == 0 && newVersion == 2) {
    return error("Must upgrade to version 1 first");
}
```

### ConfigurationControlManager
Automatically updates topic configuration during feature upgrades:
- Version 1: Applies `compact,delete` policy with retention settings
- Version 2: Changes to `compact` policy and removes overrides

## Migration Tool

### Commands

**Upgrade to Version 1**:
```bash
kafka-remote-log-metadata-migration.sh \
  --bootstrap-server localhost:9092 \
  --upgrade-to-v1 \
  --retention-ms 1209600000
```

**Validate and Upgrade to Version 2**:
```bash
kafka-remote-log-metadata-migration.sh \
  --bootstrap-server localhost:9092 \
  --check \
  --upgrade-to-v2
```

### Safety Features
- Validates current version before upgrade
- Scans entire topic for null-key messages
- Records last null-key message timestamp
- Calculates message age and suggests retry time
- Displays retention period reminder before validation
- Only upgrades if no null-key messages found (unless `--force` is used)
- `--force` flag available to bypass validation (use with caution)

## Compatibility

### New Code Reading Old Messages
✅ Compatible - Consumer reads only the value field

### Tombstone Functionality
❌ Incompatible with null-key messages - Cannot generate tombstones for segments without keys

### Rollback
⚠️ Version 2 is effectively one-way. Downgrading requires:
1. Change `cleanup.policy` back to `delete`
2. Wait for natural message expiration
3. Consider data loss implications

## Testing

### Unit Tests
- ✅ New code deserializes old format messages
- ✅ Tombstone generation with keys
- ✅ Version 1 upgrade scenarios

### Integration Tests
- ⏳ Full upgrade path (0 → 1 → 2)
- ⏳ Validation failure scenarios
- ⏳ Mixed version clusters

## References
- Commit introducing keys: `b92795741b`
- FeatureControlManager: `metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java`
- ConfigurationControlManager: `metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java`
- Migration Tool: `tools/src/main/java/org/apache/kafka/tools/RemoteLogMetadataMigrationTool.java`
