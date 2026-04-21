# TimelineHashMap 实现详解

## 核心问题

**如何高效地获取 "小于等于 currentOffset 的最大 offset 对应的值"？**

这是 MVCC (Multi-Version Concurrency Control) 的核心操作。

---

## 错误实现：线性遍历

```java
// ❌ 错误：O(n) 时间复杂度
V get(K key) {
    List<VersionedValue<V>> timeline = data.get(key);
    if (timeline == null) return null;

    V result = null;
    for (VersionedValue<V> vv : timeline) {
        if (vv.offset <= currentOffset) {
            result = vv.value;  // 不断覆盖，最后得到最大的
        } else {
            break;
        }
    }
    return result;
}
```

**问题：**
- 时间复杂度 O(n)
- 即使 timeline 已排序，也需要遍历多个元素
- 性能随版本数线性增长

---

## 正确实现：TreeMap.floorEntry()

```java
// ✅ 正确：O(log n) 时间复杂度
static class TimelineHashMap<K, V> {
    private final Map<K, TreeMap<Long, V>> data = new HashMap<>();
    private long currentOffset = 0;

    V get(K key) {
        TreeMap<Long, V> timeline = data.get(key);
        if (timeline == null) return null;

        // floorEntry(offset): 返回 <= offset 的最大 key 的 entry
        Map.Entry<Long, V> entry = timeline.floorEntry(currentOffset);
        return entry != null ? entry.getValue() : null;
    }

    void put(K key, V value) {
        data.computeIfAbsent(key, k -> new TreeMap<>())
            .put(currentOffset, value);
    }
}
```

---

## TreeMap.floorEntry() 原理

`TreeMap` 是基于**红黑树**实现的有序 Map：

```java
TreeMap<Long, V> timeline = new TreeMap<>();
timeline.put(0L, "v0");
timeline.put(5L, "v5");
timeline.put(10L, "v10");

// floorEntry(offset): 返回 <= offset 的最大 key
timeline.floorEntry(3L);   // (0, "v0")  - 最大的 <= 3 的 key 是 0
timeline.floorEntry(5L);   // (5, "v5")  - 最大的 <= 5 的 key 是 5
timeline.floorEntry(7L);   // (5, "v5")  - 最大的 <= 7 的 key 是 5
timeline.floorEntry(15L);  // (10, "v10") - 最大的 <= 15 的 key 是 10
```

**时间复杂度：O(log n)** - 红黑树的查找时间

---

## TreeMap 的相关方法

| 方法 | 含义 | 示例 (timeline: 0, 5, 10) |
|------|------|---------------------------|
| `floorEntry(7)` | <= 7 的最大 key | 返回 (5, "v5") |
| `ceilingEntry(7)` | >= 7 的最小 key | 返回 (10, "v10") |
| `lowerEntry(5)` | < 5 的最大 key | 返回 (0, "v0") |
| `higherEntry(5)` | > 5 的最小 key | 返回 (10, "v10") |

对于 MVCC 的场景，我们需要的是 `floorEntry()`。

---

## 完整示例：MVCC 读取

```java
public class MVCCDemo {
    public static void main(String[] args) {
        TimelineHashMap<String, Integer> state = new TimelineHashMap<>();

        // 模拟 3 次写入
        state.setCurrentOffset(0);
        state.put("balance", 100);  // offset=0: balance=100

        state.setCurrentOffset(5);
        state.put("balance", 200);  // offset=5: balance=200

        state.setCurrentOffset(10);
        state.put("balance", 300);  // offset=10: balance=300

        // Timeline: {0→100, 5→200, 10→300}

        // 读取不同 offset 时刻的值
        state.setCurrentOffset(0);
        System.out.println("offset=0: " + state.get("balance"));  // 100

        state.setCurrentOffset(3);
        System.out.println("offset=3: " + state.get("balance"));  // 100 (使用 offset=0 的值)

        state.setCurrentOffset(5);
        System.out.println("offset=5: " + state.get("balance"));  // 200

        state.setCurrentOffset(7);
        System.out.println("offset=7: " + state.get("balance"));  // 200 (使用 offset=5 的值)

        state.setCurrentOffset(15);
        System.out.println("offset=15: " + state.get("balance")); // 300 (使用 offset=10 的值)
    }
}
```

**输出：**
```
offset=0: 100
offset=3: 100
offset=5: 200
offset=7: 200
offset=15: 300
```

---

## 回滚机制

```java
void revertTo(long offset) {
    this.currentOffset = offset;
    // 不删除数据，只修改 currentOffset
}
```

**示例：**
```java
// Timeline: {0→100, 5→200, 10→300}
state.setCurrentOffset(10);
System.out.println(state.get("balance"));  // 300

// 回滚到 offset=5
state.revertTo(5);
System.out.println(state.get("balance"));  // 200

// Timeline 数据还在：{0→100, 5→200, 10→300}
// 但 currentOffset=5，所以只能看到 offset <= 5 的数据
```

---

## 内存清理

在实际系统中，老版本的数据需要被清理以释放内存：

```java
void deleteUpTo(long offset) {
    for (TreeMap<Long, V> timeline : data.values()) {
        // 删除所有 > offset 的版本
        timeline.tailMap(offset + 1, true).clear();
    }
}
```

**示例：**
```java
// Timeline: {0→100, 5→200, 10→300}
state.setCurrentOffset(10);

// 当 HW 推进到 10 后，offset < 10 的版本不再需要
state.deleteUpTo(10);

// Timeline: {10→300}  - 老版本被删除
```

---

## Kafka 的真实实现

Kafka 的 `TimelineHashMap` 使用了类似的设计：

```java
// org.apache.kafka.timeline.TimelineHashMap
public class TimelineHashMap<K, V> {
    private final SnapshotRegistry snapshotRegistry;

    // 每个 key 对应一个 HashTier
    private final Map<K, HashTier<V>> tiers = new HashMap<>();

    static class HashTier<V> {
        // 使用 TreeMap 存储版本历史
        private final TreeMap<Long, V> versions = new TreeMap<>();

        V get(long epoch) {
            // epoch 就是 offset
            Map.Entry<Long, V> entry = versions.floorEntry(epoch);
            return entry != null ? entry.getValue() : null;
        }
    }

    public V get(Object key) {
        HashTier<V> tier = tiers.get(key);
        if (tier == null) return null;

        // 使用当前 epoch (offset) 读取
        return tier.get(snapshotRegistry.epoch());
    }
}
```

**核心设计：**
1. `TreeMap<Long, V>` 存储 `(offset → value)` 的映射
2. `floorEntry(offset)` 获取 `<= offset` 的最大版本
3. `tailMap().clear()` 删除老版本，释放内存

---

## 性能对比

| 操作 | List 线性遍历 | TreeMap.floorEntry() |
|------|---------------|----------------------|
| **get()** | O(n) | O(log n) |
| **put()** | O(1) | O(log n) |
| **revertTo()** | O(1) | O(1) |
| **deleteUpTo()** | O(n) | O(log n) |

**结论：**
- TreeMap 的 **get() 性能远优于线性遍历**
- 在版本数较多时（Kafka 中可能有数千个版本），差异更明显
- TreeMap 是实现 MVCC 的标准选择

---

## 总结

1. **TimelineHashMap 的核心操作**：获取 `<= currentOffset` 的最大 offset 的值
2. **正确实现**：使用 `TreeMap.floorEntry()`，时间复杂度 O(log n)
3. **错误实现**：使用 List 线性遍历，时间复杂度 O(n)
4. **Kafka 的实现**：`org.apache.kafka.timeline.TimelineHashMap` 使用 TreeMap
5. **关键优势**：高效的历史版本查询 + 支持回滚 + 内存可控

**这就是为什么 Kafka 使用 TreeMap 而不是 List 来实现 TimelineHashMap！**
