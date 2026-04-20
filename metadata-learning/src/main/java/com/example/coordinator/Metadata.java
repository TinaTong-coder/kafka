package com.example.coordinator;

import com.example.cache.Cluster;
import com.example.cache.MetadataCache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 元数据管理器 - 核心版本化缓存设计
 *
 * 职责:
 * 1. 管理topic metadata缓存
 * 2. 版本号控制（updateVersion vs requestVersion）
 * 3. 跨线程协作（User Thread等待Sender Thread更新）
 * 4. Copy-On-Write缓存更新
 *
 * 线程模型:
 * - User Thread: 调用fetch()读取，调用requestUpdate()请求更新，调用awaitUpdate()等待
 * - Sender Thread: 调用update()写入新metadata
 */
public class Metadata {
    private final long refreshBackoffMs;   // 两次刷新的最小间隔（避免频繁请求）
    private final long metadataExpireMs;   // metadata过期时间

    // === 版本号（核心设计）===
    private int updateVersion = 0;   // 每次收到MetadataResponse时++
    private int requestVersion = 0;  // 每次添加新topic时++

    // === 时间戳 ===
    private long lastRefreshMs = 0;           // 最后一次发送请求的时间
    private long lastSuccessfulRefreshMs = 0; // 最后一次成功更新的时间

    // === 缓存 ===
    private MetadataCache cache = MetadataCache.empty();

    // === 更新标志 ===
    private boolean needFullUpdate = false;     // 是否需要完整更新
    private boolean needPartialUpdate = false;  // 是否需要增量更新

    // === Topic集合 ===
    private Set<String> topics = new HashSet<>();  // 当前关注的topics

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
    }

    /**
     * 获取当前集群信息（不阻塞）
     *
     * 调用栈:
     * [User Thread]
     * send(record)
     * └─ cluster = metadata.fetch()
     *    └─ synchronized { return cache.cluster(); }  // 快速获取引用
     *       └─ 释放锁后安全读取（cache不可变）
     */
    public synchronized Cluster fetch() {
        return cache.cluster();
    }

    /**
     * 添加topic到关注列表
     * 如果是新topic，会递增requestVersion
     */
    public synchronized void add(String topic) {
        if (topics.add(topic)) {
            // 新topic，递增requestVersion
            requestVersion++;
            needPartialUpdate = true;
        }
    }

    /**
     * 请求完整更新，返回当前updateVersion
     *
     * 调用栈:
     * [User Thread]
     * send(record)
     * ├─ cluster = fetch()
     * ├─ partition = cluster.partitionCountForTopic(topic)
     * └─ if (partition == null) {  // topic不存在
     *      oldVersion = metadata.requestUpdate();  ← 这里
     *      └─ synchronized {
     *          needFullUpdate = true;
     *          return updateVersion;  // 返回当前版本号（等会儿用于等待）
     *         }
     *    }
     *
     * 为什么返回updateVersion？
     * - User Thread需要知道"请求更新之前"的版本号
     * - 然后等待updateVersion变化（说明更新完成）
     */
    public synchronized int requestUpdate() {
        this.needFullUpdate = true;
        return this.updateVersion;
    }

    /**
     * 请求增量更新（只更新新添加的topic）
     */
    public synchronized int requestUpdateForNewTopics() {
        this.needPartialUpdate = true;
        this.requestVersion++;
        return this.updateVersion;
    }

    /**
     * 是否需要更新
     */
    public synchronized boolean updateRequested() {
        return needFullUpdate || needPartialUpdate;
    }

    /**
     * 等待metadata更新到指定版本之后
     *
     * @param lastVersion 旧的updateVersion（调用requestUpdate()返回的值）
     * @param timeoutMs 超时时间
     * @throws InterruptedException 被中断
     * @throws TimeoutException 超时
     *
     * 调用栈:
     * [User Thread]
     * send(record)
     * ├─ oldVersion = metadata.requestUpdate();  // oldVersion = 5
     * ├─ metadata.awaitUpdate(oldVersion=5, 60000)  ← 这里
     * │  └─ synchronized {
     * │      while (updateVersion == 5) {  // 还没更新
     * │        wait(timeout);  ───────────────┐ (释放锁，等待通知)
     * │      }                   ←────────────┼────────┐
     * │     }                    notifyAll()  │        │
     * │                                       │        │
     * └─ cluster = metadata.fetch()  // 拿到新数据    │        │
     *                                                 │        │
     *                          [Sender Thread]        │        │
     *                          poll()                 │        │
     *                          └─ update(response)    │        │
     *                             └─ synchronized {   │        │
     *                                 updateVersion++;  (5→6)  │
     *                                 notifyAll(); ───────────┘
     *                                }
     */
    public synchronized void awaitUpdate(int lastVersion, long timeoutMs)
            throws InterruptedException, TimeoutException {
        long startMs = System.currentTimeMillis();
        long remainingMs = timeoutMs;

        while (updateVersion <= lastVersion) {
            if (remainingMs <= 0) {
                throw new TimeoutException(
                    String.format("Timeout waiting for metadata update. " +
                        "Last version: %d, current version: %d", lastVersion, updateVersion)
                );
            }

            // wait会释放锁，被notify后重新获取锁
            wait(remainingMs);

            long elapsed = System.currentTimeMillis() - startMs;
            remainingMs = timeoutMs - elapsed;
        }

        // 循环退出说明updateVersion > lastVersion，metadata已更新
    }

    /**
     * 计算距离下次允许更新的时间（backoff机制）
     *
     * @param nowMs 当前时间
     * @return 剩余时间（ms），0表示可以立即更新
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        long timeSinceLastRefresh = nowMs - this.lastRefreshMs;
        long timeToWait = this.refreshBackoffMs - timeSinceLastRefresh;
        return Math.max(0, timeToWait);
    }

    /**
     * 计算距离下次需要更新的时间
     *
     * @param nowMs 当前时间
     * @return 剩余时间（ms），0表示需要立即更新
     *
     * 调用栈:
     * [Sender Thread]
     * poll()
     * └─ timeToNextUpdate = metadata.timeToNextUpdate(now);
     *    ├─ if (updateRequested()) return 0;  // 有强制更新请求
     *    ├─ timeToExpire = lastSuccessfulRefreshMs + metadataExpireMs - now
     *    │  └─ if (timeToExpire <= 0) return 0;  // 过期了
     *    └─ return max(timeToExpire, timeToAllowUpdate(now));
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        // 如果有强制更新请求，立即更新
        if (updateRequested()) {
            return 0;
        }

        // 计算距离过期的时间
        long timeSinceLastSuccess = nowMs - this.lastSuccessfulRefreshMs;
        long timeToExpire = this.metadataExpireMs - timeSinceLastSuccess;

        // 如果已过期，立即更新
        if (timeToExpire <= 0) {
            return 0;
        }

        // 返回max(过期时间, backoff时间)
        // 确保既不过期，也不违反backoff限制
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    /**
     * 更新metadata（Sender Thread调用）
     *
     * @param requestVersion 发送请求时的requestVersion快照
     * @param response metadata响应
     * @param isPartialUpdate 是否是增量更新
     * @param nowMs 当前时间
     *
     * 调用栈:
     * [Sender Thread]
     * poll()
     * ├─ if (timeToNextUpdate() <= 0) {
     * │   requestVersionSnapshot = requestVersion();  // 快照
     * │   sendMetadataRequest(requestVersionSnapshot);
     * │  }
     * │
     * └─ handleMetadataResponse(response)
     *    └─ metadata.update(requestVersionSnapshot, response, false, now)  ← 这里
     *       └─ synchronized {
     *           updateVersion++;  // 递增版本号
     *           cache = new MetadataCache(response);  // COW更新
     *           notifyAll();  // 唤醒等待的User Thread
     *          }
     */
    public synchronized void update(int requestVersion, Map<String, Integer> response,
                                    boolean isPartialUpdate, long nowMs) {
        // 检测是否有新的topic添加（需要再次更新）
        this.needPartialUpdate = requestVersion < this.requestVersion;

        this.lastRefreshMs = nowMs;
        this.updateVersion++;  // 核心：递增版本号

        if (isPartialUpdate) {
            // 增量更新：合并
            this.cache = cache.mergeWith(response);
        } else {
            // 完整更新：替换
            this.cache = new MetadataCache(response);
            this.needFullUpdate = false;
            this.lastSuccessfulRefreshMs = nowMs;
        }

        System.out.println("[Metadata] Updated to version " + updateVersion + ": " + cache);

        // 唤醒所有等待的线程
        notifyAll();
    }

    /**
     * 记录更新失败（用于backoff）
     */
    public synchronized void failedUpdate(long nowMs) {
        this.lastRefreshMs = nowMs;
    }

    // === Getters (for testing) ===

    public synchronized int updateVersion() {
        return updateVersion;
    }

    public synchronized int requestVersion() {
        return requestVersion;
    }

    public synchronized boolean needsUpdate(long nowMs) {
        return timeToNextUpdate(nowMs) <= 0;
    }
}
