/**
 * 简化版的 Coordinator 架构示例
 * 帮助理解核心概念
 */

// ============ 1. CoordinatorShard 接口 ============
// 这是你需要实现的状态机
interface CoordinatorShard<Record> {
    // 重放记录，更新内存状态
    void replay(long offset, Record record);

    // 加载完成后的回调
    void onLoaded();

    // 卸载时的清理
    void onUnloaded();
}

// ============ 2. ShareCoordinatorShard 实现 ============
class ShareCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    // 使用 TimelineHashMap 支持 MVCC（多版本并发控制）
    private final TimelineHashMap<SharePartitionKey, ShareGroupOffset> shareStateMap;
    private final SnapshotRegistry snapshotRegistry;

    public ShareCoordinatorShard(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        this.shareStateMap = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    @Override
    public void replay(long offset, CoordinatorRecord record) {
        // 根据记录类型更新状态
        if (record.key() instanceof ShareSnapshotKey) {
            // 完整快照 - 直接替换
            ShareSnapshotKey key = (ShareSnapshotKey) record.key();
            ShareSnapshotValue value = (ShareSnapshotValue) record.value();

            SharePartitionKey mapKey = SharePartitionKey.from(key);
            shareStateMap.put(mapKey, ShareGroupOffset.fromRecord(value));

        } else if (record.key() instanceof ShareUpdateKey) {
            // 增量更新 - 合并到现有状态
            ShareUpdateKey key = (ShareUpdateKey) record.key();
            ShareUpdateValue value = (ShareUpdateValue) record.value();

            SharePartitionKey mapKey = SharePartitionKey.from(key);
            shareStateMap.compute(mapKey, (k, existing) ->
                merge(existing, value)
            );
        }
    }

    // 写操作：生成需要写入 log 的 records
    public CoordinatorResult<WriteResponse, CoordinatorRecord> writeState(
        WriteStateRequest request
    ) {
        // 1. 验证请求
        validateRequest(request);

        // 2. 生成记录（但不修改状态！状态由 replay 修改）
        CoordinatorRecord record;
        if (shouldWriteSnapshot()) {
            record = generateSnapshotRecord(request);
        } else {
            record = generateUpdateRecord(request);
        }

        // 3. 构造响应
        WriteResponse response = new WriteResponse(SUCCESS);

        // 4. 返回结果
        // CoordinatorRuntime 会：
        //   a) 将 record 写入 log
        //   b) 调用 replay() 更新状态
        //   c) 等待 HW 推进后返回 response 给客户端
        return new CoordinatorResult<>(
            List.of(record),  // records 列表
            response,         // 最终返回给客户端的响应
            true              // replayRecords = true (需要 replay)
        );
    }

    // 读操作：只读取已提交的数据
    public ReadResponse readState(
        ReadStateRequest request,
        long committedOffset  // 只能读到这个 offset 的数据
    ) {
        // TimelineHashMap 会自动使用 committedOffset 对应的快照
        SharePartitionKey key = SharePartitionKey.from(request);
        ShareGroupOffset offset = shareStateMap.get(key);

        return new ReadResponse(offset);
    }
}

// ============ 3. CoordinatorRuntime ============
class CoordinatorRuntime<S extends CoordinatorShard<U>, U> {
    // 每个 partition 一个 context
    private final Map<TopicPartition, CoordinatorContext> coordinators;

    // 事件处理器（多线程）
    private final CoordinatorEventProcessor eventProcessor;

    // 写入 Kafka log
    private final PartitionWriter partitionWriter;

    // 调度写操作
    public <T> CompletableFuture<T> scheduleWriteOperation(
        String name,
        TopicPartition tp,
        CoordinatorWriteOperation<S, T, U> op
    ) {
        // 1. 创建写事件
        CoordinatorWriteEvent<T> event = new CoordinatorWriteEvent<>(name, tp, op);

        // 2. 加入事件队列
        eventProcessor.enqueue(event);

        // 3. 返回 future（稍后由事件处理器完成）
        return event.future;
    }

    // 内部类：CoordinatorContext
    class CoordinatorContext {
        final TopicPartition tp;
        final ReentrantLock lock;  // 保护 context 状态

        // 状态机（带 MVCC 支持）
        SnapshottableCoordinator<S, U> coordinator;

        // 状态：INITIAL -> LOADING -> ACTIVE -> CLOSED
        volatile CoordinatorState state;

        // 当前正在构建的 batch
        CoordinatorBatch currentBatch;

        // 等待 HW 推进的事件
        DeferredEventQueue deferredEventQueue;

        // 定时器
        EventBasedCoordinatorTimer timer;

        // 处理写操作
        void append(
            List<U> records,
            boolean replay,
            DeferredEvent event
        ) {
            if (records.isEmpty()) {
                // 纯读操作，等待之前的写完成即可
                waitForPendingWrites(event);
                return;
            }

            // 1. 分配或复用 batch
            maybeAllocateNewBatch();

            // 2. Replay 到状态机 + 写入 batch
            for (U record : records) {
                if (replay) {
                    // 更新内存状态
                    coordinator.replay(currentBatch.nextOffset, record);
                }

                // 序列化并添加到 batch
                currentBatch.builder.append(serialize(record));
                currentBatch.nextOffset++;
            }

            // 3. 将 event 加入 batch 的待完成列表
            currentBatch.deferredEvents.add(event);

            // 4. 检查是否需要 flush
            maybeFlushCurrentBatch();
        }

        void flushCurrentBatch() {
            if (currentBatch == null) return;

            try {
                // 写入 Kafka log
                long offset = partitionWriter.append(
                    tp,
                    currentBatch.builder.build()
                );

                // 更新 lastWrittenOffset
                coordinator.updateLastWrittenOffset(offset);

                // 将关联的事件加入 deferred queue
                // 等待 HW 到达 offset 后才 complete
                deferredEventQueue.add(offset, currentBatch.deferredEvents);

                // 释放 batch
                freeCurrentBatch();

            } catch (Exception e) {
                // Flush 失败，回滚状态并 fail 所有事件
                coordinator.revertLastWrittenOffset(currentBatch.baseOffset);
                currentBatch.deferredEvents.complete(e);
                freeCurrentBatch();
            }
        }
    }

    // 内部类：CoordinatorWriteEvent
    class CoordinatorWriteEvent<T> implements CoordinatorEvent, DeferredEvent {
        final TopicPartition tp;
        final String name;
        final CoordinatorWriteOperation<S, T, U> op;
        final CompletableFuture<T> future;

        CoordinatorResult<T, U> result;

        @Override
        public TopicPartition key() {
            return tp;  // EventProcessor 使用这个做分片
        }

        @Override
        public void run() {
            // 在 EventProcessor 线程中执行
            CoordinatorContext context = coordinators.get(tp);

            context.lock.lock();
            try {
                // 1. 执行业务逻辑，生成 records
                result = op.generateRecordsAndResult(context.coordinator.shard());

                // 2. Append records 并 replay
                context.append(
                    result.records(),
                    result.replayRecords(),
                    this  // 将自己作为 DeferredEvent
                );

            } finally {
                context.lock.unlock();
            }
        }

        @Override
        public void complete(Throwable exception) {
            // 由 DeferredEventQueue 在 HW 推进时调用
            if (exception == null) {
                future.complete(result.response());
            } else {
                future.completeExceptionally(exception);
            }
        }
    }

    // HighWatermark 监听器
    class HighWatermarkListener implements PartitionWriter.Listener {
        @Override
        public void onHighWatermarkUpdated(TopicPartition tp, long offset) {
            // 推送一个内部事件到队列
            eventProcessor.enqueueFirst(new CoordinatorInternalEvent(tp, () -> {
                CoordinatorContext context = coordinators.get(tp);

                context.lock.lock();
                try {
                    // 更新 lastCommittedOffset
                    context.coordinator.updateLastCommittedOffset(offset);

                    // Complete 所有 offset <= HW 的事件
                    context.deferredEventQueue.completeUpTo(offset);

                } finally {
                    context.lock.unlock();
                }
            }));
        }
    }
}

// ============ 4. 使用示例 ============
class ShareCoordinatorService {
    private final CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime;

    public CompletableFuture<WriteShareGroupStateResponse> writeShareGroupState(
        WriteShareGroupStateRequest request
    ) {
        // 1. 计算这个 group 对应的 partition
        TopicPartition tp = partitionFor(request.groupId());

        // 2. 调度写操作
        return runtime.scheduleWriteOperation(
            "WriteShareGroupState",
            tp,
            Duration.ofSeconds(30),
            // Lambda: 定义操作如何执行
            shard -> shard.writeState(request)
        ).thenApply(responseData ->
            new WriteShareGroupStateResponse(responseData)
        );
    }

    public CompletableFuture<ReadShareGroupStateResponse> readShareGroupState(
        ReadShareGroupStateRequest request
    ) {
        TopicPartition tp = partitionFor(request.groupId());

        // 读操作
        return runtime.scheduleReadOperation(
            "ReadShareGroupState",
            tp,
            // Lambda: 接收 shard 和 committedOffset
            (shard, committedOffset) -> shard.readState(request, committedOffset)
        ).thenApply(responseData ->
            new ReadShareGroupStateResponse(responseData)
        );
    }
}

// ============ 5. MVCC 原理示例 ============
class MVCCExample {
    public static void main(String[] args) {
        SnapshotRegistry registry = new SnapshotRegistry();
        TimelineHashMap<String, Integer> map = new TimelineHashMap<>(registry, 0);

        // 写操作
        map.put("key1", 100);  // offset 0
        registry.getOrCreateSnapshot(0);

        map.put("key1", 200);  // offset 1
        registry.getOrCreateSnapshot(1);

        map.put("key1", 300);  // offset 2
        registry.getOrCreateSnapshot(2);

        // 读操作
        // 读 offset 0 的快照
        registry.revertToSnapshot(0);
        System.out.println(map.get("key1"));  // 输出: 100

        // 读 offset 1 的快照
        registry.revertToSnapshot(1);
        System.out.println(map.get("key1"));  // 输出: 200

        // 读 offset 2 的快照
        registry.revertToSnapshot(2);
        System.out.println(map.get("key1"));  // 输出: 300
    }
}

// ============ 6. 事件流程总结 ============
/*
 * Write 请求完整流程:
 *
 * 1. ShareCoordinatorService.writeShareGroupState(request)
 *    └─> runtime.scheduleWriteOperation(tp, op)
 *
 * 2. CoordinatorRuntime 创建 CoordinatorWriteEvent 并加入队列
 *
 * 3. CoordinatorEventProcessor 从队列取出事件
 *    └─> event.run() [在 processor 线程执行]
 *
 * 4. CoordinatorWriteEvent.run()
 *    ├─> 获取 context.lock
 *    ├─> op.generateRecordsAndResult(shard)  [业务逻辑]
 *    │   └─> shard.writeState(request)
 *    │       └─> return CoordinatorResult(records, response)
 *    └─> context.append(records, this)
 *
 * 5. CoordinatorContext.append()
 *    ├─> coordinator.replay(record)  [更新内存状态]
 *    ├─> batch.builder.append(record)
 *    ├─> batch.deferredEvents.add(event)
 *    └─> maybeFlushCurrentBatch()
 *
 * 6. CoordinatorContext.flushCurrentBatch()
 *    ├─> partitionWriter.append(records)  [写入 Kafka log]
 *    └─> deferredEventQueue.add(offset, events)
 *
 * 7. [异步] Log 复制到 followers，HW 推进
 *
 * 8. HighWatermarkListener.onHighWatermarkUpdated(offset)
 *    └─> 推送 HighWatermarkUpdate 事件到队列
 *
 * 9. CoordinatorContext 处理 HW 更新
 *    ├─> coordinator.updateLastCommittedOffset(offset)
 *    └─> deferredEventQueue.completeUpTo(offset)
 *
 * 10. CoordinatorWriteEvent.complete(null)
 *     └─> future.complete(response)
 *
 * 11. ShareCoordinatorService 的 future 完成
 *     └─> 返回 response 给客户端
 */
