package com.example.future;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 🔥 RequestFuture 详细注释版
 *
 * 这个类是整个异步框架的核心！
 * 通过详细注释，帮助你理解每一行代码的作用。
 *
 * 关键概念：
 * 1. Future 代表一个"未来的结果"
 * 2. 可以在 Future 上注册 Listener，当结果完成时回调
 * 3. compose() 方法用于类型转换（这是三层架构的关键！）
 */
public class RequestFutureAnnotated<T> {

    // ============ 字段说明 ============

    /**
     * 哨兵值：标记 Future 尚未完成
     *
     * 为什么用哨兵值？
     * - 因为泛型 T 可能是任何类型，包括 null
     * - 所以不能用 null 来表示"未完成"
     * - INCOMPLETE 是一个特殊对象，只用于标记状态
     */
    private static final Object INCOMPLETE = new Object();

    /**
     * 存储 Future 的结果
     *
     * 可能的值：
     * 1. INCOMPLETE       - 未完成
     * 2. T 类型的值       - 成功完成
     * 3. RuntimeException - 失败
     *
     * 为什么用 AtomicReference？
     * - 保证线程安全（虽然我们是单线程，但为了代码健壮性）
     * - 提供 compareAndSet 原子操作（确保只能完成一次）
     */
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE);

    /**
     * 监听器队列
     *
     * 为什么用 ConcurrentLinkedQueue？
     * 1. 线程安全（无锁实现）
     * 2. FIFO 顺序（先注册的先触发）
     * 3. 高性能（基于 CAS）
     *
     * 存储什么？
     * - 所有注册的 RequestFutureListener
     * - 当 Future 完成时，会遍历并触发所有 listener
     */
    private final ConcurrentLinkedQueue<RequestFutureListener<T>> listeners =
        new ConcurrentLinkedQueue<>();

    // ============ 状态查询方法 ============

    /**
     * 检查 Future 是否已完成
     *
     * 工作原理：
     * - 如果 result != INCOMPLETE，说明已完成（成功或失败）
     */
    public boolean isDone() {
        return result.get() != INCOMPLETE;
    }

    /**
     * 检查 Future 是否成功完成
     *
     * 工作原理：
     * 1. 先检查是否完成（isDone）
     * 2. 再检查不是失败（!failed）
     * 3. 两个条件都满足 → 成功
     */
    public boolean succeeded() {
        return isDone() && !failed();
    }

    /**
     * 检查 Future 是否失败
     *
     * 工作原理：
     * - 如果 result 是 RuntimeException 的实例 → 失败
     */
    public boolean failed() {
        return result.get() instanceof RuntimeException;
    }

    // ============ 结果获取方法 ============

    /**
     * 获取成功的值
     *
     * 注意：
     * - 只有成功完成才能调用
     * - 如果未完成或失败，会抛异常
     */
    @SuppressWarnings("unchecked")
    public T value() {
        if (!succeeded()) {
            throw new IllegalStateException("Future has not succeeded");
        }
        return (T) result.get();
    }

    /**
     * 获取失败的异常
     *
     * 注意：
     * - 只有失败才能调用
     * - 如果未完成或成功，会抛异常
     */
    public RuntimeException exception() {
        if (!failed()) {
            throw new IllegalStateException("Future has not failed");
        }
        return (RuntimeException) result.get();
    }

    // ============ 完成方法 ============

    /**
     * 🔥 完成 Future（成功）
     *
     * 工作流程：
     * 1. 检查 value 不是异常（防止误用）
     * 2. 使用 CAS 设置结果（保证只能完成一次）
     * 3. 如果 CAS 失败 → 说明已经完成过了 → 抛异常
     * 4. 如果 CAS 成功 → 触发所有监听器
     *
     * 为什么用 CAS？
     * - 原子操作，避免竞争条件
     * - 即使多个线程同时调用 complete()，也只有一个会成功
     */
    public void complete(T value) {
        // 第 1 步：检查参数
        if (value instanceof RuntimeException) {
            throw new IllegalArgumentException("Value cannot be an exception");
        }

        // 第 2 步：尝试设置结果（CAS）
        // compareAndSet(期望值, 新值)
        // 如果当前值 == 期望值（INCOMPLETE），则设置为新值（value）
        // 返回 true 表示成功，false 表示失败
        if (!result.compareAndSet(INCOMPLETE, value)) {
            throw new IllegalStateException("Future is already complete");
        }

        // 第 3 步：触发监听器
        fireSuccess();
    }

    /**
     * 🔥 完成 Future（失败）
     *
     * 工作流程：与 complete() 类似，只是设置的是异常
     */
    public void raise(RuntimeException e) {
        if (e == null) {
            throw new IllegalArgumentException("Exception cannot be null");
        }

        if (!result.compareAndSet(INCOMPLETE, e)) {
            throw new IllegalStateException("Future is already complete");
        }

        fireFailure();
    }

    // ============ 监听器管理 ============

    /**
     * 🔥 添加监听器
     *
     * 工作流程：
     * 1. 将 listener 加入队列
     * 2. 检查 Future 是否已完成
     * 3. 如果已完成 → 立即触发 listener
     *
     * 为什么要立即触发？
     * - 避免竞争条件
     * - 场景：
     *   线程 A: complete() → fireSuccess() → 遍历 listeners（此时为空）
     *   线程 B: addListener(listener) → 加入队列
     *   结果：listener 永远不会被触发！
     * - 解决方案：addListener 时检查是否已完成，如果是则立即触发
     */
    public void addListener(RequestFutureListener<T> listener) {
        // 第 1 步：加入队列
        listeners.add(listener);

        // 第 2 步：检查是否已完成
        if (failed()) {
            fireFailure();  // 已失败 → 触发失败回调
        } else if (succeeded()) {
            fireSuccess();  // 已成功 → 触发成功回调
        }
        // 如果未完成 → 什么都不做，等待 complete/raise 触发
    }

    // ============ 内部方法：触发监听器 ============

    /**
     * 触发成功监听器
     *
     * 工作流程：
     * 1. 获取成功的值
     * 2. 遍历所有监听器
     * 3. 调用每个 listener.onSuccess(value)
     *
     * 为什么用 while + poll()？
     * - poll() 会从队列中移除元素
     * - 避免重复触发
     * - 线程安全
     */
    private void fireSuccess() {
        T value = value();  // 获取结果值

        while (true) {
            // 从队列取出一个 listener
            RequestFutureListener<T> listener = listeners.poll();

            // 队列空了 → 退出循环
            if (listener == null) {
                break;
            }

            // 调用 listener
            listener.onSuccess(value);
        }
    }

    /**
     * 触发失败监听器
     *
     * 工作流程：与 fireSuccess() 类似
     */
    private void fireFailure() {
        RuntimeException exception = exception();

        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null) {
                break;
            }
            listener.onFailure(exception);
        }
    }

    // ============ 核心方法：compose - 类型转换 ============

    /**
     * 🔥🔥🔥 compose - 三层 Future 架构的核心！
     *
     * 功能：将 RequestFuture<T> 转换为 RequestFuture<S>
     *
     * 使用场景：
     * - RequestFuture<Response> → RequestFuture<HttpResult>
     * - 网络层 Future → 业务层 Future
     *
     * 工作原理：
     * 1. 创建一个新的 Future（目标类型 S）
     * 2. 给当前 Future（源类型 T）添加 listener
     * 3. 当前 Future 完成时 → 调用 adapter 转换 → 完成新 Future
     * 4. 返回新 Future
     *
     * 图解：
     * ```
     * RequestFuture<T>  (this)
     *   ↓ addListener
     *   ├─ onSuccess(T value)
     *   │    ↓
     *   │    adapter.onSuccess(value, adapted)
     *   │    ↓
     *   │    转换 T → S
     *   │    ↓
     *   │    adapted.complete(S value)
     *   │
     *   └─ onFailure(Exception e)
     *        ↓
     *        adapter.onFailure(e, adapted)
     *        ↓
     *        adapted.raise(e)
     *
     * RequestFuture<S>  (adapted)
     *   ↓ 返回给调用者
     * ```
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        // ========== 步骤 1: 创建新 Future ==========
        // adapted 是目标类型（S）的 Future
        final RequestFuture<S> adapted = new RequestFuture<>();

        // ========== 步骤 2: 给当前 Future 添加 listener ==========
        // 这个 listener 是"桥梁"，连接两个 Future
        addListener(new RequestFutureListener<T>() {

            // ===== 当前 Future 成功时 =====
            @Override
            public void onSuccess(T value) {
                // 调用 adapter 转换
                // 注意：adapter 内部会调用 adapted.complete(s)
                adapter.onSuccess(value, adapted);

                // 📌 关键理解：
                // - 这里不直接完成 adapted
                // - 而是把控制权交给 adapter
                // - adapter 负责转换 T → S，并完成 adapted
                // - 这样 adapter 可以做更复杂的逻辑（错误处理、验证等）
            }

            // ===== 当前 Future 失败时 =====
            @Override
            public void onFailure(RuntimeException e) {
                // 调用 adapter 处理失败
                adapter.onFailure(e, adapted);

                // 📌 关键理解：
                // - adapter 可以决定如何处理错误
                // - 可以直接传播：adapted.raise(e)
                // - 也可以转换：adapted.raise(new CustomException(e))
            }
        });

        // ========== 步骤 3: 返回新 Future ==========
        return adapted;

        // 📌 总结：
        // compose() 创建了一个"转换链"：
        //   当前 Future 完成 → listener 触发 → adapter 转换 → 新 Future 完成
        //
        // 这就是为什么可以链式调用：
        //   future1.compose(adapter1).compose(adapter2).compose(adapter3)
    }

    // ============ 静态工厂方法 ============

    /**
     * 创建一个已成功的 Future
     *
     * 使用场景：
     * - 测试
     * - 缓存命中（直接返回缓存结果）
     */
    public static <T> RequestFuture<T> success(T value) {
        RequestFuture<T> future = new RequestFuture<>();
        future.complete(value);
        return future;
    }

    /**
     * 创建一个已失败的 Future
     *
     * 使用场景：
     * - 参数验证失败
     * - 前置条件不满足
     */
    public static <T> RequestFuture<T> failure(RuntimeException e) {
        RequestFuture<T> future = new RequestFuture<>();
        future.raise(e);
        return future;
    }

    // ============ 设计思想总结 ============

    /*
     * 🎯 这个类的核心设计思想：
     *
     * 1. **延迟计算**
     *    - Future 不立即计算结果
     *    - 而是等待某个时刻（complete/raise）
     *
     * 2. **观察者模式**
     *    - 可以注册多个 listener
     *    - Future 完成时，通知所有 listener
     *
     * 3. **类型转换**
     *    - compose() 实现 Future<T> → Future<S>
     *    - 不破坏 Future 的语义
     *    - 保持异步性
     *
     * 4. **线程安全**
     *    - 使用 AtomicReference + CAS
     *    - 使用 ConcurrentLinkedQueue
     *    - 确保多线程环境下正确工作
     *
     * 5. **一次性**
     *    - Future 只能完成一次
     *    - 使用 CAS 保证
     *    - 重复完成会抛异常
     *
     * 🔥 与 Kafka 的 RequestFuture 完全相同的设计！
     */
}
