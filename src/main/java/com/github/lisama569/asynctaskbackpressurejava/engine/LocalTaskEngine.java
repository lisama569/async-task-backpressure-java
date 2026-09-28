package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.BackpressurePolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.EngineMetrics;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskEngine;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskFuture;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatusSnapshot;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地任务引擎。
 *
 * <p>结构：{@code 引擎 -> 每类型一条 TypePipeline（有界优先级队列 + worker 组）}，
 * 另含一个共享调度定时器（超时判定）、一个 {@link ResultStore}（去重/状态/结果）
 * 和周期性 TTL 清理。所有内部线程都是 daemon，关闭时统一回收。
 */
public final class LocalTaskEngine implements TaskEngine, EngineSupport {

    private static final Logger log = LoggerFactory.getLogger(LocalTaskEngine.class);

    private static final java.util.concurrent.atomic.AtomicLong ENGINE_SEQ =
            new java.util.concurrent.atomic.AtomicLong();

    private final String engineId = "engine-" + ENGINE_SEQ.incrementAndGet();
    private final long agingThresholdNanos;
    private final int storeMaxEntries;
    private final long storeTtlMillis;
    private final long sweepIntervalMillis;
    private final ConcurrentHashMap<String, TypePipeline> pipelines = new ConcurrentHashMap<>();
    private final ResultStore store;
    private final AtomicBoolean lifecycle = new AtomicBoolean(false);
    private final AtomicLong totalRejected = new AtomicLong();
    private final AtomicLong totalRetried = new AtomicLong();
    private final AtomicLong totalSubmitted = new AtomicLong();
    private final AtomicLong totalQueued = new AtomicLong();

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> sweepTask;

    public LocalTaskEngine() {
        this(2_000, 50_000, 120_000L, 10_000L);
    }

    /**
     * @param agingThresholdMillis NORMAL 任务等待多久后被提升为 HIGH 参与调度
     * @param storeMaxEntries      结果存储最多保留条目数（只淘汰终态）
     * @param storeTtlMillis       终态结果保留时长
     */
    public LocalTaskEngine(long agingThresholdMillis, int storeMaxEntries, long storeTtlMillis) {
        this(agingThresholdMillis, storeMaxEntries, storeTtlMillis, 10_000L);
    }

    /**
     * @param sweepIntervalMillis 终态结果 TTL 清理扫描周期
     */
    public LocalTaskEngine(long agingThresholdMillis, int storeMaxEntries, long storeTtlMillis,
                           long sweepIntervalMillis) {
        this.agingThresholdNanos = agingThresholdMillis * 1_000_000L;
        this.storeMaxEntries = storeMaxEntries;
        this.storeTtlMillis = storeTtlMillis;
        this.sweepIntervalMillis = sweepIntervalMillis;
        this.store = new ResultStore(storeMaxEntries, storeTtlMillis);
    }

    @Override
    public void register(String taskType, TaskHandler handler, int queueCapacity, int workers) {
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("taskType must not be blank");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        if (queueCapacity <= 0 || workers <= 0) {
            throw new IllegalArgumentException("queueCapacity and workers must be positive");
        }
        TypePipeline pipeline =
                new TypePipeline(taskType, handler, queueCapacity, workers, agingThresholdNanos, this);
        if (pipelines.putIfAbsent(taskType, pipeline) != null) {
            throw new IllegalStateException("task type already registered: " + taskType);
        }
        if (lifecycle.get()) {
            pipeline.start();
        }
    }

    @Override
    public TaskFuture submit(TaskRequest request, TaskSubmitOptions options) {
        validate(request, options);
        if (!lifecycle.get()) {
            totalRejected.incrementAndGet();
            throw new TaskRejectedException(TaskErrorCode.ENGINE_SHUTDOWN, request.requestId(),
                    "task engine is not running");
        }
        TypePipeline pipeline = pipelines.get(request.taskType());
        if (pipeline == null) {
            totalRejected.incrementAndGet();
            throw new TaskRejectedException(TaskErrorCode.UNKNOWN_TASK_TYPE, request.requestId(),
                    "unknown task type: " + request.taskType());
        }
        long now = System.nanoTime();
        long deadlineNanos = options.deadline() == null ? 0L
                : now + Math.max(1L, options.deadline().toNanos());
        TaskContext context = new TaskContext(request.requestId(), request.taskType(),
                request.payload());
        TaskEntry entry = new TaskEntry(request.requestId(), context, options, now, deadlineNanos);

        // putIfAbsent 保证同一 requestId 只有一个任务进入系统（不重复执行、幂等去重）。
        TaskEntry existing = store.putIfAbsent(entry);
        if (existing != null) {
            totalRejected.incrementAndGet();
            throw new TaskRejectedException(TaskErrorCode.DUPLICATE_REQUEST, request.requestId(),
                    "duplicate request id, current status: " + existing.status());
        }
        totalSubmitted.incrementAndGet();

        boolean admitted = pipeline.admit(entry);
        if (!admitted) {
            // 入队失败：让该 requestId 的存储条目直接进入 REJECTED 终态，
            // 调用方可凭 requestId 查询到明确判定，而不是“查无此任务”。
            entry.completeRejected(now);
            throw new TaskRejectedException(TaskErrorCode.QUEUE_FULL, request.requestId(),
                    "task queue full for type: " + request.taskType());
        }
        totalQueued.incrementAndGet();
        logSubmit(entry);
        return new LocalTaskFuture(entry, this);
    }

    private void logSubmit(TaskEntry entry) {
        TypePipeline p = pipelines.get(entry.taskType());
        log.info("task accepted: requestId={}, taskType={}, priority={}, queueSize={}, options=(timeout={},deadline={},attempts={},backpressure={})",
                entry.requestId(), entry.taskType(), entry.options().priority(),
                p == null ? -1 : p.queueSize(), entry.options().timeout(),
                entry.options().deadline(), entry.options().maxAttempts(),
                entry.options().backpressure());
    }

    private void validate(TaskRequest request, TaskSubmitOptions options) {
        if (request == null) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, null, "request is null");
        }
        if (request.requestId() == null || request.requestId().isBlank()) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, request.requestId(),
                    "requestId must not be blank");
        }
        if (request.taskType() == null || request.taskType().isBlank()) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, request.requestId(),
                    "taskType must not be blank");
        }
        if (options == null || options.timeout() == null || options.timeout().isNegative()
                || options.maxAttempts() < 1 || options.retryBackoff() == null
                || options.backpressure() == null) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, request.requestId(),
                    "invalid submit options");
        }
    }

    @Override
    public boolean cancel(String requestId) {
        TaskEntry entry = store.get(requestId);
        if (entry == null) {
            return false;
        }
        if (entry.isTerminal()) {
            return false;
        }
        TypePipeline pipeline = pipelines.get(entry.taskType());
        if (pipeline != null && pipeline.cancelQueued(requestId)) {
            log.info("task cancelled while queued: requestId={}, taskType={}",
                    requestId, entry.taskType());
            return true;
        }
        // 不在队列里 => 正在执行（或退避），发中断，终态由 worker 收敛。
        entry.interruptRunning();
        log.info("task cancel requested while running: requestId={}, taskType={}",
                requestId, entry.taskType());
        return true;
    }

    @Override
    public TaskStatusSnapshot statusOf(String requestId) {
        return store.snapshot(requestId);
    }

    @Override
    public TaskResult resultOf(String requestId) {
        return store.result(requestId);
    }

    @Override
    public EngineMetrics metrics() {
        return buildMetrics();
    }

    private EngineMetrics buildMetrics() {
        boolean running = lifecycle.get();
        List<EngineMetrics.TypeMetrics> types = new ArrayList<>(pipelines.size());
        for (TypePipeline p : pipelines.values()) {
            types.add(p.metricsSnapshot());
        }
        long queuedNow = 0L;
        long runningNow = 0L;
        long succeeded = 0L;
        long failed = 0L;
        long cancelled = 0L;
        long timeout = 0L;
        for (EngineMetrics.TypeMetrics m : types) {
            queuedNow += m.queueSize();
            runningNow += m.activeWorkers();
            succeeded += m.succeeded();
            failed += m.failed();
            cancelled += m.cancelled();
            timeout += m.timeout();
        }
        return new EngineMetrics(running, pipelines.size(), totalSubmitted.get(), queuedNow,
                runningNow, succeeded, failed, cancelled, timeout,
                totalRejected.get(), totalRetried.get(), store.size(), types);
    }

    @Override
    public void start() {
        if (!lifecycle.compareAndSet(false, true)) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-engine-scheduler-" + engineId);
            t.setDaemon(true);
            return t;
        });
        for (TypePipeline p : pipelines.values()) {
            p.start();
        }
        sweepTask = scheduler.scheduleAtFixedRate(() -> {
            try {
                int removed = store.sweep();
                if (removed > 0) {
                    log.debug("result store swept: removed={}, size={}", removed, store.size());
                }
            } catch (Throwable t) {
                log.warn("sweep error (ignored)", t);
            }
        }, sweepIntervalMillis, sweepIntervalMillis, TimeUnit.MILLISECONDS);
        log.info("task engine started: types={}, agingThresholdMs={}, storeMax={}, storeTtlMs={}",
                pipelines.size(), agingThresholdNanos / 1_000_000L,
                storeMaxEntries, storeTtlMillis);
    }

    @Override
    public void shutdown() {
        if (!lifecycle.compareAndSet(true, false)) {
            return;
        }
        log.info("task engine shutting down...");
        if (sweepTask != null) {
            sweepTask.cancel(false);
        }
        for (TypePipeline p : pipelines.values()) {
            p.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        log.info("task engine stopped");
    }

    @Override
    public String engineId() {
        return engineId;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delayMillis) {
        return scheduler.schedule(command, Math.max(0L, delayMillis), TimeUnit.MILLISECONDS);
    }

    @Override
    public void incrementRetryCount() {
        totalRetried.incrementAndGet();
    }
}
