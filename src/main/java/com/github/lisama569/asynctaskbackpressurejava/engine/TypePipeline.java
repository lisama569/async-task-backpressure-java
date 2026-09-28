package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.BackpressurePolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.EngineMetrics;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一种任务类型一条独立流水线：独立的有界优先级队列 + 固定数量的调度 worker。
 *
 * <p>故障隔离：A 类型的任务把自己的队列塞满只会让 A 的提交者承受背压；
 * A 的 handler 持续失败或不响应中断，也只消耗 A 自己的 worker，
 * B 类型的队列与 worker 完全不受影响。
 *
 * <p>执行模型：worker 线程直接执行用户 handler，并负责超时与重试编排。
 * 这样超时/取消的 {@code interrupt} 目标准确；若 handler 不响应中断，
 * 最坏后果仅限本类型的 worker 被占用，配合有界队列与背压不会波及其它类型。
 */
final class TypePipeline {

    private static final Logger log = LoggerFactory.getLogger(TypePipeline.class);

    private final String taskType;
    private final TaskHandler handler;
    private final int workers;
    private final AgingPriorityQueue queue;
    private final PipelineCounters counters = new PipelineCounters();
    private final EngineSupport support;
    private final Thread[] workerThreads;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile boolean shutdown;

    TypePipeline(String taskType, TaskHandler handler, int queueCapacity, int workers,
                 long agingThresholdNanos, EngineSupport support) {
        this.taskType = taskType;
        this.handler = handler;
        this.workers = workers;
        this.support = support;
        this.queue = new AgingPriorityQueue(queueCapacity, agingThresholdNanos);
        this.workerThreads = new Thread[workers];
    }

    String taskType() {
        return taskType;
    }

    int queueSize() {
        return queue.size();
    }

    long oldestQueuedAgeMillis() {
        return queue.oldestAgeMillis(System.nanoTime());
    }

    /**
     * 按背压策略让任务进入本类型队列。调用前任务已注册进 ResultStore。
     *
     * @return true 入队成功；false 队列满（REJECT 或 BLOCK 等待超时）
     */
    boolean admit(TaskEntry entry) {
        counters.submitted.incrementAndGet();
        try {
            if (entry.options().backpressure() == BackpressurePolicy.BLOCK
                    && entry.options().maxBlockWait() != null
                    && !entry.options().maxBlockWait().isZero()) {
                boolean ok = queue.offer(entry,
                        entry.options().maxBlockWait().toMillis(), TimeUnit.MILLISECONDS);
                if (!ok) {
                    counters.rejected.incrementAndGet();
                }
                return ok;
            }
            boolean ok = queue.tryOffer(entry);
            if (!ok) {
                counters.rejected.incrementAndGet();
            }
            return ok;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            counters.rejected.incrementAndGet();
            return false;
        }
    }

    /** 排队中取消：从队列摘除并由 entry 自身收敛为 CANCELLED。 */
    boolean cancelQueued(String requestId) {
        TaskEntry entry = queue.removeIfQueued(requestId);
        if (entry == null) {
            return false;
        }
        boolean cancelled = entry.cancelWhileQueued(System.nanoTime());
        if (cancelled) {
            counters.cancelled.incrementAndGet();
        }
        return cancelled;
    }

    void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        shutdown = false;
        for (int i = 0; i < workers; i++) {
            Thread t = new Thread(this::workerLoop,
                    "task-worker-" + support.engineId() + "-" + taskType + "-" + i);
            t.setDaemon(true);
            workerThreads[i] = t;
            t.start();
        }
        log.info("pipeline started: taskType={}, workers={}, capacity={}",
                taskType, workers, queue.capacity());
    }

    void shutdown() {
        shutdown = true;
        queue.signalAllForShutdown();
        for (Thread t : workerThreads) {
            if (t != null) {
                t.interrupt();
            }
        }
    }

    EngineMetrics.TypeMetrics metricsSnapshot() {
        return new EngineMetrics.TypeMetrics(taskType, queue.capacity(), queue.size(),
                (int) counters.active.get(), workers,
                counters.submitted.get(), counters.succeeded.get(), counters.failed.get(),
                counters.cancelled.get(), counters.timeout.get(), counters.rejected.get(),
                counters.retried.get(), oldestQueuedAgeMillis());
    }

    private void workerLoop() {
        while (!shutdown) {
            TaskEntry entry;
            try {
                entry = queue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (shutdown) {
                    return;
                }
                continue;
            }
            if (entry == null) {
                continue;
            }
            if (!entry.claimForRun(System.nanoTime())) {
                // 已被取消收敛或已被他人领取（理论上不会发生）：跳过，保证不重复执行。
                continue;
            }
            counters.active.incrementAndGet();
            try {
                runWithRetries(entry);
            } catch (Throwable t) {
                // 兜底：编排期的任何意外都收敛为可解释失败，绝不让异常逃出 worker。
                log.error("worker orchestration error, requestId={}", entry.requestId(), t);
                entry.complete(TaskStatus.FAILED, TaskErrorCode.EXECUTION_ERROR,
                        "internal orchestration failure", null, System.nanoTime());
                counters.failed.incrementAndGet();
            } finally {
                counters.active.decrementAndGet();
            }
        }
    }

    /** 持有唯一 RUNNING 许可期间完成所有 attempt 与退避，终态只在这里收敛。 */
    private void runWithRetries(TaskEntry entry) {
        int maxAttempts = Math.max(1, entry.options().maxAttempts());
        while (true) {
            if (entry.context().isCancelled()) {
                finishTerminal(entry, TaskStatus.CANCELLED, TaskErrorCode.CANCELLED,
                        "task cancelled", null);
                return;
            }
            if (deadlineExceeded(entry)) {
                finishTerminal(entry, TaskStatus.TIMEOUT, TaskErrorCode.DEADLINE_EXCEEDED,
                        "task deadline exceeded before attempt " + entry.attemptCount(), null);
                return;
            }
            AttemptOutcome outcome = runSingleAttempt(entry);
            if (outcome.success) {
                if (entry.complete(TaskStatus.SUCCEEDED, null, null, outcome.value,
                        System.nanoTime())) {
                    counters.succeeded.incrementAndGet();
                }
                return;
            }
            // 取消与截止到期优先级最高，不触发重试。
            if (entry.context().isCancelled()) {
                finishTerminal(entry, TaskStatus.CANCELLED, TaskErrorCode.CANCELLED,
                        "task cancelled during execution", null);
                return;
            }
            if (deadlineExceeded(entry)) {
                finishTerminal(entry, TaskStatus.TIMEOUT, TaskErrorCode.DEADLINE_EXCEEDED,
                        "task deadline exceeded", null);
                return;
            }
            boolean hasAttemptLeft = entry.attemptCount() < maxAttempts;
            BackoffEnd backoffEnd = hasAttemptLeft ? sleepBackoff(entry) : BackoffEnd.NO_BUDGET;
            switch (backoffEnd) {
                case CANCELLED -> finishTerminal(entry, TaskStatus.CANCELLED, TaskErrorCode.CANCELLED,
                        "task cancelled during backoff", null);
                case DEADLINE -> finishTerminal(entry, TaskStatus.TIMEOUT,
                        TaskErrorCode.DEADLINE_EXCEEDED,
                        "task deadline reached before next attempt", null);
                case NO_BUDGET -> {
                    if (outcome.timedOut) {
                        finishTerminal(entry, TaskStatus.TIMEOUT, TaskErrorCode.DEADLINE_EXCEEDED,
                                "execution timed out after " + entry.attemptCount() + " attempt(s)",
                                null);
                    } else {
                        finishTerminal(entry, TaskStatus.FAILED, TaskErrorCode.EXECUTION_ERROR,
                                safeMessage(outcome.error), null);
                    }
                }
                case SLEPT -> {
                    entry.prepareNextAttempt();
                    counters.retried.incrementAndGet();
                    support.incrementRetryCount();
                    log.info("retrying task: requestId={}, taskType={}, nextAttempt={}, queueSize={}",
                            entry.requestId(), taskType, entry.attemptCount(), queue.size());
                }
            }
            if (backoffEnd != BackoffEnd.SLEPT) {
                return;
            }
        }
    }

    private boolean deadlineExceeded(TaskEntry entry) {
        long d = entry.deadlineNanos();
        return d > 0 && System.nanoTime() >= d;
    }

    /**
     * 退避睡眠并给出确定结论：
     * SLEPT——完整退避结束且仍在 deadline 之内，可发起下一次 attempt；
     * DEADLINE——剩余时间不足以容纳一次完整退避，睡眠到截止时刻后结束，不得再发起 attempt；
     * CANCELLED——等待中被取消中断。
     */
    private BackoffEnd sleepBackoff(TaskEntry entry) {
        long backoffMillis = entry.options().retryBackoff().toMillis();
        long d = entry.deadlineNanos();
        if (d > 0) {
            long remainingMillis = Math.max(0L, (d - System.nanoTime()) / 1_000_000L);
            if (remainingMillis < backoffMillis) {
                sleepQuietly(remainingMillis);
                return BackoffEnd.DEADLINE;
            }
        }
        sleepQuietly(backoffMillis);
        return entry.context().isCancelled() ? BackoffEnd.CANCELLED : BackoffEnd.SLEPT;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 退避结论。 */
    private enum BackoffEnd {
        SLEPT, DEADLINE, CANCELLED, NO_BUDGET
    }

    private void finishTerminal(TaskEntry entry, TaskStatus status,
                                TaskErrorCode code, String message, String value) {
        if (entry.complete(status, code, message, value, System.nanoTime())) {
            switch (status) {
                case SUCCEEDED -> counters.succeeded.incrementAndGet();
                case CANCELLED -> counters.cancelled.incrementAndGet();
                case TIMEOUT -> counters.timeout.incrementAndGet();
                default -> counters.failed.incrementAndGet();
            }
            log.info("task finished: requestId={}, taskType={}, verdict={}, attempts={}, queueSize={}",
                    entry.requestId(), taskType, status, entry.attemptCount(), queue.size());
        }
    }

    private AttemptOutcome runSingleAttempt(TaskEntry entry) {
        AttemptOutcome outcome = new AttemptOutcome();
        AtomicBoolean timerFired = new AtomicBoolean(false);
        Thread workerThread = Thread.currentThread();
        long timeoutMillis = entry.options().timeout().toMillis();

        ScheduledFuture<?> timeoutTask = null;
        if (timeoutMillis > 0) {
            long millis = timeoutMillis;
            timeoutTask = support.schedule(() -> {
                if (timerFired.compareAndSet(false, true)) {
                    log.warn("attempt timeout fired: requestId={}, taskType={}, attempt={}, timeoutMillis={}",
                            entry.requestId(), taskType, entry.attemptCount(), millis);
                    workerThread.interrupt();
                }
            }, timeoutMillis);
        }
        try {
            outcome.value = handler.handle(entry.context());
            // handler 正常返回即视为副作用已完成：即便定时器恰在此刻触发或收到取消，
            // 也按 SUCCEEDED 收敛，不能向调用方谎报失败而导致其重复提交产生第二次副作用。
            outcome.success = true;
        } catch (Throwable t) {
            outcome.success = false;
            outcome.timedOut = timerFired.get();
            outcome.error = t;
        } finally {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
            // 定时器可能在 handler 返回的瞬间置位中断；清掉以免污染后续退避睡眠。
            Thread.interrupted();
        }
        return outcome;
    }

    private static String safeMessage(Throwable t) {
        if (t == null) {
            return "execution failed";
        }
        String name = t.getClass().getSimpleName();
        String msg = t.getMessage() == null ? name : name + ": " + t.getMessage();
        // 截断，避免把过长的内部异常细节直接暴露给调用方。
        return msg.length() > 200 ? msg.substring(0, 200) : msg;
    }

    private static final class AttemptOutcome {
        boolean success;
        boolean timedOut;
        String value;
        Throwable error;
    }
}
