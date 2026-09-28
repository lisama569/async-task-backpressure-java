package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 单个任务的全部可变状态。
 *
 * <p>状态机只有 {@code QUEUED -> RUNNING -> 终态} 三条迁移，全部用 CAS 完成：
 * <ul>
 *   <li>{@code QUEUED -> RUNNING} 只有一个 worker 能成功，因此同一任务不可能被执行两次；</li>
 *   <li>重试不重新入队，worker 在 RUNNING 许可内做 attempt 循环和退避睡眠，
 *       取消与超时通过线程中断穿透退避，因此重试中的任务也不会“消失”或被重复领取；</li>
 *   <li>{@code -> 终态} 只发生一次，{@link #completionFuture()} 也只 complete 一次。</li>
 * </ul>
 */
final class TaskEntry {

    private final String requestId;
    private final String taskType;
    private final TaskContext context;
    private final TaskSubmitOptions options;
    private final long enqueuedAtNanos;
    private final long deadlineNanos;
    private final CompletableFuture<TaskResult> completion = new CompletableFuture<>();
    private final AtomicReference<TaskStatus> state = new AtomicReference<>(TaskStatus.QUEUED);

    private volatile long firstRunStartNanos;
    private volatile long lastRunStartNanos;
    private volatile long attemptCount = 1;
    private volatile TaskResult terminalResult;
    private volatile long completedAtNanos;
    /** 正在执行当前 attempt 的线程；退避睡眠期间也是它，供取消/超时中断。 */
    private volatile Thread runnerThread;

    TaskEntry(String requestId, TaskContext context, TaskSubmitOptions options,
              long nowNanos, long deadlineNanos) {
        this.requestId = requestId;
        this.taskType = context.taskType();
        this.context = context;
        this.options = options;
        this.enqueuedAtNanos = nowNanos;
        this.deadlineNanos = deadlineNanos;
    }

    String requestId() {
        return requestId;
    }

    String taskType() {
        return taskType;
    }

    TaskContext context() {
        return context;
    }

    TaskSubmitOptions options() {
        return options;
    }

    long enqueuedAtNanos() {
        return enqueuedAtNanos;
    }

    /** 0 表示不设整体截止时间。 */
    long deadlineNanos() {
        return deadlineNanos;
    }

    long firstRunStartNanos() {
        return firstRunStartNanos;
    }

    long lastRunStartNanos() {
        return lastRunStartNanos;
    }

    long attemptCount() {
        return attemptCount;
    }

    TaskStatus status() {
        return state.get();
    }

    CompletableFuture<TaskResult> completionFuture() {
        return completion;
    }

    boolean isTerminal() {
        return state.get().isTerminal();
    }

    /** worker 从队列取到任务后调用：CAS 保证全局只有一个线程能领取成功。 */
    boolean claimForRun(long nowNanos) {
        if (!state.compareAndSet(TaskStatus.QUEUED, TaskStatus.RUNNING)) {
            return false;
        }
        if (firstRunStartNanos == 0) {
            firstRunStartNanos = nowNanos;
        }
        lastRunStartNanos = nowNanos;
        runnerThread = Thread.currentThread();
        return true;
    }

    /** 进入下一次 attempt（仍持有 RUNNING 许可）。 */
    void prepareNextAttempt() {
        attemptCount++;
        context.advanceAttempt();
    }

    /** 排队中取消：QUEUED -> CANCELLED。 */
    boolean cancelWhileQueued(long nowNanos) {
        if (!state.compareAndSet(TaskStatus.QUEUED, TaskStatus.CANCELLED)) {
            return false;
        }
        context.markCancelled();
        finish(buildResult(TaskStatus.CANCELLED, null, TaskErrorCode.CANCELLED,
                "task cancelled while queued", nowNanos));
        return true;
    }

    /**
     * 执行/退避中取消：标记取消并中断 worker 线程。最终状态由被中断的 worker 收敛为 CANCELLED，
     * 若此刻恰好执行成功，成功判定优先（副作用已发生，不能再谎称取消）。
     */
    void interruptRunning() {
        if (state.get() != TaskStatus.RUNNING) {
            return;
        }
        context.markCancelled();
        Thread t = runnerThread;
        if (t != null) {
            t.interrupt();
        }
    }

    /** worker 或超时定时器在终态收敛时调用；CAS 保证只有一方写入终态。 */
    boolean complete(TaskStatus terminal, TaskErrorCode errorCode, String message,
                     String value, long nowNanos) {
        if (!state.compareAndSet(TaskStatus.RUNNING, terminal)) {
            return false;
        }
        finish(buildResult(terminal, value, errorCode, message, nowNanos));
        return true;
    }

    /**
     * 入队被背压拒绝时收敛：条目已注册进 ResultStore 但从未进入队列（状态仍是初始 QUEUED）。
     * 转成 REJECTED 终态，使调用方凭 requestId 可查到明确判定。
     */
    boolean completeRejected(long nowNanos) {
        if (!state.compareAndSet(TaskStatus.QUEUED, TaskStatus.REJECTED)) {
            return false;
        }
        finish(buildResult(TaskStatus.REJECTED, null, TaskErrorCode.QUEUE_FULL,
                "rejected by backpressure: queue full", nowNanos));
        return true;
    }

    private void finish(TaskResult result) {
        terminalResult = result;
        completedAtNanos = System.nanoTime();
        runnerThread = null;
        completion.complete(result);
    }

    TaskResult terminalResult() {
        return terminalResult;
    }

    long completedAtNanos() {
        return completedAtNanos;
    }

    /**
     * 老化后的有效优先级：HIGH 恒定；NORMAL 等待超过老化阈值后按 HIGH 参与调度，
     * 保证普通任务在持续高优先级流量下也有确定的最长等待上界，不被无限饿死。
     */
    TaskPriority effectivePriority(long nowNanos, long agingThresholdNanos) {
        if (options.priority() == TaskPriority.HIGH) {
            return TaskPriority.HIGH;
        }
        return nowNanos - enqueuedAtNanos >= agingThresholdNanos
                ? TaskPriority.HIGH : TaskPriority.NORMAL;
    }

    private TaskResult buildResult(TaskStatus terminal, String value,
                                   TaskErrorCode errorCode, String message, long nowNanos) {
        long queued = firstRunStartNanos == 0 ? 0 : firstRunStartNanos - enqueuedAtNanos;
        long total = nowNanos - enqueuedAtNanos;
        return new TaskResult(requestId, terminal,
                terminal == TaskStatus.SUCCEEDED ? value : null,
                errorCode, message, (int) attemptCount,
                nanosToMillis(queued), nanosToMillis(total));
    }

    static long nanosToMillis(long nanos) {
        return Math.max(0L, nanos / 1_000_000L);
    }
}
