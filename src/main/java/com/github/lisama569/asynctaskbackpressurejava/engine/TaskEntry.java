package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 任务在引擎内部的运行时记录。
 *
 * <p>{@link #stateRef} 是所有状态判定的唯一真相来源，终态只允许成功翻转一次，
 * 从根上保证取消/超时/失败/成功之间不会相互覆盖，也不会重复执行或重复回传结果。
 */
public final class TaskEntry {

    private final TaskRequest request;
    private final long enqueueNanos;
    private final long deadlineNanos;
    private final AtomicInteger attempts = new AtomicInteger(0);
    private final AtomicReference<TaskState> stateRef = new AtomicReference<>(TaskState.QUEUED);

    private volatile String workerName = "-";
    private volatile long startedNanos;
    private final AtomicReference<Thread> runnerRef = new AtomicReference<>();
    private final AtomicBoolean timedOut = new AtomicBoolean(false);
    private final AtomicBoolean interruptClaimed = new AtomicBoolean(false);

    public TaskEntry(TaskRequest request, long enqueueNanos, long deadlineNanos) {
        this.request = request;
        this.enqueueNanos = enqueueNanos;
        this.deadlineNanos = deadlineNanos;
    }

    public TaskRequest getRequest() {
        return request;
    }

    public long getEnqueueNanos() {
        return enqueueNanos;
    }

    public long getDeadlineNanos() {
        return deadlineNanos;
    }

    public AtomicInteger getAttempts() {
        return attempts;
    }

    public TaskState getState() {
        return stateRef.get();
    }

    /** 测试辅助：直接设置状态。 */
    public void setState(TaskState state) {
        stateRef.set(state);
    }

    /** 仅当当前状态为期望状态时才翻转，返回是否成功。 */
    public boolean casState(TaskState expected, TaskState next) {
        return stateRef.compareAndSet(expected, next);
    }

    /** 首次从队列取出：QUEUED -> RUNNING。 */
    public boolean casQueuedToRunning() {
        return stateRef.compareAndSet(TaskState.QUEUED, TaskState.RUNNING);
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public long getStartedNanos() {
        return startedNanos;
    }

    public void setStartedNanos(long startedNanos) {
        this.startedNanos = startedNanos;
    }

    public Thread getRunner() {
        return runnerRef.get();
    }

    public void setRunner(Thread runner) {
        this.runnerRef.set(runner);
    }

    /** 每次尝试开始：重置超时标志、中断权并登记当前执行线程。 */
    public void beginAttempt(Thread runner) {
        timedOut.set(false);
        interruptClaimed.set(false);
        runnerRef.set(runner);
    }

    /**
     * 看门狗尝试赢得本次执行的中断权：仅当执行槽登记线程仍是 expected 且尚未有人赢过时成功。
     * 与执行结束的 {@link #clearRunner} 互斥，保证中断永远只发给"仍在执行本任务"的线程。
     */
    public boolean claimInterrupt(Thread expected) {
        return runnerRef.get() == expected && interruptClaimed.compareAndSet(false, true);
    }

    public boolean interruptWasClaimed() {
        return interruptClaimed.get();
    }

    /** 执行结束摘除执行槽；返回中断权是否已被看门狗赢走（用于决定是否清理中断标记）。 */
    public boolean clearRunner(Thread expected) {
        runnerRef.compareAndSet(expected, null);
        return interruptClaimed.get();
    }

    public void markTimedOut() {
        timedOut.set(true);
    }

    public boolean isTimedOut() {
        return timedOut.get();
    }

    public TaskPriority priority() {
        return request.getPriority() != null ? request.getPriority() : TaskPriority.NORMAL;
    }

}
