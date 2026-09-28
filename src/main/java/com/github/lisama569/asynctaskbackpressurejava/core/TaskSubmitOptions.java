package com.github.lisama569.asynctaskbackpressurejava.core;

import java.time.Duration;

/**
 * 单次提交的参数。
 *
 * @param priority       优先级
 * @param timeout        单次执行超时（含所有重试在内的截止时间另行由 deadline 控制）
 * @param deadline       从提交时刻起的整体截止时间，重试不得突破它；null 表示不设整体截止
 * @param maxAttempts    最大执行次数（1 表示不重试），取消/截止到期不会触发重试
 * @param retryBackoff   重试之间的基础退避时长
 * @param backpressure   队列满时的背压策略
 * @param maxBlockWait   BLOCK 策略下最多等待多久
 */
public record TaskSubmitOptions(TaskPriority priority,
                                Duration timeout,
                                Duration deadline,
                                int maxAttempts,
                                Duration retryBackoff,
                                BackpressurePolicy backpressure,
                                Duration maxBlockWait) {

    public static TaskSubmitOptions defaults() {
        return new TaskSubmitOptions(TaskPriority.NORMAL, Duration.ofSeconds(5),
                Duration.ofSeconds(30), 1, Duration.ofMillis(100),
                BackpressurePolicy.REJECT, Duration.ZERO);
    }

    public TaskSubmitOptions withPriority(TaskPriority p) {
        return new TaskSubmitOptions(p, timeout, deadline, maxAttempts, retryBackoff, backpressure, maxBlockWait);
    }

    public TaskSubmitOptions withTimeout(Duration t) {
        return new TaskSubmitOptions(priority, t, deadline, maxAttempts, retryBackoff, backpressure, maxBlockWait);
    }

    public TaskSubmitOptions withRetries(int attempts, Duration backoff) {
        return new TaskSubmitOptions(priority, timeout, deadline, attempts, backoff, backpressure, maxBlockWait);
    }

    public TaskSubmitOptions withDeadline(Duration d) {
        return new TaskSubmitOptions(priority, timeout, d, maxAttempts, retryBackoff, backpressure, maxBlockWait);
    }

    public TaskSubmitOptions withBackpressure(BackpressurePolicy p, Duration maxWait) {
        return new TaskSubmitOptions(priority, timeout, deadline, maxAttempts, retryBackoff, p, maxWait);
    }
}
