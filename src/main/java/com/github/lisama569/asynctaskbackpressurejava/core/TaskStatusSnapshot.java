package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 单个任务的可查询状态快照（用于状态查询接口）。
 */
public record TaskStatusSnapshot(String requestId,
                                 String taskType,
                                 TaskStatus status,
                                 int priority,
                                 int attemptIndex,
                                 long enqueuedAtNanos,
                                 long ageMillis) {
}
