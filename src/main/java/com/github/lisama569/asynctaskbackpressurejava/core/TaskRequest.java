package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 调用方提交的任务。
 *
 * @param requestId 调用方指定的请求标识，全局唯一，用于去重与日志串联
 * @param taskType  任务类型，不同类型拥有独立队列与线程资源（故障隔离）
 * @param payload   业务载荷，不透明传递给对应 handler
 */
public record TaskRequest(String requestId, String taskType, String payload) {
}
