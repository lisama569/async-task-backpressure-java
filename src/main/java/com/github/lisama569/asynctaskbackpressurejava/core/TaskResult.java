package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 终态结果。errorCode 只在失败类终态出现，errorMessage 为可对外展示的摘要，不携带内部堆栈。
 */
public record TaskResult(String requestId,
                         TaskStatus status,
                         String value,
                         TaskErrorCode errorCode,
                         String errorMessage,
                         int attempts,
                         long queuedMillis,
                         long totalMillis) {

    public boolean succeeded() {
        return status == TaskStatus.SUCCEEDED;
    }
}
