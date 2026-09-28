package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 对外暴露的稳定错误码，屏蔽线程池/队列内部异常细节。
 */
public enum TaskErrorCode {
    QUEUE_FULL,
    ENGINE_SHUTDOWN,
    UNKNOWN_TASK_TYPE,
    DUPLICATE_REQUEST,
    DEADLINE_EXCEEDED,
    EXECUTION_ERROR,
    CANCELLED,
    TASK_EVICTED,
    INVALID_REQUEST
}
