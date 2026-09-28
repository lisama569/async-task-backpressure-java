package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 任务生命周期状态。SUCCEEDED / FAILED / CANCELLED / TIMEOUT / REJECTED 为终态。
 */
public enum TaskStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    TIMEOUT,
    REJECTED;

    public boolean isTerminal() {
        return this != QUEUED && this != RUNNING;
    }
}
