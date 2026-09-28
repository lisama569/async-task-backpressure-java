package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 执行期上下文。重试时同一上下文会传递给后续 attempt，业务可用 attemptIndex 做幂等保护。
 */
public final class TaskContext {

    private final String requestId;
    private final String taskType;
    private final String payload;
    private volatile int attemptIndex;
    private volatile boolean cancelled;

    public TaskContext(String requestId, String taskType, String payload) {
        this.requestId = requestId;
        this.taskType = taskType;
        this.payload = payload;
        this.attemptIndex = 1;
    }

    public String requestId() {
        return requestId;
    }

    public String taskType() {
        return taskType;
    }

    public String payload() {
        return payload;
    }

    public int attemptIndex() {
        return attemptIndex;
    }

    /** 由引擎在每次重试前调用。 */
    public void advanceAttempt() {
        this.attemptIndex++;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public void markCancelled() {
        this.cancelled = true;
    }
}
