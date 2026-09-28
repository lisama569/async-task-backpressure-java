package com.github.lisama569.asynctaskbackpressurejava.core;

public final class TaskResult {

    private String requestId;
    private TaskState state;
    private FailureKind failureKind;
    private String message;
    private String value;
    private int attempts;
    private long elapsedMillis;

    public TaskResult() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState state) {
        this.state = state;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }

    public void setFailureKind(FailureKind failureKind) {
        this.failureKind = failureKind;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    public void setElapsedMillis(long elapsedMillis) {
        this.elapsedMillis = elapsedMillis;
    }
}
