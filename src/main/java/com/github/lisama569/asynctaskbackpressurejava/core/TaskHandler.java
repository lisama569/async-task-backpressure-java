package com.github.lisama569.asynctaskbackpressurejava.core;

public interface TaskHandler {

    String type();

    boolean retryable();

    String execute(TaskRequest request, TaskContext context) throws Exception;
}
