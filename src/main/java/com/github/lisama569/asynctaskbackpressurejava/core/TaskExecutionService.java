package com.github.lisama569.asynctaskbackpressurejava.core;

import java.util.Optional;

public interface TaskExecutionService {

    TaskResult submit(TaskRequest request);

    Optional<TaskResult> get(String requestId);

    TaskResult cancel(String requestId);

    void start();

    void stop();
}
