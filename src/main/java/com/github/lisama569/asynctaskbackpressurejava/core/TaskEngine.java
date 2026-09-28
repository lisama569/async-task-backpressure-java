package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 本地任务引擎：提交、查询、取消、指标、生命周期管理。
 */
public interface TaskEngine {

    void register(String taskType, TaskHandler handler, int queueCapacity, int workers);

    TaskFuture submit(TaskRequest request, TaskSubmitOptions options);

    default TaskFuture submit(TaskRequest request) {
        return submit(request, TaskSubmitOptions.defaults());
    }

    TaskStatusSnapshot statusOf(String requestId);

    TaskResult resultOf(String requestId);

    boolean cancel(String requestId);

    EngineMetrics metrics();

    void start();

    void shutdown();
}
