package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 引擎可观测指标。perType 中包含每种任务类型的队列与计数明细。
 */
public record EngineMetrics(boolean running,
                            int registeredTypes,
                            long totalSubmitted,
                            long totalQueued,
                            long totalRunning,
                            long totalSucceeded,
                            long totalFailed,
                            long totalCancelled,
                            long totalTimeout,
                            long totalRejected,
                            long totalRetried,
                            int resultStoreSize,
                            java.util.List<TypeMetrics> perType) {

    public record TypeMetrics(String taskType,
                              int queueCapacity,
                              int queueSize,
                              int activeWorkers,
                              int workers,
                              long submitted,
                              long succeeded,
                              long failed,
                              long cancelled,
                              long timeout,
                              long rejected,
                              long retried,
                              long oldestQueuedAgeMillis) {
    }
}
