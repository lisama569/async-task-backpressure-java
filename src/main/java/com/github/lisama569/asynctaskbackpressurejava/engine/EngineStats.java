package com.github.lisama569.asynctaskbackpressurejava.engine;

import java.util.Map;

public final class EngineStats {

    private int queued;
    private int queueCapacity;
    private int running;
    private int activeWorkers;
    private long succeeded;
    private long failed;
    private long timeout;
    private long cancelled;
    private long rejected;
    private long totalCompleted;
    private Map<String, Long> completedByType = Map.of();
    private Map<String, Object> types = Map.of();

    public int getQueued() {
        return queued;
    }

    public void setQueued(int queued) {
        this.queued = queued;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getRunning() {
        return running;
    }

    public void setRunning(int running) {
        this.running = running;
    }

    public int getActiveWorkers() {
        return activeWorkers;
    }

    public void setActiveWorkers(int activeWorkers) {
        this.activeWorkers = activeWorkers;
    }

    public long getSucceeded() {
        return succeeded;
    }

    public void setSucceeded(long succeeded) {
        this.succeeded = succeeded;
    }

    public long getFailed() {
        return failed;
    }

    public void setFailed(long failed) {
        this.failed = failed;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getCancelled() {
        return cancelled;
    }

    public void setCancelled(long cancelled) {
        this.cancelled = cancelled;
    }

    public long getRejected() {
        return rejected;
    }

    public void setRejected(long rejected) {
        this.rejected = rejected;
    }

    public long getTotalCompleted() {
        return totalCompleted;
    }

    public void setTotalCompleted(long totalCompleted) {
        this.totalCompleted = totalCompleted;
    }

    public Map<String, Long> getCompletedByType() {
        return completedByType;
    }

    public void setCompletedByType(Map<String, Long> completedByType) {
        this.completedByType = completedByType;
    }

    public Map<String, Object> getTypes() {
        return types;
    }

    public void setTypes(Map<String, Object> types) {
        this.types = types;
    }
}
