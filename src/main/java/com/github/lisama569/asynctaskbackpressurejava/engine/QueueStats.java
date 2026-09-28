package com.github.lisama569.asynctaskbackpressurejava.engine;

public final class QueueStats {

    private final int queued;
    private final int capacity;
    private final int highPriorityQueued;
    private final long oldestWaitMillis;

    public QueueStats(int queued, int capacity, int highPriorityQueued, long oldestWaitMillis) {
        this.queued = queued;
        this.capacity = capacity;
        this.highPriorityQueued = highPriorityQueued;
        this.oldestWaitMillis = oldestWaitMillis;
    }

    public int getQueued() {
        return queued;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getHighPriorityQueued() {
        return highPriorityQueued;
    }

    public long getOldestWaitMillis() {
        return oldestWaitMillis;
    }
}
