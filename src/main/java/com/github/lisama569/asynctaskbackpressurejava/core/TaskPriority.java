package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 任务优先级。HIGH 任务在队列调度时优先于 NORMAL，NORMAL 任务会随等待时间老化提权，避免被饿死。
 */
public enum TaskPriority {
    NORMAL(0),
    HIGH(10);

    private final int weight;

    TaskPriority(int weight) {
        this.weight = weight;
    }

    public int weight() {
        return weight;
    }
}
