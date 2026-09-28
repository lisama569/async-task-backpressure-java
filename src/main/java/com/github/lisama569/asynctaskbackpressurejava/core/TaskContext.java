package com.github.lisama569.asynctaskbackpressurejava.core;

public interface TaskContext {

    String requestId();

    int attempt();

    boolean isCancelled();
}
