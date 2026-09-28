package com.github.lisama569.asynctaskbackpressurejava.core;

import java.util.Optional;

public interface TaskHandlerRegistry {

    void register(TaskHandler handler);

    Optional<TaskHandler> find(String taskType);
}
