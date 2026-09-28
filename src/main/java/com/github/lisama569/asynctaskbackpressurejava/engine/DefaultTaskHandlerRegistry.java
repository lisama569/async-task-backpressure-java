package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.InvalidTaskException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandlerRegistry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultTaskHandlerRegistry implements TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    @Override
    public void register(TaskHandler handler) {
        if (handler == null || handler.type() == null || handler.type().isBlank()) {
            throw new InvalidTaskException("handler and its type must not be blank");
        }
        TaskHandler previous = handlers.putIfAbsent(handler.type(), handler);
        if (previous != null && previous != handler) {
            throw new InvalidTaskException("duplicate handler for task type: " + handler.type());
        }
    }

    @Override
    public Optional<TaskHandler> find(String taskType) {
        return Optional.ofNullable(handlers.get(taskType));
    }
}
