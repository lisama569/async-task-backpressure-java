package com.github.lisama569.asynctaskbackpressurejava.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class IsolationRegistry {

    private final TaskEngineProperties properties;
    private final ConcurrentHashMap<String, TypeIsolation> isolations = new ConcurrentHashMap<>();

    public IsolationRegistry(TaskEngineProperties properties) {
        this.properties = properties;
    }

    public TypeIsolation forType(String type) {
        return isolations.computeIfAbsent(type, t -> new TypeIsolation(
                t,
                properties.getDefaultBulkhead(),
                properties.getFailureThreshold(),
                properties.getFailureWindow().toNanos(),
                properties.getOpenDuration().toNanos()));
    }

    public Map<String, TypeIsolation> all() {
        return Map.copyOf(isolations);
    }
}
