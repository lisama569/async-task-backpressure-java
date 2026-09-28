package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskExecutionService;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;
import com.github.lisama569.asynctaskbackpressurejava.engine.EngineStats;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

final class TestTasks {

    private TestTasks() {
    }

    static TaskRequest request(String id, String type, TaskPriority priority,
                               String payload) {
        return request(id, type, priority, payload, false);
    }

    static TaskRequest request(String id, String type, TaskPriority priority,
                               String payload, boolean idempotent) {
        TaskRequest req = new TaskRequest();
        req.setRequestId(id);
        req.setTaskType(type);
        req.setPriority(priority);
        req.setPayload(payload);
        req.setIdempotent(idempotent);
        return req;
    }

    static TaskResult awaitTerminal(TaskExecutionService engine, String requestId,
                                    Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<TaskResult> r = engine.get(requestId);
            if (r.isPresent() && isTerminal(r.get().getState())) {
                return r.get();
            }
            Thread.sleep(5);
        }
        throw new AssertionError("task " + requestId + " did not reach terminal state within "
                + timeout + ", last=" + engine.get(requestId).map(TaskResult::getState).orElse(null));
    }

    static boolean isTerminal(TaskState state) {
        return switch (state) {
            case SUCCEEDED, FAILED, TIMEOUT, CANCELLED, REJECTED -> true;
            default -> false;
        };
    }

    static Map<String, Object> summary(EngineStats s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("queue", s.getQueued() + "/" + s.getQueueCapacity());
        m.put("running", s.getRunning());
        m.put("succeeded", s.getSucceeded());
        m.put("failed", s.getFailed());
        m.put("timeout", s.getTimeout());
        m.put("cancelled", s.getCancelled());
        m.put("rejected", s.getRejected());
        m.put("types", s.getTypes());
        return m;
    }
}
