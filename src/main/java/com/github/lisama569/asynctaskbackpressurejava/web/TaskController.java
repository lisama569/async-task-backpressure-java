package com.github.lisama569.asynctaskbackpressurejava.web;

import com.github.lisama569.asynctaskbackpressurejava.core.InvalidTaskException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskExecutionService;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.engine.EngineStats;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngine;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskExecutionService service;
    private final TaskEngine engine;

    public TaskController(TaskExecutionService service, TaskEngine engine) {
        this.service = service;
        this.engine = engine;
    }

    /**
     * 提交任务。
     * 返回 202 + 当前判定（排队中返回中间态，CALLER_RUNS 满队时可能直接返回终态）。
     */
    @PostMapping
    public ResponseEntity<TaskResult> submit(@RequestBody TaskRequest request) {
        TaskResult result = service.submit(request);
        if (result == null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(service.get(request.getRequestId()).orElseThrow());
        }
        return ResponseEntity.status(HttpStatus.OK).body(result);
    }

    @GetMapping("/{requestId}")
    public ResponseEntity<TaskResult> get(@PathVariable String requestId) {
        return service.get(requestId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{requestId}/cancel")
    public ResponseEntity<TaskResult> cancel(@PathVariable String requestId) {
        TaskResult result = service.cancel(requestId);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/stats")
    public EngineStats stats() {
        return engine.stats();
    }

    @ExceptionHandler(TaskRejectedException.class)
    public ResponseEntity<Map<String, Object>> rejected(TaskRejectedException e) {
        return error(HttpStatus.TOO_MANY_REQUESTS, "REJECTED", e.getMessage());
    }

    @ExceptionHandler(InvalidTaskException.class)
    public ResponseEntity<Map<String, Object>> invalid(InvalidTaskException e) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_TASK", e.getMessage());
    }

    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", code);
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
