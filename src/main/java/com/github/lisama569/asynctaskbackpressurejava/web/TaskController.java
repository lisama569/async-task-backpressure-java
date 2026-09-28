package com.github.lisama569.asynctaskbackpressurejava.web;

import com.github.lisama569.asynctaskbackpressurejava.core.BackpressurePolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.EngineMetrics;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskEngine;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskFuture;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions;
import com.github.lisama569.asynctaskbackpressurejava.web.dto.StatusView;
import com.github.lisama569.asynctaskbackpressurejava.web.dto.SubmitTaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.web.dto.SubmitTaskResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.UUID;

/**
 * 任务 REST 接口：
 * POST /api/tasks                 提交
 * GET  /api/tasks/{requestId}     状态与终态结果
 * DELETE /api/tasks/{requestId}   取消
 * GET  /api/metrics               引擎与各类型队列指标
 */
@RestController
@RequestMapping("/api")
public class TaskController {

    private static final Logger log = LoggerFactory.getLogger(TaskController.class);

    private final TaskEngine engine;

    public TaskController(TaskEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/tasks")
    public ResponseEntity<SubmitTaskResponse> submit(@RequestBody SubmitTaskRequest body) {
        String requestId = body.getRequestId() == null || body.getRequestId().isBlank()
                ? UUID.randomUUID().toString() : body.getRequestId();
        TaskSubmitOptions options = toOptions(body);
        TaskRequest request = new TaskRequest(requestId, body.getTaskType(), body.getPayload());
        TaskFuture future = engine.submit(request, options);
        log.info("http submit ok: requestId={}, taskType={}, status={}",
                requestId, body.getTaskType(), future.status());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new SubmitTaskResponse(requestId, body.getTaskType(),
                        future.status().name(), "queued for execution"));
    }

    @GetMapping("/tasks/{requestId}")
    public ResponseEntity<StatusView> status(@PathVariable String requestId) {
        var snapshot = engine.statusOf(requestId);
        if (snapshot == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        return ResponseEntity.ok(new StatusView(snapshot, engine.resultOf(requestId)));
    }

    @DeleteMapping("/tasks/{requestId}")
    public ResponseEntity<Void> cancel(@PathVariable String requestId) {
        boolean cancelled = engine.cancel(requestId);
        return cancelled ? ResponseEntity.accepted().build()
                : ResponseEntity.status(HttpStatus.CONFLICT).build();
    }

    @GetMapping("/metrics")
    public EngineMetrics metrics() {
        return engine.metrics();
    }

    private TaskSubmitOptions toOptions(SubmitTaskRequest body) {
        if ((body.getTimeoutMillis() != null && body.getTimeoutMillis() < 0)
                || (body.getMaxAttempts() != null && body.getMaxAttempts() < 1)
                || (body.getRetryBackoffMillis() != null && body.getRetryBackoffMillis() < 0)
                || (body.getDeadlineMillis() != null && body.getDeadlineMillis() <= 0)
                || (body.getMaxBlockWaitMillis() != null && body.getMaxBlockWaitMillis() < 0)) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, body.getRequestId(),
                    "timeoutMillis/deadlineMillis/maxAttempts/retryBackoffMillis/maxBlockWaitMillis "
                            + "must be positive or zero as documented");
        }
        TaskSubmitOptions options = TaskSubmitOptions.defaults();
        options = options.withPriority(parsePriority(body.getPriority(), body.getRequestId()));
        if (body.getTimeoutMillis() != null) {
            options = options.withTimeout(Duration.ofMillis(body.getTimeoutMillis()));
        }
        if (body.getDeadlineMillis() != null) {
            options = options.withDeadline(Duration.ofMillis(body.getDeadlineMillis()));
        }
        int attempts = body.getMaxAttempts() == null ? 1 : body.getMaxAttempts();
        long backoff = body.getRetryBackoffMillis() == null ? 100L : body.getRetryBackoffMillis();
        options = options.withRetries(attempts, Duration.ofMillis(backoff));
        BackpressurePolicy policy = parseBackpressure(body.getBackpressure(), body.getRequestId());
        Duration maxWait = Duration.ofMillis(body.getMaxBlockWaitMillis() == null
                ? 0L : body.getMaxBlockWaitMillis());
        return options.withBackpressure(policy, maxWait);
    }

    private TaskPriority parsePriority(String raw, String requestId) {
        if (raw == null || raw.isBlank()) {
            return TaskPriority.NORMAL;
        }
        try {
            return TaskPriority.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, requestId,
                    "invalid priority: " + raw);
        }
    }

    private BackpressurePolicy parseBackpressure(String raw, String requestId) {
        if (raw == null || raw.isBlank()) {
            return BackpressurePolicy.REJECT;
        }
        try {
            return BackpressurePolicy.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new TaskRejectedException(TaskErrorCode.INVALID_REQUEST, requestId,
                    "invalid backpressure: " + raw);
        }
    }
}
