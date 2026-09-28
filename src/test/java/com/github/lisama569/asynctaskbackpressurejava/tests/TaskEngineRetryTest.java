package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.FailureKind;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;
import com.github.lisama569.asynctaskbackpressurejava.engine.DefaultTaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngine;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngineProperties;
import com.github.lisama569.asynctaskbackpressurejava.handlers.FaultInjectedTaskHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.summary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskEngineRetryTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineRetryTest.class);
    private TaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private static TaskEngineProperties props() {
        TaskEngineProperties p = new TaskEngineProperties();
        p.setWorkerThreads(2);
        p.setQueueCapacity(20);
        p.setDefaultBulkhead(2);
        p.setDefaultTimeout(Duration.ofSeconds(2));
        p.setDefaultDeadline(Duration.ofSeconds(10));
        p.getRetry().setMaxAttempts(3);
        p.getRetry().setBackoff(Duration.ofMillis(30));
        p.getRetry().setBackoffMultiplier(2.0);
        p.getRetry().setMaxBackoff(Duration.ofMillis(200));
        return p;
    }

    private TaskEngine engineWith(TaskHandler... handlers) {
        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        for (TaskHandler h : handlers) {
            registry.register(h);
        }
        engine = new TaskEngine(props(), registry);
        engine.start();
        return engine;
    }

    @Test
    void retryableIdempotentTaskSucceedsWithinAttempts() throws Exception {
        TaskEngine engine = engineWith(new FaultInjectedTaskHandler());
        // 前 2 次失败，第 3 次成功；maxAttempts=3
        TaskRequest req = request("retry-ok", "fault", TaskPriority.NORMAL,
                "failUntil=2", true);
        engine.submit(req);

        TaskResult result = TestTasks.awaitTerminal(engine, "retry-ok", Duration.ofSeconds(5));
        log.info("[retry-ok] requestId=retry-ok verdict={} kind={} attempts={} elapsedMs={}",
                result.getState(), result.getFailureKind(), result.getAttempts(),
                result.getElapsedMillis());
        assertEquals(TaskState.SUCCEEDED, result.getState());
        assertEquals(3, result.getAttempts());
        assertTrue(result.getValue().startsWith("ok:retry-ok#3"));
    }

    @Test
    void retriesExhaustedIsReportedSeparatelyFromUserException() throws Exception {
        TaskEngine engine = engineWith(new FaultInjectedTaskHandler());
        TaskRequest req = request("retry-exhaust", "fault", TaskPriority.NORMAL,
                "alwaysFail=true", true);
        engine.submit(req);

        TaskResult result = TestTasks.awaitTerminal(engine, "retry-exhaust", Duration.ofSeconds(5));
        log.info("[retry-exhaust] requestId=retry-exhaust verdict={} kind={} attempts={} msg={}",
                result.getState(), result.getFailureKind(), result.getAttempts(),
                result.getMessage());
        assertEquals(TaskState.FAILED, result.getState());
        assertEquals(FailureKind.RETRIES_EXHAUSTED, result.getFailureKind());
        assertEquals(3, result.getAttempts());
    }

    @Test
    void nonIdempotentTaskIsNeverRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override
            public String type() {
                return "flaky";
            }

            @Override
            public boolean retryable() {
                return true;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) {
                calls.incrementAndGet();
                throw new IllegalStateException("boom");
            }
        };
        TaskEngine engine = engineWith(handler);
        // 非幂等：即使处理器声明 retryable 也不允许重试，避免重复副作用
        engine.submit(request("non-idem", "flaky", TaskPriority.NORMAL, null, false));

        TaskResult result = TestTasks.awaitTerminal(engine, "non-idem", Duration.ofSeconds(3));
        assertEquals(TaskState.FAILED, result.getState());
        assertEquals(FailureKind.USER_EXCEPTION, result.getFailureKind());
        assertEquals(1, result.getAttempts());
        assertEquals(1, calls.get());
        log.info("[non-idempotent] requestId=non-idem verdict=FAILED kind=USER_EXCEPTION attempts=1 stats={}",
                summary(engine.stats()));
    }

    @Test
    void retryDoesNotBreakDeadline() throws Exception {
        // 每次尝试 300ms，退避 30/60ms，截止时间 400ms：首次失败后来不及完成第二次，
        // 必须在截止时间处给出 TIMEOUT 而不是无限重试
        TaskEngineProperties p = props();
        p.setDefaultDeadline(Duration.ofMillis(400));
        TaskEngineProperties.Retry r = p.getRetry();
        r.setMaxAttempts(5);
        r.setBackoff(Duration.ofMillis(30));
        r.setMaxBackoff(Duration.ofMillis(60));
        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        registry.register(new FaultInjectedTaskHandler());
        engine = new TaskEngine(p, registry);
        engine.start();

        TaskRequest req = request("deadline-cap", "fault", TaskPriority.NORMAL,
                "alwaysFail=true,sleepMs=300", true);
        engine.submit(req);

        long t0 = System.nanoTime();
        TaskResult result = TestTasks.awaitTerminal(engine, "deadline-cap", Duration.ofSeconds(3));
        long wallMs = (System.nanoTime() - t0) / 1_000_000;
        log.info("[deadline-cap] requestId=deadline-cap verdict={} attempts={} wallMs={} msg={}",
                result.getState(), result.getAttempts(), wallMs, result.getMessage());
        assertEquals(TaskState.TIMEOUT, result.getState());
        assertTrue(wallMs < 1200, "deadline must bound retries, wallMs=" + wallMs);
    }
}
