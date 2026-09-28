package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskFuture;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import com.github.lisama569.asynctaskbackpressurejava.engine.LocalTaskEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 基础能力：任务提交、并发执行、结果回收，以及“同一任务恰好执行一次”。
 */
class BasicExecutionTest {

    private static final Logger log = LoggerFactory.getLogger(BasicExecutionTest.class);

    private LocalTaskEngine engine;

    @BeforeEach
    void setUp() {
        engine = TestEngines.create();
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    void submitsAndReturnsResult() {
        engine.register("echo", ctx -> "ok:" + ctx.payload(), 10, 2);
        engine.start();

        TaskFuture future = engine.submit(new TaskRequest("r-1", "echo", "hello"));
        TaskResult result = future.await(Duration.ofSeconds(5));

        assertNotNull(result);
        log.info("requestId={} verdict={} value={} attempts={} queuedMs={} totalMs={}",
                result.requestId(), result.status(), result.value(), result.attempts(),
                result.queuedMillis(), result.totalMillis());
        assertEquals(TaskStatus.SUCCEEDED, result.status());
        assertEquals("ok:hello", result.value());

        // 结果可再次通过 requestId 回收
        TaskResult again = engine.resultOf("r-1");
        assertEquals(result, again);
        assertEquals(TaskStatus.SUCCEEDED, engine.statusOf("r-1").status());
    }

    @Test
    void executesEachTaskExactlyOnce() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        // 容量必须不小于任务总数：本用例只验证恰好执行一次，不验证背压，
        // 避免高负载下 worker 调度滞后导致瞬时队列满而干扰结论。
        engine.register("counter", ctx -> {
            invocations.incrementAndGet();
            return "n";
        }, 256, 8);
        engine.start();

        int n = 200;
        TaskFuture[] futures = new TaskFuture[n];
        for (int i = 0; i < n; i++) {
            futures[i] = engine.submit(new TaskRequest("exact-" + i, "counter", null));
        }
        for (TaskFuture f : futures) {
            TaskResult r = f.await(Duration.ofSeconds(10));
            assertEquals(TaskStatus.SUCCEEDED, r.status(), "requestId=" + f.requestId());
        }
        log.info("exactly-once check: submitted={}, handlerInvocations={}", n, invocations.get());
        assertEquals(n, invocations.get(), "handler must be invoked exactly once per task");
    }

    @Test
    void unknownTypeAndBlankRequestAreRejected() {
        engine.start();
        assertNull(engine.statusOf("missing"));

        var ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException.class,
                () -> engine.submit(new TaskRequest("x", "nope", null)));
        assertEquals(com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode.UNKNOWN_TASK_TYPE,
                ex.errorCode());
        log.info("unknown type rejected as expected: requestId={}, code={}",
                ex.requestId(), ex.errorCode());

        var blank = org.junit.jupiter.api.Assertions.assertThrows(
                com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException.class,
                () -> engine.submit(new TaskRequest("  ", "echo", null)));
        assertTrue(blank.errorCode().name().equals("INVALID_REQUEST"));
    }
}
