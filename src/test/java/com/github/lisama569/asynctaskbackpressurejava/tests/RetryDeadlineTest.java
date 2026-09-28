package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskFuture;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions;
import com.github.lisama569.asynctaskbackpressurejava.engine.LocalTaskEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重试与截止时间：
 * 1) 临时故障在 attempt 预算内被重试并成功；2) 超过 attempt 上限后 FAILED；
 * 3) 整体截止时间优先，重试不得突破它，到期给 TIMEOUT+DEADLINE_EXCEEDED。
 */
class RetryDeadlineTest {

    private static final Logger log = LoggerFactory.getLogger(RetryDeadlineTest.class);

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
    void retriesUntilSuccessWithinAttemptBudget() {
        AtomicInteger calls = new AtomicInteger();
        engine.register("flaky", ctx -> {
            int n = calls.incrementAndGet();
            if (n < 3) {
                throw new IllegalStateException("transient #" + n);
            }
            return "recovered";
        }, 10, 2);
        engine.start();

        TaskFuture f = engine.submit(new TaskRequest("retry-ok", "flaky", null),
                TaskSubmitOptions.defaults()
                        .withRetries(5, Duration.ofMillis(20))
                        .withTimeout(Duration.ofSeconds(2)));
        TaskResult r = f.await(Duration.ofSeconds(5));
        assertEquals(TaskStatus.SUCCEEDED, r.status());
        assertEquals(3, r.attempts());
        assertEquals("recovered", r.value());
        log.info("retry success: requestId={}, verdict={}, attempts={}, totalMs={}",
                r.requestId(), r.status(), r.attempts(), r.totalMillis());
    }

    @Test
    void failsAfterAttemptBudgetExhausted() {
        AtomicInteger calls = new AtomicInteger();
        engine.register("boom", ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always failing");
        }, 10, 4);
        engine.start();

        TaskFuture f = engine.submit(new TaskRequest("retry-fail", "boom", null),
                TaskSubmitOptions.defaults()
                        .withRetries(3, Duration.ofMillis(10))
                        .withTimeout(Duration.ofSeconds(1)));
        TaskResult r = f.await(Duration.ofSeconds(5));
        assertEquals(TaskStatus.FAILED, r.status());
        assertEquals(3, r.attempts());
        assertEquals(3, calls.get(), "handler must be called exactly maxAttempts times");
        // 对外只给简短摘要，不暴露内部堆栈
        assertTrue(r.errorMessage().contains("IllegalStateException"));
        log.info("retry exhausted: requestId={}, verdict={}, attempts={}, code={}, message={}",
                r.requestId(), r.status(), r.attempts(), r.errorCode(), r.errorMessage());
    }

    @Test
    void retryNeverBreachesDeadline() {
        AtomicInteger calls = new AtomicInteger();
        long start = System.nanoTime();
        // handler 快速失败（毫秒级），因此 deadline（而非单次 timeout）是唯一的终止源。
        engine.register("late", ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("fast transient failure");
        }, 10, 2);
        engine.start();

        // deadline 250ms，退避 200ms、最多 5 次：
        // attempt1 在 0ms 失败 -> 退避 200ms -> attempt2 在 200ms 失败 -> 再退避会越过 250ms，截止生效。
        TaskFuture f = engine.submit(new TaskRequest("deadline-1", "late", null),
                TaskSubmitOptions.defaults()
                        .withTimeout(Duration.ofMillis(2_000))
                        .withDeadline(Duration.ofMillis(250))
                        .withRetries(5, Duration.ofMillis(200)));
        TaskResult r = f.await(Duration.ofSeconds(3));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(TaskStatus.TIMEOUT, r.status());
        assertEquals(com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode.DEADLINE_EXCEEDED,
                r.errorCode());
        assertEquals(2, r.attempts(), "only the 2 attempts that fit before deadline may run");
        assertTrue(elapsedMs < 900,
                "must stop at deadline instead of exhausting all retries, elapsed=" + elapsedMs);
        log.info("deadline enforced: requestId={}, verdict={}, attempts={}, calls={}, elapsedMs={}",
                r.requestId(), r.status(), r.attempts(), calls.get(), elapsedMs);
    }
}
