package com.github.lisama569.asynctaskbackpressurejava.tests;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.summary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TaskEngineCancelTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineCancelTest.class);
    private TaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private static TaskEngineProperties props() {
        TaskEngineProperties p = new TaskEngineProperties();
        p.setWorkerThreads(1);
        p.setQueueCapacity(10);
        p.setDefaultTimeout(Duration.ofSeconds(5));
        p.setDefaultDeadline(Duration.ofSeconds(10));
        p.getRetry().setMaxAttempts(1);
        return p;
    }

    private TaskEngine newEngine(TaskHandler handler) {
        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        registry.register(handler);
        engine = new TaskEngine(props(), registry);
        engine.start();
        return engine;
    }

    @Test
    void cancelQueuedTaskNeverExecutes() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch blockerStarted = new CountDownLatch(1);
        TaskHandler handler = new TaskHandler() {
            @Override
            public String type() {
                return "block";
            }

            @Override
            public boolean retryable() {
                return false;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) throws Exception {
                blockerStarted.countDown();
                Thread.sleep(500);
                executions.incrementAndGet();
                return "ok";
            }
        };
        TaskEngine engine = newEngine(handler);

        engine.submit(request("blocker", "block", TaskPriority.NORMAL, null));
        blockerStarted.await();
        // 单 worker 正在执行 blocker，queued-* 必然排队
        for (int i = 0; i < 3; i++) {
            engine.submit(request("queued-" + i, "block", TaskPriority.NORMAL, null));
        }
        Thread.sleep(50);

        TaskResult cancelled = engine.cancel("queued-1");
        assertNotNull(cancelled);
        assertEquals(TaskState.CANCELLED, cancelled.getState());
        // 终态幂等：重复取消/查询得到同一判定
        assertEquals(TaskState.CANCELLED, engine.cancel("queued-1").getState());

        for (int i = 0; i < 3; i++) {
            if (i == 1) {
                continue;
            }
            TaskResult r = TestTasks.awaitTerminal(engine, "queued-" + i, Duration.ofSeconds(5));
            assertEquals(TaskState.SUCCEEDED, r.getState(), "queued-" + i);
        }
        TaskResult cancelledFinal = engine.get("queued-1").orElseThrow();
        assertEquals(TaskState.CANCELLED, cancelledFinal.getState());
        // blocker + 2 个未取消任务 = 3 次执行，queued-1 从未执行
        assertEquals(3, executions.get());
        log.info("[cancel-queued] requestId=queued-1 verdict=CANCELLED executions={} stats={}",
                executions.get(), summary(engine.stats()));
    }

    @Test
    void cancelRunningTaskInterruptsAndReportsCancelled() throws Exception {
        TaskHandler handler = new TaskHandler() {
            @Override
            public String type() {
                return "slow";
            }

            @Override
            public boolean retryable() {
                return false;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) throws Exception {
                Thread.sleep(2000);
                return "should-not-return";
            }
        };
        TaskEngine engine = newEngine(handler);
        engine.submit(request("running-1", "slow", TaskPriority.NORMAL, null));
        Thread.sleep(100);

        long t0 = System.nanoTime();
        TaskResult result = engine.cancel("running-1");
        assertNotNull(result);
        assertEquals(TaskState.CANCELLED, result.getState());
        long cancelMs = (System.nanoTime() - t0) / 1_000_000;
        // 取消后很快返回，而不是等 2 秒睡眠结束
        org.junit.jupiter.api.Assertions.assertTrue(cancelMs < 500,
                "cancel took " + cancelMs + "ms");
        log.info("[cancel-running] requestId=running-1 verdict=CANCELLED cancelMs={} stats={}",
                cancelMs, summary(engine.stats()));
    }
}
