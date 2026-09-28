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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 取消与超时：排队取消不入执行、运行中取消可中断、超时给出独立 TIMEOUT 判定。
 */
class CancelTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(CancelTimeoutTest.class);

    private LocalTaskEngine engine;
    private CountDownLatch started;
    private CountDownLatch canFinish;

    @BeforeEach
    void setUp() {
        engine = TestEngines.create();
        started = new CountDownLatch(1);
        canFinish = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() {
        canFinish.countDown();
        engine.shutdown();
    }

    @Test
    void cancelWhileQueuedNeverExecutes() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        // 唯一 worker 先被 holder 占死（阻塞在 canFinish），后续任务只能排队。
        engine.register("q", ctx -> {
            runs.incrementAndGet();
            if ("holder".equals(ctx.payload())) {
                started.countDown();
                canFinish.await();
            }
            return "x:" + ctx.payload();
        }, 10, 1);
        engine.start();

        TaskSubmitOptions longOpts = TaskSubmitOptions.defaults().withTimeout(Duration.ofSeconds(30));
        engine.submit(new TaskRequest("holder", "q", "holder"), longOpts);
        assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS),
                "holder must be running before we queue the victim");

        TaskFuture queued = engine.submit(new TaskRequest("queued-1", "q", "victim"), longOpts);
        // 确保 victim 已进入队列
        waitForQueueSize(1);

        boolean cancelled = queued.cancel();
        assertTrue(cancelled, "cancel of a QUEUED task must succeed");
        TaskResult r = queued.await(Duration.ofSeconds(2));
        assertEquals(TaskStatus.CANCELLED, r.status());
        log.info("queued cancel: requestId={}, verdict={}, errorCode={}, handlerRuns={}",
                r.requestId(), r.status(), r.errorCode(), runs.get());
        assertEquals(TaskStatus.CANCELLED, engine.statusOf("queued-1").status());

        // victim 从未被执行：放开 holder 后只有 holder 跑过一次
        canFinish.countDown();
        engine.resultOf("holder");
        Thread.sleep(100);
        assertEquals(1, runs.get(), "cancelled queued task must never reach the handler");
        assertFalse(queued.cancel());
    }

    private void waitForQueueSize(int size) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            if (engine.metrics().perType().get(0).queueSize() == size) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("queue did not reach size " + size);
    }

    @Test
    void cancelWhileRunningInterruptsHandler() throws Exception {
        engine.register("run", ctx -> {
            started.countDown();
            try {
                Thread.sleep(30_000);
                return "unexpected";
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        }, 5, 2);
        engine.start();

        TaskFuture f = engine.submit(new TaskRequest("run-1", "run", null),
                TaskSubmitOptions.defaults().withTimeout(Duration.ofSeconds(30)));
        assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(f.cancel());
        TaskResult r = f.await(Duration.ofSeconds(3));
        assertEquals(TaskStatus.CANCELLED, r.status());
        log.info("running cancel: requestId={}, verdict={}, code={}, totalMs={}",
                r.requestId(), r.status(), r.errorCode(), r.totalMillis());
    }

    @Test
    void executionTimeoutProducesDistinctVerdict() throws InterruptedException {
        engine.register("slow", ctx -> {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            return "late";
        }, 5, 2);
        engine.start();

        TaskFuture f = engine.submit(new TaskRequest("to-1", "slow", null),
                TaskSubmitOptions.defaults().withTimeout(Duration.ofMillis(300)));
        TaskResult r = f.await(Duration.ofSeconds(3));
        assertEquals(TaskStatus.TIMEOUT, r.status());
        log.info("timeout: requestId={}, verdict={}, code={}, attempts={}, totalMs={}",
                r.requestId(), r.status(), r.errorCode(), r.attempts(), r.totalMillis());
        assertTrue(r.totalMillis() < 2_000, "timeout should be enforced promptly");
        // future 在终态 CAS 成功时即完成，而 per-type 计数在同一线程随后递增；
        // 指标聚合读取做短暂轮询，避免与计数自增的极小窗口竞争。
        long timeoutCount = 0L;
        long ddl = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < ddl) {
            timeoutCount = engine.metrics().totalTimeout();
            if (timeoutCount == 1L) {
                break;
            }
            Thread.sleep(10);
        }
        assertEquals(1L, timeoutCount);
    }
}
