package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.RejectPolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.handlers.FaultInjectedTaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;
import com.github.lisama569.asynctaskbackpressurejava.engine.DefaultTaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngine;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngineProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.summary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskEngineBackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineBackpressureTest.class);
    private TaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private static TaskHandler slowHandler(long sleepMs) {
        return new TaskHandler() {
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
                Thread.sleep(sleepMs);
                return "ok";
            }
        };
    }

    private static TaskEngineProperties props(RejectPolicy policy) {
        TaskEngineProperties p = new TaskEngineProperties();
        p.setWorkerThreads(2);
        p.setQueueCapacity(4);
        p.setDefaultBulkhead(2);
        p.setRejectPolicy(policy);
        p.setDefaultTimeout(Duration.ofSeconds(5));
        p.setDefaultDeadline(Duration.ofSeconds(20));
        p.getRetry().setMaxAttempts(1);
        return p;
    }

    private static TaskEngine engine(TaskEngineProperties p, TaskHandler h) {
        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        registry.register(h);
        TaskEngine engine = new TaskEngine(p, registry);
        engine.start();
        return engine;
    }

    @Test
    void abortPolicyRejectsBurstBeyondCapacityWithDistinguishableError() throws Exception {
        engine = engine(props(RejectPolicy.ABORT), slowHandler(100));
        int accepted = 0;
        int rejected = 0;
        List<String> acceptedIds = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            String id = "burst-" + i;
            try {
                TaskResult inline = engine.submit(request(id, "slow", TaskPriority.NORMAL, null));
                accepted++;
                acceptedIds.add(id);
                if (inline != null) {
                    // ABORT 不会出现内联完成
                    throw new AssertionError("unexpected inline result");
                }
            } catch (TaskRejectedException e) {
                rejected++;
            }
        }
        log.info("[abort] burst=40 accepted={} rejected={} queueAtSubmitFull=true stats={}",
                accepted, rejected, summary(engine.stats()));
        // 确定的不变量（与线程调度时序无关）：每个任务要么被接受要么被显式拒绝；
        // 突发瞬间至少填满 4 容量的队列，故必然发生过拒绝；突发期间最多处理极少量 100ms 任务。
        assertEquals(40, accepted + rejected);
        assertTrue(accepted >= 4, "queue capacity of accepts, was " + accepted);
        assertTrue(accepted <= 8, "accepted count bounded by processing speed, was " + accepted);
        assertTrue(rejected >= 32, "most of the burst must be rejected, was " + rejected);

        for (String id : acceptedIds) {
            TaskResult r = TestTasks.awaitTerminal(engine, id, Duration.ofSeconds(10));
            assertEquals(TaskState.SUCCEEDED, r.getState());
        }
        // 压力解除后恢复：又可以提交并成功
        TaskResult after = engine.submit(request("after-drain", "slow", TaskPriority.NORMAL, null));
        assertEquals(null, after);
        TaskResult drained = TestTasks.awaitTerminal(engine, "after-drain", Duration.ofSeconds(5));
        assertEquals(TaskState.SUCCEEDED, drained.getState());
        log.info("[abort-recovery] requestId=after-drain verdict={} stats={}",
                drained.getState(), summary(engine.stats()));
    }

    @Test
    void callerRunsPolicySlowsDownSubmitterAndLosesNoTask() throws Exception {
        engine = engine(props(RejectPolicy.CALLER_RUNS), slowHandler(40));
        AtomicInteger completedInline = new AtomicInteger();
        int n = 30;
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            String id = "cr-" + i;
            TaskResult result = engine.submit(request(id, "slow", TaskPriority.NORMAL, null));
            if (result != null) {
                assertEquals(TaskState.SUCCEEDED, result.getState());
                completedInline.incrementAndGet();
            }
        }
        long submitMillis = (System.nanoTime() - t0) / 1_000_000;
        log.info("[caller-runs] submitted={} inlineCompleted={} submitWallMs={}",
                n, completedInline.get(), submitMillis);
        assertTrue(completedInline.get() > 0, "expected some tasks executed inline on caller thread");

        for (int i = 0; i < n; i++) {
            TaskResult r = TestTasks.awaitTerminal(engine, "cr-" + i, Duration.ofSeconds(15));
            assertEquals(TaskState.SUCCEEDED, r.getState());
        }
        assertEquals(n, engine.stats().getSucceeded());
        assertEquals(0, engine.stats().getQueued());
        log.info("[caller-runs] all {} tasks done, no loss, stats={}", n, summary(engine.stats()));
    }

    @Test
    void blockPolicyThrottlesSubmissionThenDrainsAndRecovers() throws Exception {
        engine = engine(props(RejectPolicy.BLOCK), slowHandler(50));
        long t0 = System.nanoTime();
        int n = 20;
        for (int i = 0; i < n; i++) {
            engine.submit(request("blk-" + i, "slow", TaskPriority.NORMAL, null));
        }
        long submitMillis = (System.nanoTime() - t0) / 1_000_000;
        log.info("[block] submitters were throttled for submitWallMs={} (n={}, capacity=4)",
                submitMillis, n);
        assertTrue(submitMillis >= 200, "BLOCK should throttle the producer, was " + submitMillis);

        for (int i = 0; i < n; i++) {
            TaskResult r = TestTasks.awaitTerminal(engine, "blk-" + i, Duration.ofSeconds(15));
            assertEquals(TaskState.SUCCEEDED, r.getState());
        }
        assertNotNull(engine.stats());
        assertEquals(n, engine.stats().getSucceeded());
        assertEquals(0, engine.stats().getFailed() + engine.stats().getTimeout());
        log.info("[block-recovery] all drained, queue={}/{}, stats={}",
                engine.stats().getQueued(), engine.stats().getQueueCapacity(),
                summary(engine.stats()));
    }

    @Test
    void callerRunsInlineRetryRunsToCompletionOnSubmitter() throws Exception {
        TaskEngineProperties p = props(RejectPolicy.CALLER_RUNS);
        p.setWorkerThreads(1);
        p.setQueueCapacity(1);
        p.setDefaultTimeout(Duration.ofSeconds(5));
        p.setDefaultDeadline(Duration.ofSeconds(10));
        p.getRetry().setMaxAttempts(3);
        p.getRetry().setBackoff(Duration.ofMillis(10));
        // 唯一类型名，避免与同类其他测试的隔离舱/熔断状态相互干扰
        String slowType = "slow-inline-" + System.nanoTime();
        TaskHandlerRegistry reg = new DefaultTaskHandlerRegistry();
        reg.register(slowHandlerOfType(slowType, 400));
        reg.register(new FaultInjectedTaskHandler());
        engine = new TaskEngine(p, reg);
        engine.start();

        // 先确保 occ-1 真正进入 RUNNING，再投 occ-2 占满队列；
        // 否则两个任务可能都在"等待中的 worker 被调度前"瞬间提交，触发非预期的内联执行。
        engine.submit(request("occ-1", slowType, TaskPriority.NORMAL, null));
        long waitUntil = System.nanoTime() + java.time.Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < waitUntil && engine.stats().getRunning() < 1) {
            Thread.sleep(2);
        }
        assertEquals(1, engine.stats().getRunning());
        engine.submit(request("occ-2", slowType, TaskPriority.NORMAL, null));
        waitUntil = System.nanoTime() + java.time.Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < waitUntil && engine.stats().getQueued() < 1) {
            Thread.sleep(2);
        }
        assertEquals(1, engine.stats().getQueued());

        TaskRequest req = request("inline-retry", "fault", TaskPriority.NORMAL,
                "failUntil=2", true);
        TaskResult result = engine.submit(req);
        assertNotNull(result, "inline submit must return the terminal verdict");
        assertEquals(TaskState.SUCCEEDED, result.getState());
        assertEquals(3, result.getAttempts());
        log.info("[caller-runs-retry] requestId=inline-retry verdict={} attempts={} elapsedMs={}",
                result.getState(), result.getAttempts(), result.getElapsedMillis());
    }

    private static TaskHandler slowHandlerOfType(String type, long sleepMs) {
        return new TaskHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public boolean retryable() {
                return false;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) throws Exception {
                Thread.sleep(sleepMs);
                return "ok";
            }
        };
    }
}
