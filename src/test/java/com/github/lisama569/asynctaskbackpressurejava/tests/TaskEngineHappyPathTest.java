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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskEngineHappyPathTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineHappyPathTest.class);

    private TaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private TaskEngine newEngine(TaskEngineProperties props, TaskHandler... handlers) {
        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        for (TaskHandler h : handlers) {
            registry.register(h);
        }
        engine = new TaskEngine(props, registry);
        engine.start();
        return engine;
    }

    private static TaskEngineProperties smallProps() {
        TaskEngineProperties props = new TaskEngineProperties();
        props.setWorkerThreads(4);
        props.setQueueCapacity(200);
        props.setDefaultTimeout(Duration.ofSeconds(5));
        props.setDefaultDeadline(Duration.ofSeconds(10));
        props.getRetry().setMaxAttempts(1);
        return props;
    }

    @Test
    void tasksExecuteExactlyOnceAndResultIsRetrievable() throws Exception {
        ConcurrentHashMap<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        TaskHandler countingHandler = new TaskHandler() {
            @Override
            public String type() {
                return "count";
            }

            @Override
            public boolean retryable() {
                return false;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) {
                executions.computeIfAbsent(request.getRequestId(),
                        k -> new AtomicInteger()).incrementAndGet();
                return "done-" + request.getRequestId();
            }
        };
        TaskEngine engine = newEngine(smallProps(), countingHandler);

        int n = 100;
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = "happy-" + i;
            ids.add(id);
            assertNull(engine.submit(request(id, "count", TaskPriority.NORMAL, null)));
        }

        for (String id : ids) {
            TaskResult result = TestTasks.awaitTerminal(engine, id, Duration.ofSeconds(10));
            assertNotNull(result, "result for " + id);
            assertEquals(TaskState.SUCCEEDED, result.getState(),
                    () -> id + " -> " + result.getMessage());
            assertEquals("done-" + id, result.getValue());
            assertEquals(1, executions.get(id).get(), "task " + id + " executed more than once");
        }
        assertEquals(n, engine.stats().getSucceeded());
        assertEquals(0, engine.stats().getQueued());
        log.info("[happy-path] all {} tasks succeeded, stats={}", n,
                TestTasks.summary(engine.stats()));
    }

    @Test
    void highPriorityTasksCompleteBeforeQueuedLowPriorityBurst() throws Exception {
        TaskHandler slow = new TaskHandler() {
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
                Thread.sleep(30);
                return "ok";
            }
        };
        TaskEngine engine = newEngine(smallProps(), slow);

        // 先用 4 个慢任务占满 worker，随后大量 LOW 排队，再插入一个 HIGH
        for (int i = 0; i < 4; i++) {
            engine.submit(request("warm-" + i, "slow", TaskPriority.LOW, null));
        }
        Thread.sleep(80); // 让 LOW 任务执行中，后续任务开始积压

        int lowCount = 20;
        long highSubmitTime;
        long highCompleteTime;
        for (int i = 0; i < lowCount; i++) {
            engine.submit(request("low-" + i, "slow", TaskPriority.LOW, null));
        }
        String highId = "high-urgent";
        engine.submit(request(highId, "slow", TaskPriority.HIGH, null));
        highSubmitTime = System.nanoTime();

        TaskResult high = TestTasks.awaitTerminal(engine, highId, Duration.ofSeconds(10));
        highCompleteTime = System.nanoTime();
        assertEquals(TaskState.SUCCEEDED, high.getState());

        // HIGH 从提交到完成不应排在全部 LOW 之后：给出宽松上界
        // 4 worker * 30ms：若排在 20 个 LOW 后，至少需要 ~(24*30/4)=180ms 以上；
        // HIGH 插到队列最前，只等当前在跑的任务（约 30ms）+ 自己 30ms。
        long highWaitMs = (highCompleteTime - highSubmitTime) / 1_000_000;
        log.info("[priority] high task waited {}ms (queued LOW={})", highWaitMs, lowCount);
        assertTrue(highWaitMs < 120, "high priority task starved, waited " + highWaitMs + "ms");

        List<Long> lowCompleteMs = new ArrayList<>();
        for (int i = 0; i < lowCount; i++) {
            TaskResult r = TestTasks.awaitTerminal(engine, "low-" + i, Duration.ofSeconds(10));
            assertEquals(TaskState.SUCCEEDED, r.getState());
        }
        log.info("[priority] all {} LOW tasks eventually completed (no permanent starvation)",
                lowCount);
    }
}
