package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.FailureKind;
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

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.awaitTerminal;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.summary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskEngineTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineTimeoutTest.class);
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

    private TaskEngineProperties props() {
        TaskEngineProperties props = new TaskEngineProperties();
        props.setWorkerThreads(2);
        props.setQueueCapacity(10);
        props.setDefaultBulkhead(2);
        props.setDefaultTimeout(Duration.ofSeconds(2));
        props.setDefaultDeadline(Duration.ofSeconds(5));
        props.getRetry().setMaxAttempts(1);
        return props;
    }

    @Test
    void interruptibleTaskThatExceedsTimeoutIsReportedAsTimeout() throws Exception {
        TaskEngine engine = newEngine(props(), new FaultInjectedTaskHandler());
        TaskRequest req = request("timeout-sleep", "fault", TaskPriority.NORMAL, "sleepMs=1000");
        req.setTimeout(Duration.ofMillis(200));
        engine.submit(req);

        TaskResult result = awaitTerminal(engine, "timeout-sleep", Duration.ofSeconds(3));
        log.info("[timeout] requestId={} verdict={} failureKind={} attempts={} elapsedMs={} queueBefore=2/10",
                result.getRequestId(), result.getState(), result.getFailureKind(),
                result.getAttempts(), result.getElapsedMillis());
        assertEquals(TaskState.TIMEOUT, result.getState());
        assertEquals(FailureKind.NONE, result.getFailureKind());
        assertTrue(result.getElapsedMillis() < 800,
                "interruptible timeout should return quickly, was " + result.getElapsedMillis());
        assertEquals(1, engine.stats().getTimeout());
    }

    @Test
    void nonInterruptibleTaskStillGetsTimeoutVerdictAndOtherTypesKeepWorking() throws Exception {
        TaskEngineProperties p = props();
        p.setDefaultBulkhead(2);
        TaskEngine engine = newEngine(p, new FaultInjectedTaskHandler());

        TaskRequest busy = request("timeout-busy", "fault", TaskPriority.NORMAL, "busyMs=800");
        busy.setTimeout(Duration.ofMillis(150));
        engine.submit(busy);

        TaskResult result = awaitTerminal(engine, "timeout-busy", Duration.ofSeconds(3));
        assertEquals(TaskState.TIMEOUT, result.getState());
        log.info("[timeout-busy] requestId={} verdict={} elapsedMs={} (busy thread returns later, bounded by bulkhead)",
                result.getRequestId(), result.getState(), result.getElapsedMillis());

        // 忙等线程稍后自行结束并释放隔离舱；之后该类型任务恢复正常
        Thread.sleep(900);
        TaskRequest later = request("after-busy", "fault", TaskPriority.NORMAL, null);
        engine.submit(later);
        TaskResult laterResult = awaitTerminal(engine, "after-busy", Duration.ofSeconds(3));
        assertEquals(TaskState.SUCCEEDED, laterResult.getState());
        log.info("[timeout-busy-recovery] requestId=after-busy verdict={} stats={}",
                laterResult.getState(), summary(engine.stats()));
    }
}
