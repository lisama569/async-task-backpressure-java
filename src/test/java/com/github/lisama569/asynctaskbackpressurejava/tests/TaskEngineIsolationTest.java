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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.summary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskEngineIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineIsolationTest.class);
    private TaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    private static TaskHandler alwaysFailing(String type) {
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
            public String execute(TaskRequest request, TaskContext context) {
                throw new IllegalStateException(type + " always fails");
            }
        };
    }

    private static TaskHandler ok(String type) {
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
            public String execute(TaskRequest request, TaskContext context) {
                return "ok-" + type;
            }
        };
    }

    @Test
    void oneTypeFailingOpensCircuitButOtherTypesStayHealthyAndItRecovers() throws Exception {
        TaskEngineProperties p = new TaskEngineProperties();
        p.setWorkerThreads(4);
        p.setQueueCapacity(50);
        p.setDefaultBulkhead(2);
        p.setFailureThreshold(5);
        p.setFailureWindow(Duration.ofSeconds(5));
        p.setOpenDuration(Duration.ofMillis(400));
        p.setDefaultTimeout(Duration.ofSeconds(2));
        p.setDefaultDeadline(Duration.ofSeconds(10));
        p.getRetry().setMaxAttempts(1);

        // 可切换行为的 "bad" 处理器：先持续失败，恢复标志打开后成功（模拟故障解除）
        boolean[] recovered = {false};
        TaskHandler bad = new TaskHandler() {
            @Override
            public String type() {
                return "bad";
            }

            @Override
            public boolean retryable() {
                return false;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) {
                if (recovered[0]) {
                    return "bad-recovered";
                }
                throw new IllegalStateException("bad is down");
            }
        };
        TaskHandler healthy = ok("healthy");

        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        registry.register(bad);
        registry.register(healthy);
        engine = new TaskEngine(p, registry);
        engine.start();

        // 1) 打满失败窗口，触发熔断
        for (int i = 0; i < 5; i++) {
            engine.submit(request("bad-fail-" + i, "bad", TaskPriority.NORMAL, null));
        }
        for (int i = 0; i < 5; i++) {
            TaskResult r = TestTasks.awaitTerminal(engine, "bad-fail-" + i, Duration.ofSeconds(3));
            assertEquals(TaskState.FAILED, r.getState());
        }
        log.info("[isolation] after failures stats={}", summary(engine.stats()));

        // 2) 熔断打开期间：bad 类型快速失败且带 CIRCUIT_OPEN；healthy 类型完全不受影响
        engine.submit(request("bad-while-open", "bad", TaskPriority.NORMAL, null));
        TaskResult badOpen = TestTasks.awaitTerminal(engine, "bad-while-open", Duration.ofSeconds(3));
        assertEquals(TaskState.FAILED, badOpen.getState());
        assertEquals(FailureKind.CIRCUIT_OPEN, badOpen.getFailureKind());

        for (int i = 0; i < 10; i++) {
            engine.submit(request("healthy-" + i, "healthy", TaskPriority.NORMAL, null));
        }
        for (int i = 0; i < 10; i++) {
            TaskResult r = TestTasks.awaitTerminal(engine, "healthy-" + i, Duration.ofSeconds(3));
            assertEquals(TaskState.SUCCEEDED, r.getState());
        }
        log.info("[isolation] bad circuit open; healthy tasks all succeeded. stats={}",
                summary(engine.stats()));

        // 3) 等待 OPEN 过去 -> 半开探测成功 -> 熔断关闭，bad 类型恢复
        Thread.sleep(500);
        recovered[0] = true;
        engine.submit(request("bad-probe", "bad", TaskPriority.NORMAL, null));
        TaskResult probe = TestTasks.awaitTerminal(engine, "bad-probe", Duration.ofSeconds(3));
        log.info("[isolation] half-open probe verdict={} value={}", probe.getState(), probe.getValue());

        engine.submit(request("bad-after-recovery", "bad", TaskPriority.NORMAL, null));
        TaskResult after = TestTasks.awaitTerminal(engine, "bad-after-recovery", Duration.ofSeconds(3));
        assertEquals(TaskState.SUCCEEDED, after.getState());
        assertEquals("bad-recovered", after.getValue());

        Object badInfo = engine.stats().getTypes().get("bad");
        log.info("[isolation-recovery] bad type recovered, typeInfo={} fullStats={}",
                badInfo, summary(engine.stats()));
        assertTrue(badInfo.toString().contains("CLOSED"),
                "circuit should be closed after successful probe: " + badInfo);
    }
}
