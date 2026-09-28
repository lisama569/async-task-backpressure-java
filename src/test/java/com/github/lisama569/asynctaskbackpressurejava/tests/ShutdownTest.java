package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import com.github.lisama569.asynctaskbackpressurejava.engine.LocalTaskEngine;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 生命周期：关闭后 worker/调度线程退出、提交被 ENGINE_SHUTDOWN 明确拒绝、
 * 关闭操作幂等。
 */
class ShutdownTest {

    private static final Logger log = LoggerFactory.getLogger(ShutdownTest.class);

    @Test
    void shutdownStopsThreadsAndRejectsSubmissions() throws Exception {
        LocalTaskEngine engine = TestEngines.create();
        engine.register("echo", ctx -> "ok", 10, 3);
        engine.start();

        TaskResult r = engine.submit(new TaskRequest("alive", "echo", null))
                .await(Duration.ofSeconds(3));
        assertEquals(TaskStatus.SUCCEEDED, r.status());

        engine.shutdown();
        engine.shutdown(); // 幂等，不抛异常
        assertFalse(engine.metrics().running());
        String id = engine.engineId();

        // 等待本引擎的 worker 与调度线程退出（其他引擎实例（如 Spring 缓存上下文）的线程不计入）
        long deadline = System.currentTimeMillis() + 3_000;
        int remaining;
        do {
            remaining = countEngineThreads(id);
            if (remaining == 0) {
                break;
            }
            Thread.sleep(20);
        } while (System.currentTimeMillis() < deadline);
        log.info("engine threads remaining after shutdown: {}", remaining);
        assertEquals(0, remaining, "all worker/scheduler threads must exit after shutdown");

        TaskRejectedException ex = assertThrows(TaskRejectedException.class,
                () -> engine.submit(new TaskRequest("dead", "echo", null)));
        assertEquals(TaskErrorCode.ENGINE_SHUTDOWN, ex.errorCode());
        log.info("post-shutdown submit rejected: code={}", ex.errorCode());
    }

    private int countEngineThreads(String engineId) {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        Thread[] all = new Thread[root.activeCount() * 2 + 64];
        int n = root.enumerate(all, true);
        int count = 0;
        for (int i = 0; i < n; i++) {
            String name = all[i].getName();
            if (name.startsWith("task-worker-" + engineId + "-")
                    || name.equals("task-engine-scheduler-" + engineId)) {
                if (all[i].isAlive()) {
                    count++;
                }
            }
        }
        return count;
    }
}
