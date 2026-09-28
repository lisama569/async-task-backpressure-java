package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.EngineMetrics;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 故障隔离：
 * 1) alwaysFail 类型持续失败只增加自己的 failed 计数，其他类型照常成功；
 * 2) 把一个类型的队列塞满并触发背压，只拒绝该类型，其他类型提交与执行不受影响。
 */
class FailureIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(FailureIsolationTest.class);

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
    void persistentFailuresDoNotHurtOtherTypes() throws InterruptedException {
        engine.register("bad", ctx -> {
            throw new IllegalStateException("permanent injected failure");
        }, 50, 4);
        engine.register("good", ctx -> "ok:" + ctx.payload(), 50, 4);
        engine.start();

        // bad 类型持续失败一批
        for (int i = 0; i < 30; i++) {
            TaskResult r = engine.submit(new TaskRequest("bad-" + i, "bad", null),
                    com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions.defaults()
                            .withTimeout(Duration.ofSeconds(2)))
                    .await(Duration.ofSeconds(5));
            assertEquals(TaskStatus.FAILED, r.status());
            assertEquals(TaskErrorCode.EXECUTION_ERROR, r.errorCode());
        }
        // 同期 good 类型必须全部成功、无排队堆积
        for (int i = 0; i < 30; i++) {
            TaskResult r = engine.submit(new TaskRequest("good-" + i, "good", String.valueOf(i)))
                    .await(Duration.ofSeconds(5));
            assertEquals(TaskStatus.SUCCEEDED, r.status());
            assertEquals("ok:" + i, r.value());
        }

        // future 在终态 CAS 成功时即完成，per-type 计数在同一 worker 线程随后递增；
        // 轮询等待两类计数收敛，避免读到计数器自增前的瞬时值。
        long deadline = System.currentTimeMillis() + 3_000;
        EngineMetrics m;
        while (true) {
            m = engine.metrics();
            long badFailed = typeMetric(m, "bad").failed();
            long goodSucceeded = typeMetric(m, "good").succeeded();
            if ((badFailed == 30 && goodSucceeded == 30)
                    || System.currentTimeMillis() > deadline) {
                break;
            }
            Thread.sleep(10);
        }
        EngineMetrics.TypeMetrics bad = typeMetric(m, "bad");
        EngineMetrics.TypeMetrics good = typeMetric(m, "good");
        log.info("isolation metrics: bad(failed={},succeeded={},queue={}) good(failed={},succeeded={},queue={})",
                bad.failed(), bad.succeeded(), bad.queueSize(),
                good.failed(), good.succeeded(), good.queueSize());
        assertEquals(30, bad.failed());
        assertEquals(0, bad.succeeded());
        assertEquals(30, good.succeeded());
        assertEquals(0, good.failed());
        assertEquals(0, good.queueSize());
    }

    private static EngineMetrics.TypeMetrics typeMetric(EngineMetrics m, String type) {
        return m.perType().stream()
                .filter(t -> t.taskType().equals(type)).findFirst().orElseThrow();
    }

    @Test
    void saturatedTypeOnlyRejectsItsOwnTraffic() throws InterruptedException {
        CountDownHolder holder = new CountDownHolder();
        // toxic 类型：1 worker 被占死、队列容量 2，塞满后只拒绝 toxic
        engine.register("toxic", ctx -> {
            holder.await();
            return "x";
        }, 2, 1);
        engine.register("healthy", ctx -> "fine", 10, 2);
        engine.start();

        var opts = com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions.defaults()
                .withTimeout(Duration.ofSeconds(30));
        // 1 running + 2 queued = 3 accepted, 4th rejected。
        // 先占执行位并等待其真正运行，避免 worker 尚未取任务时队列位误判。
        engine.submit(new TaskRequest("toxic-run", "toxic", null), opts);
        long ddl = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < ddl
                && engine.metrics().perType().get(0).activeWorkers() != 1) {
            Thread.sleep(5);
        }
        assertEquals(1, engine.metrics().perType().get(0).activeWorkers());
        engine.submit(new TaskRequest("toxic-q1", "toxic", null), opts);
        engine.submit(new TaskRequest("toxic-q2", "toxic", null), opts);
        ddl = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < ddl
                && engine.metrics().perType().get(0).queueSize() != 2) {
            Thread.sleep(5);
        }
        assertEquals(2, engine.metrics().perType().get(0).queueSize());
        TaskRejectedException ex = assertThrows(TaskRejectedException.class,
                () -> engine.submit(new TaskRequest("toxic-overflow", "toxic", null), opts));
        assertEquals(TaskErrorCode.QUEUE_FULL, ex.errorCode());
        log.info("toxic type correctly rejected: requestId={}, code={}",
                ex.requestId(), ex.errorCode());

        // healthy 类型完全不受 toxic 塞满影响
        TaskResult r = engine.submit(new TaskRequest("healthy-1", "healthy", null))
                .await(Duration.ofSeconds(3));
        assertEquals(TaskStatus.SUCCEEDED, r.status());
        assertEquals("fine", r.value());

        holder.release();
    }

    /** 极简可中断门闩，避免在本文件引入多个 CountDownLatch 字段。 */
    private static final class CountDownHolder {
        private final java.util.concurrent.CountDownLatch latch =
                new java.util.concurrent.CountDownLatch(1);

        void await() throws InterruptedException {
            latch.await();
        }

        void release() {
            latch.countDown();
        }
    }
}
