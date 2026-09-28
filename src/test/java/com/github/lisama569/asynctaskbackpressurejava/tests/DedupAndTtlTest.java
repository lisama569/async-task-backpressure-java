package com.github.lisama569.asynctaskbackpressurejava.tests;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 幂等去重与结果回收：
 * 1) 同一 requestId 的重复提交被 409 式拒绝，且任务只执行一次；
 * 2) 终态结果在 TTL 内可查询，过期后被清理（测试引擎 TTL=2s），存储不会无限增长。
 */
class DedupAndTtlTest {

    private static final Logger log = LoggerFactory.getLogger(DedupAndTtlTest.class);

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
    void duplicateRequestIdIsRejectedAndTaskRunsOnce() {
        AtomicInteger calls = new AtomicInteger();
        engine.register("svc", ctx -> {
            calls.incrementAndGet();
            return "v";
        }, 10, 2);
        engine.start();

        TaskResult first = engine.submit(new TaskRequest("dup-1", "svc", null))
                .await(Duration.ofSeconds(3));
        assertEquals(TaskStatus.SUCCEEDED, first.status());

        TaskRejectedException ex = assertThrows(TaskRejectedException.class,
                () -> engine.submit(new TaskRequest("dup-1", "svc", null)));
        assertEquals(TaskErrorCode.DUPLICATE_REQUEST, ex.errorCode());
        log.info("duplicate rejected: requestId={}, code={}, message={}",
                ex.requestId(), ex.errorCode(), ex.getMessage());
        assertEquals(1, calls.get(), "duplicate submission must not cause a second execution");
    }

    @Test
    void rejectedByBackpressureIsQueryableThenEvictedAfterTtl() throws Exception {
        java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        engine.register("tight", ctx -> {
            gate.await();
            return "ok";
        }, 1, 1);
        engine.start();

        var opts = com.github.lisama569.asynctaskbackpressurejava.core.TaskSubmitOptions.defaults()
                .withTimeout(Duration.ofSeconds(30));
        engine.submit(new TaskRequest("fill-run", "tight", null), opts);
        // 等唯一 worker 开始执行 fill-run（阻塞在 gate）
        long ddl = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < ddl
                && engine.metrics().perType().get(0).activeWorkers() != 1) {
            Thread.sleep(5);
        }
        assertEquals(1, engine.metrics().perType().get(0).activeWorkers());
        // 容量 1：再提交一个占满唯一队列位
        engine.submit(new TaskRequest("fill-queue", "tight", null), opts);
        ddl = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < ddl
                && engine.metrics().perType().get(0).queueSize() != 1) {
            Thread.sleep(5);
        }
        assertEquals(1, engine.metrics().perType().get(0).queueSize());

        // 执行位与队列位都满 -> overflow 被背压拒绝
        TaskRejectedException ex = assertThrows(TaskRejectedException.class,
                () -> engine.submit(new TaskRequest("overflow", "tight", null), opts));
        assertEquals(TaskErrorCode.QUEUE_FULL, ex.errorCode());

        // 被拒绝的 requestId 也能查到明确终态 REJECTED，而不是“查无此任务”
        TaskResult rejectedResult = engine.resultOf("overflow");
        assertNotNull(rejectedResult);
        assertEquals(TaskStatus.REJECTED, rejectedResult.status());
        assertEquals(TaskErrorCode.QUEUE_FULL, rejectedResult.errorCode());
        log.info("rejected task queryable: requestId={}, verdict={}",
                rejectedResult.requestId(), rejectedResult.status());

        gate.countDown();
        // TTL=2s：终态条目过期后被清理（周期清理之外，这里等待足够时间确认最终被回收）
        Thread.sleep(1_500);
        assertNull(engine.resultOf("overflow"), "terminal result should be evicted after TTL");
        assertNull(engine.statusOf("overflow"));
        log.info("terminal result evicted after TTL; storeSize={}", engine.metrics().resultStoreSize());
    }
}
