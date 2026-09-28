package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.BackpressurePolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.EngineMetrics;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 背压：突发流量超过处理能力时，REJECT 立即拒绝、BLOCK 降速等待；
 * 压力解除后队列自行排空、计数自洽、系统恢复可提交。
 */
class BackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureTest.class);

    private LocalTaskEngine engine;
    private CountDownLatch gate;

    @BeforeEach
    void setUp() {
        engine = TestEngines.create();
        gate = new CountDownLatch(1);
    }

    @AfterEach
    void tearDown() {
        gate.countDown();
        engine.shutdown();
    }

    @Test
    void rejectPolicyReturnsQueueFullDeterministically() throws Exception {
        // 2 个 worker 会先各拿走 1 个任务并阻塞在 latch 上，队列容量 4。
        engine.register("slow", ctx -> {
            gate.await();
            return "done";
        }, 4, 2);
        engine.start();

        TaskSubmitOptions opts = TaskSubmitOptions.defaults()
                .withTimeout(Duration.ofSeconds(30));

        // 先占满 2 个执行位并等待 worker 真正开始，避免突发早于 worker 启动导致容量观测不确定
        engine.submit(new TaskRequest("bp-run-0", "slow", null), opts);
        engine.submit(new TaskRequest("bp-run-1", "slow", null), opts);
        waitUntil(() -> engine.metrics().perType().get(0).activeWorkers() == 2, 2_000);

        int accepted = 2;
        int rejected = 0;
        // 执行位已满：只剩 4 个队列位，随后 18 个提交应为 4 接受 / 14 拒绝
        for (int i = 0; i < 18; i++) {
            try {
                engine.submit(new TaskRequest("bp-q-" + i, "slow", null), opts);
                accepted++;
            } catch (TaskRejectedException e) {
                rejected++;
                assertEquals(TaskErrorCode.QUEUE_FULL, e.errorCode());
            }
        }
        EngineMetrics m = engine.metrics();
        log.info("REJECT burst: accepted={}, rejected={}, queueSize={}, perTypeRejected={}",
                accepted, rejected, m.perType().get(0).queueSize(),
                m.perType().get(0).rejected());
        assertEquals(6, accepted, "2 running + 4 queued capacity");
        assertEquals(14, rejected);
        assertEquals(4, m.perType().get(0).queueSize());

        // 解除阻塞：所有已接受任务必须完成（不丢失），队列恢复为空
        gate.countDown();
        waitUntil(() -> engine.metrics().perType().get(0).queueSize() == 0
                && engine.metrics().perType().get(0).activeWorkers() == 0, 5_000);
        EngineMetrics after = engine.metrics();
        log.info("after recovery: succeeded={}, failed={}, queueSize={}",
                after.totalSucceeded(), after.totalFailed(),
                after.perType().get(0).queueSize());
        assertEquals(6, after.totalSucceeded());
        assertEquals(0, after.perType().get(0).queueSize());

        // 恢复后新的提交立即被接受
        TaskResult r = engine.submit(new TaskRequest("bp-recover", "slow", null),
                opts.withTimeout(Duration.ofSeconds(5))).await(Duration.ofSeconds(5));
        assertEquals(TaskStatus.SUCCEEDED, r.status());
    }

    @Test
    void blockPolicySlowsDownInsteadOfFailing() throws Exception {
        engine.register("slow", ctx -> {
            gate.await();
            return "done";
        }, 2, 1);
        engine.start();

        // BLOCK + 30s 等待预算：容量（1 running + 2 queued=3）先填满，再用独立线程提交第 4 个。
        // 预算设得很大只为吸收高负载抖动；测试在 300ms 处断言其确实被阻塞，随后立即放开 gate。
        TaskSubmitOptions block = TaskSubmitOptions.defaults()
                .withTimeout(Duration.ofSeconds(30))
                .withBackpressure(BackpressurePolicy.BLOCK, Duration.ofSeconds(30));

        for (int i = 0; i < 3; i++) {
            engine.submit(new TaskRequest("blk-fill-" + i, "slow", null), block);
        }
        // 等唯一 worker 取走第一个任务并阻塞在 latch 上
        waitUntil(() -> engine.metrics().perType().get(0).activeWorkers() == 1, 1_000);
        assertEquals(2, engine.metrics().perType().get(0).queueSize());

        AtomicInteger accepted = new AtomicInteger(3);
        AtomicInteger rejected = new AtomicInteger();
        Thread submitter = new Thread(() -> {
            try {
                engine.submit(new TaskRequest("blk-waiter", "slow", null), block);
                accepted.incrementAndGet();
            } catch (TaskRejectedException e) {
                rejected.incrementAndGet();
            }
        });
        submitter.start();

        // 队列已满：等待中的提交必须被“降速”阻塞，而不是立即失败
        Thread.sleep(300);
        assertTrue(submitter.isAlive(), "BLOCK submitter must be blocked while queue is full");
        assertEquals(3, accepted.get());
        assertEquals(0, rejected.get());

        gate.countDown(); // 压力解除：worker 排空，等待中的提交成功入队并完成
        submitter.join(5_000);
        log.info("BLOCK burst final: accepted={}, rejected={}", accepted.get(), rejected.get());
        assertEquals(4, accepted.get(), "blocked submission must be admitted once capacity frees");
        assertEquals(0, rejected.get());
        // 入队后还需 worker 执行，轮询等待终态结果
        TaskResult waiter = null;
        long resultDeadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < resultDeadline) {
            waiter = engine.resultOf("blk-waiter");
            if (waiter != null) {
                break;
            }
            Thread.sleep(10);
        }
        org.junit.jupiter.api.Assertions.assertNotNull(waiter);
        assertEquals(TaskStatus.SUCCEEDED, waiter.status());
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("condition not met within " + timeoutMs + "ms");
    }

    @FunctionalInterface
    interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
