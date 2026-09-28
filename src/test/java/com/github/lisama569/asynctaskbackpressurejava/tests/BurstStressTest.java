package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.BackpressurePolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.EngineMetrics;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskFuture;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 可重复的突发流量压力测试（纯本地、确定性、不依赖外部服务）：
 * 多线程突发提交超过处理能力 -> 观察 REJECT 背压与队列状态 -> 压力解除后全部已接受任务完成、
 * 队列归零、worker 线程数恒定（无线程泄漏）、结果计数守恒。
 */
class BurstStressTest {

    private static final Logger log = LoggerFactory.getLogger(BurstStressTest.class);

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
    void burstTrafficIsBoundedAndSystemRecoversWithoutLeak() throws Exception {
        int workers = 4;
        int capacity = 100;
        AtomicInteger handled = new AtomicInteger();
        engine.register("load", ctx -> {
            handled.incrementAndGet();
            Thread.sleep(2); // 轻微处理耗时，制造 >处理能力 的突发
            return "ok";
        }, capacity, workers);
        engine.start();

        int threads = 8;
        int perThread = 250; // 共 2000，远超 100+4 的瞬时容量
        TaskSubmitOptions opts = TaskSubmitOptions.defaults()
                .withTimeout(Duration.ofSeconds(10))
                .withBackpressure(BackpressurePolicy.REJECT, Duration.ZERO);

        List<TaskFuture> accepted = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicInteger rejected = new AtomicInteger();
        List<Thread> producers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            Thread p = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    String id = "burst-" + (base + i);
                    try {
                        accepted.add(engine.submit(new TaskRequest(id, "load", null), opts));
                    } catch (TaskRejectedException e) {
                        assertEquals(TaskErrorCode.QUEUE_FULL, e.errorCode());
                        rejected.incrementAndGet();
                    }
                }
            }, "producer-" + t);
            producers.add(p);
        }
        int threadsBefore = countWorkerThreads();
        long t0 = System.currentTimeMillis();
        producers.forEach(Thread::start);
        for (Thread p : producers) {
            p.join(30_000);
        }
        long burstMs = System.currentTimeMillis() - t0;

        EngineMetrics mid = engine.metrics();
        log.info("burst done in {}ms: accepted={}, rejected={}, peakObservedQueue={}, activeWorkers={}",
                burstMs, accepted.size(), rejected.get(),
                mid.perType().get(0).queueSize(), mid.perType().get(0).activeWorkers());
        assertEquals(threads * perThread, accepted.size() + rejected.get(),
                "every submission must be either accepted or explicitly rejected, none lost");
        assertTrue(rejected.get() > 0, "over-capacity burst must trigger backpressure rejections");
        // 背压上界：任何时刻在系统里的任务数不可能超过 capacity+workers
        // （这里用最终守恒 + 队列容量保证，队列大小由独立背压测试覆盖）

        // 等待全部已接受任务完成
        long deadline = System.currentTimeMillis() + 30_000;
        int succeeded = 0;
        for (TaskFuture f : accepted) {
            TaskResult r = f.await(Duration.ofMillis(Math.max(1, deadline - System.currentTimeMillis())));
            assertEquals(TaskStatus.SUCCEEDED, r.status(), "accepted task must not be lost: " + f.requestId());
            succeeded++;
        }
        assertEquals(accepted.size(), handled.get(), "handler invocations == accepted tasks (exactly-once)");

        // 终态已确定；worker 的瞬时活跃计数在 finally 中归零，做短暂轮询等待指标收敛
        long settleDeadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < settleDeadline) {
            EngineMetrics probe = engine.metrics();
            if (probe.perType().get(0).queueSize() == 0
                    && probe.perType().get(0).activeWorkers() == 0) {
                break;
            }
            Thread.sleep(20);
        }
        EngineMetrics end = engine.metrics();
        log.info("recovered: succeeded={}, failed={}, timeout={}, cancelled={}, queueSize={}, active={}",
                end.totalSucceeded(), end.totalFailed(), end.totalTimeout(), end.totalCancelled(),
                end.perType().get(0).queueSize(), end.perType().get(0).activeWorkers());
        assertEquals(0, end.totalFailed() + end.totalTimeout() + end.totalCancelled());
        assertEquals(0, end.perType().get(0).queueSize(), "queue must drain after relief");
        assertEquals(0, end.perType().get(0).activeWorkers(), "no task should remain running");
        assertEquals(accepted.size(), end.totalSucceeded());

        // 线程数稳定：worker 数恒定，不随突发增长（无泄漏）
        int threadsAfterBurst = countWorkerThreads();
        assertEquals(workers, threadsAfterBurst, "worker pool must stay fixed at configured size");
        assertEquals(threadsBefore, threadsAfterBurst, "no worker threads may be created/leaked");

        // 恢复后仍能正常提供服务
        TaskResult r = engine.submit(new TaskRequest("post-burst", "load", null))
                .await(Duration.ofSeconds(5));
        assertEquals(TaskStatus.SUCCEEDED, r.status());
    }

    private int countWorkerThreads() {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        Thread[] all = new Thread[root.activeCount() * 2 + 64];
        int n = root.enumerate(all, true);
        int count = 0;
        for (int i = 0; i < n; i++) {
            // 线程名格式 task-worker-<engineId>-load-<i>，用片段匹配本引擎本类型的 worker
            if (all[i].getName().contains("-load-")
                    && all[i].getName().startsWith("task-worker-")) {
                count++;
            }
        }
        return count;
    }
}
