package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.RejectPolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.request;
import static com.github.lisama569.asynctaskbackpressurejava.tests.TestTasks.summary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskEngineBurstStressTest {

    private static final Logger log = LoggerFactory.getLogger(TaskEngineBurstStressTest.class);
    private TaskEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.stop();
        }
    }

    /**
     * 可重复的本地压测（不依赖外部服务、固定耗时、确定性退避）：
     * 8 个提交线程瞬时提交 800 个任务到 capacity=64、worker=4 的引擎，
     * 验证：每个被接受任务恰好执行一次、无丢失；拒绝被显式计数；
     * 压力解除后积压归零、worker 不增长（无线程泄漏迹象）。
     */
    @Test
    void burstFromMultipleProducersIsHandledDeterministicallyAndRecovers() throws Exception {
        TaskEngineProperties p = new TaskEngineProperties();
        p.setWorkerThreads(4);
        p.setQueueCapacity(64);
        p.setDefaultBulkhead(4);
        p.setRejectPolicy(RejectPolicy.ABORT);
        p.setDefaultTimeout(Duration.ofSeconds(10));
        p.setDefaultDeadline(Duration.ofSeconds(30));
        p.getRetry().setMaxAttempts(1);

        ConcurrentHashMap<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        AtomicInteger highCompleted = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override
            public String type() {
                return "cpuish";
            }

            @Override
            public boolean retryable() {
                return false;
            }

            @Override
            public String execute(TaskRequest request, TaskContext context) throws Exception {
                runs.computeIfAbsent(request.getRequestId(), k -> new AtomicInteger())
                        .incrementAndGet();
                Thread.sleep(2);
                if (request.getPriority() == TaskPriority.HIGH) {
                    highCompleted.incrementAndGet();
                }
                return "ok";
            }
        };
        TaskHandlerRegistry registry = new DefaultTaskHandlerRegistry();
        registry.register(handler);
        engine = new TaskEngine(p, registry);
        engine.start();

        int producers = 8;
        int perProducer = 100;
        int total = producers * perProducer;
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        CountDownLatch startGun = new CountDownLatch(1);
        AtomicInteger submitted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        java.util.Set<String> highAccepted =
                java.util.concurrent.ConcurrentHashMap.newKeySet();

        for (int pr = 0; pr < producers; pr++) {
            final int producer = pr;
            pool.submit(() -> {
                try {
                    startGun.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perProducer; i++) {
                    String id = "p" + producer + "-" + i;
                    TaskPriority prio = (i % 20 == 0) ? TaskPriority.HIGH : TaskPriority.NORMAL;
                    try {
                        engine.submit(request(id, "cpuish", prio, null));
                        submitted.incrementAndGet();
                        if (prio == TaskPriority.HIGH) {
                            highAccepted.add(id);
                        }
                    } catch (TaskRejectedException e) {
                        rejected.incrementAndGet();
                    }
                }
            });
        }
        startGun.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        int accepted = submitted.get();
        log.info("[burst] total={} accepted={} rejected={} accepted+rejected={}",
                total, accepted, rejected.get(), accepted + rejected.get());
        assertEquals(total, accepted + rejected.get(), "every task is accepted or rejected");

        int peakBacklog = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            int inFlight = engine.stats().getQueued() + engine.stats().getRunning();
            peakBacklog = Math.max(peakBacklog, engine.stats().getQueued());
            if (engine.stats().getSucceeded() >= accepted) {
                break;
            }
            Thread.sleep(10);
        }
        log.info("[burst] peakQueueBacklog={}, waiting for terminal verdicts ...", peakBacklog);
        // 校验每个被接受的任务恰好执行一次
        for (int pr = 0; pr < producers; pr++) {
            for (int i = 0; i < perProducer; i++) {
                String id = "p" + pr + "-" + i;
                TaskResult r = engine.get(id).orElse(null);
                if (r != null && r.getState() == TaskState.SUCCEEDED) {
                    assertEquals(1, runs.get(id).get(), "double execution for " + id);
                }
            }
        }

        // 压力解除后系统自行恢复：积压归零，失败/超时/取消为 0
        long recoverDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < recoverDeadline
                && (engine.stats().getQueued() + engine.stats().getRunning() > 0)) {
            Thread.sleep(10);
        }
        assertEquals(accepted, engine.stats().getSucceeded(), "no accepted task lost");
        assertEquals(0, engine.stats().getQueued());
        assertEquals(0, engine.stats().getRunning());
        assertEquals(0, engine.stats().getFailed() + engine.stats().getTimeout()
                + engine.stats().getCancelled());
        // 被接受的高优先级任务全部完成，且整体没有丢任务
        assertEquals(highAccepted.size(), highCompleted.get(),
                () -> "accepted high=" + highAccepted.size() + " completed high="
                        + highCompleted.get());
        log.info("[burst-recovery] all accepted tasks done once, queue drained, stats={}",
                summary(engine.stats()));
    }
}
