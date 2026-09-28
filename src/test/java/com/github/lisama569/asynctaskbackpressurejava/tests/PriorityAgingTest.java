package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 优先级与老化：单 worker 下可确定执行顺序。
 * HIGH 在短期内严格先于 NORMAL；NORMAL 等待超过老化阈值（测试引擎为 300ms）后被提升，
 * 因此持续的 HIGH 流量不能让 NORMAL 无限饿死。
 */
class PriorityAgingTest {

    private static final Logger log = LoggerFactory.getLogger(PriorityAgingTest.class);

    private LocalTaskEngine engine;
    private CountDownLatch holderStarted;
    private CountDownLatch release;
    private List<String> finishOrder;

    @BeforeEach
    void setUp() {
        engine = TestEngines.create();
        holderStarted = new CountDownLatch(1);
        release = new CountDownLatch(1);
        finishOrder = Collections.synchronizedList(new ArrayList<>());
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        engine.shutdown();
    }

    @Test
    void highPriorityJumpsAheadOfNormalButNormalEventuallyRuns() throws Exception {
        engine.register("ordered", ctx -> {
            if ("holder".equals(ctx.payload())) {
                holderStarted.countDown();
                release.await();
                return "holder";
            }
            finishOrder.add(ctx.payload());
            return ctx.payload();
        }, 50, 1); // 单 worker，保证调度顺序可判定
        engine.start();

        TaskSubmitOptions normal = TaskSubmitOptions.defaults()
                .withPriority(TaskPriority.NORMAL).withTimeout(Duration.ofSeconds(30));
        TaskSubmitOptions high = TaskSubmitOptions.defaults()
                .withPriority(TaskPriority.HIGH).withTimeout(Duration.ofSeconds(30));

        // 先占住唯一 worker
        engine.submit(new TaskRequest("holder", "ordered", "holder"),
                TaskSubmitOptions.defaults().withTimeout(Duration.ofSeconds(30)));
        assertTrue(holderStarted.await(2, java.util.concurrent.TimeUnit.SECONDS));

        // 排队：normal-old、normal-a 间隔提交（两者会在 release 前先后老化），
        // 随后持续插入 HIGH，最后放一个来不及老化的 normal-young。
        engine.submit(new TaskRequest("normal-old", "ordered", "normal-old"), normal);
        Thread.sleep(50);
        engine.submit(new TaskRequest("normal-a", "ordered", "normal-a"), normal);
        engine.submit(new TaskRequest("high-a", "ordered", "high-a"), high);
        engine.submit(new TaskRequest("high-b", "ordered", "high-b"), high);

        // 等待 normal-old / normal-a 超过 300ms 老化阈值，期间再补一个 HIGH 流量
        Thread.sleep(250);
        engine.submit(new TaskRequest("high-c", "ordered", "high-c"), high);

        // 最后 50ms 内提交的 normal-young 在 release 时尚未老化
        Thread.sleep(50);
        engine.submit(new TaskRequest("normal-young", "ordered", "normal-young"), normal);
        release.countDown();

        // 收集完成顺序
        String[] ids = {"normal-old", "normal-a", "high-a", "high-b", "high-c", "normal-young"};
        for (String id : ids) {
            TaskResult r = waitResult(id, 5_000);
            assertEquals(TaskStatus.SUCCEEDED, r.status(), id);
        }
        synchronized (finishOrder) {
            log.info("actual finish order: {}", finishOrder);
            // 规则1：已老化的普通任务（normal-old 最早入队）必须在最前，证明 HIGH 流量无法饿死它
            assertEquals("normal-old", finishOrder.get(0),
                    "aged normal task must be promoted and not starved");
            // 规则2：未老化的 normal-young 必须排在所有 HIGH 之后（HIGH 短期严格优先）
            for (String h : new String[]{"high-a", "high-b", "high-c"}) {
                assertTrue(finishOrder.indexOf(h) < finishOrder.indexOf("normal-young"),
                        h + " must run before un-aged normal-young");
            }
            // 规则3：normal-young 最终也执行了（没有被无限饿死，它只要再等 300ms 就会老化）
            assertEquals("normal-young", finishOrder.get(finishOrder.size() - 1));
        }
    }

    private TaskResult waitResult(String id, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            TaskResult r = engine.resultOf(id);
            if (r != null) {
                return r;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no result for " + id);
    }
}
