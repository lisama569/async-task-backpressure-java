package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;
import com.github.lisama569.asynctaskbackpressurejava.engine.PriorityTaskQueue;
import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEntry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PriorityTaskQueueTest {

    private static TaskEntry entry(String id, TaskPriority priority, long enqueueAgoMillis) {
        TaskRequest req = new TaskRequest();
        req.setRequestId(id);
        req.setTaskType("echo");
        req.setPriority(priority);
        req.setTimeout(Duration.ofSeconds(1));
        long now = System.nanoTime();
        TaskEntry entry = new TaskEntry(req,
                now - TimeUnit.MILLISECONDS.toNanos(enqueueAgoMillis),
                now + Duration.ofSeconds(10).toNanos());
        return entry;
    }

    @Test
    void higherPriorityServedFirstAndFifoWithinSamePriority() throws Exception {
        PriorityTaskQueue queue = new PriorityTaskQueue(10, 10_000);
        TaskEntry low1 = entry("low1", TaskPriority.LOW, 0);
        TaskEntry normal1 = entry("normal1", TaskPriority.NORMAL, 0);
        TaskEntry high1 = entry("high1", TaskPriority.HIGH, 0);
        TaskEntry high2 = entry("high2", TaskPriority.HIGH, 0);
        queue.offer(low1);
        queue.offer(normal1);
        queue.offer(high1);
        queue.offer(high2);

        assertEquals("high1", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
        assertEquals("high2", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
        assertEquals("normal1", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
        assertEquals("low1", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
    }

    @Test
    void boundedQueueRejectsWhenFullAndPutBlocksUntilSpace() throws Exception {
        PriorityTaskQueue queue = new PriorityTaskQueue(2, 10_000);
        assertTrue(queue.offer(entry("a", TaskPriority.NORMAL, 0)));
        assertTrue(queue.offer(entry("b", TaskPriority.NORMAL, 0)));
        assertFalse(queue.offer(entry("c", TaskPriority.NORMAL, 0)));
        assertEquals(2, queue.size());

        TaskEntry blocked = entry("d", TaskPriority.NORMAL, 0);
        Thread putter = new Thread(() -> {
            try {
                queue.put(blocked);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        putter.start();
        Thread.sleep(100);
        assertTrue(putter.isAlive(), "putter should be blocked by backpressure");

        TaskEntry taken = queue.poll(1, TimeUnit.SECONDS);
        taken.setState(TaskState.RUNNING);
        putter.join(2000);
        assertFalse(putter.isAlive(), "putter should proceed once space is available");
        assertEquals(2, queue.size());
    }

    @Test
    void removeCancelledQueuedTaskById() {
        PriorityTaskQueue queue = new PriorityTaskQueue(10, 10_000);
        TaskEntry e = entry("cancel-me", TaskPriority.NORMAL, 0);
        queue.offer(e);
        assertSame(e, queue.remove("cancel-me"));
        assertNull(queue.remove("cancel-me"));
        assertEquals(0, queue.size());
    }

    @Test
    void agingPromotesLongWaitingNormalTaskBeforeFreshHighPriority() throws Exception {
        // 老化步进 40ms：NORMAL 等待 1 个步进后被视为 HIGH，且 HIGH 桶内按实际入队时间 FIFO
        PriorityTaskQueue queue = new PriorityTaskQueue(10, 40);
        TaskEntry freshHigh = entry("fresh-high", TaskPriority.HIGH, 0);
        queue.offer(freshHigh);
        TaskEntry olderHigh = entry("older-high", TaskPriority.HIGH, 5);
        queue.offer(olderHigh);
        // 一个已等待 60ms（>=2 步进）的 LOW 任务应晋升到 HIGH，
        // 并且因入队时间最早而在 HIGH 桶中排最先
        TaskEntry agedLow = entry("aged-low", TaskPriority.LOW, 60);
        queue.offer(agedLow);

        assertEquals("aged-low", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
        assertEquals("older-high", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
        assertEquals("fresh-high", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
    }

    @Test
    void freshHighPriorityStillBeatsFreshNormalTask() throws Exception {
        PriorityTaskQueue queue = new PriorityTaskQueue(10, 60_000);
        queue.offer(entry("normal", TaskPriority.NORMAL, 0));
        queue.offer(entry("high", TaskPriority.HIGH, 0));
        assertEquals("high", queue.poll(1, TimeUnit.SECONDS).getRequest().getRequestId());
    }
}
