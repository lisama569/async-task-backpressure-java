package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 有界阻塞队列，按“有效优先级 + FIFO”调度，并支持普通任务老化提权。
 *
 * <p>调度规则（每次取任务时按当前时间重新计算，可解释、自稳定）：
 * <ol>
 *   <li>已经老化（等待时长达到阈值）的 NORMAL 任务与 HIGH 任务排在未老化的 NORMAL 之前；</li>
 *   <li>同一档内严格按入队时间先后（FIFO）；</li>
 *   <li>HIGH 任务在短期内严格先于 NORMAL，但 NORMAL 最多等待一个老化阈值后必然被提升，
 *       因此不会被 HIGH 流量无限饿死。</li>
 * </ol>
 *
 * <p>容量有界：{@link #tryOffer} 在满时立即返回 false，是否阻塞等待由上层背压策略决定，
 * 队列自身不向调用方抛出任何运行时异常。
 */
final class AgingPriorityQueue {

    private final int capacity;
    private final long agingThresholdNanos;
    private final ArrayList<TaskEntry> items;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    AgingPriorityQueue(int capacity, long agingThresholdNanos) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.agingThresholdNanos = agingThresholdNanos;
        this.items = new ArrayList<>(capacity);
    }

    int capacity() {
        return capacity;
    }

    int size() {
        lock.lock();
        try {
            return items.size();
        } finally {
            lock.unlock();
        }
    }

    /** 非阻塞入队，满则返回 false（REJECT 背压策略）。 */
    boolean tryOffer(TaskEntry entry) {
        lock.lock();
        try {
            if (items.size() >= capacity) {
                return false;
            }
            items.add(entry);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 限时等待空位的入队（BLOCK 背压策略），超时仍满则返回 false。 */
    boolean offer(TaskEntry entry, long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (items.size() >= capacity) {
                if (nanos <= 0L) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            items.add(entry);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 阻塞获取一个任务；引擎关闭时通过中断解除。 */
    TaskEntry take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (items.isEmpty()) {
                notEmpty.await();
            }
            return removeBestLocked();
        } finally {
            lock.unlock();
        }
    }

    /** 限时获取；超时返回 null。 */
    TaskEntry poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (items.isEmpty()) {
                if (nanos <= 0L) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            return removeBestLocked();
        } finally {
            lock.unlock();
        }
    }

    /** 删除仍在排队的任务（排队取消用）。返回被删除条目，找不到返回 null。 */
    TaskEntry removeIfQueued(String requestId) {
        lock.lock();
        try {
            for (int i = 0; i < items.size(); i++) {
                TaskEntry e = items.get(i);
                if (e.requestId().equals(requestId)) {
                    items.remove(i);
                    notFull.signal();
                    return e;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /** 队中最老任务的等待毫秒数；空队列为 0。 */
    long oldestAgeMillis(long nowNanos) {
        lock.lock();
        try {
            long oldest = 0L;
            for (TaskEntry e : items) {
                oldest = Math.max(oldest, nowNanos - e.enqueuedAtNanos());
            }
            return TaskEntry.nanosToMillis(oldest);
        } finally {
            lock.unlock();
        }
    }

    /** 唤醒所有等待消费者，配合关闭流程排空。 */
    void signalAllForShutdown() {
        lock.lock();
        try {
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private TaskEntry removeBestLocked() {
        long now = System.nanoTime();
        int best = 0;
        for (int i = 1; i < items.size(); i++) {
            if (shouldPrecede(items.get(i), items.get(best), now)) {
                best = i;
            }
        }
        TaskEntry winner = items.remove(best);
        notFull.signal();
        return winner;
    }

    /** a 是否应先于 b 执行：先比档位（HIGH/已老化=0），同档位比入队时间（FIFO）。 */
    private boolean shouldPrecede(TaskEntry a, TaskEntry b, long nowNanos) {
        int tierA = tier(a, nowNanos);
        int tierB = tier(b, nowNanos);
        if (tierA != tierB) {
            return tierA < tierB;
        }
        return a.enqueuedAtNanos() < b.enqueuedAtNanos();
    }

    private int tier(TaskEntry e, long nowNanos) {
        TaskPriority p = e.effectivePriority(nowNanos, agingThresholdNanos);
        return p == TaskPriority.HIGH ? 0 : 1;
    }
}
