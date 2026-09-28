package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 有界优先级队列。
 *
 * <p>实现采用三个 FIFO 桶（HIGH/NORMAL/LOW），避免使用"比较结果随当前时间变化"的堆比较器
 * （那会破坏 {@link java.util.PriorityQueue} 的堆不变量）。调度规则（确定性）：
 * <ul>
 *   <li>任务入队时进入其原始优先级对应的桶；每次取任务前，把等待时间达到 agingNanos 整数倍、
 *       且应晋升的队首任务迁移到更高优先级桶尾部（LOW→NORMAL→HIGH）。</li>
 *   <li>因此普通/低优先级任务在等待约 agingMillis 后即按高优先级参与调度，不会被高优先级流量
 *       无限饿死；桶内严格 FIFO（以入队时间为准），高优先级任务等待只受"容量×单次执行时长"约束。</li>
 *   <li>容量固定；满队时由上层按拒绝策略处理（拒绝 / 调用方降速执行 / 阻塞等待空位）。</li>
 * </ul>
 */
public final class PriorityTaskQueue {

    private final int capacity;
    private final long agingNanos;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private final LinkedList<TaskEntry> high = new LinkedList<>();
    private final LinkedList<TaskEntry> normal = new LinkedList<>();
    private final LinkedList<TaskEntry> low = new LinkedList<>();
    private int size;

    private final AtomicInteger idleConsumers = new AtomicInteger(0);

    public PriorityTaskQueue(int capacity, long agingMillis) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.agingNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1, agingMillis));
    }

    public boolean offer(TaskEntry entry) {
        lock.lock();
        try {
            if (size >= capacity) {
                return false;
            }
            insertByEnqueueTime(bucketFor(entry.priority()), entry);
            size++;
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void put(TaskEntry entry) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (size >= capacity) {
                notFull.await();
            }
            insertByEnqueueTime(bucketFor(entry.priority()), entry);
            size++;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 取出一个可执行任务：在锁内完成优先级晋升与 QUEUED->RUNNING 翻转，
     * 保证同一任务只会被一个工作线程取走（不重复执行、不丢失）。
     */
    public TaskEntry poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (true) {
                promoteAged();
                TaskEntry entry = pollTakeableLocked();
                if (entry != null) {
                    return entry;
                }
                if (nanos <= 0) {
                    return null;
                }
                idleConsumers.incrementAndGet();
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } finally {
                    idleConsumers.decrementAndGet();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** 重试重入队：任务处于 RUNNING 且 attempts>0，取出时不再做状态翻转。 */
    public boolean requeue(TaskEntry entry) {
        lock.lock();
        try {
            if (size >= capacity) {
                return false;
            }
            // 重试任务重新参与调度，按入队时间插入原始优先级桶，同样受老化规则约束
            insertByEnqueueTime(bucketFor(entry.priority()), entry);
            size++;
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public TaskEntry remove(String requestId) {
        lock.lock();
        try {
            TaskEntry removed = removeFrom(high, requestId);
            if (removed == null) {
                removed = removeFrom(normal, requestId);
            }
            if (removed == null) {
                removed = removeFrom(low, requestId);
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    public List<TaskEntry> snapshot() {
        lock.lock();
        try {
            List<TaskEntry> all = new ArrayList<>(size);
            all.addAll(high);
            all.addAll(normal);
            all.addAll(low);
            return all;
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public int idleConsumers() {
        return idleConsumers.get();
    }

    public QueueStats stats() {
        lock.lock();
        try {
            long now = System.nanoTime();
            long oldestEnqueue = Long.MAX_VALUE;
            for (TaskEntry e : high) {
                oldestEnqueue = Math.min(oldestEnqueue, e.getEnqueueNanos());
            }
            for (TaskEntry e : normal) {
                oldestEnqueue = Math.min(oldestEnqueue, e.getEnqueueNanos());
            }
            for (TaskEntry e : low) {
                oldestEnqueue = Math.min(oldestEnqueue, e.getEnqueueNanos());
            }
            long oldestWait = size == 0 ? 0
                    : TimeUnit.NANOSECONDS.toMillis(now - oldestEnqueue);
            return new QueueStats(size, capacity, high.size(), oldestWait);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------- internals

    private LinkedList<TaskEntry> bucketFor(TaskPriority priority) {
        return switch (priority) {
            case HIGH -> high;
            case NORMAL -> normal;
            case LOW -> low;
        };
    }

    /**
     * 老化晋升：从每个非最高优先级桶的队首开始，把"等待时间已达到晋升步进"的任务迁入上一级桶尾。
     * 只迁移桶首连续的一串已到期任务，保证 FIFO 与晋升的确定性。
     */
    private void promoteAged() {
        long now = System.nanoTime();
        // 级联晋升直到本轮不再发生迁移：
        // LOW 等 1 步进->NORMAL；NORMAL（含刚从 LOW 升入者）等 1 步进->HIGH。
        // 因此等待了 2 个步进的 LOW 会在同一轮一直升到 HIGH。
        boolean moved;
        do {
            moved = false;
            while (!low.isEmpty()) {
                TaskEntry head = low.peekFirst();
                if ((now - head.getEnqueueNanos()) / agingNanos < 1) {
                    break;
                }
                low.pollFirst();
                insertByEnqueueTime(normal, head);
                moved = true;
            }
            while (!normal.isEmpty()) {
                TaskEntry head = normal.peekFirst();
                if ((now - head.getEnqueueNanos()) / agingNanos < 1) {
                    break;
                }
                normal.pollFirst();
                insertByEnqueueTime(high, head);
                moved = true;
            }
        } while (moved);
    }

    /** 桶内保持按入队时间有序，晋升进来的旧任务插入到其应处的 FIFO 位置。 */
    private void insertByEnqueueTime(LinkedList<TaskEntry> bucket, TaskEntry entry) {
        int idx = 0;
        for (TaskEntry e : bucket) {
            if (entry.getEnqueueNanos() < e.getEnqueueNanos()) {
                break;
            }
            idx++;
        }
        bucket.add(idx, entry);
    }

    /** 从 HIGH 起依次取一个可执行条目；跳过并丢弃已取消/已终态的陈旧条目。 */
    private TaskEntry pollTakeableLocked() {
        TaskEntry entry = takeFrom(high);
        if (entry == null) {
            entry = takeFrom(normal);
        }
        if (entry == null) {
            entry = takeFrom(low);
        }
        return entry;
    }

    private TaskEntry takeFrom(LinkedList<TaskEntry> bucket) {
        while (!bucket.isEmpty()) {
            TaskEntry entry = bucket.pollFirst();
            size--;
            notFull.signal();
            if (entry.getState() == TaskState.QUEUED && entry.getAttempts().get() == 0) {
                if (entry.casQueuedToRunning()) {
                    return entry;
                }
            } else if (entry.getState() == TaskState.RUNNING && entry.getAttempts().get() > 0) {
                // 退避后重新入队的重试任务，直接执行
                return entry;
            }
            // 已取消或已终态的陈旧条目，丢弃后继续尝试本桶后续条目
        }
        return null;
    }

    private TaskEntry removeFrom(LinkedList<TaskEntry> bucket, String requestId) {
        for (TaskEntry entry : bucket) {
            if (entry.getRequest().getRequestId().equals(requestId)) {
                bucket.remove(entry);
                size--;
                notFull.signal();
                return entry;
            }
        }
        return null;
    }
}
