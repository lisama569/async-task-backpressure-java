package com.github.lisama569.asynctaskbackpressurejava.engine;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单个任务类型的故障隔离单元，包含两部分：
 * <ol>
 *   <li><b>隔离舱（bulkhead）</b>：信号量限制该类型并发执行数，避免某类慢任务占满全部工作线程。</li>
 *   <li><b>熔断器（circuit breaker）</b>：在 failureWindow 时间窗内失败次数达到阈值即 OPEN，
 *       OPEN 期间该类型新任务立即以 CIRCUIT_OPEN 判定失败（不占用线程）；
 *       openDuration 后进入 HALF_OPEN 放行一次探测：成功则关闭，失败则继续 OPEN。</li>
 * </ol>
 * 因此"某一类任务持续失败"只会导致该类型被熔断，其他类型正常服务；
 * 故障解除后半开探测成功即可自行恢复。
 */
public final class TypeIsolation {

    public enum CircuitState {
        CLOSED, OPEN, HALF_OPEN
    }

    private final String type;
    private final Semaphore permits;
    private final int concurrency;
    private final int failureThreshold;
    private final long failureWindowNanos;
    private final long openDurationNanos;

    private final Object breakerLock = new Object();
    private final Deque<Long> failureTimes = new ArrayDeque<>();
    private volatile CircuitState circuit = CircuitState.CLOSED;
    private volatile long openedAtNanos;

    private final AtomicInteger inflight = new AtomicInteger();

    public TypeIsolation(String type, int concurrency, int failureThreshold,
                         long failureWindowNanos, long openDurationNanos) {
        this.type = type;
        this.concurrency = Math.max(1, concurrency);
        this.permits = new Semaphore(this.concurrency, true);
        this.failureThreshold = Math.max(1, failureThreshold);
        this.failureWindowNanos = failureWindowNanos;
        this.openDurationNanos = openDurationNanos;
    }

    public String type() {
        return type;
    }

    public int concurrency() {
        return concurrency;
    }

    /**
     * 尝试获取该类型的执行许可。
     *
     * @return null 表示许可获取成功；否则返回拒绝原因（熔断打开 / 隔离舱已满）
     */
    public AcquireResult tryAcquire() {
        synchronized (breakerLock) {
            long now = System.nanoTime();
            if (circuit == CircuitState.OPEN) {
                if (now - openedAtNanos >= openDurationNanos) {
                    circuit = CircuitState.HALF_OPEN;
                } else {
                    return AcquireResult.CIRCUIT_OPEN;
                }
            }
            if (!permits.tryAcquire()) {
                return circuit == CircuitState.HALF_OPEN
                        ? AcquireResult.CIRCUIT_OPEN
                        : AcquireResult.BULKHEAD_FULL;
            }
            inflight.incrementAndGet();
            return AcquireResult.OK;
        }
    }

    /**
     * 在剩余时间内等待许可（隔离舱满时调用方在工作线程上降速等待，形成对下游处理器的背压）。
     *
     * @param nanos 最多等待纳秒数
     * @return null 表示拿到许可；否则为拒绝原因
     */
    public AcquireResult acquire(long nanos) throws InterruptedException {
        synchronized (breakerLock) {
            long now = System.nanoTime();
            if (circuit == CircuitState.OPEN) {
                if (now - openedAtNanos >= openDurationNanos) {
                    circuit = CircuitState.HALF_OPEN;
                } else {
                    return AcquireResult.CIRCUIT_OPEN;
                }
            }
        }
        boolean got = permits.tryAcquire(Math.max(1, nanos), TimeUnit.NANOSECONDS);
        if (!got) {
            return circuitState() == CircuitState.OPEN ? AcquireResult.CIRCUIT_OPEN
                    : AcquireResult.BULKHEAD_FULL;
        }
        inflight.incrementAndGet();
        if (circuitState() == CircuitState.OPEN) {
            permits.release();
            inflight.decrementAndGet();
            return AcquireResult.CIRCUIT_OPEN;
        }
        return AcquireResult.OK;
    }

    public void release() {
        permits.release();
        inflight.decrementAndGet();
    }

    public void recordSuccess() {
        synchronized (breakerLock) {
            failureTimes.clear();
            circuit = CircuitState.CLOSED;
        }
    }

    public void recordFailure() {
        synchronized (breakerLock) {
            long now = System.nanoTime();
            if (circuit == CircuitState.HALF_OPEN) {
                circuit = CircuitState.OPEN;
                openedAtNanos = now;
                return;
            }
            failureTimes.addLast(now);
            evictExpired(now);
            if (failureTimes.size() >= failureThreshold) {
                circuit = CircuitState.OPEN;
                openedAtNanos = now;
            }
        }
    }

    private void evictExpired(long now) {
        while (!failureTimes.isEmpty() && now - failureTimes.peekFirst() > failureWindowNanos) {
            failureTimes.pollFirst();
        }
    }

    public CircuitState circuitState() {
        synchronized (breakerLock) {
            long now = System.nanoTime();
            if (circuit == CircuitState.OPEN && now - openedAtNanos >= openDurationNanos) {
                return CircuitState.HALF_OPEN;
            }
            return circuit;
        }
    }

    public int inflight() {
        return inflight.get();
    }

    public enum AcquireResult {
        OK, BULKHEAD_FULL, CIRCUIT_OPEN
    }
}
