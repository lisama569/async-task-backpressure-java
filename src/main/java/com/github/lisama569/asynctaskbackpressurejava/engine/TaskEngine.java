package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.FailureKind;
import com.github.lisama569.asynctaskbackpressurejava.core.InvalidTaskException;
import com.github.lisama569.asynctaskbackpressurejava.core.RejectPolicy;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskExecutionService;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandlerRegistry;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskPriority;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地任务处理引擎。
 *
 * <p>并发模型：固定数量的工作线程（可选虚拟线程）从单个有界优先级队列拉取任务；
 * 任务状态用 CAS 保证唯一判定；按任务类型用信号量隔离舱限并发、用熔断器隔离持续失败；
 * 超时通过守护调度线程对执行线程发中断实现，绝不把线程池/队列内部异常透传给调用方。
 */
public final class TaskEngine implements TaskExecutionService {

    private static final Logger log = LoggerFactory.getLogger(TaskEngine.class);

    private final TaskEngineProperties props;
    private final TaskHandlerRegistry registry;
    private final RetryPolicy retryPolicy;
    private final IsolationRegistry isolations;
    private final PriorityTaskQueue queue;

    private final ConcurrentHashMap<String, TaskEntry> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskResult> completed = new ConcurrentHashMap<>();
    private final AtomicLong completedTotal = new AtomicLong();

    private final AtomicLong succeededCount = new AtomicLong();
    private final AtomicLong failedCount = new AtomicLong();
    private final AtomicLong timeoutCount = new AtomicLong();
    private final AtomicLong cancelledCount = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> succeededByType = new ConcurrentHashMap<>();

    private volatile ExecutorService workers;
    private volatile ScheduledExecutorService retryScheduler;
    private volatile ScheduledExecutorService watchdog;
    private volatile boolean running;

    public TaskEngine(TaskEngineProperties props, TaskHandlerRegistry registry) {
        this.props = props;
        this.registry = registry;
        this.retryPolicy = new RetryPolicy(props.getRetry());
        this.isolations = new IsolationRegistry(props);
        this.queue = new PriorityTaskQueue(props.getQueueCapacity(), props.getAgingMillis());
    }

    // ------------------------------------------------------------------ submit

    @Override
    public TaskResult submit(TaskRequest request) {
        normalize(request);
        long now = System.nanoTime();
        Duration deadline = request.getDeadline() != null ? request.getDeadline()
                : props.getDefaultDeadline();
        TaskEntry entry = new TaskEntry(request, now, now + deadline.toNanos());

        if (tasks.putIfAbsent(request.getRequestId(), entry) != null) {
            throw new InvalidTaskException("duplicate requestId: " + request.getRequestId());
        }
        registry.find(request.getTaskType())
                .orElseThrow(() -> new InvalidTaskException(
                        "unknown taskType: " + request.getTaskType()));

        if (!running) {
            tasks.remove(request.getRequestId());
            throw new TaskRejectedException("engine is not running");
        }

        if (queue.offer(entry)) {
            log.info("submit accepted requestId={} type={} priority={} queue={}/{}",
                    request.getRequestId(), request.getTaskType(), request.getPriority(),
                    queue.size(), queue.capacity());
            return null;
        }

        return applyBackpressure(entry);
    }

    private TaskResult applyBackpressure(TaskEntry entry) {
        RejectPolicy policy = props.getRejectPolicy();
        TaskRequest req = entry.getRequest();
        switch (policy) {
            case ABORT -> {
                tasks.remove(req.getRequestId());
                rejectedCount.incrementAndGet();
                throw new TaskRejectedException("queue full, capacity=" + queue.capacity());
            }
            case BLOCK -> {
                try {
                    queue.put(entry);
                    log.info("submit unblocked requestId={} type={} queue={}/{}",
                            req.getRequestId(), req.getTaskType(), queue.size(), queue.capacity());
                    return null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    tasks.remove(req.getRequestId());
                    rejectedCount.incrementAndGet();
                    return terminal(entry, TaskState.REJECTED, FailureKind.INTERNAL,
                            "interrupted while waiting for queue space", null);
                }
            }
            case CALLER_RUNS -> {
                // 保持活动记录可查询；内联执行同样经历 QUEUED->RUNNING->终态，终态时统一清理
                rejectedCount.incrementAndGet();
                log.warn("submit caller-runs requestId={} type={} queue={}/{} -> execute on caller thread",
                        req.getRequestId(), req.getTaskType(), queue.size(), queue.capacity());
                return runInline(entry);
            }
            default -> throw new TaskRejectedException("unsupported reject policy: " + policy);
        }
    }

    private void normalize(TaskRequest request) {
        if (request == null) {
            throw new InvalidTaskException("request must not be null");
        }
        if (request.getRequestId() == null || request.getRequestId().isBlank()) {
            request.setRequestId(UUID.randomUUID().toString());
        }
        if (request.getTaskType() == null || request.getTaskType().isBlank()) {
            throw new InvalidTaskException("taskType must not be blank");
        }
        if (request.getPriority() == null) {
            request.setPriority(TaskPriority.NORMAL);
        }
        if (request.getTimeout() == null) {
            request.setTimeout(props.getDefaultTimeout());
        }
        if (request.getDeadline() == null) {
            request.setDeadline(props.getDefaultDeadline());
        }
    }

    /** CALLER_RUNS 策略：满队时由提交线程自己把任务跑完，天然对提交方降速。 */
    private TaskResult runInline(TaskEntry entry) {
        if (entry.casQueuedToRunning()) {
            entry.setWorkerName(Thread.currentThread().getName());
            entry.setStartedNanos(System.nanoTime());
        }
        return runAttemptLoop(entry, true);
    }

    // ------------------------------------------------------------------ query

    @Override
    public Optional<TaskResult> get(String requestId) {
        TaskResult done = completed.get(requestId);
        if (done != null) {
            return Optional.of(done);
        }
        TaskEntry entry = tasks.get(requestId);
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.of(intermediate(entry));
    }

    @Override
    public TaskResult cancel(String requestId) {
        TaskResult done = completed.get(requestId);
        if (done != null) {
            return done;
        }
        TaskEntry entry = tasks.get(requestId);
        if (entry == null) {
            return null;
        }
        if (queue.remove(requestId) != null
                && entry.casState(TaskState.QUEUED, TaskState.CANCELLED)) {
            cancelledCount.incrementAndGet();
            return terminal(entry, TaskState.CANCELLED, FailureKind.NONE,
                    "cancelled while queued", null);
        }
        if (entry.casState(TaskState.RUNNING, TaskState.CANCELLED)) {
            Thread runner = entry.getRunner();
            // claimInterrupt 与执行结束的 clearRunner 互斥：
            // 赢了才发中断，避免中断错发给已开始执行下一任务的 worker
            if (runner != null && entry.claimInterrupt(runner)) {
                runner.interrupt();
            }
            cancelledCount.incrementAndGet();
            return terminal(entry, TaskState.CANCELLED, FailureKind.NONE,
                    "cancelled while running", null);
        }
        return completed.get(requestId);
    }

    public EngineStats stats() {
        EngineStats s = new EngineStats();
        QueueStats q = queue.stats();
        s.setQueued(q.getQueued());
        s.setQueueCapacity(q.getCapacity());
        int running = 0;
        for (TaskEntry e : tasks.values()) {
            if (e.getState() == TaskState.RUNNING) {
                running++;
            }
        }
        s.setRunning(running);
        s.setActiveWorkers(running);
        s.setSucceeded(succeededCount.get());
        s.setFailed(failedCount.get());
        s.setTimeout(timeoutCount.get());
        s.setCancelled(cancelledCount.get());
        s.setRejected(rejectedCount.get());
        s.setTotalCompleted(completedTotal.get());
        Map<String, Long> byType = new LinkedHashMap<>();
        succeededByType.forEach((k, v) -> byType.put(k, v.get()));
        s.setCompletedByType(byType);
        Map<String, Object> typeInfo = new LinkedHashMap<>();
        isolations.all().forEach((type, iso) -> {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("concurrency", iso.concurrency());
            info.put("inflight", iso.inflight());
            info.put("circuit", iso.circuitState().name());
            typeInfo.put(type, info);
        });
        s.setTypes(typeInfo);
        return s;
    }

    // ----------------------------------------------------------------- worker

    @Override
    public void start() {
        if (running) {
            return;
        }
        running = true;
        ThreadFactory tf = r -> {
            Thread t = props.isUseVirtualThreads()
                    ? Thread.ofVirtual().unstarted(r)
                    : Thread.ofPlatform().daemon().name("task-worker-", 0).unstarted(r);
            return t;
        };
        workers = Executors.newThreadPerTaskExecutor(tf);
        ThreadFactory daemonFactory = r -> {
            Thread t = Thread.ofPlatform().daemon().name("task-scheduler-", 0).unstarted(r);
            return t;
        };
        retryScheduler = Executors.newScheduledThreadPool(1, daemonFactory);
        watchdog = Executors.newSingleThreadScheduledExecutor(daemonFactory);
        for (int i = 0; i < props.getWorkerThreads(); i++) {
            workers.submit(this::workerLoop);
        }
        try {
            // 等待所有 worker 真正阻塞在队列上等待任务，避免 start() 返回瞬间提交把队列打满
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (queue.idleConsumers() < props.getWorkerThreads()
                    && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            if (queue.idleConsumers() < props.getWorkerThreads()) {
                throw new IllegalStateException("workers did not become idle in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for workers", e);
        }
        log.info("task engine started workers={} queueCapacity={} rejectPolicy={} virtualThreads={}",
                props.getWorkerThreads(), props.getQueueCapacity(), props.getRejectPolicy(),
                props.isUseVirtualThreads());
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        shutdown(retryScheduler);
        shutdown(watchdog);
        shutdown(workers);
        log.info("task engine stopped");
    }

    private static void shutdown(ExecutorService es) {
        if (es == null) {
            return;
        }
        es.shutdownNow();
        try {
            es.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void workerLoop() {
        while (running) {
            try {
                TaskEntry entry = queue.poll(500, TimeUnit.MILLISECONDS);
                if (entry == null) {
                    continue;
                }
                if (entry.getAttempts().get() == 0 && entry.getState() == TaskState.QUEUED) {
                    // poll 已翻转，兜底
                    entry.casQueuedToRunning();
                }
                entry.setWorkerName(Thread.currentThread().getName());
                entry.setStartedNanos(System.nanoTime());
                runAttemptLoop(entry, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                // 任何内部异常都不能杀死工作线程，也不能透传给调用方
                log.error("worker internal error, swallowed", t);
            }
        }
    }

    // -------------------------------------------------------------- execution

    private TaskResult runAttemptLoop(TaskEntry entry, boolean inline) {
        while (true) {
            int attempt = entry.getAttempts().incrementAndGet();
            TaskContext context = new EngineTaskContext(entry, attempt);
            TaskRequest request = entry.getRequest();

            if (System.nanoTime() >= entry.getDeadlineNanos()) {
                return finish(entry, TaskState.TIMEOUT, FailureKind.NONE,
                        "skipped: deadline already reached before attempt " + attempt,
                        null, inline);
            }
            if (entry.getState() == TaskState.CANCELLED) {
                // 状态只可能由 cancel() 翻转为 CANCELLED（并由其负责发布结果）；
                // 此处读等待其发布，避免重复发布
                return awaitPublishedResult(entry);
            }

            TypeIsolation isolation = isolations.forType(request.getTaskType());
            long remaining = entry.getDeadlineNanos() - System.nanoTime();
            TypeIsolation.AcquireResult acquired;
            try {
                // 正常路径与 CALLER_RUNS 内联路径都在隔离舱满时按剩余时间等待（对下游处理器背压），
                // 只有熔断打开才立即失败。
                acquired = isolation.acquire(remaining);
            } catch (InterruptedException e) {
                if (entry.casState(TaskState.RUNNING, TaskState.CANCELLED)) {
                    cancelledCount.incrementAndGet();
                    return terminal(entry, TaskState.CANCELLED, FailureKind.NONE,
                            "interrupted while waiting for bulkhead", null);
                }
                return completed.get(request.getRequestId());
            }

            if (acquired != TypeIsolation.AcquireResult.OK) {
                FailureKind kind = acquired == TypeIsolation.AcquireResult.CIRCUIT_OPEN
                        ? FailureKind.CIRCUIT_OPEN : FailureKind.BULKHEAD_REJECTED;
                TaskResult result = finish(entry, TaskState.FAILED, kind,
                        acquired == TypeIsolation.AcquireResult.CIRCUIT_OPEN
                                ? "circuit breaker open for type: " + request.getTaskType()
                                : "bulkhead saturated for type: " + request.getTaskType(),
                        null, inline);
                isolation.recordFailure();
                return result;
            }

            AttemptOutcome outcome;
            try {
                outcome = executeOnce(entry, context, isolation);
            } finally {
                isolation.release();
            }

            switch (outcome.state()) {
                case SUCCEEDED -> {
                    isolation.recordSuccess();
                    return finish(entry, TaskState.SUCCEEDED, FailureKind.NONE, null,
                            outcome.value(), inline);
                }
                case CANCELLED -> {
                    // outcome 为 CANCELLED 意味着 cancel() 已赢得 CAS 并负责发布结果；
                    // 若本路径恰好先观察到（理论上状态翻转权只在 cancel/timeout），等待赢家发布
                    return awaitPublishedResult(entry);
                }
                case TIMEOUT -> {
                    isolation.recordFailure();
                    return finish(entry, TaskState.TIMEOUT, FailureKind.NONE,
                            outcome.message(), null, inline);
                }
                case FAILED -> {
                    isolation.recordFailure();
                    TaskHandler handler = registry.find(request.getTaskType()).orElse(null);
                    boolean canRetry = handler != null && handler.retryable() && request.isIdempotent()
                            && attempt < retryPolicy.maxAttempts();
                    if (canRetry) {
                        Duration backoff = retryPolicy.backoffForAttempt(attempt);
                        long backoffNanos = backoff.toNanos();
                        if (System.nanoTime() + backoffNanos < entry.getDeadlineNanos()) {
                            if (inline) {
                                // CALLER_RUNS：提交线程同步退避后在本线程继续尝试，
                                // 保证 submit() 返回时一定持有终态结果
                                log.info("inline retry backoff requestId={} attempt={} backoffMs={}",
                                        request.getRequestId(), attempt + 1, backoff.toMillis());
                                try {
                                    Thread.sleep(backoff.toMillis());
                                } catch (InterruptedException e) {
                                    if (entry.casState(TaskState.RUNNING, TaskState.CANCELLED)) {
                                        cancelledCount.incrementAndGet();
                                        return terminal(entry, TaskState.CANCELLED,
                                                FailureKind.NONE,
                                                "interrupted during inline retry backoff", null);
                                    }
                                    return completed.get(request.getRequestId());
                                }
                                continue;
                            }
                            scheduleRetry(entry, backoff);
                            return null;
                        }
                        return finish(entry, TaskState.FAILED, FailureKind.RETRIES_EXHAUSTED,
                                outcome.message() + " (retry stopped: deadline would be exceeded)",
                                null, inline);
                    }
                    boolean hadRetriesLeft = attempt < retryPolicy.maxAttempts();
                    FailureKind kind = hadRetriesLeft
                            ? FailureKind.USER_EXCEPTION
                            : FailureKind.RETRIES_EXHAUSTED;
                    return finish(entry, TaskState.FAILED, kind, outcome.message(), null, inline);
                }
                default -> {
                    return finish(entry, TaskState.FAILED, FailureKind.INTERNAL,
                            "unexpected internal state", null, inline);
                }
            }
        }
    }

    private AttemptOutcome executeOnce(TaskEntry entry, TaskContext context, TypeIsolation isolation) {
        TaskRequest request = entry.getRequest();
        TaskHandler handler = registry.find(request.getTaskType()).orElseThrow();
        long timeoutNanos = Math.min(request.getTimeout().toNanos(),
                Math.max(1, entry.getDeadlineNanos() - System.nanoTime()));

        Thread runner = Thread.currentThread();
        entry.beginAttempt(runner);
        ScheduledFutureTicket ticket = new ScheduledFutureTicket();
        ticket.future = watchdog.schedule(() -> {
            // 必须赢得本次执行的中断权：任务仍 RUNNING、执行线程未切换、且无他人先赢
            if (entry.getState() == TaskState.RUNNING && entry.claimInterrupt(runner)) {
                entry.markTimedOut();
                runner.interrupt();
            }
        }, timeoutNanos, TimeUnit.NANOSECONDS);

        long start = System.nanoTime();
        String returnedValue = null;
        try {
            try {
                returnedValue = handler.execute(request, context);
            } catch (InterruptedException e) {
                if (entry.getState() == TaskState.CANCELLED) {
                    return new AttemptOutcome(TaskState.CANCELLED, null,
                            "cancelled during execution");
                }
                return new AttemptOutcome(TaskState.TIMEOUT, null,
                        "interrupted: execution timeout");
            } catch (Throwable t) {
                if (entry.getState() == TaskState.CANCELLED) {
                    return new AttemptOutcome(TaskState.CANCELLED, null,
                            "cancelled during execution");
                }
                if (entry.isTimedOut()) {
                    return new AttemptOutcome(TaskState.TIMEOUT, null,
                            "timed out and task threw: " + t.getClass().getSimpleName());
                }
                return new AttemptOutcome(TaskState.FAILED, null,
                        "task threw " + t.getClass().getSimpleName()
                                + (t.getMessage() != null ? ": " + t.getMessage() : ""));
            }

            // 正常返回后的裁决只看权威状态，绝不使用墙上时间二次判断（避免与看门狗竞争导致不一致）：
            //  - CANCELLED：取消已赢下终态；
            //  - 中断权已被看门狗赢走：超时已赢下终态（处理器可能忽略了中断而正常返回）；
            //  - 否则成功，携带处理器返回值。
            if (entry.getState() == TaskState.CANCELLED) {
                return new AttemptOutcome(TaskState.CANCELLED, null,
                        "cancelled during execution");
            }
            if (entry.interruptWasClaimed()) {
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                return new AttemptOutcome(TaskState.TIMEOUT, null,
                        "timeout after " + elapsed + "ms (limit="
                                + TimeUnit.NANOSECONDS.toMillis(timeoutNanos) + "ms)");
            }
            return new AttemptOutcome(TaskState.SUCCEEDED, returnedValue, null);
        } finally {
            // 摘除执行槽；看门狗未赢走中断权时清掉可能残留的中断标记，
            // 既不把中断带给下一个任务，也不影响下一次尝试的中断权判定
            boolean claimed = entry.clearRunner(runner);
            ticket.future.cancel(true);
            if (!claimed) {
                Thread.interrupted();
            }
        }
    }

    private void scheduleRetry(TaskEntry entry, Duration backoff) {
        log.info("retry scheduled requestId={} type={} nextAttempt={} backoffMs={} deadlineInMs={}",
                entry.getRequest().getRequestId(), entry.getRequest().getTaskType(),
                entry.getAttempts().get() + 1, backoff.toMillis(),
                TimeUnit.NANOSECONDS.toMillis(entry.getDeadlineNanos() - System.nanoTime()));
        retryScheduler.schedule(() -> {
            try {
                if (entry.getState() != TaskState.RUNNING) {
                    return;
                }
                if (System.nanoTime() >= entry.getDeadlineNanos()) {
                    finish(entry, TaskState.TIMEOUT, FailureKind.NONE,
                            "retry aborted: deadline reached during backoff", null, false);
                    return;
                }
                if (queue.requeue(entry)) {
                    return;
                }
                // 重试时队列恰好满：提交到 worker 线程池直接继续，避免任务丢失
                workers.submit(() -> runAttemptLoop(entry, false));
            } catch (Throwable t) {
                log.error("retry dispatch error requestId={}", entry.getRequest().getRequestId(), t);
            }
        }, backoff.toMillis(), TimeUnit.MILLISECONDS);
    }

    // --------------------------------------------------------------- terminal

    private TaskResult finish(TaskEntry entry, TaskState state, FailureKind kind,
                              String message, String value, boolean inline) {
        // 统一的终态裁决：RUNNING -> 目标状态。只有 CAS 赢家才允许发布结果；
        // 输家（取消/超时/成功相互竞争时）读取赢家发布的结果，绝不用本路径的 value 再造结果。
        if (!entry.casState(TaskState.RUNNING, state)) {
            return awaitPublishedResult(entry);
        }
        return terminal(entry, state, kind, message, value);
    }

    /** CAS 输家等待赢家（cancel/timeout 路径）把终态结果发布出来。 */
    private TaskResult awaitPublishedResult(TaskEntry entry) {
        String id = entry.getRequest().getRequestId();
        TaskResult existing = completed.get(id);
        for (int i = 0; existing == null && i < 1000; i++) {
            if (i < 100) {
                Thread.onSpinWait();
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            existing = completed.get(id);
        }
        if (existing != null) {
            return existing;
        }
        // 理论不可达：赢家始终在 CAS 后同步发布。兜底返回当前观测状态，绝不覆盖赢家结果。
        log.error("winner did not publish terminal result requestId={} observedState={}",
                id, entry.getState());
        return completed.computeIfAbsent(id, k -> {
            TaskResult fallback = new TaskResult();
            fallback.setRequestId(k);
            fallback.setState(entry.getState());
            fallback.setFailureKind(FailureKind.INTERNAL);
            fallback.setMessage("terminal publication fallback");
            fallback.setAttempts(entry.getAttempts().get());
            return fallback;
        });
    }

    private TaskResult terminal(TaskEntry entry, TaskState state, FailureKind kind,
                                String message, String value) {
        TaskRequest req = entry.getRequest();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - entry.getEnqueueNanos());
        TaskResult result = new TaskResult();
        result.setRequestId(req.getRequestId());
        result.setState(state);
        result.setFailureKind(kind != null ? kind : FailureKind.NONE);
        result.setMessage(message);
        result.setValue(value);
        result.setAttempts(entry.getAttempts().get());
        result.setElapsedMillis(elapsed);

        TaskResult previous = completed.putIfAbsent(req.getRequestId(), result);
        if (previous != null) {
            return previous;
        }
        completedTotal.incrementAndGet();
        switch (state) {
            case SUCCEEDED -> {
                succeededCount.incrementAndGet();
                succeededByType.computeIfAbsent(req.getTaskType(), k -> new AtomicLong())
                        .incrementAndGet();
            }
            case FAILED -> failedCount.incrementAndGet();
            case TIMEOUT -> timeoutCount.incrementAndGet();
            case CANCELLED -> cancelledCount.incrementAndGet();
            case REJECTED -> rejectedCount.incrementAndGet();
            default -> { }
        }
        evictIfNeeded();
        QueueStats q = queue.stats();
        log.info("terminal requestId={} type={} verdict={} failureKind={} attempts={} elapsedMs={} worker={} queue={}/{} runningHigh={} msg={}",
                req.getRequestId(), req.getTaskType(), state, result.getFailureKind(),
                result.getAttempts(), elapsed, entry.getWorkerName(),
                q.getQueued(), q.getCapacity(), q.getHighPriorityQueued(), message);
        // 终态结果发布后再移除活动记录，保证任意时刻 get() 都能查到中间态或终态之一
        tasks.remove(req.getRequestId());
        return result;
    }

    private void evictIfNeeded() {
        int retention = props.getCompletedRetention();
        while (completed.size() > retention) {
            String oldest = completed.keys().nextElement();
            completed.remove(oldest);
        }
    }

    private TaskResult intermediate(TaskEntry entry) {
        TaskResult r = new TaskResult();
        r.setRequestId(entry.getRequest().getRequestId());
        r.setState(entry.getState());
        r.setFailureKind(FailureKind.NONE);
        r.setAttempts(entry.getAttempts().get());
        r.setElapsedMillis(TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - entry.getEnqueueNanos()));
        return r;
    }

    // ----------------------------------------------------------------- inner

    private record AttemptOutcome(TaskState state, String value, String message) {
    }

    private static final class ScheduledFutureTicket {
        volatile java.util.concurrent.ScheduledFuture<?> future;
    }

    private static final class EngineTaskContext implements TaskContext {
        private final TaskEntry entry;
        private final int attempt;

        EngineTaskContext(TaskEntry entry, int attempt) {
            this.entry = entry;
            this.attempt = attempt;
        }

        @Override
        public String requestId() {
            return entry.getRequest().getRequestId();
        }

        @Override
        public int attempt() {
            return attempt;
        }

        @Override
        public boolean isCancelled() {
            return entry.getState() == TaskState.CANCELLED;
        }
    }
}
