package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskFuture;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 基于 {@link TaskEntry} 内部 CompletableFuture 的句柄实现。
 * 所有底层受检异常都被翻译，调用方不会见到线程池/队列内部异常类型。
 */
final class LocalTaskFuture implements TaskFuture {

    private final TaskEntry entry;
    private final LocalTaskEngine engine;

    LocalTaskFuture(TaskEntry entry, LocalTaskEngine engine) {
        this.entry = entry;
        this.engine = engine;
    }

    @Override
    public String requestId() {
        return entry.requestId();
    }

    @Override
    public TaskStatus status() {
        return entry.status();
    }

    @Override
    public TaskResult poll() {
        return entry.terminalResult();
    }

    @Override
    public TaskResult await() {
        try {
            return entry.completionFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException("await interrupted for request " + entry.requestId(), e);
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        }
    }

    @Override
    public TaskResult await(Duration timeout) {
        try {
            return entry.completionFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException("await interrupted for request " + entry.requestId(), e);
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        }
    }

    @Override
    public boolean cancel() {
        return engine.cancel(entry.requestId());
    }
}
