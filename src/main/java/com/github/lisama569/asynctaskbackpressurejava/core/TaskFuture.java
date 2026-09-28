package com.github.lisama569.asynctaskbackpressurejava.core;

import java.time.Duration;
import java.util.concurrent.CompletionException;

/**
 * 提交成功后返回的句柄：可轮询状态、阻塞等待结果或取消。
 */
public interface TaskFuture {

    String requestId();

    TaskStatus status();

    /** 已终态时返回结果快照，否则返回 null。 */
    TaskResult poll();

    /** 阻塞等待终态；线程中断时包装成 CompletionException，不抛底层异常。 */
    TaskResult await() throws CompletionException;

    TaskResult await(Duration timeout) throws CompletionException;

    /**
     * 取消任务：排队中则直接转为 CANCELLED；执行中则发出中断，最终判定仍以终态为准。
     * @return 若任务已终态返回 false
     */
    boolean cancel();
}
