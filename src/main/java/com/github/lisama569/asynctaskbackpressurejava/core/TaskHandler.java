package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 业务任务处理器。要求：
 * 1) 实现自身的幂等性，引擎可能在失败后用相同 payload 重新调用（ctx.attemptIndex() 递增）；
 * 2) 及时响应中断（超时时线程会被 interrupt），不要吞掉 InterruptedException。
 */
@FunctionalInterface
public interface TaskHandler {

    String handle(TaskContext ctx) throws Exception;
}
