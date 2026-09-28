package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 队列满时的背压策略：
 * REJECT：立即拒绝（返回可解释的业务异常，绝不抛出线程池内部异常）；
 * BLOCK：在限定时间内阻塞等待空位，超时后仍按 REJECT 语义拒绝。
 */
public enum BackpressurePolicy {
    REJECT,
    BLOCK
}
