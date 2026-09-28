package com.github.lisama569.asynctaskbackpressurejava.engine;

import java.util.concurrent.ScheduledFuture;

/**
 * 引擎提供给各条类型流水线的共享设施：
 * 一个共享的调度定时器，用于执行超时判定与（未来的）周期性维护任务。
 *
 * <p>handler 直接在各类型自己的 worker 线程上执行，保证超时/取消的中断目标准确，
 * 也让不响应中断的 handler 最多只拖垮它所属类型的 worker，天然实现类型间故障隔离。
 */
interface EngineSupport {

    /** 引擎实例标识，用于线程命名，避免多实例下线程名混淆。 */
    String engineId();

    /** 在固定延迟后调度命令（执行超时中断等）。 */
    ScheduledFuture<?> schedule(Runnable command, long delayMillis);

    /** 记录一次“重试已发生”（引擎级聚合指标）。 */
    void incrementRetryCount();
}
