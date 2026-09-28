package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.engine.TaskEngineProperties.Retry;

import java.time.Duration;

/**
 * 重试规则：
 * <ul>
 *   <li>maxAttempts 表示包含首次执行在内的总尝试次数（1 表示不重试）。</li>
 *   <li>第 attempt 次失败后的退避 = min(backoff * multiplier^(attempt-1), maxBackoff)，确定性、无抖动，
 *       保证压力测试结果可稳定复现。</li>
 * </ul>
 * 是否允许重试还由调用方结合"处理器声明可重试 + 请求幂等 + 尚未超过截止时间"共同判定，
 * 本类只负责次数与退避的计算。
 */
public final class RetryPolicy {

    private final Retry config;

    public RetryPolicy(Retry config) {
        this.config = config;
    }

    public int maxAttempts() {
        return Math.max(1, config.getMaxAttempts());
    }

    /** attempt 从 1 开始，表示"第 attempt 次执行失败之后"的等待时长。 */
    public Duration backoffForAttempt(int attempt) {
        long base = config.getBackoff().toMillis();
        long cap = config.getMaxBackoff().toMillis();
        double factor = Math.pow(config.getBackoffMultiplier(), Math.max(0, attempt - 1));
        long backoff = (long) Math.min(cap, base * factor);
        return Duration.ofMillis(Math.max(0, backoff));
    }
}
