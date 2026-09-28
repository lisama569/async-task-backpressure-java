package com.github.lisama569.asynctaskbackpressurejava.demo;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 内置演示处理器集合，供手工验证与压力测试/异常注入使用，全部本地实现、无外部依赖。
 *
 * <ul>
 *   <li>{@code echo}：立即回显 payload；</li>
 *   <li>{@code slow}：按 payload 指定毫秒睡眠（默认 200ms），响应中断，用于制造处理瓶颈与超时；</li>
 *   <li>{@code flaky}：前 N 次（按 attempt 计数）失败后成功，用于验证重试与幂等；
 *       payload 形如 {@code failUntil=2} 表示 attempt 1、2 失败，第 3 次成功；</li>
 *   <li>{@code alwaysFail}：恒抛异常，用于验证持续失败隔离与失败计数。</li>
 * </ul>
 */
@Component
public class DemoHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoHandlers.class);

    public TaskHandler echo() {
        return ctx -> {
            log.debug("echo handling: requestId={}, attempt={}", ctx.requestId(), ctx.attemptIndex());
            return "echo:" + ctx.payload();
        };
    }

    public TaskHandler slow() {
        return ctx -> {
            long sleepMs = parseLong(ctx.payload(), 200L);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            return "slept:" + sleepMs;
        };
    }

    public TaskHandler flaky() {
        // 以 requestId 为键记录这一逻辑请求已经执行到第几个 attempt，
        // 配合 handler 幂等要求：同一 attemptIndex 的重放仍给出一致结论。
        ConcurrentHashMap<String, AtomicInteger> seen = new ConcurrentHashMap<>();
        return ctx -> {
            int failUntil = (int) parseLong(ctx.payload(), 2L);
            AtomicInteger counter = seen.computeIfAbsent(ctx.requestId(), k -> new AtomicInteger(0));
            int observed = counter.incrementAndGet();
            // 引擎保证 attemptIndex 单调递增；这里以它作为幂等判定依据。
            if (ctx.attemptIndex() <= failUntil) {
                log.info("flaky injected failure: requestId={}, attempt={}, failUntil={}",
                        ctx.requestId(), ctx.attemptIndex(), failUntil);
                throw new IllegalStateException("injected transient failure #" + ctx.attemptIndex());
            }
            return "flaky-ok-after-" + ctx.attemptIndex() + "(observedCalls=" + observed + ")";
        };
    }

    public TaskHandler alwaysFail() {
        return ctx -> {
            log.warn("alwaysFail injected failure: requestId={}, attempt={}",
                    ctx.requestId(), ctx.attemptIndex());
            throw new IllegalStateException("injected permanent failure");
        };
    }

    private static long parseLong(String payload, long dflt) {
        if (payload == null || payload.isBlank()) {
            return dflt;
        }
        try {
            String p = payload.trim();
            if (p.startsWith("sleep=")) {
                return Long.parseLong(p.substring("sleep=".length()));
            }
            if (p.startsWith("failUntil=")) {
                return Long.parseLong(p.substring("failUntil=".length()));
            }
            return Long.parseLong(p);
        } catch (NumberFormatException e) {
            return dflt;
        }
    }
}
