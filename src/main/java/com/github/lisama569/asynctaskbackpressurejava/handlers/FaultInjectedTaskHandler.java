package com.github.lisama569.asynctaskbackpressurejava.handlers;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import org.springframework.stereotype.Component;

/**
 * 故障注入处理器，全部行为由 payload 控制，不依赖任何外部服务：
 * <ul>
 *   <li>{@code failUntil=N}：前 N 次尝试抛异常，第 N+1 次起成功（验证重试在截止时间内收敛）；</li>
 *   <li>{@code alwaysFail=true}：每次尝试都抛异常（验证重试耗尽、熔断与故障隔离）；</li>
 *   <li>{@code sleepMs=N}：每次尝试延迟 N 毫秒，响应中断（用于制造稳定的处理耗时）；</li>
 *   <li>{@code busyMs=N}：使用不可中断的忙等，模拟不响应中断的处理器，
 *       验证超时判定仍会返回 TIMEOUT（线程泄漏风险通过隔离舱限制并发）。</li>
 * </ul>
 */
@Component
public class FaultInjectedTaskHandler implements TaskHandler {

    @Override
    public String type() {
        return "fault";
    }

    @Override
    public boolean retryable() {
        return true;
    }

    @Override
    public String execute(TaskRequest request, TaskContext context) throws Exception {
        String payload = request.getPayload();
        long sleepMs = Payloads.parseLong(payload, "sleepMs", 0);
        long busyMs = Payloads.parseLong(payload, "busyMs", 0);
        long failUntil = Payloads.parseLong(payload, "failUntil", 0);
        boolean alwaysFail = Boolean.parseBoolean(
                Payloads.parseString(payload, "alwaysFail", "false"));

        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
        if (busyMs > 0) {
            long until = System.nanoTime() + busyMs * 1_000_000L;
            while (System.nanoTime() < until) {
                // busy spin, intentionally non-interruptible
            }
        }

        if (alwaysFail || context.attempt() <= failUntil) {
            throw new IllegalStateException("injected failure on attempt " + context.attempt());
        }
        return "ok:" + request.getRequestId() + "#" + context.attempt();
    }
}
