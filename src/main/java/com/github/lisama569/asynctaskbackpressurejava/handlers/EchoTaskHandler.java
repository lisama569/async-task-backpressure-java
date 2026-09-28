package com.github.lisama569.asynctaskbackpressurejava.handlers;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskContext;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskHandler;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRequest;
import org.springframework.stereotype.Component;

/**
 * 最基础的本地处理器：按 payload 中的 sleepMs 睡眠指定时间后回显，
 * 睡眠可被中断，因此超时/取消能及时生效。payload 留空时等价于 sleepMs=0。
 */
@Component
public class EchoTaskHandler implements TaskHandler {

    @Override
    public String type() {
        return "echo";
    }

    @Override
    public boolean retryable() {
        return false;
    }

    @Override
    public String execute(TaskRequest request, TaskContext context) throws Exception {
        long sleepMs = Payloads.parseLong(request.getPayload(), "sleepMs", 0);
        long deadline = System.nanoTime() + sleepMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (context.isCancelled()) {
                throw new InterruptedException("cancelled");
            }
            long remaining = deadline - System.nanoTime();
            Thread.sleep(Math.min(50, Math.max(1, remaining / 1_000_000L)));
        }
        return "echo:" + request.getRequestId();
    }
}
