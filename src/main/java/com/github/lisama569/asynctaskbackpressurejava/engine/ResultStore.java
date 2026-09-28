package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatus;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatusSnapshot;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务注册表与结果回收存储。
 *
 * <p>以 requestId 为主键持有 {@link TaskEntry}，因此它同时承担：
 * <ul>
 *   <li>幂等去重：提交时 putIfAbsent，同一 requestId 第二次提交得到可解释拒绝；</li>
 *   <li>状态查询：活跃或未淘汰的任务都能给出 {@link TaskStatusSnapshot}；</li>
 *   <li>结果回收：终态结果保存在 entry 内，配合 CompletableFuture 支持阻塞等待。</li>
 * </ul>
 *
 * <p>内存保护：只有终态条目会按 TTL 过期；容量超限时只淘汰最老的终态条目，
 * 活跃（QUEUED/RUNNING）任务永不淘汰，因此压力解除后不会出现任务丢失。
 */
final class ResultStore {

    private final ConcurrentHashMap<String, TaskEntry> entries = new ConcurrentHashMap<>();
    private final int maxEntries;
    private final long ttlNanos;

    ResultStore(int maxEntries, long ttlMillis) {
        this.maxEntries = maxEntries;
        this.ttlNanos = ttlMillis * 1_000_000L;
    }

    /** 不存在时放入；已存在（活跃或结果尚未过期）返回旧条目，用于重复提交检测。 */
    TaskEntry putIfAbsent(TaskEntry entry) {
        return entries.putIfAbsent(entry.requestId(), entry);
    }

    TaskEntry get(String requestId) {
        return entries.get(requestId);
    }

    int size() {
        return entries.size();
    }

    TaskStatusSnapshot snapshot(String requestId) {
        TaskEntry e = entries.get(requestId);
        if (e == null) {
            return null;
        }
        long now = System.nanoTime();
        long age = TaskEntry.nanosToMillis(now - e.enqueuedAtNanos());
        return new TaskStatusSnapshot(e.requestId(), e.taskType(), e.status(),
                e.options().priority().weight(), (int) e.attemptCount(),
                e.enqueuedAtNanos(), age);
    }

    TaskResult result(String requestId) {
        TaskEntry e = entries.get(requestId);
        return e == null ? null : e.terminalResult();
    }

    /**
     * 淘汰扫描：先移除 TTL 已过期的终态条目；若仍超容量，再按完成时间淘汰最老的终态条目，
     * 直到数量不超限或只剩活跃任务。引擎周期性调用。
     *
     * @return 本次淘汰条数
     */
    int sweep() {
        long now = System.nanoTime();
        int removed = 0;
        Iterator<Map.Entry<String, TaskEntry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            TaskEntry e = it.next().getValue();
            if (e.isTerminal() && now - e.completedAtNanos() >= ttlNanos) {
                it.remove();
                removed++;
            }
        }
        while (entries.size() > maxEntries) {
            TaskEntry oldest = null;
            for (TaskEntry e : entries.values()) {
                if (!e.isTerminal()) {
                    continue;
                }
                if (oldest == null || e.completedAtNanos() < oldest.completedAtNanos()) {
                    oldest = e;
                }
            }
            if (oldest == null) {
                break; // 剩下的全是活跃任务，绝不淘汰
            }
            if (entries.remove(oldest.requestId(), oldest)) {
                removed++;
            } else {
                break;
            }
        }
        return removed;
    }
}
