package com.github.lisama569.asynctaskbackpressurejava.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 单条类型流水线的计数：提交量与各终态数量。全部使用原子计数，指标查询无锁。
 */
final class PipelineCounters {

    final AtomicLong submitted = new AtomicLong();
    final AtomicLong succeeded = new AtomicLong();
    final AtomicLong failed = new AtomicLong();
    final AtomicLong cancelled = new AtomicLong();
    final AtomicLong timeout = new AtomicLong();
    final AtomicLong rejected = new AtomicLong();
    final AtomicLong retried = new AtomicLong();
    final AtomicLong active = new AtomicLong();
}
