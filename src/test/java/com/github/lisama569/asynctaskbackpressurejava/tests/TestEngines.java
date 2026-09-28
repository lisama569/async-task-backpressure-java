package com.github.lisama569.asynctaskbackpressurejava.tests;

import com.github.lisama569.asynctaskbackpressurejava.engine.LocalTaskEngine;

/**
 * 测试辅助：构造短老化阈值、短 TTL、短清理周期的引擎，便于确定性观测。
 */
final class TestEngines {

    private TestEngines() {
    }

    static LocalTaskEngine create() {
        // 老化阈值 300ms，存储 10000 条，终态 TTL 1s，清理周期 200ms
        return new LocalTaskEngine(300L, 10_000, 1_000L, 200L);
    }
}
