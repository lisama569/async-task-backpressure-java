package com.github.lisama569.asynctaskbackpressurejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 引擎可调参数，前缀 task-engine。
 *
 * <pre>
 * task-engine:
 *   aging-threshold-millis: 2000      # 普通任务老化提权阈值（防饿死）
 *   result-store-max-entries: 50000   # 状态/结果存储上限（只淘汰终态）
 *   result-ttl-millis: 120000         # 终态结果保留时长
 *   default-timeout-millis: 5000
 *   default-deadline-millis: 30000
 *   default-max-attempts: 1
 *   types:
 *     echo: {queue-capacity: 200, workers: 4}
 *     slow: {queue-capacity: 50, workers: 2}
 * </pre>
 */
@ConfigurationProperties(prefix = "task-engine")
public class TaskEngineProperties {

    /** 引擎开关，关闭后不注册演示流水线（手工装配时仍可自行 new LocalTaskEngine）。 */
    private boolean enabled = true;
    private long agingThresholdMillis = 2_000L;
    private int resultStoreMaxEntries = 50_000;
    private long resultTtlMillis = 120_000L;
    private long defaultTimeoutMillis = 5_000L;
    private long defaultDeadlineMillis = 30_000L;
    private int defaultMaxAttempts = 1;
    private long defaultRetryBackoffMillis = 100L;
    private Map<String, TypeConfig> types = new LinkedHashMap<>();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public long getAgingThresholdMillis() { return agingThresholdMillis; }
    public void setAgingThresholdMillis(long v) { this.agingThresholdMillis = v; }

    public int getResultStoreMaxEntries() { return resultStoreMaxEntries; }
    public void setResultStoreMaxEntries(int v) { this.resultStoreMaxEntries = v; }

    public long getResultTtlMillis() { return resultTtlMillis; }
    public void setResultTtlMillis(long v) { this.resultTtlMillis = v; }

    public long getDefaultTimeoutMillis() { return defaultTimeoutMillis; }
    public void setDefaultTimeoutMillis(long v) { this.defaultTimeoutMillis = v; }

    public long getDefaultDeadlineMillis() { return defaultDeadlineMillis; }
    public void setDefaultDeadlineMillis(long v) { this.defaultDeadlineMillis = v; }

    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int v) { this.defaultMaxAttempts = v; }

    public long getDefaultRetryBackoffMillis() { return defaultRetryBackoffMillis; }
    public void setDefaultRetryBackoffMillis(long v) { this.defaultRetryBackoffMillis = v; }

    public Map<String, TypeConfig> getTypes() { return types; }
    public void setTypes(Map<String, TypeConfig> types) { this.types = types; }

    /** 单个任务类型的流水线配置。 */
    public static class TypeConfig {
        private int queueCapacity = 200;
        private int workers = 2;

        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }

        public int getWorkers() { return workers; }
        public void setWorkers(int workers) { this.workers = workers; }
    }
}
