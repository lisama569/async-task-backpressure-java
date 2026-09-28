package com.github.lisama569.asynctaskbackpressurejava.engine;

import com.github.lisama569.asynctaskbackpressurejava.core.RejectPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "task.engine")
public class TaskEngineProperties {

    private int workerThreads = 4;
    private int queueCapacity = 100;
    private RejectPolicy rejectPolicy = RejectPolicy.ABORT;
    private Duration defaultTimeout = Duration.ofSeconds(5);
    private Duration defaultDeadline = Duration.ofSeconds(30);
    private long agingMillis = 200;
    private boolean useVirtualThreads = false;
    private int completedRetention = 10000;
    private int defaultBulkhead = 2;
    private int failureThreshold = 5;
    private Duration failureWindow = Duration.ofSeconds(10);
    private Duration openDuration = Duration.ofSeconds(2);
    private Retry retry = new Retry();

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public RejectPolicy getRejectPolicy() {
        return rejectPolicy;
    }

    public void setRejectPolicy(RejectPolicy rejectPolicy) {
        this.rejectPolicy = rejectPolicy;
    }

    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Duration defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    public Duration getDefaultDeadline() {
        return defaultDeadline;
    }

    public void setDefaultDeadline(Duration defaultDeadline) {
        this.defaultDeadline = defaultDeadline;
    }

    public long getAgingMillis() {
        return agingMillis;
    }

    public void setAgingMillis(long agingMillis) {
        this.agingMillis = agingMillis;
    }

    public boolean isUseVirtualThreads() {
        return useVirtualThreads;
    }

    public void setUseVirtualThreads(boolean useVirtualThreads) {
        this.useVirtualThreads = useVirtualThreads;
    }

    public int getCompletedRetention() {
        return completedRetention;
    }

    public void setCompletedRetention(int completedRetention) {
        this.completedRetention = completedRetention;
    }

    public int getDefaultBulkhead() {
        return defaultBulkhead;
    }

    public void setDefaultBulkhead(int defaultBulkhead) {
        this.defaultBulkhead = defaultBulkhead;
    }

    public int getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(int failureThreshold) {
        this.failureThreshold = failureThreshold;
    }

    public Duration getFailureWindow() {
        return failureWindow;
    }

    public void setFailureWindow(Duration failureWindow) {
        this.failureWindow = failureWindow;
    }

    public Duration getOpenDuration() {
        return openDuration;
    }

    public void setOpenDuration(Duration openDuration) {
        this.openDuration = openDuration;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public static class Retry {
        private int maxAttempts = 3;
        private Duration backoff = Duration.ofMillis(100);
        private double backoffMultiplier = 2.0;
        private Duration maxBackoff = Duration.ofSeconds(2);

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getBackoff() {
            return backoff;
        }

        public void setBackoff(Duration backoff) {
            this.backoff = backoff;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }

        public Duration getMaxBackoff() {
            return maxBackoff;
        }

        public void setMaxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
        }
    }
}
