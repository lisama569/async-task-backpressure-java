package com.github.lisama569.asynctaskbackpressurejava.web.dto;

/**
 * 提交任务的 HTTP 请求体。所有控制字段可选，缺省取系统默认值。
 */
public class SubmitTaskRequest {

    private String requestId;
    private String taskType;
    private String payload;
    /** NORMAL / HIGH，默认 NORMAL。 */
    private String priority;
    /** 单次执行超时毫秒。 */
    private Long timeoutMillis;
    /** 整体截止时间毫秒（从提交时刻起），重试不得突破。 */
    private Long deadlineMillis;
    /** 最大执行次数，1 表示不重试。 */
    private Integer maxAttempts;
    /** 重试退避毫秒。 */
    private Long retryBackoffMillis;
    /** REJECT / BLOCK，默认 REJECT。 */
    private String backpressure;
    /** BLOCK 策略最长等待毫秒。 */
    private Long maxBlockWaitMillis;

    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getPriority() { return priority; }
    public void setPriority(String priority) { this.priority = priority; }
    public Long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(Long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
    public Long getDeadlineMillis() { return deadlineMillis; }
    public void setDeadlineMillis(Long deadlineMillis) { this.deadlineMillis = deadlineMillis; }
    public Integer getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
    public Long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(Long retryBackoffMillis) { this.retryBackoffMillis = retryBackoffMillis; }
    public String getBackpressure() { return backpressure; }
    public void setBackpressure(String backpressure) { this.backpressure = backpressure; }
    public Long getMaxBlockWaitMillis() { return maxBlockWaitMillis; }
    public void setMaxBlockWaitMillis(Long maxBlockWaitMillis) { this.maxBlockWaitMillis = maxBlockWaitMillis; }
}
