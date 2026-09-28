package com.github.lisama569.asynctaskbackpressurejava.web.dto;

/** 提交成功响应（任务已入队）。 */
public record SubmitTaskResponse(String requestId, String taskType, String status, String detail) {
}
