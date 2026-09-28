package com.github.lisama569.asynctaskbackpressurejava.web.dto;

/** 统一错误响应（骨架）。 */
public record ApiError(String requestId, String errorCode, String message) {
}
