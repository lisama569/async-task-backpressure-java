package com.github.lisama569.asynctaskbackpressurejava.web;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskErrorCode;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskRejectedException;
import com.github.lisama569.asynctaskbackpressurejava.web.dto.ApiError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一异常翻译：线程池/队列/框架的内部异常永远不以原始形态透传给调用方，
 * 调用方只见到稳定的 errorCode + 简短 message。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /** 引擎拒绝提交：队列满 / 重复请求 / 未知类型 / 非法参数 / 引擎未运行。 */
    @ExceptionHandler(TaskRejectedException.class)
    public ResponseEntity<ApiError> handleRejected(TaskRejectedException ex) {
        HttpStatus http = switch (ex.errorCode()) {
            case QUEUE_FULL, ENGINE_SHUTDOWN -> HttpStatus.SERVICE_UNAVAILABLE; // 503：背压，可重试
            case DUPLICATE_REQUEST -> HttpStatus.CONFLICT;                      // 409
            case UNKNOWN_TASK_TYPE, INVALID_REQUEST -> HttpStatus.BAD_REQUEST;  // 400
            default -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        log.info("task rejected: requestId={}, errorCode={}, httpStatus={}",
                ex.requestId(), ex.errorCode(), http.value());
        return ResponseEntity.status(http)
                .body(new ApiError(ex.requestId(), ex.errorCode().name(), ex.getMessage()));
    }

    /** 兜底：任何未预期异常都收敛成 EXECUTION_ERROR，不泄露堆栈与内部类名。 */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex) {
        log.error("unexpected controller error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiError(null, TaskErrorCode.EXECUTION_ERROR.name(), "internal error"));
    }
}
