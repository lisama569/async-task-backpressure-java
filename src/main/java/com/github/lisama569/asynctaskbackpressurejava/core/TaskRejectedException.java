package com.github.lisama569.asynctaskbackpressurejava.core;

/**
 * 提交被拒绝（队列满 / 引擎关闭 / 类型未注册 / 重复请求）时抛出的唯一异常类型，
 * 调用方只需处理它，永远接触不到线程池/队列内部异常。
 */
public class TaskRejectedException extends RuntimeException {

    private final TaskErrorCode errorCode;
    private final String requestId;

    public TaskRejectedException(TaskErrorCode errorCode, String requestId, String message) {
        super(message);
        this.errorCode = errorCode;
        this.requestId = requestId;
    }

    public TaskErrorCode errorCode() {
        return errorCode;
    }

    public String requestId() {
        return requestId;
    }
}
