package com.github.lisama569.asynctaskbackpressurejava.core;

public enum FailureKind {
    NONE,
    USER_EXCEPTION,
    RETRIES_EXHAUSTED,
    BULKHEAD_REJECTED,
    CIRCUIT_OPEN,
    INTERNAL
}
