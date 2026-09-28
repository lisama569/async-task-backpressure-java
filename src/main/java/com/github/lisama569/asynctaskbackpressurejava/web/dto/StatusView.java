package com.github.lisama569.asynctaskbackpressurejava.web.dto;

import com.github.lisama569.asynctaskbackpressurejava.core.TaskResult;
import com.github.lisama569.asynctaskbackpressurejava.core.TaskStatusSnapshot;

/** 状态查询视图：任务未到终态时 result 为 null。 */
public record StatusView(TaskStatusSnapshot status, TaskResult result) {
}
