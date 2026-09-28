# 本地异步任务处理与背压引擎（async-task-backpressure-java）

一套面向高并发场景的**本地**任务提交、并发执行与结果回收能力。不依赖任何外部服务
（无 MQ / 无 Redis / 无数据库），通过有界队列、确定性背压、优先级老化、截止时间约束的重试
与按任务类型隔离的流水线，保证系统在负载变化时仍然稳定、结果可解释。

## 1. 并发模型

```
                         提交 (submit)
                              │
                   参数校验 / 引擎存活检查
                              │
              ResultStore.putIfAbsent(requestId)   ← 幂等去重（同一请求只入系统一次）
                              │
        ┌──────────────────────┴───────────────────────┐
        │            按 taskType 路由（故障隔离）         │
        ▼                    ▼                  ▼
 TypePipeline[echo]   TypePipeline[slow]  TypePipeline[flaky] ...
 有界优先级队列(独立)   有界优先级队列(独立)   有界优先级队列(独立)
 worker 组(独立)        worker 组(独立)        worker 组(独立)
        │                    │                  │
        └──────────────┬─────┴──────────────────┘
                       ▼
          worker 直接执行用户 handler，编排：
          领取(CAS) → 超时定时器 → (失败)退避/重试 → 收敛终态
                       │
                       ▼
   ResultStore（状态/结果，TTL + 容量上限淘汰） + CompletableFuture（结果回收）
```

### 关键不变量

1. **同一任务恰好执行一次、不丢失**
   - 每个任务只有 `QUEUED → RUNNING → 终态` 三条状态迁移，且全部由 CAS 完成；
     `QUEUED → RUNNING` 全局只有一个 worker 能成功，因此不可能被执行两次。
   - 每次提交先以 `requestId` 做 `putIfAbsent` 去重：重复提交得到 `DUPLICATE_REQUEST` 拒绝，
     不会产生第二次执行或副作用。
   - 已被接受（入队）的任务一定会被执行并给出终态；突发时要么接受、要么得到明确的拒绝，
     不存在“静默丢弃”。
2. **重试不重新入队**：worker 在唯一的 RUNNING 许可内完成所有 attempt 与退避睡眠，
   取消与超时通过线程中断穿透退避，重试中的任务不会消失、不会被重复领取。
3. **内部异常不外泄**：调用方只会收到稳定的业务异常 `TaskRejectedException`（含 `TaskErrorCode`）
   或终态 `TaskResult`，永远接触不到线程池/队列内部异常类型；HTTP 层再翻译成稳定错误码。

## 2. 终态判定（互相可区分）

| 终态        | 触发条件                                   | errorCode            | HTTP |
|-------------|--------------------------------------------|----------------------|------|
| SUCCEEDED   | handler 正常返回（副作用已完成，成功优先） | -                    | -    |
| FAILED      | 执行抛异常且重试预算耗尽                   | EXECUTION_ERROR      | -    |
| CANCELLED   | 排队中被取消，或执行/退避中响应取消中断     | CANCELLED            | -    |
| TIMEOUT     | 单次执行超时，或整体截止时间到期           | DEADLINE_EXCEEDED    | -    |
| REJECTED    | 队列满 / 引擎关闭 / 类型未注册 / 重复请求  | QUEUE_FULL 等        | 见下 |

提交被拒绝时的 HTTP 语义：`QUEUE_FULL`/`ENGINE_SHUTDOWN → 503`（可重试）、
`DUPLICATE_REQUEST → 409`、`UNKNOWN_TASK_TYPE`/`INVALID_REQUEST → 400`。

## 3. 背压规则（确定策略）

每种任务类型一条**有界**队列，容量为 `workers + queue-capacity` 的在途上限。
提交速率超过处理能力时，按每次提交指定的策略处理：

- **REJECT（默认）**：队列满立即拒绝，返回 `QUEUE_FULL`（HTTP 503）。被拒绝的 requestId 也会
  以 `REJECTED` 终态登记，可凭 requestId 查询到明确判定，而不是“查无此任务”。
- **BLOCK**：在 `maxBlockWait` 内等待空位（调用方被“降速”），超时仍满则按 REJECT 语义拒绝。

压力解除后队列自行排空、worker 池大小恒定（不随负载创建线程），系统自动恢复；
结果存储只淘汰终态条目（TTL + 容量上限），活跃任务永不淘汰，因此不会因清理而丢任务。

## 4. 优先级、老化与重试

- **优先级**：`HIGH` 在短期内严格先于 `NORMAL`，同档内严格 FIFO。
- **老化提权（防饿死）**：`NORMAL` 任务等待时间达到 `aging-threshold-millis` 后自动按 HIGH 参与
  调度。这给普通任务一个**确定的最长等待上界**，持续的 HIGH 流量也无法让它无限饿死。
- **重试**：仅对“执行抛异常”重试；取消与截止时间到期不重试。
  - 重试受双重预算约束：`maxAttempts`（最大次数，1 表示不重试）与整体 `deadline`。
  - 退避时长若会把下一次 attempt 推过 deadline，则退避到截止时刻即结束并判 `TIMEOUT`，
    **重试绝不突破截止时间**。
  - handler 必须幂等：重试以相同 payload、递增的 `attemptIndex` 调用；handler 正常返回即视为
    副作用已完成（即使取消/超时恰好同时发生也按 SUCCEEDED 收敛，避免调用方误判后重复提交）。

## 5. 故障隔离

队列与 worker 均按 `taskType` 独立：A 类型把自己的队列塞满，只会让 A 的提交者承受背压；
A 的 handler 持续失败或长时间不响应中断，也只占用 A 自己的 worker，B 类型的提交与执行不受影响。
一个共享的 daemon 调度器只负责超时判定，不执行业务逻辑。

关闭语义：`shutdown()` 会中断全部 worker 与调度线程并等待它们退出（无线程泄漏）；
作为进程内本地引擎，关闭时仍在队列中的任务不再执行（不做跨进程持久化）。
正在执行的任务若响应中断，会按 `CANCELLED`/`FAILED`/`TIMEOUT` 收敛终态；Spring 容器关闭时
会自动调用 `shutdown()`。

## 6. 关键配置

前缀 `task-engine.*`（见 `src/main/resources/application.properties`）：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | true | 引擎开关 |
| `aging-threshold-millis` | 2000 | 普通任务老化提权阈值 |
| `result-store-max-entries` | 50000 | 状态/结果存储上限（只淘汰终态） |
| `result-ttl-millis` | 120000 | 终态结果保留时长 |
| `default-timeout-millis` | 5000 | 单次执行超时缺省值 |
| `default-deadline-millis` | 30000 | 整体截止时间缺省值 |
| `default-max-attempts` | 1 | 最大执行次数缺省值 |
| `default-retry-backoff-millis` | 100 | 重试退避缺省值 |
| `types.<name>.queue-capacity` | 200/100 | 该类型队列容量 |
| `types.<name>.workers` | 4/2 | 该类型 worker 数 |

内置演示任务类型：`echo`（立即回显）、`slow`（`payload=sleep=毫秒`，响应中断）、
`flaky`（`payload=failUntil=N`，前 N 次失败后成功）、`alwaysFail`（恒失败，用于异常注入）。

## 7. HTTP 接口

```
POST   /api/tasks                 提交任务（202 已接受；错误见上表）
GET    /api/tasks/{requestId}     查询状态与终态结果（404 不存在或已过期清理）
DELETE /api/tasks/{requestId}     取消（202 已受理；409 已终态无法取消）
GET    /api/metrics               引擎与各类型的队列/执行/终态计数
```

提交请求体（所有控制字段可选）：

```json
{
  "requestId": "req-001",
  "taskType": "slow",
  "payload": "sleep=500",
  "priority": "HIGH",
  "timeoutMillis": 2000,
  "deadlineMillis": 8000,
  "maxAttempts": 3,
  "retryBackoffMillis": 100,
  "backpressure": "BLOCK",
  "maxBlockWaitMillis": 1000
}
```

curl 示例：

```bash
curl -s localhost:8080/api/tasks -H 'Content-Type: application/json' \
  -d '{"requestId":"demo-1","taskType":"echo","payload":"hello"}'
curl -s localhost:8080/api/tasks/demo-1
curl -s localhost:8080/api/metrics
```

> 注：默认 `server.port=0`（随机端口，便于测试）。手工验证时改为固定端口或在日志中查看端口。

## 8. 本地验证方法

无需任何外部服务，JDK 21+、Maven 3.9+ 即可。

```bash
# 运行全部 24 个单测/集成测试（含可重复压力测试与异常注入）
mvn test

# 只跑某一类场景
mvn -Dtest=BurstStressTest test          # 突发流量 + 背压 + 恢复 + 无线程泄漏
mvn -Dtest=BackpressureTest test        # REJECT 立即拒绝 / BLOCK 降速
mvn -Dtest=CancelTimeoutTest test       # 排队取消 / 运行中取消 / 超时
mvn -Dtest=RetryDeadlineTest test       # 重试成功 / 预算耗尽 / 不突破 deadline
mvn -Dtest=PriorityAgingTest test       # HIGH 优先 / NORMAL 老化不饿死
mvn -Dtest=FailureIsolationTest test    # 持续失败与队列塞满的类型隔离
mvn -Dtest=DedupAndTtlTest test         # 重复请求去重 / 终态 TTL 回收
mvn -Dtest=ShutdownTest test            # 关闭后线程退出 / 拒绝再提交

# 启动应用后用 curl 验证
mvn spring-boot:run
```

测试日志对每个任务打印 **请求标识、队列状态与最终判定**，例如：

```
task accepted: requestId=bp-3, taskType=slow, priority=NORMAL, queueSize=4, options=(...)
attempt timeout fired: requestId=to-1, taskType=slow, attempt=1, timeoutMillis=300
retrying task: requestId=retry-ok, taskType=flaky, nextAttempt=3, queueSize=0
task finished: requestId=retry-ok, taskType=flaky, verdict=SUCCEEDED, attempts=3, queueSize=0
burst done in 33ms: accepted=141, rejected=1859, activeWorkers=4
recovered: succeeded=141, failed=0, queueSize=0, active=0
```

测试全部为本地确定性设计（用闩锁/睡眠注入瓶颈与故障），不依赖外部服务，可稳定复现。

## 9. 代码结构

- `core/`：对外接口与值对象（`TaskEngine`、`TaskRequest/Result/Future`、选项、枚举、错误码、异常）
- `engine/`：实现（`LocalTaskEngine`、`TypePipeline`、`AgingPriorityQueue`、`TaskEntry`、`ResultStore`）
- `config/`：Spring 装配与配置属性
- `web/`：REST 控制器与统一异常翻译
- `demo/`：内置异常注入/演示 handler
- `src/test/.../tests/`：基础执行、背压、取消/超时、重试/截止、优先级/老化、
  故障隔离、突发压测、去重/TTL、关闭回收、HTTP 端到端共 24 个测试
