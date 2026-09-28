# async-task-backpressure-java

面向高并发场景的**本地任务处理与结果回传**能力：有界队列 + 确定性背压、优先级调度（防饿死）、
超时/取消/失败可区分、指数退避重试（截止时间约束、幂等约束）、按任务类型的隔离舱与熔断，
配套可重复的本地压力测试与异常注入测试，全程不依赖任何外部服务。

技术栈：Java 21、Spring Boot 3.4、SLF4J；无数据库、无消息队列、无外部 HTTP 依赖。

---

## 1. 并发模型

```
                         ┌─────────────────────────── 单个有界优先级队列（容量 N）──────────────────────────┐
 submit() ──背压判定──▶  │  有效优先级 = 原始优先级按等待时间逐级晋升(aging)；同级按入队时间 FIFO          │
 (REST/Java)             └───────────────────────────────┬───────────────────────────────────────────┘
        │                                                 │ poll()（锁内 CAS：QUEUED→RUNNING）
        │ ABORT / CALLER_RUNS / BLOCK                     ▼
        │                                      ┌──────────────────────┐
        │                                      │  W 个 worker 线程     │ （平台线程，可选虚拟线程）
        │                                      └──────────┬───────────┘
        │                                                 │ 按 taskType 获取
        │                                                 ▼
        │                              ┌──────────────────────────────────────┐
        │                              │ 每类型隔离舱 Semaphore(concurrency)   │
        │                              │ + 时间窗熔断器（CLOSED/OPEN/HALF_OPEN）│
        │                              └──────────────────┬───────────────────┘
        │                                                 ▼
        │                                       TaskHandler.execute()
        │                                  超时：守护调度线程对执行线程发中断
        ▼
 终态结果表（有界保留） ◀── CAS 唯一终态：SUCCEEDED / FAILED / TIMEOUT / CANCELLED / REJECTED
 统计计数器（queued/running/各终态/每类型熔断与在途）
```

关键正确性保证：

1. **不重复执行、不丢失**
   - 任务只有在队列锁内完成 `QUEUED -> RUNNING` 的 CAS 才能被 worker 取走，重试重入队的条目
     状态为 `RUNNING + attempts>0`，二者互斥，任何任务只会被一个执行路径拿到。
   - 终态由 `AtomicReference<TaskState>` 上的 CAS 保证只翻转一次，取消/超时/失败/成功互不覆盖。
   - 重试调度时若队列恰好满，退化为直接在 worker 上继续尝试，不丢任务。
2. **判定可区分**：`SUCCEEDED / FAILED / TIMEOUT / CANCELLED / REJECTED` 五种终态 +
   `FailureKind`（`USER_EXCEPTION / RETRIES_EXHAUSTED / BULKHEAD_REJECTED / CIRCUIT_OPEN / INTERNAL`）。
   调用方只会看到结构化结果；worker 捕获一切 `Throwable` 但不外抛，线程池/队列内部异常不会透传。
3. **超时与取消的中断归属**：每次尝试在 entry 上登记执行线程并由看门狗/取消方通过 CAS **争夺唯一
   中断权**。只有赢得中断权的一方（任务仍 RUNNING 且执行线程未切换）才能对该线程 `interrupt()`，
   从根本上杜绝"上一个任务的看门狗误中断已开始执行下一个任务的 worker"。执行结束后仅清属于本次
   尝试的中断标记，中断不会跨任务泄漏。每次尝试超时取 `min(任务 timeout, 剩余 deadline)`；
   即使处理器不响应中断（忙等），引擎仍按看门狗时刻给出 `TIMEOUT`，被占用线程由**按类型隔离舱
   并发上限**约束，不会无限膨胀。
4. **终态发布唯一**：成功/失败/超时/取消都必须先在 `AtomicReference<TaskState>` 上 CAS 赢下终态，
   由赢家携带自己的返回值同步发布结果；竞争输家只等待并读取赢家的结果，绝不二次发布。因此取消与
   正常完成在极小窗口内竞争时，也不会出现"状态 SUCCEEDED 但返回值丢失"之类的不一致。
5. **线程生命周期**：worker、重试调度器、超时看门狗均为守护线程；`start()` 等待所有 worker 真正
   停泊到队列后才返回；`stop()` 统一 `shutdownNow + awaitTermination`；终态结果有保留上限
   （`completed-retention`），不会无限堆积。

## 2. 背压规则（确定性）

队列固定容量。提交速率超过处理能力时，按 `task.engine.reject-policy` 三选一：

| 策略 | 行为 | 调用方表现 |
|---|---|---|
| `ABORT`（默认） | 满队立即拒绝 | 抛 `TaskRejectedException`；REST 返回 `429 {"errorCode":"REJECTED",...}` |
| `CALLER_RUNS` | 满队时由**提交线程自己**同步执行该任务 | 提交方天然降速；任务不丢失 |
| `BLOCK` | 提交方阻塞等待队列空位 | 可中断的反压节流；中断时返回 `REJECTED/INTERNAL` |

除队列背压外，worker 在某类型隔离舱占满时会按任务剩余截止时间**等待许可**，形成对下游处理器的
第二层背压；只有熔断器 OPEN 时才立即以 `CIRCUIT_OPEN` 失败。

状态可查：`GET /api/tasks/{requestId}` 返回中间态（QUEUED/RUNNING，含 attempts、已等待毫秒）
或终态；`GET /api/tasks/stats` 返回队列占用、在途数、各终态计数与每类型熔断/在途视图。
压力解除后积压自动排空，计数器体现系统自行恢复；无任务丢失、无线程/任务记录泄漏（有终态淘汰）。

## 3. 优先级与防饿死

- 有效优先级 = 原始优先级（HIGH/NORMAL/LOW）随**在队等待时间逐级晋升**：每经过一次
  `aging-millis`（默认 200ms）晋升一级，LOW→NORMAL→HIGH。
- 同有效优先级内严格按入队时间 FIFO。
- 效果：普通/低优先级任务最长等待约 `aging-millis` 后即按高优先级参与 FIFO，**不会被高优先级
  流量无限饿死**；而在它之后到达的 HIGH 任务，晋升后仍按 FIFO 排在其后，HIGH 任务的等待只受
  “队列容量 × 单次执行时间”这一有界值约束。由 `TaskEngineHappyPathTest` 和
  `PriorityTaskQueueTest.agingPromotes...` 验证。

## 4. 重试规则与副作用约束

- `max-attempts` 为**包含首次执行**的总尝试次数（1 = 不重试）。
- 退避确定、无随机抖动（保证压测可复现）：
  `backoff(n) = min(backoff * multiplier^(n-1), max-backoff)`。
- 允许重试必须**同时**满足：处理器声明 `retryable()` **且**请求声明 `idempotent=true`。
  非幂等任务即使处理器标记可重试也只执行一次（`FailureKind.USER_EXCEPTION`），防止重复副作用。
- **重试绝不突破截止时间**：
  - 每次尝试前检查整体 `deadline`；
  - 退避调度前检查 `now + backoff < deadline`，不满足则立即终态（`TIMEOUT` 或
    `RETRIES_EXHAUSTED`）；
  - 单次尝试超时取 `min(timeout, 剩余 deadline)`。
- 重试耗尽与普通用户异常区分：`RETRIES_EXHAUSTED` vs `USER_EXCEPTION`。

## 5. 故障隔离与熔断

按 `taskType` 维护 `TypeIsolation`：

- **隔离舱**：信号量限制该类型并发（默认每类型 2）。某类慢任务占不满全局 worker。
- **熔断器**：`failure-window`（默认 10s）滚动时间窗内失败达到 `failure-threshold`（默认 5）
  即 OPEN；OPEN 期间新任务立即 `CIRCUIT_OPEN` 失败、不占线程；经过 `open-duration`（默认 2s）
  进入 HALF_OPEN，仅放行一次探测——探测成功则 CLOSED（故障解除自动恢复），失败则继续 OPEN。
- 一类任务持续失败只影响该类型；`TaskEngineIsolationTest` 验证 healthy 类型全程成功、
  bad 类型熔断后随故障解除自动恢复。

## 6. 关键配置（`src/main/resources/application.properties`）

| 配置键 | 默认 | 说明 |
|---|---|---|
| `task.engine.worker-threads` | 4 | worker 数量 |
| `task.engine.queue-capacity` | 200 | 有界队列容量 |
| `task.engine.reject-policy` | ABORT | `ABORT` / `CALLER_RUNS` / `BLOCK` |
| `task.engine.default-timeout` | 5s | 单次尝试默认超时 |
| `task.engine.default-deadline` | 30s | 任务整体默认截止时间（含重试退避） |
| `task.engine.aging-millis` | 200 | 优先级老化步进 |
| `task.engine.use-virtual-threads` | false | worker 是否使用虚拟线程 |
| `task.engine.default-bulkhead` | 2 | 每任务类型并发上限 |
| `task.engine.failure-threshold` | 5 | 熔断失败阈值 |
| `task.engine.failure-window` | 10s | 熔断统计时间窗 |
| `task.engine.open-duration` | 2s | OPEN 持续时间，之后半开探测 |
| `task.engine.retry.max-attempts` | 3 | 总尝试次数 |
| `task.engine.retry.backoff` | 50ms | 初始退避 |
| `task.engine.retry.backoff-multiplier` | 2.0 | 退避倍率 |
| `task.engine.retry.max-backoff` | 1s | 退避上限 |
| `task.engine.completed-retention` | 10000 | 终态结果保留条数 |

任务级可覆盖字段（提交体 JSON）：`priority`、`timeout`、`deadline`、`idempotent`、`payload`。

## 7. REST 接口

| 方法与路径 | 说明 |
|---|---|
| `POST /api/tasks` | 提交任务；排队返回 `202`，CALLER_RUNS 内联完成可能直接返回终态 200；满队 429；非法请求 400 |
| `GET /api/tasks/{requestId}` | 查询当前判定（中间态/终态）；不存在 404 |
| `POST /api/tasks/{requestId}/cancel` | 取消排队中（移出队列）或运行中（中断执行线程）任务 |
| `GET /api/tasks/stats` | 队列/在途/各终态计数、每类型隔离舱与熔断视图 |

提交示例：

```bash
curl -s localhost:$PORT/api/tasks -H 'Content-Type: application/json' -d '{
  "requestId":"t-1","taskType":"echo","priority":"HIGH",
  "timeout":"PT0.2S","deadline":"PT2S","idempotent":false,"payload":"sleepMs=50"
}'
```

内置本地处理器（无外部依赖、行为由 payload 精确控制）：

- `echo`：`payload="sleepMs=N"`，可中断睡眠后回显。
- `fault`：故障注入，`sleepMs=N`（可中断延迟）、`busyMs=N`（**不可中断**忙等，用于验证
  超时判定与隔离舱）、`failUntil=N`（前 N 次尝试失败）、`alwaysFail=true`（持续失败）。

## 8. 本地验证方法

要求 JDK 21+、Maven 3.9+（已验证 JDK 21 编译；本机可用更高 JDK 运行）。

```bash
# 全量测试（25 个用例，单测 + Spring Boot 集成测试，全部本地、确定性、可重复）
mvn test

# 只跑某类场景
mvn test -Dtest=RetryPolicyTest, PriorityTaskQueueTest            # 退避 / 优先级队列
mvn test -Dtest=TaskEngineHappyPathTest                           # 恰好一次 + 高优先级不饿死
mvn test -Dtest=TaskEngineBackpressureTest                        # 三种背压策略 + 恢复
mvn test -Dtest=TaskEngineTimeoutTest                             # 可中断/不可中断超时
mvn test -Dtest=TaskEngineCancelTest                              # 排队取消 + 运行中取消
mvn test -Dtest=TaskEngineRetryTest                               # 重试、幂等、截止时间约束
mvn test -Dtest=TaskEngineIsolationTest                           # 熔断隔离 + 自动恢复
mvn test -Dtest=TaskEngineBurstStressTest                         # 8x100 突发压测
mvn test -Dtest=TaskControllerIntegrationTest                     # REST 全链路 + 错误契约

# 启动应用（随机端口；如指定端口可改 application.properties）
mvn spring-boot:run
```

测试日志固定打印**请求标识、队列状态与最终判定**，例如：

```
INFO [task-worker-2] TaskEngine - terminal requestId=retry-exhaust type=fault
     verdict=FAILED failureKind=RETRIES_EXHAUSTED attempts=3 elapsedMs=92
     worker=task-worker-0 queue=0/20 runningHigh=0 msg=task threw IllegalStateException: ...
```

覆盖的场景与对应判定：

| 场景 | 用例 | 期望判定 |
|---|---|---|
| 突发流量超过处理能力（ABORT/CALLER_RUNS/BLOCK） | `TaskEngineBackpressureTest` | 429/内联完成/提交节流，最终全部成功且无丢失 |
| 高优先级突发 + 普通积压 | `TaskEngineHappyPathTest`、队列老化测试 | HIGH 快速完成，普通任务老化后不饿死 |
| 取消排队/运行中任务 | `TaskEngineCancelTest` | `CANCELLED`，排队任务零执行，终态幂等 |
| 执行超时（响应/不响应中断） | `TaskEngineTimeoutTest` | `TIMEOUT`，忙等任务不拖垮后续任务 |
| 持续失败、重试耗尽 | `TaskEngineRetryTest` | `RETRIES_EXHAUSTED`；非幂等为 `USER_EXCEPTION` |
| 重试突破截止时间风险 | `TaskEngineRetryTest.deadline...` | 截止时间处 `TIMEOUT`，不再发起重试 |
| 单类型持续故障 | `TaskEngineIsolationTest` | 该类型 `CIRCUIT_OPEN`，其他类型照常，故障解除后恢复 |
| 多生产者突发压测 | `TaskEngineBurstStressTest` | 接受任务恰好一次、拒绝显式计数、压力解除后归零恢复 |
