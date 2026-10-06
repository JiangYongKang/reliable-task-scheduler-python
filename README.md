# 可靠本地后台任务调度器

基于 FastAPI + SQLite（标准库）实现的本地后台任务调度服务。任务提交、领取、
执行、取消、重试与查询均有确定且可解释的结果，并发运行与进程重启后保持一致。
**服务内置自驱执行引擎**：任务提交后由服务自己的工作线程按可配置并发度领取、
续约、执行并回报，无需外部手工触发；执行时间长于租约时自动续约，执行中取消可
真正打断执行体，执行者失联（进程崩溃/重启）后任务会在租约到期后被安全接管重跑。
不依赖任何外部服务或真实凭据。

## 自驱执行（新增）

任务无需再由外部反复调用 `claim`/`complete`：服务启动时会运行
`ExecutionEngine`，由固定数量的工作线程循环领取可运行任务并调用注册的执行体，
成功、临时失败重试、永久失败、取消全部自动发生，并如实体现在既有的任务查询、
状态历史（events）与统计（stats）里。

### 执行体注册与任务类型

```python
from scheduler.executor import ExecutionEngine, RetryTaskError, PermanentTaskError

engine = ExecutionEngine(service)

def order_handler(payload, ctx: ExecutionContext):
    ctx.raise_if_cancelled()            # 协作式取消检查点
    ok = do_work(payload["order_id"])
    if not ok:
        raise RetryTaskError("temporary")  # 临时失败 -> 退避重试
    return {"delivered": True}            # 正常返回 -> SUCCEEDED

engine.register_handler("order", order_handler)
engine.start()
```

- 按 `payload["type"]` 选择执行体；未提供 `type` 时使用 `"default"`。
  （以 `main.py` 直接启动时内置了回显执行体 `default`，未注册类型会以
  `PermanentTaskError` 永久失败，不会被静默吞掉。）
- 执行体正常返回任意可 JSON 序列化对象 → `SUCCEEDED`；
  抛 `RetryTaskError` 或任何未声明异常 → 按既有指数退避回到 `PENDING` 重试；
  抛 `PermanentTaskError` → 直接 `FAILED`，不再重试。
- 同一 `idempotency_key` 无论提交多少次都只有一条任务记录、只真正执行一次。
- 并发度固定为 `worker_concurrency`：超出并发度的任务保持 `PENDING` 排队，
  不会报错、丢弃或被漏掉；工作线程空下来后自动继续领取。

### 执行期间租约续约（新增）

- 任务被引擎领取后立即续约一次，随后由独立续约线程周期性续约；
  有效续约间隔取 `min(lease_renew_interval_seconds, lease_seconds/2)`，
  即使单次续约失败，下一次仍能在租约到期前补上。
- 只要执行者进程存活、执行体在正常工作，租约就一直被延长，任务不会被其他
  执行者当成超时抢走重跑。
- 执行者进程崩溃/被重启后续约随之停止；租约到期后任务才可被其他执行者
  （或重启后的新引擎）接管，事件记为 `lease_expired_takeover`，`attempt`
  递增。接管后旧执行者的迟到 `complete`/`fail` 会被拒绝
  （`lease_conflict`/`invalid_state_transition`），因此**不会重复成功，
  最终结果唯一**；续约失败时引擎也会主动打断本进程执行体，避免"僵尸执行者"
  继续写入。
- 重启前后任务状态与完整执行历史都在 SQLite 中保留，接管后续写在同一条
  历史链路上。

### 执行中取消打断（新增）

- 对 `RUNNING` 任务调用取消后，任务立即在存储层变为 `CANCELLED`，同时引擎
  通过 `ExecutionContext` 上的取消信号通知执行体；执行体应在检查点调用
  `ctx.raise_if_cancelled()`，或用 `ctx.sleep_interruptible(秒)` 进行可被
  打断的等待，从而在一小段可观察时间内停下来。
- 取消是**协作式**的：忽略取消信号的执行体无法被强杀（Python 不提供安全的
  强杀线程手段），但其取消后的任何 `complete`/`fail` 回报都会被存储层拒绝，
  结果绝不落库——因此不会产生半成品数据，终态始终是 `CANCELLED`。
- 取消与执行体完成几乎同时发生时，二者在同一个 `BEGIN IMMEDIATE` 事务语义下
  竞争：先提交者决定结局，另一方被明确拒绝，任务状态与数据结果必居其一且
  全局一致。排队中（尚未领取）的任务被取消后永远不会被执行。

## 状态机定义

```
                 claim                  complete
  PENDING ─────────────────► RUNNING ──────────────► SUCCEEDED
     │                         │  fail(临时, 未超次数, 退避后重试)
     │                         ├──────────────────► PENDING
     │ cancel                  │  fail(永久 或 达到 max_attempts)
     ▼                         ├──────────────────► FAILED
 CANCELLED ◄───────────────────┤  cancel(与执行竞争，取消获胜)
                               │  租约超时被接管(attempt+1)
                               └──────────────────► RUNNING(新 owner)
```

- `PENDING`：待执行；`not_before` 到达前不可领取（用于重试退避）。
- `RUNNING`：已被某执行者领取，持有租约至 `lease_expires_at`。
- `SUCCEEDED` / `FAILED` / `CANCELLED`：终态，任何迁移都会被拒绝
  （`invalid_state_transition`）。

## 幂等语义

- 提交时必须携带 `idempotency_key`（数据库唯一约束）。
- 相同幂等键的重复提交返回**既有任务**（`created=false`），不产生重复任务、
  不覆盖原 payload、不会被重复执行。
- 幂等性跨进程重启保持（唯一约束持久化在 SQLite 中）。

## 租约语义

- `POST /tasks/claim?worker_id=W` 原子领取一个可运行任务：单条
  `BEGIN IMMEDIATE` 事务内完成「选择 + 更新 owner/租约 + 记录事件」，
  多个执行者不可能同时成功领取同一任务。
- 租约有效期 `lease_seconds`（默认 30s）。租约到期且任务仍为 `RUNNING`
  时，其他执行者可安全接管（`attempt` 递增，事件原因
  `lease_expired_takeover`）。
- 被接管后，原持有者的 `complete`/`fail` 会被拒绝
  （`lease_conflict`，HTTP 409），其结果不会落库。

## 重试与取消规则

- `fail(permanent=false)` 且 `attempt < max_attempts`：回到 `PENDING`，
  `not_before = now + min(base * 2^(attempt-1), max)`（指数退避）。
- `fail(permanent=true)` 或达到 `max_attempts`：进入 `FAILED`，不再重试。
- `cancel` 仅允许 `PENDING`/`RUNNING`；对 `RUNNING` 任务的取消立即生效，
  执行者随后的 `complete`/`fail` 以 `invalid_state_transition` 被拒，
  终态与数据结果由先提交的一方决定，二者必居其一且全局一致。
- 重复取消、未知任务、终态上的任何操作都会被拒绝，错误按类型区分：

| 场景 | code | HTTP |
|---|---|---|
| 未知任务 | `task_not_found` | 404 |
| 非法状态迁移（含重复取消、终态操作） | `invalid_state_transition` | 409 |
| 租约冲突（非持有者完成/失败） | `lease_conflict` | 409 |
| 参数非法（空幂等键等） | `bad_input` / 校验错误 | 400 / 422 |

## 查询与数据隔离

- `GET /tasks/{id}?requester=R`：任务被持有时，非持有者查询会被脱敏
  （`payload`/`result` 置空），不向不相关调用方泄露数据。
- `GET /tasks/{id}/history`：完整状态迁移历史（重启后保留）。
- `GET /tasks`：各状态计数、`claimable`（当前可领取数，积压指标）、
  `retrying`（退避中的重试数），足以判断积压、重试与失败原因
  （失败原因见任务的 `error` 字段与历史事件）。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 既有接口，行为不变 |
| POST | `/tasks` | 提交 `{idempotency_key, payload, max_attempts?}` |
| POST | `/tasks/claim?worker_id=W` | 领取任务 |
| POST | `/tasks/{id}/complete` | `{owner, result}` 完成 |
| POST | `/tasks/{id}/fail` | `{owner, error, permanent}` 失败 |
| POST | `/tasks/{id}/cancel` | 取消 |
| GET | `/tasks/{id}` | 查询（`requester` 控制脱敏） |
| GET | `/tasks/{id}/history` | 状态迁移历史 |
| GET | `/tasks` | 统计信息 |

## 可配置项（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SCHEDULER_DB` | `scheduler.db` | SQLite 数据库路径 |
| `SCHEDULER_LEASE_SECONDS` | `30` | 租约时长（秒） |
| `SCHEDULER_MAX_ATTEMPTS` | `3` | 默认最大尝试次数 |
| `SCHEDULER_BACKOFF_BASE_SECONDS` | `1` | 退避基数（秒） |
| `SCHEDULER_BACKOFF_MAX_SECONDS` | `60` | 退避上限（秒） |
| `SCHEDULER_ENABLE_EXECUTOR` | `true` | 是否启动自驱执行引擎；设为 `false` 退回纯 API 手工领取模式 |
| `SCHEDULER_WORKER_CONCURRENCY` | `4` | 自驱执行的工作线程并发度；超出部分排队等待 |
| `SCHEDULER_LEASE_RENEW_INTERVAL_SECONDS` | `10` | 租约续约间隔（秒），实际取它与 `lease_seconds/2` 的较小值 |
| `SCHEDULER_POLL_INTERVAL_SECONDS` | `0.1` | 无任务可领取时工作线程的轮询间隔（秒） |
| `SCHEDULER_LOG_LEVEL` | `INFO` | 日志级别 |

## 与既有接口的兼容范围

- 所有既有 HTTP 接口（路径、请求/响应字段、错误码）行为不变；`GET /`
  保持返回 `{"Hello": "World"}`。
- `SchedulerService` 的 `submit/claim/complete/fail/cancel/get/history/stats`
  签名与语义不变，`claim/complete/fail` 仍可供外部执行者使用；引擎与外部
  执行者通过同一套租约与事务规则共存。
- 数据库 schema 未变更（续约只更新已有的 `lease_expires_at`，不产生事件
  噪音），旧数据库文件可直接打开。
- 唯一新增的进程内行为：`cancel` 成功后会通知已注册的取消监听器（引擎启动时
  注册），供同进程执行体接收打断信号；不影响任何持久化与 HTTP 契约。
- 不希望服务自动执行时，设置 `SCHEDULER_ENABLE_EXECUTOR=false` 即恢复
  上一版完全靠外部领取/回报的运行方式。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -v # 运行全部测试（日志含请求输入、任务流转、续约、打断、拒绝依据）
uv run uvicorn main:app # 启动服务（默认开启自驱执行，内置 default 回显执行体）

# 手工冒烟：提交后无需 claim，轮询查询即可看到自动执行完成
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"demo","payload":{"x":1}}'
curl localhost:8000/tasks/$(...)   # 轮询：PENDING -> RUNNING -> SUCCEEDED
curl localhost:8000/tasks          # 统计
```

关键自动化用例（`tests/test_executor.py`）：

- 自动跑完：无需外部领取即成功；同幂等键重复提交只执行一次；
  20 个任务对 4 并发表现为排队且一个不漏；临时失败自动退避重试成功；
  永久失败不重试；执行体未声明异常不拖垮引擎。
- 续约防误接管：执行时长为租约 3 倍的任务始终由原执行者持有，
  外部持续接管尝试全部失败，历史无 `lease_expired_takeover`。
- 执行中取消：取消后执行体在 1 秒内观察到信号并停下，终态 `CANCELLED`、
  返回结果不落库；排队中取消的任务从未执行。
- 失联接管：模拟执行者崩溃且不续约，租约到期后被新引擎接管（`attempt=2`）
  并唯一成功，旧执行者的僵尸回报被拒；进程重启后接管，状态与历史完整对得上。

测试覆盖：重复提交幂等、并发领取互斥、租约超时接管、取消/完成竞争
（两个分支 + 随机交错一致性）、临时/永久失败与退避、重启恢复
（状态、历史、租约、退避计划、幂等键）、自驱执行与续约、执行中取消打断、
失联/重启后接管、HTTP 错误分类与既有接口回归。
