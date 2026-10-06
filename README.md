# 可靠本地后台任务调度器

基于 FastAPI + SQLite（标准库）实现的本地后台任务调度服务。任务提交后由
内置执行器按可配置并发度**自动执行完毕**，无需外部轮询领取；执行期间的
租约续约、取消打断、失败重试、崩溃接管与进程重启恢复均有确定且可解释的
结果。不依赖任何外部服务或真实凭据。

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
- 被接管后，原持有者的 `complete`/`fail`/`renew` 会被拒绝
  （`lease_conflict`，HTTP 409），其结果不会落库。
- `POST /tasks/{id}/renew` `{owner}`：持有者续约租约
  （`lease_expires_at = now + lease_seconds`）。任务已取消或已易主时
  续约被拒，这是执行者感知「被取消/被接管」的信号。

## 自驱执行（内置执行器）

- 服务启动后，`TaskExecutor` 的调度线程按 `executor_concurrency`
  （默认 4）的并发度自动领取并执行任务；任务数超过并发度时排队等待，
  不报错、不丢弃、不漏执行（积压可通过 `GET /tasks` 的 `claimable`
  观察）。
- 执行体签名 `handler(task, ctx) -> result`：返回值作为任务结果落库；
  抛 `PermanentTaskError` 进入 `FAILED`（不重试）；抛其他异常按临时
  失败走指数退避重试；抛/传播 `TaskCancelledError` 表示响应取消。
- 同一幂等键的任务无论被调度多少次，只会被真正执行一次（幂等提交 +
  原子领取共同保证）。
- 执行结果（成功/重试/永久失败/取消）与手工驱动路径完全一致，照常
  体现在 `GET /tasks/{id}`、`/history` 与统计接口中。

## 执行期间租约续约

- 每个执行中的任务由心跳线程按 `renew_interval_seconds`（默认 10s，
  应明显小于 `lease_seconds`）自动续约；只要执行者存活，长任务不会被
  他人误判超时抢走。
- 只有当执行者确实失联（进程崩溃/重启，心跳停止）后，租约才会到期，
  任务才会被接管重跑；接管后最终结果唯一，状态与执行历史跨重启一致。

## 执行中取消

- 对 `RUNNING` 任务取消后，下一次续约（≤ `renew_interval_seconds`）
  会被存储层拒绝，执行器随即通过 `ctx` 置位取消标记；执行体调用
  `ctx.check_cancelled()`（或读取 `ctx.cancelled`）即可在可观察的
  一小段时间内停下，不再写入任何结果，终态为 `CANCELLED`。
- 取消与完成几乎同时发生时，由存储层仲裁：最终只会是 `SUCCEEDED`
  （结果完整落库）或 `CANCELLED`（结果不落库）之一，状态与数据始终
  一致；引入自动执行后该一致性不变。


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
| POST | `/tasks/{id}/renew` | `{owner}` 续约租约（新增，纯增量） |
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
| `SCHEDULER_EXECUTOR_ENABLED` | `1` | 是否启用内置执行器（`0` 关闭，退回纯手工驱动） |
| `SCHEDULER_EXECUTOR_CONCURRENCY` | `4` | 执行器并发度 |
| `SCHEDULER_EXECUTOR_POLL_INTERVAL_SECONDS` | `0.5` | 调度线程空闲轮询间隔（秒） |
| `SCHEDULER_RENEW_INTERVAL_SECONDS` | `10` | 执行期间租约续约间隔（秒），应 < 租约时长 |

## 兼容性说明

- 既有公开接口（`GET /`、`POST /tasks`、`claim`、`complete`、`fail`、
  `cancel`、`GET /tasks/{id}`、`/history`、`GET /tasks`）的语义与错误
  分类完全不变；手工 `claim`/`complete`/`fail` 驱动路径仍然可用。
- 新增接口仅 `POST /tasks/{id}/renew`，为纯增量。
- 新增配置项均有默认值，不设置任何新环境变量时行为为：执行器开启、
  并发 4、续约间隔 10s。关闭执行器（`SCHEDULER_EXECUTOR_ENABLED=0`）
  即恢复上一版的纯手工驱动行为。
- 内置演示执行体按 payload 模拟工作：`sleep_seconds`（耗时，可响应
  取消）、`fail`（`transient`/`permanent`）、`result`（成功结果，
  缺省回显 payload），便于本地验证；生产接入时替换 `main.py` 中的
  `demo_handler` 即可。


## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -v # 运行全部测试（日志含请求输入、状态迁移、拒绝/打断依据）
uv run uvicorn main:app # 启动服务（内置执行器随服务自动启动）

# 手工冒烟：提交后无需领取，执行器会自动跑完
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"demo","payload":{"sleep_seconds":1,"result":{"ok":1}}}'
sleep 2 && curl localhost:8000/tasks            # 统计中 SUCCEEDED=1
curl localhost:8000/tasks                       # claimable 反映积压

# 验证执行中取消：提交一个长任务后立刻取消
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"long","payload":{"sleep_seconds":60}}'
TASK_ID=$(curl -s localhost:8000/tasks | true)  # 或从提交响应中取 task_id
curl -X POST localhost:8000/tasks/<task_id>/cancel
curl localhost:8000/tasks/<task_id>/history     # submitted -> claimed -> cancelled
```

测试覆盖：重复提交幂等、并发领取互斥、租约超时接管、取消/完成竞争
（两个分支 + 随机交错一致性）、临时/永久失败与退避、重启恢复
（状态、历史、租约、退避计划、幂等键）、HTTP 错误分类与既有接口回归；
新增：自驱执行跑完、超并发排队、同幂等键仅执行一次、长任务续约不被
误接管、执行中取消被打断且无半成品数据、取消/完成竞争唯一结局、
执行者失联后被接管重跑、重启后接管且历史对得上、renew 接口契约。
