# 可靠本地后台任务调度器

基于 FastAPI + SQLite（标准库）实现的本地后台任务调度服务。任务提交、领取、
执行、取消、重试与查询均有确定且可解释的结果，并发运行与进程重启后保持一致。
不依赖任何外部服务或真实凭据。

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

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -v # 运行全部测试（日志含请求输入、状态迁移、拒绝依据）
uv run uvicorn main:app # 启动服务

# 手工冒烟
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
     -d '{"idempotency_key":"demo","payload":{"x":1}}'
curl -X POST 'localhost:8000/tasks/claim?worker_id=w1'
curl localhost:8000/tasks            # 统计
```

测试覆盖：重复提交幂等、并发领取互斥、租约超时接管、取消/完成竞争
（两个分支 + 随机交错一致性）、临时/永久失败与退避、重启恢复
（状态、历史、租约、退避计划、幂等键）、HTTP 错误分类与既有接口回归。
