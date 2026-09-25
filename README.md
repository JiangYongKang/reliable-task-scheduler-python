# 本地可靠任务调度服务（Local Reliable Task Scheduler）

在现有 FastAPI HTTP 服务之上，提供一套**本地可运行、无外部服务依赖**的后台任务调度能力：
任务的提交、领取、执行、取消、重试、查询都有确定且可解释的结果，并在**并发运行**与
**进程重启**后保持一致。

- 存储：单个 SQLite 文件（WAL 模式），无外部数据库 / 消息队列 / 凭据。
- 并发：进程内线程池 + 进程间基于 SQLite `BEGIN IMMEDIATE` 与条件 UPDATE 的原子状态迁移。
- 持久化：任务、租约、取消标记、执行历史全部落盘；重启后继续调度，过期租约可被安全接管。
- 既有接口：`GET /` 仍返回 `{"Hello": "World"}`，行为不变。

---

## 1. 快速开始

```bash
# 安装（仅 fastapi[standard]；测试额外需要 pytest、httpx）
uv sync                # 或 pip install -e .

# 运行（默认启动 HTTP 服务 + 后台 worker）
python main.py
# 自定义端口
PORT=9000 python main.py
```

提交并观察任务：

```bash
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"echo","payload":{"x":1},"idempotency_key":"k1","owner":"alice"}'

curl -s "http://127.0.0.1:8000/tasks/<task_id>?owner=alice"
curl -s "http://127.0.0.1:8000/tasks/<task_id>/history?owner=alice"
```

---

## 2. 任务状态定义与状态机

| 状态 | 含义 |
|---|---|
| `PENDING` | 已提交、等待领取（含退避结束后重新可调度的任务） |
| `RUNNING` | 已被某执行者凭 **租约 token** 领取，正在执行 |
| `RETRY_WAIT` | 临时失败，按退避时间等待下一次重试（期间不可被领取） |
| `SUCCEEDED` | 终态：成功完成，结果已持久化 |
| `PERMANENTLY_FAILED` | 终态：永久失败或重试次数耗尽，不再被调度 |
| `CANCELLED` | 终态：被取消（取消在与完成/重试/接管的竞争中始终优先） |

合法迁移：

```
PENDING ──claim──▶ RUNNING ──success──────────────────▶ SUCCEEDED
                     │  ├─ permanent failure──────────▶ PERMANENTLY_FAILED
                     │  └─ transient failure──────────▶ RETRY_WAIT
                     │                                     │（退避到期）
                     │                                     └──▶ PENDING
                     └─ lease timeout ──▶（被其他执行者）TAKEN_OVER（仍为 RUNNING，attempt+1）
PENDING / RETRY_WAIT / RUNNING ──cancel──▶ CANCELLED
```

非法迁移一律以 **409 + 明确错误码** 拒绝；未知任务返回 **404**。

### 错误码（HTTP `detail.code`）

| code | HTTP | 触发场景 |
|---|---|---|
| `task_not_found` | 404 | 查询/取消/操作不存在的任务 |
| `idempotency_key_payload_mismatch` | 409 | 同一幂等键被不同 `task_type/payload/owner` 复用 |
| `invalid_state_transition` | 409 | 不允许的状态迁移（如取消永久失败任务） |
| `already_cancelled` | 409 | 对已 `CANCELLED` 任务重复取消 |
| `cancel_in_progress` | 409 | 取消已在进行（RUNNING 租约未到期），重复请求 |
| `task_already_completed` | 409 | 重复完成 / 取消已成功任务 |
| `not_lease_holder` | 409 | token 无效或租约已被接管后仍上报结果 |
| `unauthorized` | 401 | 管理接口 token 缺失或错误 |
| `forbidden` | 403 | 以非属主 `owner` 访问任务（不泄露任务是否存在） |

---

## 3. 幂等语义

- 提交时可带 `idempotency_key`。键与 `(task_type, payload, owner)` 绑定。
- **相同键、相同请求参数**：返回同一个 `task_id`，`created=false, deduplicated=true`，
  数据库只产生一条任务、一条 `SUBMITTED` 历史，绝不重复执行。
- **相同键、不同参数**：返回 409 `idempotency_key_payload_mismatch`。
- 并发下相同键的提交由 SQLite 唯一约束兜底，仍只会创建一个任务。
- 不带键的提交每次都会创建新任务。

---

## 4. 租约（Lease）与接管语义

- 领取成功后任务进入 `RUNNING`，执行者拿到随机 `lease_token`（不通过 API 对外暴露）。
- 租约默认 30s。执行者每 10s 发送心跳续租；进程崩溃则停止心跳。
- 租约到期后，任务可被**其他执行者安全接管**：状态仍为 `RUNNING`，`attempt + 1`，
  写入 `TAKEN_OVER` 历史（含前一个执行者）。
- 接管后旧 token 立即失效：旧执行者晚到的完成/失败/重试上报返回 `not_lease_holder`，
  **不会覆盖新执行者的结果**。
- “领取”在单条 SQL 事务中以条件 `UPDATE ... WHERE state=PENDING`（或
  `state=RUNNING AND lease_expires_at<=now`）完成，因此多线程/多进程下同一任务
  **至多被一个执行者成功领取**。

---

## 5. 重试规则

- 处理器抛出：
  - `PermanentTaskError` → 立即 `PERMANENTLY_FAILED`，**永不重试**；
  - `TransientTaskError` → 进入 `RETRY_WAIT`，退避后重试，直到 `max_attempts`；
  - 其它任意异常 → 视为临时失败，重试到上限后记为永久失败，
    `last_error_type` 形如 `ValueError(max_attempts_exhausted)`。
- 退避：`min(base * 2**(attempt-1), max_backoff)`（确定性指数退避）。
- 未注册的 `task_type` 记为 `UnknownTaskType`（永久失败，属于配置/编程错误）。
- `max_attempts` 可在提交时按任务指定（1–100），否则取全局配置。
- `last_error_type` / `last_error_message` 持久化在任务上，便于失败归因。

---

## 6. 取消规则

- 对 `PENDING` / `RETRY_WAIT`：立即进入 `CANCELLED`（尚未开始，无副作用）。
- 对 `RUNNING`：先置 `cancel_requested=1`（任务仍运行直到执行者响应或租约到期）。
  - 处理器可通过 `ctx.cancelled()` / `ctx.raise_if_cancelled()` **协作式取消**；
  - 若执行者仍上报完成或重试，因 `cancel_requested` 已置位，结果被拒绝
    （历史写 `COMPLETION_REJECTED`），任务最终为 `CANCELLED` —— **取消始终胜出**；
  - 若执行者已崩溃，则租约到期后由接管者或维护任务直接终态化为 `CANCELLED`，不会被再执行。
- 重复取消：已取消 → `already_cancelled`；取消进行中 → `cancel_in_progress`。
- 终态任务（成功/永久失败）不可取消，返回 409。

---

## 7. 数据隔离与查询

- 每个任务有 `owner`（默认 `"default"`）。查询状态/历史、取消都必须带属主：
  - 查询参数 `?owner=<name>` 或请求头 `X-Owner: <name>`。
- 非属主访问返回 **403 `forbidden`**（统一错误，不向不相关调用方确认任务是否存在）。
- `GET /internal/stats`（需管理 token）提供积压判断：
  `backlog`(PENDING)、`in_flight`(RUNNING)、`retry_wait`、各状态计数。
- 每个任务都有追加式**执行历史**（`GET /tasks/{id}/history`），事件类型包括：
  `SUBMITTED, CLAIMED, COMPLETED, RETRY_SCHEDULED, RETRY_AVAILABLE,
  TAKEN_OVER, PERMANENT_FAILURE, CANCEL_REQUESTED, CANCELLED, COMPLETION_REJECTED`，
  含时间、attempt、执行者与结构化 detail（退避时长、错误类型、前任执行者等）。
  租约存活通过任务上的 `lease_expires_at` 体现，常规心跳不写入历史以避免事件无限增长；
  丢约会由 `TAKEN_OVER` / `CANCELLED` 事件明确解释。

---

## 8. HTTP 接口

| 方法/路径 | 说明 | 鉴权 |
|---|---|---|
| `GET /` | 既有接口，返回 `{"Hello":"World"}` | 无 |
| `POST /tasks` | 提交任务（body：`task_type,payload,idempotency_key?,owner?,max_attempts?`） | 无（owner 在 body） |
| `GET /tasks/{id}` | 查询任务状态/结果 | `owner` 参数或 `X-Owner` |
| `GET /tasks/{id}/history` | 查询执行历史 | 同上 |
| `POST /tasks/{id}/cancel` | 取消任务 | 同上 |
| `GET /internal/stats` | 积压/状态统计 | `X-Management-Token` |
| `POST /internal/maintenance` | 手动推进退避到期/取消到期 | `X-Management-Token` |
| `GET /healthz` | 健康检查 | 无 |

任务状态对象包含：`id, state, attempt, max_attempts, lease_owner, lease_expires_at,
cancel_requested, next_run_at, result, last_error_type, last_error_message, created_at,
updated_at` 等字段（内部 `lease_token` 不暴露）。

---

## 9. 可配置项（环境变量，前缀 `SCHEDULER_`）

| 变量 | 默认值 | 含义 |
|---|---|---|
| `SCHEDULER_DB_PATH` | `scheduler.db` | SQLite 数据文件路径 |
| `SCHEDULER_LEASE_SECONDS` | `30` | 租约时长 |
| `SCHEDULER_HEARTBEAT_INTERVAL_SECONDS` | `10` | 心跳续租间隔 |
| `SCHEDULER_HEARTBEAT_MAX_MISSED` | `3` | 允许错过的心跳数（保留配置） |
| `SCHEDULER_MAX_ATTEMPTS` | `5` | 默认最大尝试次数 |
| `SCHEDULER_BASE_BACKOFF_SECONDS` | `1` | 初始退避 |
| `SCHEDULER_MAX_BACKOFF_SECONDS` | `60` | 退避上限 |
| `SCHEDULER_POLL_INTERVAL_SECONDS` | `1` | worker 空闲轮询间隔 |
| `SCHEDULER_WORKER_ENABLED` | `true` | 进程内是否启动 worker |
| `SCHEDULER_WORKER_CONCURRENCY` | `4` | 单个 worker 的并发执行数 |
| `SCHEDULER_MANAGEMENT_TOKEN` | `local-dev-token` | 管理接口令牌（本地默认，仅本地使用） |
| `SCHEDULER_SKIP_WORKER` | `0` | 置 `1` 时即使 worker_enabled 也不启动 |
| `SCHEDULER_LOG_LEVEL` | `INFO` | 日志级别 |
| `PORT` | `8000` | HTTP 端口 |

多进程部署：可以启动多个指向**同一个 DB 文件**的进程共同调度（领取/接管互斥）；
也可以让某些进程仅提供 HTTP（`SCHEDULER_WORKER_ENABLED=false`），另一些仅执行。

---

## 10. 本地验证

```bash
# 全部单测（约数秒；含真实多进程互斥、线程并发、重启恢复、worker 端到端）
python -m pytest tests/ -q

# 按主题运行
python -m pytest tests/test_submit_idempotency.py tests/test_concurrent_claim.py -q
python -m pytest tests/test_lease_takeover.py tests/test_cancel_race.py -q
python -m pytest tests/test_retry_policy.py tests/test_restart_recovery.py -q
python -m pytest tests/test_multiprocess_claim.py -q   # 两个 OS 进程抢同一批任务
python -m pytest tests/test_worker_pool.py -q          # 真实后台 worker 执行/重试/取消
```

测试日志（caplog，INFO 级别）会打印：
- **请求输入**：提交的 `task_type/payload/idempotency_key/owner`、取消/查询属主等；
- **状态迁移**：如 `PENDING -> RUNNING claimed by w1 (attempt 1)`、
  `RUNNING -> RETRY_WAIT until ...`、`lease ... expired, taken over by ...`、
  `RUNNING -> SUCCEEDED/PERMANENTLY_FAILED/CANCELLED`；
- **拒绝依据**：每个被拒绝操作的错误码（`code=...`）、HTTP 状态与原因消息。

测试覆盖的关键场景：
1. 重复提交（幂等去重 / 参数冲突 409）；
2. 并发领取（8 线程、2 OS 进程均无重复领取，历史仅一条 `CLAIMED`）；
3. 租约接管（到期被接管、attempt 递增、旧 token 上报被拒、结果不被覆盖）；
4. 取消竞争（取消 vs 完成/重试/接管，取消始终胜出，重复取消分类拒绝）；
5. 重启恢复（PENDING 可继续领取、RETRY_WAIT 到期重试、RUNNING 僵死租约被接管、
   取消请求跨重启生效、历史与统计保留）；
6. 重试策略（临时失败有限退避、永久失败不重试、未知异常归类）。

---

## 11. 代码结构

```
main.py                # 入口：构建 service/worker/app（保留 GET /）
scheduler/config.py    # 环境变量配置
scheduler/errors.py    # 类型化错误（code + http_status）
scheduler/models.py    # 状态枚举、Task/TaskEvent/请求模型、对外视图
scheduler/store.py     # SQLite：schema、条件 UPDATE、领取/接管/取消/退避/统计
scheduler/service.py   # 编排：幂等校验、退避计算、错误映射、日志
scheduler/worker.py    # 线程池执行器：领取、心跳、协作式取消、失败分类
scheduler/api.py       # FastAPI 路由与错误到 HTTP 的映射
tests/                 # 全部单测/集成测（见第 10 节）
```
