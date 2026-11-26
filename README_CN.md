# fusion-ops（中文版）

Fusion 飞轮的运营数据仓库（architecture/fusion-flywheel-1119.md）：拥有 `fusion_ops` PostgreSQL 数据库、喂给 `mined_issues` 的失败聚类引擎，以及控制增长边界、满足隐私合规的保留/清理任务。

## 组件

| 二进制 | 用途 |
|---|---|
| `cmd/migrate` | 独立的部署期迁移执行器（幂等，advisory-locked） |
| `cmd/fusion-ops` | API 服务：阈值查询 + issue 回填 + 周期性聚类；缺少 DB 权限时 fail-closed |
| `cmd/fusion-ops-job` | 一次性维护任务（`-job=retention\|cluster\|migrate`），供 supervisor T2 调度 |
| `cmd/fusion-ops-probe` | 黑盒站点健康探测（issue #4）：HTTP 路径 + TLS 到期 + DNS 与期望 IP 比对；2 轮防抖；经 supervisor 通道 webhook 告警 |
| `cmd/fusion-broker` | fusion-link 设备 broker（issue #10）：nginx `/link/` 后的 WSS 端点，`fusion_ops` 中的设备注册表 + 24h 任务邮箱，Prometheus 指标在 `:11401` |

## 黑盒站点健康探测（issue #4）

在 Mac 侧、VPS 之外运行（off-host 视角：能看到 on-box 进程监控看不见的 TLS 链、DNS、网关级故障）。确定性判定 —— 不用 LLM。每轮向 `ops_events` 追加 `health_probe` 事件；告警（`health_alert`）仅在连续 2 轮失败后触发，处于告警 FAIL 状态后的首次成功发恢复通知；探测端自身断网只记日志、绝不告警（连通性哨兵）。

```bash
FUSION_OPS_EXPECTED_SITE_IP=47.82.117.121 go run ./cmd/fusion-ops-probe        # 守护进程，5m 间隔
FUSION_OPS_EXPECTED_SITE_IP=... go run ./cmd/fusion-ops-probe -once            # 单轮（supervisor T2 风格）
```

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `FUSION_OPS_PROBE_SITE` | `https://fusion-mlx.cn` | 探测目标基址 |
| `FUSION_OPS_PROBE_PATHS` | `/,/auth/` | HTTP 路径（2xx + 非空 body = 通过） |
| `FUSION_OPS_EXPECTED_SITE_IP` | 未设置（跳过 DNS 探测） | DNS 检查用的期望 VPS IP |
| `FUSION_OPS_PROBE_INTERVAL` | `5m` | 守护进程节奏 |
| `FUSION_OPS_ALERT_WEBHOOK` | 未设置（仅记日志） | 告警 payload 的 POST 目标（ingest 契约 body；生产：fusion-sv `:9100/api/v1/alerts/ingest`） |
| `FUSION_OPS_ALERT_PUSH_URL` | 未设置（不推送） | 人类可读通知的直接推送目标；生产：自托管 ntfy 主题 `https://ntfy.fusion-mlx.cn/ops-health-…` |
| `FUSION_OPS_ALERT_PUSH_TOKEN` | 未设置（无认证头） | 推送目标的 Bearer token —— 自托管 ntfy（deny-all 认证）必填；公开 ntfy.sh 主题可不设 |
| `FUSION_OPS_PROBE_CONTROL` | `cloudflare.com` | 探测端连通性哨兵 |

TLS 探测在剩余有效期 <14 天时 FAIL；判定状态从 ops_events 历史播种，单轮运行会延续 FAIL 状态而不是重置。

## 部署（issue #5）

唯一事实源：生产 ops 存储是 VPS PostgreSQL `fusion_ops`。任务（`fusion-ops-job` 经 systemd timers：retention 每日 03:30，聚类每小时）与 API 服务（`fusion-ops.service`，监听 `127.0.0.1:11400`）运行在 VPS；黑盒探测留在 Mac，通过 launchd 保活的 SSH 隧道写入（`127.0.0.1:15432` → VPS PG，生产 PG 绝不暴露公网端口）。单一存储接收 gateway + 探测 + 任务的全部输出。

```bash
./scripts/deploy-vps.sh    # 构建 linux/amd64、上传、安装、迁移、启用
```

细节、Mac 探测安装与告警通道状态见 `docs/operations.md` § Deployment topology。

## Schema v1

- `ops_events` — 运营事件（gateway/identity/commerce/cowork/autotest/support/ops/broker）；索引 `(source, ts)`、`(event_type, ts)`
- `ops_prompts` — prompt 样本（仅 opt-in 或双重脱敏文本，上游采样上限 10%）
- `mined_issues` — 失败聚类（`cluster_key = source|error_signature`），阈值 + 回填工作流（`new` → `open` → `closed`）
- `feedback` — support-agent 结构化消息分类
- `drop_counters` — 发射端背压丢弃审计；每月 rollup 进 `ops_events`（`emitter_drop`）
- `device_registry` — fusion-link 绑定设备（issue #10）；token 以 sha256 哈希存储，状态镜像在线连接；hello 上报的 hostname/platform、心跳健康快照与 console `tags`（issue #11）
- `task_mailbox` — 为离线设备暂存的任务；由 retention 任务执行 24h TTL，按 (device, task) 幂等；宽状态机 + steps/error 读模型（issue #11）
- `task_events` — 尽力而为的任务进度历史（设备 `task.event` 上行，issue #11）

## Broker（issue #10）

设备向外连接 `wss://fusion-mlx.cn/link/ws`（nginx TLS → `127.0.0.1:11401`）。协议 = PRD §7 envelope `{v, type, ts, device_id, seq, payload}`；通过绑定 token（hello/hello_ack）认证。任务直接派发给在线设备；离线设备进入 PG 邮箱、重连时排空（重连风暴安全）。`ops_events` 只为失败/状态迁移增加 `broker` 发射行（`device_online`、`device_offline`、`task_failed`）—— 心跳流量绝不入流。

开通设备（token 走环境变量，绝不走 argv）：

```bash
FUSION_OPS_DATABASE_URL=... FUSION_BROKER_PROVISION_DEVICE=<id> \
FUSION_BROKER_PROVISION_TOKEN=<secret> FUSION_BROKER_PROVISION_VERSION=<v> \
go run ./cmd/fusion-broker
```

派发：`POST /api/tasks`，body `{device_id, task_id, type, params}`（仅 localhost）。指标：`GET /metrics`（`broker_devices_online`、`broker_tasks_queued`、`broker_tasks_dispatched_total`、`broker_wss_errors_total`）。

面向 site console 的读接口（issue #11，bearer key 经 `FUSION_BROKER_API_KEY`，请求头 `Authorization: Bearer <key>`）：

| 端点 | 说明 |
|---|---|
| `GET /api/devices` | 设备表：hostname/platform、实时 `online` 标志、`connected_since`、心跳推导的 `services`、`tags`（`is_default_execution` = tags 包含 `default-execution`） |
| `GET /api/tasks?limit=50` | 任务列表（最新在前，上限 200）；超过 TTL 的滞留 queued 任务在读取时翻转为 `expired` |
| `GET /api/tasks/{task_id}` | 任务详情：状态、`steps`（设备上报进度）、结构化 `error` `{code,message}` |
| `GET /api/tasks/{task_id}/events` | 进度历史，最旧在前（轮询 feed，活跃时约 2s） |

任务状态机：`queued → dispatched → running → awaiting_local_confirm → completed|failed|expired`。设备经 `task.event` 帧上报进度（`started`/`running`、`awaiting_local_confirm`、`completed`/`succeeded`、`failed`）；失败上行同时发出 `task_failed` ops_event。

## API（默认 `127.0.0.1:11400`，仅 localhost）

| 端点 | 说明 |
|---|---|
| `GET /healthz` | 存活检查 + DB ping |
| `GET /api/clusters?threshold=5&hours=24` | 可起草 issue 的聚类（窗口内 count >= threshold，或 P0 单例）；输出为脱敏 v1 |
| `POST /api/clusters/backfill` | `{"cluster_key","issue_url"}` —— 记录已起草 issue，停止重复起草 |
| `POST /api/clusters/close` | `{"cluster_key"}` —— 标记 issue 已关闭，停止起草 |
| `POST /api/jobs/retention` | 触发一轮 retention（演练 / 手动运行） |

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `FUSION_OPS_DATABASE_URL` | `postgres://127.0.0.1:5432/fusion_ops?sslmode=disable` | DSN，符合 DB-3 |
| `FUSION_OPS_LISTEN` | `127.0.0.1:11400` | API 绑定地址 |
| `FUSION_OPS_CLUSTER_INTERVAL` | `15m` | 周期聚类节奏 |
| `FUSION_OPS_THRESHOLD` | `5` | 默认起草阈值 |
| `FUSION_OPS_MIGRATIONS_DIR` | `migrations` | 迁移 SQL 目录 |
| `FUSION_BROKER_LISTEN` | `127.0.0.1:11401` | Broker 绑定地址（issue #10） |
| `FUSION_BROKER_API_KEY` | _（空）_ | 门禁 `GET /api/*` 读接口的 Bearer key（issue #11）；空 = 仅限 loopback 调用方 |
| `FUSION_BROKER_PROVISION_TAGS` | _（空）_ | 开通时的逗号分隔设备 tags（issue #11） |
| `FUSION_BROKER_PROVISION_HOSTNAME` | _（空）_ | 开通时预置 `device_registry.hostname`（issue #12） |
| `FUSION_BROKER_PROVISION_PLATFORM` | _（空）_ | 开通时预置 `device_registry.platform`（issue #12） |

## 开发

```bash
go build ./...
go test ./...          # 需要本地 PostgreSQL；使用 fusion_ops_test DB（每次运行重建）
go test ./internal/desensitize/   # 纯单元测试，无需 DB
```

CI 在 Postgres 16 service 容器上跑同一套测试。

## 数据策略

- **保留**（issue #2）：`ops_events` 原始 90d；`ops_prompts.prompt_text` 180d（哈希 + 计数器永久保留）；`mined_issues` / `feedback` 不限。夜间分批清理、避开高峰；`ops_events` 超过约 5000 万行后分区才开始划算 —— 触发点见 `docs/operations.md`。
- **脱敏 v1**（`internal/desensitize`）：按飞轮方案 §3 的 regex 层 —— 邮箱、中国大陆手机号、身份证、API key/token、Luhn 校验的银行卡、IP、URL 凭证、租户/用户哈希。所有经 API 输出的内容都过这一层；原始 PII 绝不持久化进 `mined_issues`。
- **权限探针**：服务启动时 fail-closed，除非角色对全部表持有完整 DML（catalog 检查 + 回滚的金丝雀往返）。

数据库/角色开通与备份/恢复见 `docs/provisioning.md`。
