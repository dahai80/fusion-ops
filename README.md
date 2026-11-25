# fusion-ops

Operational data store for the Fusion flywheel (architecture/fusion-flywheel-1119.md):
owns the `fusion_ops` PostgreSQL database, the failure clustering engine that feeds
`mined_issues`, and the retention/cleanup jobs that keep growth bounded and
privacy-compliant.

## Components

| Binary | Purpose |
|---|---|
| `cmd/migrate` | Standalone deploy-time migration runner (idempotent, advisory-locked) |
| `cmd/fusion-ops` | API server: threshold query + issue backfill + periodic clustering; fails closed on missing DB privileges |
| `cmd/fusion-ops-job` | One-shot maintenance jobs (`-job=retention\|cluster\|migrate`) for supervisor T2 scheduling |
| `cmd/fusion-ops-probe` | Blackbox site-health probe (issue #4): HTTP paths + TLS expiry + DNS vs expected IP; 2-round debounce; alerts via supervisor channel webhook |
| `cmd/fusion-broker` | fusion-link device broker (issue #10): WSS endpoint behind nginx `/link/`, device registry + 24h task mailbox in `fusion_ops`, Prometheus metrics on `:11401` |

## Blackbox site-health probe (issue #4)

Runs on the Mac side, outside the VPS (off-host view: TLS chain, DNS,
gateway-level failures invisible to on-box process monitoring). Deterministic
verdicts — no LLM. Every round appends `health_probe` events to
`ops_events`; alerts (`health_alert`) fire only after 2 consecutive failed
rounds, recovery notice on first success after an alerted FAIL episode,
prober-side network outages logged but never alerted (connectivity sentinel).

```bash
FUSION_OPS_EXPECTED_SITE_IP=47.82.117.121 go run ./cmd/fusion-ops-probe        # daemon, 5m interval
FUSION_OPS_EXPECTED_SITE_IP=... go run ./cmd/fusion-ops-probe -once            # single round (supervisor T2 style)
```

| Env | Default | Meaning |
|---|---|---|
| `FUSION_OPS_PROBE_SITE` | `https://fusion-mlx.cn` | Probe target base |
| `FUSION_OPS_PROBE_PATHS` | `/,/auth/` | HTTP paths (2xx + non-empty body = pass) |
| `FUSION_OPS_EXPECTED_SITE_IP` | unset (DNS probe skips) | Expected VPS IP for the DNS check |
| `FUSION_OPS_PROBE_INTERVAL` | `5m` | Daemon cadence |
| `FUSION_OPS_ALERT_WEBHOOK` | unset (log only) | POST target for alert payloads (ingest-contract body; prod: fusion-sv `:9100/api/v1/alerts/ingest`) |
| `FUSION_OPS_ALERT_PUSH_URL` | unset (no push) | Direct push target for the human-readable notification; prod: self-hosted ntfy topic `https://ntfy.fusion-mlx.cn/ops-health-…` |
| `FUSION_OPS_ALERT_PUSH_TOKEN` | unset (no auth header) | Bearer token for the push target — required for the self-hosted ntfy (deny-all auth); leave unset for public ntfy.sh topics |
| `FUSION_OPS_PROBE_CONTROL` | `cloudflare.com` | Prober connectivity sentinel |

TLS probe FAILs when <14 days remain; verdict state seeds from ops_events
history so one-shot runs continue FAIL episodes instead of resetting them.

## Deployment (issue #5)

Single source of truth: the prod ops store is the VPS PostgreSQL
`fusion_ops`. Jobs (`fusion-ops-job` via systemd timers: retention daily
03:30, clustering hourly) and the API server (`fusion-ops.service` on
`127.0.0.1:11400`) run on the VPS; the blackbox probe stays on the Mac and
writes through a launchd-kept SSH tunnel (`127.0.0.1:15432` → VPS PG, the
prod PG never exposes a public port). One store receives gateway + probe +
job outputs.

```bash
./scripts/deploy-vps.sh    # build linux/amd64, stage, install, migrate, enable
```

Details, Mac probe install and the alert-channel status:
`docs/operations.md` § Deployment topology.

## Schema v1

- `ops_events` — operational events (gateway/identity/commerce/cowork/autotest/support/ops/broker); indexed `(source, ts)`, `(event_type, ts)`
- `ops_prompts` — prompt samples (only opt-in or double-pass desensitized text, 10% sampling cap upstream)
- `mined_issues` — failure clusters (`cluster_key = source|error_signature`), threshold + backfill workflow (`new` → `open` → `closed`)
- `feedback` — support-agent structured message classifications
- `drop_counters` — emitter backpressure drop audit; monthly rollup into `ops_events` as `emitter_drop`
- `device_registry` — fusion-link bound devices (issue #10); tokens stored sha256-hashed, state mirrors live connections
- `task_mailbox` — tasks parked for offline devices; 24h TTL enforced by the retention job, idempotent per (device, task)

## Broker (issue #10)

Devices connect out to `wss://fusion-mlx.cn/link/ws` (nginx TLS → `127.0.0.1:11401`).
Protocol = PRD §7 envelope `{v, type, ts, device_id, seq, payload}`; auth via bound
token (hello/hello_ack). Tasks dispatch straight to online devices; offline devices
get a PG-backed mailbox drained on reconnect (reconnect-storm safe). `ops_events`
gains `broker` emitter rows only for failures/state transitions (`device_online`,
`device_offline`, `task_failed`) — heartbeat traffic never enters the stream.

Provision a device (token via env, never argv):

```bash
FUSION_OPS_DATABASE_URL=... FUSION_BROKER_PROVISION_DEVICE=<id> \
FUSION_BROKER_PROVISION_TOKEN=<secret> FUSION_BROKER_PROVISION_VERSION=<v> \
go run ./cmd/fusion-broker
```

Dispatch: `POST /api/tasks` with `{device_id, task_id, type, params}` (localhost only).
Metrics: `GET /metrics` (`broker_devices_online`, `broker_tasks_queued`,
`broker_tasks_dispatched_total`, `broker_wss_errors_total`).

## API (default `127.0.0.1:11400`, localhost only)

| Endpoint | Description |
|---|---|
| `GET /healthz` | Liveness + DB ping |
| `GET /api/clusters?threshold=5&hours=24` | Clusters ready for issue drafting (count >= threshold in window, or P0 singletons); output desensitized v1 |
| `POST /api/clusters/backfill` | `{"cluster_key","issue_url"}` — record drafted issue, stops re-drafting |
| `POST /api/clusters/close` | `{"cluster_key"}` — mark issue closed, stops drafting |
| `POST /api/jobs/retention` | Trigger one retention pass (drills / manual runs) |

## Configuration (env)

| Variable | Default | Notes |
|---|---|---|
| `FUSION_OPS_DATABASE_URL` | `postgres://127.0.0.1:5432/fusion_ops?sslmode=disable` | DSN, DB-3 compliant |
| `FUSION_OPS_LISTEN` | `127.0.0.1:11400` | API bind address |
| `FUSION_OPS_CLUSTER_INTERVAL` | `15m` | Periodic clustering cadence |
| `FUSION_OPS_THRESHOLD` | `5` | Default draft threshold |
| `FUSION_OPS_MIGRATIONS_DIR` | `migrations` | Migration SQL directory |
| `FUSION_BROKER_LISTEN` | `127.0.0.1:11401` | Broker bind address (issue #10) |

## Development

```bash
go build ./...
go test ./...          # needs local PostgreSQL; uses fusion_ops_test DB (recreated each run)
go test ./internal/desensitize/   # pure unit tests, no DB
```

CI runs the same suite against a Postgres 16 service container.

## Data policies

- **Retention** (issue #2): `ops_events` raw 90d; `ops_prompts.prompt_text` 180d
  (hash + counters persist); `mined_issues` / `feedback` unlimited. Nightly
  batched cleanup, off-peak; partitioning of `ops_events` becomes worthwhile
  past ~50M rows — see `docs/operations.md` for the trigger point.
- **Desensitize v1** (`internal/desensitize`): regex layer per flywheel plan §3 —
  email, CN phone, ID card, API keys/tokens, Luhn-checked cards, IPs, URL
  credentials, tenant/user hashing. Anything served over the API passes this
  layer; raw PII is never persisted into `mined_issues`.
- **Privilege probe**: server startup fails closed unless the role holds full
  DML on all tables (catalog check + rolled-back canary roundtrip).

See `docs/provisioning.md` for database/role provisioning and backup/restore.
