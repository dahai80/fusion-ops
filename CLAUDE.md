# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

fusion-ops owns the `fusion_ops` PostgreSQL database and the ops-data flywheel backend (architecture/fusion-flywheel-1119.md, Loop A/C): schema v1 + migrations, failure clustering into `mined_issues`, retention/cleanup jobs, desensitize v1 regex layer, blackbox site-health probe.

Go 1.26. Part of the Fusion monorepo workspace (`/Users/dahai/fusion`); workspace rules (port registry, DB-1~DB-10 conventions, English-only GitHub artifacts) apply. Conventions mirror fusion-commerce (pgx/v5, advisory-locked idempotent migrations).

## Commands

```bash
go build ./...
go test ./...              # per-package test DBs, auto-created; skips if no local PG
go test ./internal/desensitize/   # pure unit tests, no DB
go run ./cmd/migrate       # apply migrations (admin role needed for first apply)
go run ./cmd/fusion-ops    # API server on 127.0.0.1:11400 (registered port)
go run ./cmd/fusion-ops-job -job=retention|cluster|migrate   # one-shot jobs for supervisor T2
go run ./cmd/fusion-ops-probe -once   # single health-probe round
```

## Architecture

- `migrations/` — numbered SQL, `schema_migrations` bookkeeping + advisory lock (`internal/store.Migrate`). First apply needs CREATE (admin role); app role `fusion_ops_app` holds DML only.
- `internal/store` — Connect/Migrate + fail-closed privilege probe (`ProbePrivileges` catalog check + `ProbeCanary` rolled-back roundtrip). Server refuses to start when probe fails.
- `internal/cluster` — watermark-based clustering (`MAX(last_event_id)` global watermark; late events below it are not retro-clustered). `cluster_key = source|error_signature` (payload `error_signature` wins, else `event_type:status`). States: new → open (issue backfilled) → closed; only `new` clusters appear in threshold query.
- `internal/retention` — batched deletes: `ops_events` raw 90d, `ops_prompts.prompt_text` 180d (hash persists), `drop_counters` monthly rollup into `ops_events` as `emitter_drop` (idempotent).
- `internal/desensitize` — regex-only PII scrub (v1, flywheel §3). All API output passes it; raw PII never persisted into `mined_issues`.
- `internal/api` — stdlib net/http, localhost bind only; front traffic goes through fusion-gateway.
- `internal/testutil` — `Fresh(t)` recreates the per-package test DB + applies migrations.
- `internal/probe` — blackbox site-health probe (issue #4): HTTP paths (2xx + non-empty body), TLS expiry (FAIL <14d), DNS vs expected IP. Debounce: FAIL only after 2 consecutive rounds; recovery notice after alerted episode; prober-down sentinel (control-domain lookup) skips verdicts. `cmd/fusion-ops-probe -once` is stateless — verdict state seeds from ops_events history (`LoadLastResults` + `LoadActiveAlerts`) so supervisor one-shot runs continue episodes.
- `cmd/fusion-ops-probe` — probe daemon (5m) / one-shot; alert payloads = 2 consecutive failing results; optional `FUSION_OPS_ALERT_WEBHOOK` POSTs to the supervisor channel.

## Environment facts (verified 2026-11-19)

- Live DB: `127.0.0.1:5432/fusion_ops`, role `fusion_ops_app` (local dev password, rotate for prod; never commit credentials).
- Port 11400 registered in architecture/port-registry.yaml (11436 is fusion-rag — do not use).
- Gateway emitter writes `public.ops_events` v1 (legacy schema dropped, fusion-gateway#692 resolved); fusion-core Python emitter alignment still open (fusion-core#58).
- Test DBs are per-package (`fusion_ops_test_<pkg>` via `testutil.Fresh`) — parallel `go test ./...` never collides; no env setup needed.
- Job scheduling in fusion-sv T2 tier is upstream: fusion-supervisor#137.
