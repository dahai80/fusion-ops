# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

fusion-ops owns the `fusion_ops` PostgreSQL database and the ops-data flywheel backend (architecture/fusion-flywheel-1119.md, Loop A/C): schema v1 + migrations, failure clustering into `mined_issues`, retention/cleanup jobs, desensitize v1 regex layer.

Go 1.26. Part of the Fusion monorepo workspace (`/Users/dahai/fusion`); workspace rules (port registry, DB-1~DB-10 conventions, English-only GitHub artifacts) apply. Conventions mirror fusion-commerce (pgx/v5, advisory-locked idempotent migrations).

## Commands

```bash
go build ./...
go test ./...              # needs local PG; recreates fusion_ops_test DB per run (skips if no PG)
go test ./internal/desensitize/   # pure unit tests, no DB
go run ./cmd/migrate       # apply migrations (admin role needed for first apply)
go run ./cmd/fusion-ops    # API server on 127.0.0.1:11400 (registered port)
go run ./cmd/fusion-ops-job -job=retention|cluster|migrate   # one-shot jobs for supervisor T2
```

Test DSN override: `FUSION_OPS_TEST_DATABASE_URL` / `FUSION_OPS_TEST_ADMIN_URL`.

## Architecture

- `migrations/` — numbered SQL, `schema_migrations` bookkeeping + advisory lock (`internal/store.Migrate`). First apply needs CREATE (admin role); app role `fusion_ops_app` holds DML only.
- `internal/store` — Connect/Migrate + fail-closed privilege probe (`ProbePrivileges` catalog check + `ProbeCanary` rolled-back roundtrip). Server refuses to start when probe fails.
- `internal/cluster` — watermark-based clustering (`MAX(last_event_id)` global watermark; late events below it are not retro-clustered). `cluster_key = source|error_signature` (payload `error_signature` wins, else `event_type:status`). States: new → open (issue backfilled) → closed; only `new` clusters appear in threshold query.
- `internal/retention` — batched deletes: `ops_events` raw 90d, `ops_prompts.prompt_text` 180d (hash persists), `drop_counters` monthly rollup into `ops_events` as `emitter_drop` (idempotent).
- `internal/desensitize` — regex-only PII scrub (v1, flywheel §3). All API output passes it; raw PII never persisted into `mined_issues`.
- `internal/api` — stdlib net/http, localhost bind only; front traffic goes through fusion-gateway.
- `internal/testutil` — `Fresh(t)` recreates test DB + applies migrations.

## Environment facts (verified 2026-11-19)

- Live DB: `127.0.0.1:5432/fusion_ops`, role `fusion_ops_app` (local dev password, rotate for prod; never commit credentials).
- Legacy conflict: gateway/core emitters still write a separate `fusion_ops` schema (status TEXT, composite event_type) — coexists with authoritative `public` v1; see docs/provisioning.md §5. Do not delete the legacy schema from this repo; upstream owns migration.
- Port 11400 registered in architecture/port-registry.yaml (11436 is fusion-rag — do not use).
- Job scheduling in fusion-sv T2 tier is upstream: file issues on dahai80/fusion-supervisor.
