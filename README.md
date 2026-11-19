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

## Schema v1

- `ops_events` — operational events (gateway/identity/commerce/cowork/autotest/support); indexed `(source, ts)`, `(event_type, ts)`
- `ops_prompts` — prompt samples (only opt-in or double-pass desensitized text, 10% sampling cap upstream)
- `mined_issues` — failure clusters (`cluster_key = source|error_signature`), threshold + backfill workflow (`new` → `open` → `closed`)
- `feedback` — support-agent structured message classifications
- `drop_counters` — emitter backpressure drop audit; monthly rollup into `ops_events` as `emitter_drop`

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
