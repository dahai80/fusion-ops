# fusion_ops Provisioning (issue #1 req 3)

Shared PostgreSQL instance: `127.0.0.1:5432` (brew `postgresql@15`, sole
instance per DB-1). One database per app (DB-2). Run all SQL below as a
superuser (`postgres`).

## 1. Database + role

```sql
CREATE DATABASE fusion_ops;
CREATE ROLE fusion_ops_app LOGIN PASSWORD '<set-in-env, never in repo>';
REVOKE ALL ON DATABASE fusion_ops FROM PUBLIC;
GRANT CONNECT ON DATABASE fusion_ops TO fusion_ops_app;
```

```sql
\c fusion_ops
GRANT USAGE ON SCHEMA public TO fusion_ops_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fusion_ops_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO fusion_ops_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fusion_ops_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO fusion_ops_app;
```

Migration runs (initial + each new migration) execute as an admin/superuser
connection — they need CREATE. The app role's boot-time migrate is a no-op
gate over already-applied migrations.

Least privilege on purpose: no CREATE, no SUPERUSER, no cross-database rights.
The role cannot touch `fusion_identity`, `fusion_commerce`, or any other
database — verified in the role-isolation check below.

DSN goes in env only (DB-3/DB-4): `FUSION_OPS_DATABASE_URL=postgres://fusion_ops_app:<password>@127.0.0.1:5432/fusion_ops?sslmode=disable`.

## 2. Migrations

```bash
FUSION_OPS_DATABASE_URL=postgres://fusion_ops_app:...@127.0.0.1:5432/fusion_ops \
    go run ./cmd/migrate -dir migrations
```

Idempotent (schema_migrations bookkeeping + advisory lock, DB-9). The server
also migrates at boot as a second gate.

## 3. Fail-closed privilege probe

`cmd/fusion-ops` refuses to start unless the connected role holds
SELECT/INSERT/UPDATE/DELETE on every required table (catalog check via
`information_schema.table_privileges` plus a rolled-back canary INSERT/DELETE
roundtrip on `ops_events`). A mis-provisioned role therefore surfaces at
deploy time, not as runtime write failures.

## 4. Verification checklist

```sql
-- Role cannot see other databases' relations: connect as fusion_ops_app and
-- attempt cross-database access — must fail.
```

- [ ] Fresh-apply migrations twice on empty DB: second run is a no-op (tested in `internal/store`)
- [ ] Smoke insert/read per table (tested in `internal/store`)
- [ ] `fusion_ops_app` blocked from other databases on :5432
- [ ] Probe fails closed for read-only role (tested in `internal/store`)
- [ ] Restore drill: `fusion_ops` section extractable from a real `fusion-sv backup` (see operations.md)

## 5. Known conflict: legacy `fusion_ops` schema (2026-11-19)

The live `fusion_ops` database contains a pre-existing `fusion_ops` schema
(single table `ops_events`, status as TEXT, event_type as
`source|event_type` composite) written by the fusion-gateway emitter
(`internal/ops/ops.go`, ensureSchema) and the fusion-core emitter
(`fusion_core/ops.py`). Issue #1's schema v1 — tables in `public`, separate
`source`/`event_type`, INTEGER status — is authoritative; alignment requests
filed upstream (fusion-gateway, fusion-core). Until they migrate, both
schemas coexist; fusion-ops reads and writes only `public`.

## 6. Registry

- Port: 5432 already registered in `architecture/port-registry.yaml` (`external_tools`).
- Database listing: `fusion_ops` row added to `architecture/fusion-db-conventions-1030.md` §4.
- API port `11400` for `fusion-ops` registered in `architecture/port-registry.yaml`.
