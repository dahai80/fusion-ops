# fusion_ops Operations

## Retention jobs (issue #2)

Nightly, off-peak (supervisor T2 schedules `fusion-ops-job -job=retention`):

1. `ops_events` rows older than 90 days deleted in batches of 5000 (logged).
2. `ops_prompts.prompt_text` older than 180 days set to NULL — `prompt_hash`,
   category, and counters persist (issue #2 req 1).
3. `drop_counters` buckets from past months roll up into `ops_events` as
   `emitter_drop` rows and are marked aggregated — idempotent, reruns never
   duplicate (issue #2 req 3).

Failures exit non-zero and surface in supervisor health history.

## Partitioning trigger point (issue #2 req 4)

`ops_events` stays unpartitioned until it exceeds ~50M rows. At that point:

1. Check actual size: `SELECT reltuples::bigint FROM pg_class WHERE relname='ops_events';`
2. Create `ops_events_yYYmMM` partitions by `ts` range (monthly), attach to a
   partitioned parent in a new migration (DB-9: replayable).
3. Only implement when the trigger is hit — do not pre-partition.

## Restore drill (issue #1 req 4)

`fusion-sv backup` covers fusion_ops via `pg_dumpall` (plane=native). Extract
and verify quarterly:

```bash
# 1. Locate latest backup
ls -t ~/.fusion-sv/backups/*.sql.gz | head -1

# 2. Extract the fusion_ops section
gunzip -c ~/.fusion-sv/backups/fusion-pg-<date>.sql.gz \
    | sed -n '/^--.*Database: fusion_ops/,/^--.*Database: /p' > /tmp/fusion_ops_restore.sql

# 3. Restore into a scratch database
psql postgres -c "CREATE DATABASE fusion_ops_drill"
psql fusion_ops_drill -f /tmp/fusion_ops_restore.sql

# 4. Verify: table count + row counts match live, then drop scratch
psql fusion_ops_drill -c "\dt"
psql postgres -c "DROP DATABASE fusion_ops_drill"
rm /tmp/fusion_ops_restore.sql
```

A full drill result is recorded in the release notes of v0.0.1.

Drill 2026-11-19 (v0.0.1): fresh `pg_dumpall` (3.0G) → `fusion_ops` section
extracted → restored to scratch DB `fusion_ops_drill` → all 6 tables created,
legacy `fusion_ops.ops_events` 26/26 rows and `public` schema v1 verified,
row counts matched live → scratch dropped. Note: the dump section begins with
`\connect fusion_ops` — strip that line when restoring into a differently
named scratch database, or psql reconnects to the live DB mid-restore.

## Supervisor wiring (upstream)

Job scheduling belongs to fusion-supervisor's T2 tier. Registration request
filed upstream (dahai80/fusion-supervisor#137) — this repo ships the one-shot
binary (`fusion-ops-job`) and does not modify the supervisor codebase.
