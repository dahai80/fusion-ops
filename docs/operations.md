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

## Deployment topology (issue #5)

Decision: **option (a) — jobs + API server run on the VPS next to the prod
ops store; the blackbox probe stays on the Mac.** One store
(`fusion-vps` PG `fusion_ops`) receives gateway, probe and job outputs.

| Component | Where | Managed by | Notes |
|---|---|---|---|
| ops store | VPS PG `fusion_ops` | PostgreSQL 16 | gateway emitter + jobs + probe all write here |
| `fusion-ops` API server | VPS `127.0.0.1:11400` | systemd `fusion-ops.service` | healthz + threshold API |
| retention job | VPS | systemd timer, daily 03:30 | `fusion-ops-retention.{service,timer}` |
| cluster job | VPS | systemd timer, hourly | `fusion-ops-cluster.{service,timer}` |
| blackbox probe | Mac | launchd `com.fusion.ops-probe` | true off-VPS vantage; 5-min daemon |
| PG tunnel | Mac | launchd `com.fusion.ops-pg-tunnel` | `127.0.0.1:15432` → VPS `127.0.0.1:5432`; prod PG never exposes a public port |

Deploy: `./scripts/deploy-vps.sh` builds linux/amd64 binaries, stages to
`/tmp/fusion-ops-deploy`, installs into `/opt/fusion-ops/{bin,migrations,env}`,
runs `cmd/migrate` once as the postgres admin (peer auth), then enables the
service + timers. The env file (`/opt/fusion-ops/env/fusion-ops.env`,
root:ops 640) holds the app-role DSN; the Mac-side copy is `~/.fusion-ops/env`
(600). Passwords are generated locally into `deploy/env/` (git-ignored) and
never committed. Re-running the script upgrades in place and never overwrites
the env file.

Mac probe install (one-off):
```bash
mkdir -p ~/.fusion-ops/bin ~/.fusion-ops/logs
go build -o ~/.fusion-ops/bin/fusion-ops-probe ./cmd/fusion-ops-probe
cp deploy/launchd/run-probe.sh ~/.fusion-ops/bin/ && chmod +x ~/.fusion-ops/bin/run-probe.sh
cp deploy/launchd/com.fusion.ops-*.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/501 ~/Library/LaunchAgents/com.fusion.ops-pg-tunnel.plist
launchctl bootstrap gui/501 ~/Library/LaunchAgents/com.fusion.ops-probe.plist
```
Bootstrap the tunnel first — the probe retries via KeepAlive, but starting
both at once costs one failed round to the race.

Alert channel (issues #5/#6/#7/#8): `DeliverAlert` posts the
2-consecutive-fail payload to `FUSION_OPS_ALERT_WEBHOOK` — the fusion-sv
ingest route (`http://127.0.0.1:9100/api/v1/alerts/ingest`,
fusion-supervisor#139). The webhook body follows the ingest contract
(`source`/`kind`/`severity`/`detail`/`dedup_key`; structured `probe` +
`results` ride along as extra keys). The ingest gate records the event
into history.db with `plane=external`. `dedup_key` carries direction
(`<probe>` for fail, `<probe>:recover` for recovery) so a recovery
arriving inside the ingest dedup window is recorded instead of deduped
away (issue #7); repeated fail alerts for the same probe stay suppressed
inside the window.

Push topology (issue #8): in addition to the ingest record, the probe
pushes a human-readable notification (plain-text body, `X-Title`,
`X-Priority` high for fails) to `FUSION_OPS_ALERT_PUSH_URL` — a dedicated
ntfy topic used only by the health probe (topic name lives in
`~/.fusion-ops/env`, never committed; subscribe from a phone). Topic
naming rules: **high-entropy topic names only** (>= 16 random chars from
`openssl rand -hex 8` or stronger — ntfy topics are a global public
namespace, low-entropy names collide with strangers and leak the feed)
and **one producer class per topic** — health alerts get their own topic
so a flapping service flooding the supervisor-wide `alert_url` topic
cannot drown them (seen live 2026-11-19: 109 flood messages in 30 min on
the shared topic).
