# fusion_ops Operations

## fusion-broker (issue #10)

Runs on the VPS as `fusion-broker.service` (systemd, port 11401, localhost bind;
public entry is nginx 443 `/link/` → WSS). Durable state in `fusion_ops`
(`device_registry`, `task_mailbox`, migration 0003) — a broker restart preserves
registry and mailbox; stale online rows are reset at boot without events.

Event discipline: `ops_events` gains `broker` rows only for state transitions
and failures (`device_online`, `device_offline`, `task_failed`). Heartbeats
update `last_seen` only. The 24h mailbox TTL is purged by the nightly
retention job (`mailbox_deleted` in the job result).

Provisioning (token via env, hashed at rest):

```bash
sudo systemctl set-environment FUSION_BROKER_PROVISION_DEVICE=<id> \
    FUSION_BROKER_PROVISION_TOKEN=<secret> FUSION_BROKER_PROVISION_VERSION=<v>
sudo systemctl restart fusion-broker   # provision mode runs and exits
sudo systemctl start fusion-broker     # serve mode
sudo systemctl unset-environment FUSION_BROKER_PROVISION_DEVICE FUSION_BROKER_PROVISION_TOKEN FUSION_BROKER_PROVISION_VERSION
```

Dispatch: `POST http://127.0.0.1:11401/api/tasks` `{device_id, task_id, type,
params}`. Metrics: `GET /metrics`. Verified live 2026-11-25: offline park →
dispatch API → reconnect drain; real systemd restart preserved a parked task;
event counters showed exactly 4 `device_online` + 4 `device_offline` and zero
heartbeat rows; 1k-device reconnect storm covered by
`TestBrokerReconnectStormDrainsMailbox`.

## fusion-broker read surface (issue #11)

Migration 0004 widens the read model: `task_mailbox` gains the full status
machine (`queued/dispatched/running/awaiting_local_confirm/completed/failed/
expired`) plus `updated_at`/`steps`/`error_code`/`error_message`;
`device_registry` gains hello-reported `hostname`/`platform`, `connected_since`,
heartbeat `last_health` and provisioning `tags`; new `task_events` table stores
progress history. Read endpoints (issue #11 contract, consumed by the
fusion-site console + task bridge):

- `GET /api/devices` — device table; `online` = live WSS session, `services`
  parsed from heartbeat `last_health.services` (empty until devices report it),
  `is_default_execution` = tags contains `default-execution`
- `GET /api/tasks?limit=50` (cap 200) and `GET /api/tasks/{task_id}` — list
  newest-first; queued tasks past TTL flip to `expired` on read
- `GET /api/tasks/{task_id}/events` — polling progress feed (v1; SSE deferred)

Auth: `FUSION_BROKER_API_KEY` env (static bearer key, provisioned alongside
the other broker env). All `GET /api/*` require `Authorization: Bearer <key>`;
`POST /api/tasks` stays localhost-only; `/healthz` + `/metrics` stay
unauthenticated for supervisor probes. Task state transitions ride `task.event`
uplinks (`started|running → running`, `awaiting_local_confirm`, `completed|
succeeded → completed`, `failed → failed`); `failed` uplinks keep emitting the
`task_failed` ops_event (issue #10 discipline unchanged). Device tags are set
at provisioning via `FUSION_BROKER_PROVISION_TAGS` (comma-separated).

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

Push topology (issue #8, updated 2026-11-20 for the self-hosted ntfy): in
addition to the ingest record, the probe pushes a human-readable
notification (plain-text body, `X-Title`, `X-Priority` high for fails) to
`FUSION_OPS_ALERT_PUSH_URL` — a dedicated ntfy topic used only by the
health probe (URL + token live in `~/.fusion-ops/env`, never committed;
subscribe from a phone).

Migrated 2026-11-20 from public ntfy.sh to the self-hosted server
`https://ntfy.fusion-mlx.cn` (fusion-remote issue #3, deny-all token
auth): the probe now sends `Authorization: Bearer <FUSION_OPS_ALERT_PUSH_TOKEN>`
when the token env is set (unset = old public-topic behavior, no header).
Server-side principals (ntfy 2.11 CLI, run as root on the VPS):

- producer: user `ops-monitor`, token in `~/.fusion-ops/env`,
  `ntfy access ops-monitor 'ops-health-*' write-only`
- consumer: user `dahai-phone` (password handed to the owner, not
  committed), `ntfy access dahai-phone 'ops-health-*' read-only`

Topic rules that survive the migration: **high-entropy topic names
still required** (defense in depth — the ACL is per-topic-pattern, and
the name must not leak the feed even if a token leaks) and **one
producer class per topic** — health alerts keep their own topic so a
flapping service flooding the supervisor-wide `alert_url` topic cannot
drown them (seen live 2026-11-19: 109 flood messages in 30 min on the
shared topic).

## VPS nginx hardening (issue #9)

Applied 2026-11-19 (nginx 1.24.0, Ubuntu; pre-change copies in
`/root/nginx-issue9-backup/`):

- `/etc/nginx/nginx.conf`: global `ssl_protocols TLSv1.2 TLSv1.3;`
  (was TLSv1/1.1/1.2/1.3 — inert under OpenSSL 3 seclevel but able to
  regress), `server_tokens off;` uncommented.
- `headscale.conf`: added `ssl_protocols TLSv1.2 TLSv1.3;`, HSTS,
  `X-Content-Type-Options nosniff`.
- `client_max_body_size` explicit per vhost: 20m on fusion-mlx.cn and
  bench-site (headroom for API/BFF payloads; no known large uploads),
  1m on headscale (control plane).
- Stale `bench-site.bak-xray-out` moved out of `sites-enabled/` — nginx
  includes every file in that directory, so the old full server copy was
  live configuration with duplicate `server_name`s.
- The reported duplicate `location /api/auth/` blocks turned out to be in
  *different* `server` blocks (http vs https) — legal config, no same-block
  duplicate existed.
- Live acceptance: TLS1.0 handshake rejected, `Server: nginx` (no
  version), headscale headers present, fusion-mlx.cn root/auth/BFF/shell
  unchanged (gateway 404s verified identical to loopback baseline).

Item 4 (0.0.0.0:8100 listener): the listener was the legacy LinguaKids
deploy (`frontend/serve.py`, not tracked in any repo tree). Loopback bind
was applied live (`127.0.0.1`, verified via `ss`), then the owner
decommissioned LinguaKids on this VPS the same evening — the whole app
moved to `/root/decommissioned/linguakids-1119/`, `linguakids-serve` and
`linguakids-agent` are stopped + disabled, port 8100 closed. An upstream
issue filed against fusion-k12-teacher (#42) was closed as wrong-repo —
the code never lived there. Default-host (bench.dpdns.org) surface
trimming and the xray location remain owner decisions.
