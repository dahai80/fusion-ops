-- Issue #10: fusion-broker (device registry + task mailbox).
-- New emitter source 'broker' (ops_events gains broker rows only for
-- failures/state transitions; heartbeat-level normal events stay out).

ALTER TABLE ops_events DROP CONSTRAINT ops_events_source_check;
ALTER TABLE ops_events ADD CONSTRAINT ops_events_source_check
    CHECK (source IN ('gateway', 'identity', 'commerce', 'cowork', 'autotest', 'support', 'ops', 'broker'));

ALTER TABLE ops_prompts DROP CONSTRAINT ops_prompts_source_check;
ALTER TABLE ops_prompts ADD CONSTRAINT ops_prompts_source_check
    CHECK (source IN ('gateway', 'identity', 'commerce', 'cowork', 'autotest', 'support', 'ops', 'broker'));

-- Registered devices: token stored as sha256 hex (raw tokens never
-- persisted). state mirrors the broker's live connection map; PG keeps the
-- durable truth so a broker restart preserves registry + mailbox.
CREATE TABLE IF NOT EXISTS device_registry (
    device_id   TEXT PRIMARY KEY,
    token_hash  TEXT NOT NULL,
    version     TEXT NOT NULL DEFAULT '',
    capabilities TEXT[] NOT NULL DEFAULT '{}',
    state       TEXT NOT NULL DEFAULT 'offline' CHECK (state IN ('online', 'offline')),
    bound_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Tasks for offline devices: TTL 24h enforced by the existing retention job
-- (issue #10 scope). UNIQUE keeps enqueue idempotent per device+task.
CREATE TABLE IF NOT EXISTS task_mailbox (
    id            BIGSERIAL PRIMARY KEY,
    device_id     TEXT NOT NULL REFERENCES device_registry(device_id) ON DELETE CASCADE,
    task_id       TEXT NOT NULL,
    task_type     TEXT NOT NULL,
    params        JSONB NOT NULL DEFAULT '{}'::jsonb,
    state         TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'dispatched')),
    ttl           TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    dispatched_at TIMESTAMPTZ,
    UNIQUE (device_id, task_id)
);
CREATE INDEX IF NOT EXISTS idx_task_mailbox_device_state ON task_mailbox (device_id, state);

-- Grants for the DML-only app role (idempotent; role absent in dev/test DBs).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fusion_ops_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON device_registry TO fusion_ops_app;
        GRANT SELECT, INSERT, UPDATE, DELETE ON task_mailbox TO fusion_ops_app;
        GRANT USAGE, SELECT ON SEQUENCE task_mailbox_id_seq TO fusion_ops_app;
    END IF;
END $$;
