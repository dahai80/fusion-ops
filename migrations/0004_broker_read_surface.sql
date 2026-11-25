-- Issue #11: broker read surface for the site console + task bridge.
-- Widens the task state machine, adds device description fields reported at
-- hello, stores the last heartbeat health snapshot (service list for the
-- console device table) and appends an event history for task progress.

ALTER TABLE task_mailbox DROP CONSTRAINT task_mailbox_state_check;
ALTER TABLE task_mailbox ADD CONSTRAINT task_mailbox_state_check
    CHECK (state IN ('queued', 'dispatched', 'running', 'awaiting_local_confirm', 'completed', 'failed', 'expired'));
ALTER TABLE task_mailbox ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE task_mailbox ADD COLUMN steps JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE task_mailbox ADD COLUMN error_code TEXT NOT NULL DEFAULT '';
ALTER TABLE task_mailbox ADD COLUMN error_message TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_task_mailbox_updated ON task_mailbox (updated_at DESC);

ALTER TABLE device_registry ADD COLUMN hostname TEXT NOT NULL DEFAULT '';
ALTER TABLE device_registry ADD COLUMN platform TEXT NOT NULL DEFAULT '';
ALTER TABLE device_registry ADD COLUMN connected_since TIMESTAMPTZ;
ALTER TABLE device_registry ADD COLUMN last_health JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE device_registry ADD COLUMN tags TEXT[] NOT NULL DEFAULT '{}';

-- Best-effort progress history per task (device task.event uplinks).
CREATE TABLE IF NOT EXISTS task_events (
    id      BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    event   TEXT NOT NULL,
    detail  TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events (task_id, id);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fusion_ops_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON task_events TO fusion_ops_app;
        GRANT USAGE, SELECT ON SEQUENCE task_events_id_seq TO fusion_ops_app;
    END IF;
END $$;
