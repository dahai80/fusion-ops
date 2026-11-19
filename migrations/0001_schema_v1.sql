-- fusion_ops schema v1 (flywheel Loop A, issue #1)
-- Sources: architecture/fusion-flywheel-1119.md §2, fusion-db-conventions-1030.md DB-9.

CREATE TABLE IF NOT EXISTS ops_events (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL CHECK (source IN ('gateway', 'identity', 'commerce', 'cowork', 'autotest', 'support')),
    tenant_hash TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    latency_ms INTEGER,
    status INTEGER,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_ops_events_source_ts ON ops_events (source, ts);
CREATE INDEX IF NOT EXISTS idx_ops_events_event_type_ts ON ops_events (event_type, ts);

CREATE TABLE IF NOT EXISTS ops_prompts (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL CHECK (source IN ('gateway', 'identity', 'commerce', 'cowork', 'autotest', 'support')),
    prompt_hash TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '',
    pii_classes TEXT[] NOT NULL DEFAULT '{}',
    prompt_text TEXT,
    model TEXT NOT NULL DEFAULT '',
    latency_ms INTEGER,
    outcome TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS mined_issues (
    id BIGSERIAL PRIMARY KEY,
    cluster_key TEXT NOT NULL UNIQUE,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    sample_event_ids BIGINT[] NOT NULL DEFAULT '{}',
    last_event_id BIGINT NOT NULL DEFAULT 0,
    severity TEXT NOT NULL DEFAULT 'normal',
    issue_url TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'new' CHECK (state IN ('new', 'open', 'closed'))
);

CREATE TABLE IF NOT EXISTS feedback (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL UNIQUE,
    class TEXT NOT NULL DEFAULT '',
    sentiment TEXT NOT NULL DEFAULT '',
    urgency TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'new',
    sla_deadline TIMESTAMPTZ
);

-- Emitter drop-counter audit (issue #2 / fusion-core#56): backpressure drops
-- land here per source+day; monthly rollup writes an ops_events row and marks
-- rows aggregated so silent loss stays visible.
CREATE TABLE IF NOT EXISTS drop_counters (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    bucket_day DATE NOT NULL,
    dropped BIGINT NOT NULL DEFAULT 0,
    aggregated BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (source, bucket_day)
);
