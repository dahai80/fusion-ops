-- Issue #15: deletion audit for the scheduled cleanup pass (fusion-ops-clean).
-- Every candidate gets one row (deleted / would_delete / kept + reason) so
-- cleanup is reviewable and queryable.

CREATE TABLE IF NOT EXISTS cleanup_audit (
    id       BIGSERIAL PRIMARY KEY,
    ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
    path     TEXT NOT NULL,
    bytes    BIGINT NOT NULL DEFAULT 0,
    decision TEXT NOT NULL CHECK (decision IN ('deleted', 'would_delete', 'kept')),
    reason   TEXT NOT NULL DEFAULT '',
    dry_run  BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_cleanup_audit_ts ON cleanup_audit (ts DESC);
CREATE INDEX IF NOT EXISTS idx_cleanup_audit_decision ON cleanup_audit (decision, ts DESC);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fusion_ops_app') THEN
        GRANT SELECT, INSERT, DELETE ON cleanup_audit TO fusion_ops_app;
        GRANT USAGE, SELECT ON SEQUENCE cleanup_audit_id_seq TO fusion_ops_app;
    END IF;
END $$;
