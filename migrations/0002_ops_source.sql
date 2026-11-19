-- Issue #4: blackbox health probe agent appends health_probe events with
-- source='ops'. Extend the source CHECK constraint accordingly.

ALTER TABLE ops_events DROP CONSTRAINT ops_events_source_check;
ALTER TABLE ops_events ADD CONSTRAINT ops_events_source_check
    CHECK (source IN ('gateway', 'identity', 'commerce', 'cowork', 'autotest', 'support', 'ops'));
