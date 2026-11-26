-- Migration 0006 (issue #17): two-stage confirmation + quarantine.
-- The cleanup pass now quarantines confirmed candidates instead of
-- deleting them outright; add the 'quarantined' audit decision.

ALTER TABLE cleanup_audit DROP CONSTRAINT cleanup_audit_decision_check;
ALTER TABLE cleanup_audit ADD CONSTRAINT cleanup_audit_decision_check
    CHECK (decision IN ('deleted', 'would_delete', 'kept', 'quarantined'));
