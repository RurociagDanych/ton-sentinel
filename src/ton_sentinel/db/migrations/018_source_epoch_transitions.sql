-- Audited source-epoch transitions (source upgrades).
--
-- Additive only. A source upgrade never compares revisions across providers: it opens a new
-- active source epoch under the shared lease, records the approved bounded before/after audit
-- window and advances the dependency epoch so frozen repair work from the old epoch cannot
-- promote. Rows are immutable audit evidence.
CREATE TABLE IF NOT EXISTS source_epoch_transitions (
    transition_id bigserial PRIMARY KEY,
    previous_epoch jsonb NOT NULL,
    new_epoch jsonb NOT NULL,
    authority_id text NOT NULL CHECK (length(btrim(authority_id)) > 0),
    reason text NOT NULL CHECK (length(btrim(reason)) > 0),
    audit_start bigint NOT NULL CHECK (audit_start >= 0 AND audit_start % 300 = 0),
    audit_end bigint NOT NULL CHECK (audit_end % 300 = 0),
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch > 0),
    lease_owner text NOT NULL,
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (audit_start < audit_end),
    CHECK (previous_epoch <> new_epoch)
);

CREATE OR REPLACE FUNCTION reject_source_epoch_transition_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'source epoch transitions are immutable';
END;
$$;
DROP TRIGGER IF EXISTS source_epoch_transitions_immutable ON source_epoch_transitions;
CREATE TRIGGER source_epoch_transitions_immutable BEFORE UPDATE OR DELETE ON source_epoch_transitions
FOR EACH ROW EXECUTE FUNCTION reject_source_epoch_transition_change();
DROP TRIGGER IF EXISTS source_epoch_transitions_no_truncate ON source_epoch_transitions;
CREATE TRIGGER source_epoch_transitions_no_truncate BEFORE TRUNCATE ON source_epoch_transitions
FOR EACH STATEMENT EXECUTE FUNCTION reject_source_epoch_transition_change();
