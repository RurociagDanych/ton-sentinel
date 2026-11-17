-- 30-day derived-history retention, late-delivery quarantine
-- and the storage low-water pause of ingestion. Additive only; every statement replays
-- safely after 001..024.
--
-- Earlier files keep their tables. Inline CHECKs of 003 (revision_quarantine.reason) and 024
-- (retention_runs.kind) are only widened here; replaying 003/024 cannot re-add the narrower
-- CHECK because their CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so no
-- supersession record is needed. The 021/022 "never delete" triggers of the derived audit
-- tables are narrowed to UPDATE and replaced, for DELETE, by a gate that admits a delete only
-- inside the transaction of an open derived retention run whose horizon lies above the row's
-- window. Replaying 021/022 re-creates their stricter triggers; this file, replayed after
-- them, narrows them again (same pattern as 024 for the canonical tables).

-- 1. Derived retention runs share the 024 audit table.
ALTER TABLE retention_runs DROP CONSTRAINT IF EXISTS retention_runs_kind_check;
ALTER TABLE retention_runs ADD CONSTRAINT retention_runs_kind_check
    CHECK (kind IN ('payload', 'guards', 'derived'));

-- 2. DELETE gate for derived history (features, pinned baselines, results, alerts).
CREATE OR REPLACE FUNCTION derived_retention_delete_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM retention_runs r WHERE r.txid = txid_current()
                   AND r.kind = 'derived' AND r.network = OLD.network
                   AND r.counts_after IS NULL AND OLD.window_start < r.to_window) THEN
        RAISE EXCEPTION '% rows are immutable: deleted only by an open derived retention '
            'run covering window %', TG_TABLE_NAME, OLD.window_start;
    END IF;
    RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS baseline_inputs_immutable ON baseline_inputs;
CREATE TRIGGER baseline_inputs_immutable BEFORE UPDATE ON baseline_inputs
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS baseline_inputs_retention_delete ON baseline_inputs;
CREATE TRIGGER baseline_inputs_retention_delete BEFORE DELETE ON baseline_inputs
FOR EACH ROW EXECUTE FUNCTION derived_retention_delete_gate();

DROP TRIGGER IF EXISTS detector_results_immutable ON detector_results;
CREATE TRIGGER detector_results_immutable BEFORE UPDATE ON detector_results
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS detector_results_retention_delete ON detector_results;
CREATE TRIGGER detector_results_retention_delete BEFORE DELETE ON detector_results
FOR EACH ROW EXECUTE FUNCTION derived_retention_delete_gate();

DROP TRIGGER IF EXISTS alert_revisions_immutable ON alert_revisions;
CREATE TRIGGER alert_revisions_immutable BEFORE UPDATE ON alert_revisions
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS alert_revisions_retention_delete ON alert_revisions;
CREATE TRIGGER alert_revisions_retention_delete BEFORE DELETE ON alert_revisions
FOR EACH ROW EXECUTE FUNCTION derived_retention_delete_gate();

DROP TRIGGER IF EXISTS detector_current_guard ON detector_current;
CREATE TRIGGER detector_current_guard BEFORE INSERT OR UPDATE ON detector_current
FOR EACH ROW EXECUTE FUNCTION guard_detector_current();
DROP TRIGGER IF EXISTS detector_current_retention_delete ON detector_current;
CREATE TRIGGER detector_current_retention_delete BEFORE DELETE ON detector_current
FOR EACH ROW EXECUTE FUNCTION derived_retention_delete_gate();

-- 3. Expiring features is not a feature change: retained scores keep their pinned input and
--    stay as they are (non-recomputable, see the insert gate below). Every other delete of
--    served features (a repair replacement) still invalidates exactly as in 022.
CREATE OR REPLACE FUNCTION invalidate_on_deleted_features() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT DISTINCT c.network, c.window_start FROM changed_features c
             WHERE NOT EXISTS (
                 SELECT 1 FROM retention_runs x WHERE x.txid = txid_current()
                 AND x.kind = 'derived' AND x.network = c.network
                 AND x.counts_after IS NULL AND c.window_start < x.to_window) LOOP
        PERFORM invalidate_scores(r.network, NULL, r.window_start, r.window_start,
                                  'features_replaced');
    END LOOP;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS served_features_invalidate_delete ON served_features;
CREATE TRIGGER served_features_invalidate_delete AFTER DELETE ON served_features
REFERENCING OLD TABLE AS changed_features
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_on_deleted_features();

-- 4. INSERT gate: expired derived history never re-enters, and no baseline whose leading
--    context lies below the retained derived horizon is pinned (a score paused across a
--    retention run, or a rescore of a window whose context expired, is refused).
CREATE OR REPLACE FUNCTION derived_retention_insert_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    lead bigint;
BEGIN
    lead := NEW.window_start;
    IF TG_TABLE_NAME = 'baseline_inputs' THEN
        lead := greatest(0, NEW.window_start - NEW.lookback_seconds);
    END IF;
    IF EXISTS (SELECT 1 FROM retained_context_horizons h WHERE h.network = NEW.network
               AND h.data_class = 'derived_features' AND lead < h.retained_from) THEN
        RAISE EXCEPTION 'derived context of window % on % is no longer retained: '
            'backfill only', NEW.window_start, NEW.network;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS served_features_retention_insert ON served_features;
CREATE TRIGGER served_features_retention_insert BEFORE INSERT ON served_features
FOR EACH ROW EXECUTE FUNCTION derived_retention_insert_gate();
DROP TRIGGER IF EXISTS baseline_inputs_retention_insert ON baseline_inputs;
CREATE TRIGGER baseline_inputs_retention_insert BEFORE INSERT ON baseline_inputs
FOR EACH ROW EXECUTE FUNCTION derived_retention_insert_gate();

-- 5. Late source deliveries refused by retention: quarantined as expired/unrepairable,
--    never inserted as new canonical rows (spec section 9: backfill planning only).
CREATE TABLE IF NOT EXISTS delivery_quarantine (
    range_id uuid NOT NULL REFERENCES source_ranges(range_id),
    event_id text NOT NULL,
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    reasons jsonb NOT NULL CHECK (jsonb_typeof(reasons) = 'array'
                                  AND jsonb_array_length(reasons) > 0),
    semantic_digest text NOT NULL,
    proposed_action jsonb NOT NULL CHECK (jsonb_typeof(proposed_action) = 'object'),
    payload_from bigint NOT NULL CHECK (payload_from >= 0),
    guard_retired_before bigint NOT NULL CHECK (guard_retired_before >= 0),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    quarantined_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (range_id, event_id)
);
CREATE INDEX IF NOT EXISTS delivery_quarantine_event ON delivery_quarantine (event_id);

CREATE OR REPLACE FUNCTION reject_delivery_quarantine_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'delivery quarantine rows are immutable audit records';
END $$;
DROP TRIGGER IF EXISTS delivery_quarantine_immutable ON delivery_quarantine;
CREATE TRIGGER delivery_quarantine_immutable BEFORE UPDATE OR DELETE ON delivery_quarantine
FOR EACH ROW EXECUTE FUNCTION reject_delivery_quarantine_change();
DROP TRIGGER IF EXISTS delivery_quarantine_no_truncate ON delivery_quarantine;
CREATE TRIGGER delivery_quarantine_no_truncate BEFORE TRUNCATE ON delivery_quarantine
FOR EACH STATEMENT EXECUTE FUNCTION reject_delivery_quarantine_change();

-- 6. A revision whose new contribution window has expired is quarantined (not raised).
ALTER TABLE revision_quarantine DROP CONSTRAINT IF EXISTS revision_quarantine_reason_check;
ALTER TABLE revision_quarantine ADD CONSTRAINT revision_quarantine_reason_check CHECK (reason IN (
    'unauthorized_change', 'unauthorized_disappearance', 'unauthorized_trace_replacement',
    'incomplete_trace', 'stale_authorization', 'stale_revision', 'equal_revision_conflict',
    'superseded_digest', 'no_ordered_baseline', 'expired_identity'));

-- 7. Storage low-water pause of ingestion (spec section 9: growing holds or disk low water
--    pause ingestion; evidence is never deleted to make room). One active pause at a time;
--    a pause is only released, never changed or deleted.
CREATE TABLE IF NOT EXISTS ingestion_pauses (
    pause_id bigserial PRIMARY KEY,
    reason text NOT NULL CHECK (reason IN ('low_water')),
    used_bytes bigint NOT NULL CHECK (used_bytes >= 0),
    budget_bytes bigint NOT NULL CHECK (budget_bytes > 0),
    min_free_percent integer NOT NULL CHECK (min_free_percent BETWEEN 1 AND 99),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    paused_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    released_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS ingestion_pauses_one_active
    ON ingestion_pauses ((true)) WHERE released_at IS NULL;

CREATE OR REPLACE FUNCTION ingestion_pauses_release_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.released_at IS NULL AND NEW.released_at IS NOT NULL
       AND (NEW.pause_id, NEW.reason, NEW.used_bytes, NEW.budget_bytes, NEW.min_free_percent,
            NEW.lease_owner, NEW.lease_fence, NEW.paused_at)
           IS NOT DISTINCT FROM
           (OLD.pause_id, OLD.reason, OLD.used_bytes, OLD.budget_bytes, OLD.min_free_percent,
            OLD.lease_owner, OLD.lease_fence, OLD.paused_at) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'ingestion pauses are only released, never changed or deleted';
END $$;
DROP TRIGGER IF EXISTS ingestion_pauses_release_only ON ingestion_pauses;
CREATE TRIGGER ingestion_pauses_release_only BEFORE UPDATE OR DELETE ON ingestion_pauses
FOR EACH ROW EXECUTE FUNCTION ingestion_pauses_release_only();
DROP TRIGGER IF EXISTS ingestion_pauses_no_truncate ON ingestion_pauses;
CREATE TRIGGER ingestion_pauses_no_truncate BEFORE TRUNCATE ON ingestion_pauses
FOR EACH STATEMENT EXECUTE FUNCTION ingestion_pauses_release_only();

-- 8. A range is accepted with every walked key accounted for exactly once: either a member
--    (canonical row, outbox intent) or durably quarantined above as an expired identity.
--    Same body as 004 otherwise; replaying 004 re-creates its stricter function and this
--    file, replayed after it, replaces it again.
CREATE OR REPLACE FUNCTION enforce_source_range_acceptance() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected_keys jsonb; actual_keys jsonb;
BEGIN
    IF NEW.status<>'accepted' THEN
        IF TG_OP='UPDATE' AND OLD.status='accepted' THEN
            RAISE EXCEPTION 'accepted range status is terminal';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='INSERT' OR NOT NEW.corroborated THEN
        RAISE EXCEPTION 'accepted range requires atomic corroborated promotion';
    END IF;
    SELECT w.key_set INTO expected_keys
      FROM corroboration_groups g JOIN corroboration_walks w
        ON w.group_id=g.group_id AND w.range_id=g.accepted_range_id
      WHERE g.accepted_range_id=NEW.range_id
        AND g.status IN ('corroborated','accepted') AND w.complete;
    SELECT COALESCE(jsonb_agg(event_id ORDER BY event_id COLLATE "C"),'[]'::jsonb) INTO actual_keys
      FROM (SELECT event_id FROM scan_membership WHERE range_id=NEW.range_id
            UNION ALL
            SELECT event_id FROM delivery_quarantine WHERE range_id=NEW.range_id) k;
    IF expected_keys IS NULL OR expected_keys<>actual_keys
       OR NOT EXISTS (SELECT 1 FROM active_source_epoch WHERE singleton AND epoch=NEW.epoch)
       OR NOT EXISTS (SELECT 1 FROM ingest_state WHERE singleton AND cursor>=NEW.end_exclusive)
       OR EXISTS (
           SELECT 1 FROM scan_membership m JOIN canonical_actions c USING(event_id)
           LEFT JOIN outbox o USING(event_id) WHERE m.range_id=NEW.range_id
             AND (o.event_id IS NULL OR o.payload<>c.action)
       ) THEN
        RAISE EXCEPTION 'accepted range requires atomic canonical, membership, outbox and cursor effects';
    END IF;
    RETURN NEW;
END;
$$;
