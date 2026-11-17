-- Authority-gated canonical revisions, quarantine and dirty-window work.
--
-- Additive only (no earlier migration is edited). This file sorts between 003_durable_*
-- and 004_*, so it references nothing created after 001; the canonical-generation columns
-- from 016 are only touched at run time, inside trigger bodies and the decision procedure.
--
-- canonical_actions stays the immutable first-acceptance audit row. The *current* value
-- of an event is canonical_current, which only a matching immutable canonical_revisions
-- row can change; disappearance never deletes or rewrites anything.

CREATE TABLE IF NOT EXISTS dependency_epoch (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch bigint NOT NULL CHECK (epoch >= 0)
);
INSERT INTO dependency_epoch(singleton, epoch) VALUES (true, 0) ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION protect_dependency_epoch() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.singleton AND OLD.singleton AND NEW.epoch = OLD.epoch + 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'dependency epoch only advances by exactly one';
END;
$$;
DROP TRIGGER IF EXISTS dependency_epoch_monotonic ON dependency_epoch;
CREATE TRIGGER dependency_epoch_monotonic BEFORE UPDATE OR DELETE ON dependency_epoch
FOR EACH ROW EXECUTE FUNCTION protect_dependency_epoch();
DROP TRIGGER IF EXISTS dependency_epoch_no_truncate ON dependency_epoch;
CREATE TRIGGER dependency_epoch_no_truncate BEFORE TRUNCATE ON dependency_epoch
FOR EACH STATEMENT EXECUTE FUNCTION protect_dependency_epoch();

CREATE TABLE IF NOT EXISTS canonical_current (
    event_id text PRIMARY KEY,
    network text NOT NULL,
    trace_id text NOT NULL,
    action_id text NOT NULL,
    accepted_generation bigint CHECK (accepted_generation > 0),
    disposition text NOT NULL CHECK (disposition IN ('active', 'retracted')),
    semantic_digest text NOT NULL,
    action jsonb NOT NULL,
    source_revision bigint CHECK (source_revision > 0),
    eligibility_suppressed boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS canonical_current_trace_idx ON canonical_current(network, trace_id);

CREATE TABLE IF NOT EXISTS canonical_revisions (
    event_id text NOT NULL,
    accepted_generation bigint NOT NULL CHECK (accepted_generation > 0),
    change_kind text NOT NULL CHECK (change_kind IN ('revision', 'retraction')),
    semantic_digest text NOT NULL,
    action jsonb NOT NULL,
    previous_generation bigint CHECK (previous_generation > 0),
    previous_digest text,
    source_revision bigint CHECK (source_revision > 0),
    reason text NOT NULL,
    contract text NOT NULL CHECK (contract IN ('source_monotonic_revision', 'owner_resolution')),
    authority_id text NOT NULL CHECK (length(btrim(authority_id)) > 0),
    authority_grant jsonb NOT NULL,
    range_id uuid REFERENCES source_ranges(range_id),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, accepted_generation),
    CHECK (previous_generation IS NULL OR previous_generation < accepted_generation)
);
CREATE UNIQUE INDEX IF NOT EXISTS canonical_revisions_generation_key
    ON canonical_revisions(accepted_generation);

CREATE OR REPLACE FUNCTION reject_revision_history_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'canonical revision history is immutable';
END;
$$;
DROP TRIGGER IF EXISTS canonical_revisions_immutable ON canonical_revisions;
CREATE TRIGGER canonical_revisions_immutable BEFORE UPDATE OR DELETE ON canonical_revisions
FOR EACH ROW EXECUTE FUNCTION reject_revision_history_change();
DROP TRIGGER IF EXISTS canonical_revisions_no_truncate ON canonical_revisions;
CREATE TRIGGER canonical_revisions_no_truncate BEFORE TRUNCATE ON canonical_revisions
FOR EACH STATEMENT EXECUTE FUNCTION reject_revision_history_change();

-- A current row is either the unchanged first acceptance or backed by a revision row.
CREATE OR REPLACE FUNCTION guard_canonical_current() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE base record;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'canonical current data is never deleted';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF (NEW.event_id, NEW.network, NEW.trace_id, NEW.action_id)
           IS DISTINCT FROM (OLD.event_id, OLD.network, OLD.trace_id, OLD.action_id) THEN
            RAISE EXCEPTION 'canonical current identity is immutable';
        END IF;
        IF (NEW.accepted_generation, NEW.disposition, NEW.semantic_digest, NEW.action,
            NEW.source_revision)
           IS NOT DISTINCT FROM
           (OLD.accepted_generation, OLD.disposition, OLD.semantic_digest, OLD.action,
            OLD.source_revision) THEN
            RETURN NEW;      -- only the eligibility flag changed
        END IF;
        IF NEW.accepted_generation IS NULL OR (OLD.accepted_generation IS NOT NULL
           AND NEW.accepted_generation <= OLD.accepted_generation) THEN
            RAISE EXCEPTION 'canonical current can only advance to a newer generation';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM canonical_revisions r
            WHERE r.event_id = NEW.event_id AND r.accepted_generation = NEW.accepted_generation
              AND r.semantic_digest = NEW.semantic_digest AND r.action = NEW.action
              AND r.previous_generation IS NOT DISTINCT FROM OLD.accepted_generation
              AND r.change_kind = CASE NEW.disposition
                  WHEN 'retracted' THEN 'retraction' ELSE 'revision' END
              AND (NEW.disposition = 'retracted'
                   OR r.source_revision IS NOT DISTINCT FROM NEW.source_revision)
        ) THEN
            RAISE EXCEPTION 'canonical current change requires its immutable revision';
        END IF;
        RETURN NEW;
    END IF;
    -- INSERT: an untouched first acceptance, or an addition backed by a revision.
    SELECT accepted_generation AS generation, semantic_digest AS digest, action
        INTO base FROM canonical_actions WHERE event_id = NEW.event_id;
    IF FOUND THEN
        IF NEW.disposition <> 'active'
           OR NEW.accepted_generation IS DISTINCT FROM base.generation
           OR NEW.semantic_digest <> base.digest OR NEW.action <> base.action THEN
            RAISE EXCEPTION 'canonical current must start equal to the first acceptance';
        END IF;
    ELSIF NOT EXISTS (
        SELECT 1 FROM canonical_revisions r
        WHERE r.event_id = NEW.event_id AND r.accepted_generation = NEW.accepted_generation
          AND r.semantic_digest = NEW.semantic_digest AND r.action = NEW.action
          AND r.change_kind = 'revision' AND r.previous_generation IS NULL
    ) THEN
        RAISE EXCEPTION 'canonical current addition requires its immutable revision';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS canonical_current_guard ON canonical_current;
CREATE TRIGGER canonical_current_guard BEFORE INSERT OR UPDATE OR DELETE ON canonical_current
FOR EACH ROW EXECUTE FUNCTION guard_canonical_current();
DROP TRIGGER IF EXISTS canonical_current_no_truncate ON canonical_current;
CREATE TRIGGER canonical_current_no_truncate BEFORE TRUNCATE ON canonical_current
FOR EACH STATEMENT EXECUTE FUNCTION reject_revision_history_change();

-- Durable disposition of a change that was not admitted. Opening one never changes
-- the accepted value; it only suppresses eligibility and dirties the affected windows.
CREATE TABLE IF NOT EXISTS revision_quarantine (
    quarantine_id bigserial PRIMARY KEY,
    subject text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('event', 'trace')),
    network text NOT NULL,
    trace_id text NOT NULL,
    reason text NOT NULL CHECK (reason IN (
        'unauthorized_change', 'unauthorized_disappearance', 'unauthorized_trace_replacement',
        'incomplete_trace', 'stale_authorization', 'stale_revision',
        'equal_revision_conflict', 'superseded_digest', 'no_ordered_baseline')),
    content_digest text NOT NULL,
    range_id uuid NOT NULL REFERENCES source_ranges(range_id),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    resolved_generation bigint CHECK (resolved_generation > 0),
    CHECK ((status = 'resolved') = (resolved_at IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS revision_quarantine_one_open
    ON revision_quarantine(subject) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS revision_quarantine_trace_idx
    ON revision_quarantine(network, trace_id) WHERE status = 'open';

CREATE OR REPLACE FUNCTION protect_revision_quarantine() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR (TG_OP = 'UPDATE' AND OLD.status = 'resolved') THEN
        RAISE EXCEPTION 'resolved quarantine evidence is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS revision_quarantine_guard ON revision_quarantine;
CREATE TRIGGER revision_quarantine_guard BEFORE UPDATE OR DELETE ON revision_quarantine
FOR EACH ROW EXECUTE FUNCTION protect_revision_quarantine();

-- Dirty-window work from old/new contribution identities. One row per window and role;
-- 'baseline_dependents' covers the later window starts whose trailing baseline holds it.
CREATE TABLE IF NOT EXISTS dirty_windows (
    network text NOT NULL,
    token text NOT NULL,
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    role text NOT NULL CHECK (role IN ('target', 'baseline_dependents')),
    window_end bigint NOT NULL,
    covers_start bigint NOT NULL,
    covers_end bigint NOT NULL,
    dirty_generation bigint NOT NULL DEFAULT 1 CHECK (dirty_generation > 0),
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch > 0),
    reason text NOT NULL,
    cause_event_id text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, token, window_start, role),
    CHECK (window_end = window_start + 300 AND covers_start < covers_end)
);

-- Active key set per trace: current rows, plus first-acceptance rows never yet revised.
CREATE OR REPLACE VIEW trace_keysets AS
SELECT network, trace_id,
       jsonb_agg(action_id ORDER BY action_id COLLATE "C") AS action_ids
FROM (
    SELECT network, trace_id, action_id FROM canonical_current WHERE disposition = 'active'
    UNION ALL
    SELECT ca.action->'identity'->>'network', ca.action->'identity'->>'trace_id',
           ca.action->'identity'->>'action_id'
    FROM canonical_actions ca
    WHERE NOT EXISTS (SELECT 1 FROM canonical_current cc WHERE cc.event_id = ca.event_id)
) members
GROUP BY network, trace_id;
