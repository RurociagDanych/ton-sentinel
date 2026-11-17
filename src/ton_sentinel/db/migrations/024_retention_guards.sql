-- Bounded canonical-payload retention and identity-guard retirement.
-- Additive only; every statement replays safely. -- 
--
-- Earlier files keep their tables; this file narrows the 003 "never delete" triggers of
-- canonical_current / canonical_revisions to UPDATE and adds a DELETE gate instead: a canonical
-- row may be deleted only inside a transaction that already recorded an open payload
-- retention run (same transaction id) whose new horizon lies above the row's window. Replaying
-- 003 re-creates its stricter triggers; this file, replayed after it, narrows them again.

-- 1. Audit of every retention transition, written before the first delete of its transaction.
CREATE TABLE IF NOT EXISTS retention_runs (
    run_id text PRIMARY KEY CHECK (run_id ~ '^sha256:[0-9a-f]{64}$'),
    kind text NOT NULL CHECK (kind IN ('payload', 'guards')),
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    now_at bigint NOT NULL CHECK (now_at >= 0),
    cutoff bigint NOT NULL CHECK (cutoff >= 0 AND cutoff % 300 = 0),
    from_window bigint NOT NULL CHECK (from_window >= 0 AND from_window % 300 = 0),
    to_window bigint NOT NULL CHECK (to_window % 300 = 0 AND to_window > from_window),
    windows jsonb NOT NULL CHECK (jsonb_typeof(windows) = 'array'),
    plan_digest text NOT NULL CHECK (plan_digest ~ '^sha256:[0-9a-f]{64}$'),
    counts_before jsonb NOT NULL CHECK (jsonb_typeof(counts_before) = 'object'),
    counts_after jsonb CHECK (counts_after IS NULL OR jsonb_typeof(counts_after) = 'object'),
    deleted jsonb CHECK (deleted IS NULL OR jsonb_typeof(deleted) = 'object'),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    txid bigint NOT NULL DEFAULT txid_current(),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((counts_after IS NULL) = (deleted IS NULL))
);

CREATE OR REPLACE FUNCTION retention_runs_audit() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    -- The only change: the same transaction closes its own open run with its result counts.
    IF TG_OP = 'UPDATE' AND OLD.counts_after IS NULL AND NEW.counts_after IS NOT NULL
       AND OLD.txid = txid_current()
       AND (NEW.run_id, NEW.kind, NEW.network, NEW.now_at, NEW.cutoff, NEW.from_window,
            NEW.to_window, NEW.windows, NEW.plan_digest, NEW.counts_before, NEW.lease_owner,
            NEW.lease_fence, NEW.txid, NEW.recorded_at)
           IS NOT DISTINCT FROM
           (OLD.run_id, OLD.kind, OLD.network, OLD.now_at, OLD.cutoff, OLD.from_window,
            OLD.to_window, OLD.windows, OLD.plan_digest, OLD.counts_before, OLD.lease_owner,
            OLD.lease_fence, OLD.txid, OLD.recorded_at) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'retention runs are immutable audit records';
END $$;
DROP TRIGGER IF EXISTS retention_runs_audit ON retention_runs;
CREATE TRIGGER retention_runs_audit BEFORE UPDATE OR DELETE ON retention_runs
FOR EACH ROW EXECUTE FUNCTION retention_runs_audit();
DROP TRIGGER IF EXISTS retention_runs_no_truncate ON retention_runs;
CREATE TRIGGER retention_runs_no_truncate BEFORE TRUNCATE ON retention_runs
FOR EACH STATEMENT EXECUTE FUNCTION retention_runs_audit();

-- 2. Holds of admitted work: retention never deletes inside an active hold's span.
CREATE TABLE IF NOT EXISTS retention_holds (
    admission_digest text NOT NULL CHECK (admission_digest ~ '^sha256:[0-9a-f]{64}$'),
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    hold_from bigint NOT NULL CHECK (hold_from >= 0 AND hold_from % 300 = 0),
    hold_until bigint NOT NULL CHECK (hold_until % 300 = 0 AND hold_until > hold_from),
    work_windows jsonb NOT NULL CHECK (jsonb_typeof(work_windows) = 'array'),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    released_at timestamptz,
    PRIMARY KEY (admission_digest, network)
);
CREATE INDEX IF NOT EXISTS retention_holds_active
    ON retention_holds (network, hold_from) WHERE released_at IS NULL;

CREATE OR REPLACE FUNCTION retention_holds_release_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.released_at IS NULL AND NEW.released_at IS NOT NULL
       AND (NEW.admission_digest, NEW.network, NEW.hold_from, NEW.hold_until, NEW.work_windows,
            NEW.lease_owner, NEW.lease_fence, NEW.created_at)
           IS NOT DISTINCT FROM
           (OLD.admission_digest, OLD.network, OLD.hold_from, OLD.hold_until, OLD.work_windows,
            OLD.lease_owner, OLD.lease_fence, OLD.created_at) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'retention holds are only released, never changed or deleted';
END $$;
DROP TRIGGER IF EXISTS retention_holds_release_only ON retention_holds;
CREATE TRIGGER retention_holds_release_only BEFORE UPDATE OR DELETE ON retention_holds
FOR EACH ROW EXECUTE FUNCTION retention_holds_release_only();
DROP TRIGGER IF EXISTS retention_holds_no_truncate ON retention_holds;
CREATE TRIGGER retention_holds_no_truncate BEFORE TRUNCATE ON retention_holds
FOR EACH STATEMENT EXECUTE FUNCTION retention_holds_release_only();

-- 3. Compact identity guards left when an event's payload expires: accepted
--    generation, current digest and disposition, superseded digests and every original
--    contribution window. Written only by a payload run; deleted only by a guard run.
CREATE TABLE IF NOT EXISTS event_identity_guards (
    event_id text PRIMARY KEY,
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    trace_id text NOT NULL,
    action_id text NOT NULL,
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    last_window bigint NOT NULL CHECK (last_window >= window_start AND last_window % 300 = 0),
    windows jsonb NOT NULL CHECK (jsonb_typeof(windows) = 'array'
                                  AND jsonb_array_length(windows) > 0),
    accepted_generation bigint CHECK (accepted_generation > 0),
    disposition text NOT NULL CHECK (disposition IN ('active', 'retracted')),
    eligible boolean NOT NULL,
    semantic_digest text NOT NULL,
    superseded_digests jsonb NOT NULL CHECK (jsonb_typeof(superseded_digests) = 'array'),
    source_revision bigint,
    payload_run text NOT NULL REFERENCES retention_runs(run_id),
    guarded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS event_identity_guards_window
    ON event_identity_guards (network, window_start);
CREATE INDEX IF NOT EXISTS event_identity_guards_trace
    ON event_identity_guards (network, trace_id);

-- 4. Retired-guard boundary per network: deliveries for windows before it are never new.
CREATE TABLE IF NOT EXISTS identity_guard_horizons (
    network text PRIMARY KEY CHECK (network IN ('mainnet', 'testnet')),
    retired_before bigint NOT NULL CHECK (retired_before >= 0 AND retired_before % 300 = 0),
    advanced_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION identity_guard_horizon_forward() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.network = OLD.network
       AND NEW.retired_before >= OLD.retired_before
       AND EXISTS (SELECT 1 FROM retention_runs r WHERE r.txid = txid_current()
                   AND r.kind = 'guards' AND r.network = NEW.network
                   AND r.counts_after IS NULL AND r.to_window = NEW.retired_before) THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' AND EXISTS (
            SELECT 1 FROM retention_runs r WHERE r.txid = txid_current()
            AND r.kind = 'guards' AND r.network = NEW.network
            AND r.counts_after IS NULL AND r.to_window = NEW.retired_before) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'identity guard boundary only moves forward inside a guard retention run';
END $$;
DROP TRIGGER IF EXISTS identity_guard_horizon_forward ON identity_guard_horizons;
CREATE TRIGGER identity_guard_horizon_forward
BEFORE INSERT OR UPDATE OR DELETE ON identity_guard_horizons
FOR EACH ROW EXECUTE FUNCTION identity_guard_horizon_forward();

-- 5. Window and network of a canonical payload row (action-end clock).
CREATE OR REPLACE FUNCTION canonical_payload_window(action jsonb) RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
    SELECT (action->>'action_end')::bigint / 300 * 300
$$;

-- 6. DELETE gate: canonical payload rows and guards leave only inside their open run.
CREATE OR REPLACE FUNCTION retention_delete_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE run_kind text; net text; win bigint;
BEGIN
    IF TG_TABLE_NAME = 'event_identity_guards' THEN
        run_kind := 'guards'; net := OLD.network; win := OLD.last_window;
    ELSE
        run_kind := 'payload';
        net := OLD.action->'identity'->>'network';
        win := canonical_payload_window(OLD.action);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM retention_runs r WHERE r.txid = txid_current()
                   AND r.kind = run_kind AND r.network = net AND r.counts_after IS NULL
                   AND win < r.to_window) THEN
        RAISE EXCEPTION '% rows are deleted only by an open % retention run covering window %',
            TG_TABLE_NAME, run_kind, win;
    END IF;
    RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS canonical_current_guard ON canonical_current;
CREATE TRIGGER canonical_current_guard BEFORE INSERT OR UPDATE ON canonical_current
FOR EACH ROW EXECUTE FUNCTION guard_canonical_current();
DROP TRIGGER IF EXISTS canonical_current_retention_delete ON canonical_current;
CREATE TRIGGER canonical_current_retention_delete BEFORE DELETE ON canonical_current
FOR EACH ROW EXECUTE FUNCTION retention_delete_gate();

DROP TRIGGER IF EXISTS canonical_revisions_immutable ON canonical_revisions;
CREATE TRIGGER canonical_revisions_immutable BEFORE UPDATE ON canonical_revisions
FOR EACH ROW EXECUTE FUNCTION reject_revision_history_change();
DROP TRIGGER IF EXISTS canonical_revisions_retention_delete ON canonical_revisions;
CREATE TRIGGER canonical_revisions_retention_delete BEFORE DELETE ON canonical_revisions
FOR EACH ROW EXECUTE FUNCTION retention_delete_gate();

DROP TRIGGER IF EXISTS canonical_actions_retention_delete ON canonical_actions;
CREATE TRIGGER canonical_actions_retention_delete BEFORE DELETE ON canonical_actions
FOR EACH ROW EXECUTE FUNCTION retention_delete_gate();
DROP TRIGGER IF EXISTS canonical_actions_no_truncate ON canonical_actions;
CREATE TRIGGER canonical_actions_no_truncate BEFORE TRUNCATE ON canonical_actions
FOR EACH STATEMENT EXECUTE FUNCTION reject_revision_history_change();

DROP TRIGGER IF EXISTS event_identity_guards_retention_delete ON event_identity_guards;
CREATE TRIGGER event_identity_guards_retention_delete BEFORE DELETE ON event_identity_guards
FOR EACH ROW EXECUTE FUNCTION retention_delete_gate();

CREATE OR REPLACE FUNCTION event_identity_guards_write_once() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' AND EXISTS (
            SELECT 1 FROM retention_runs r WHERE r.run_id = NEW.payload_run
            AND r.txid = txid_current() AND r.kind = 'payload' AND r.network = NEW.network
            AND r.counts_after IS NULL AND NEW.last_window < r.to_window) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'identity guards are written once, by their payload retention run';
END $$;
DROP TRIGGER IF EXISTS event_identity_guards_write_once ON event_identity_guards;
CREATE TRIGGER event_identity_guards_write_once BEFORE INSERT OR UPDATE ON event_identity_guards
FOR EACH ROW EXECUTE FUNCTION event_identity_guards_write_once();
DROP TRIGGER IF EXISTS event_identity_guards_no_truncate ON event_identity_guards;
CREATE TRIGGER event_identity_guards_no_truncate BEFORE TRUNCATE ON event_identity_guards
FOR EACH STATEMENT EXECUTE FUNCTION reject_revision_history_change();

-- 7. INSERT gate: an expired or guarded identity never re-enters as a new canonical row.
CREATE OR REPLACE FUNCTION retention_insert_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE net text; win bigint;
BEGIN
    net := NEW.action->'identity'->>'network';
    win := canonical_payload_window(NEW.action);
    IF EXISTS (SELECT 1 FROM event_identity_guards g WHERE g.event_id = NEW.event_id) THEN
        RAISE EXCEPTION 'event % is held by its identity guard: payload expired, backfill only',
            NEW.event_id;
    END IF;
    IF EXISTS (SELECT 1 FROM retained_context_horizons h WHERE h.network = net
               AND h.data_class = 'canonical_payload' AND win < h.retained_from)
       OR EXISTS (SELECT 1 FROM identity_guard_horizons g WHERE g.network = net
                  AND win < g.retired_before) THEN
        RAISE EXCEPTION 'window % of % is no longer retained: expired identity, backfill only',
            win, net;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS canonical_actions_retention_insert ON canonical_actions;
CREATE TRIGGER canonical_actions_retention_insert BEFORE INSERT ON canonical_actions
FOR EACH ROW EXECUTE FUNCTION retention_insert_gate();
DROP TRIGGER IF EXISTS canonical_current_retention_insert ON canonical_current;
CREATE TRIGGER canonical_current_retention_insert BEFORE INSERT ON canonical_current
FOR EACH ROW EXECUTE FUNCTION retention_insert_gate();
DROP TRIGGER IF EXISTS canonical_revisions_retention_insert ON canonical_revisions;
CREATE TRIGGER canonical_revisions_retention_insert BEFORE INSERT ON canonical_revisions
FOR EACH ROW EXECUTE FUNCTION retention_insert_gate();
