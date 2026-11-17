-- Persisted detector results, revision-fenced alert state and score invalidation.
--
-- Additive only: no earlier migration is edited or superseded. Python writes these tables
-- only through ton_sentinel.db.work.promote_scores (results computed by
-- ton_sentinel.detectors.score_baseline from the input pinned in the same transaction).
-- 

-- 1. The active detector policy (config identity). Changing it is a config change: the
--    dependency epoch advances and every current result becomes stale (trigger below).
CREATE TABLE IF NOT EXISTS active_detector_policy (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    policy_id text NOT NULL REFERENCES detector_policies(policy_id),
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO active_detector_policy(singleton, policy_id) VALUES
    (true, 'sha256:34fc97d20f4907d727db53efd921830cdbb3ae17f877d67a1654d591db6830fc')
ON CONFLICT (singleton) DO NOTHING;

-- 2. Immutable detector result history. One revision per recomputation of a key; the full
--    explainable document is kept in ``result``; typed columns repeat what queries need.
CREATE TABLE IF NOT EXISTS detector_results (
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    token text NOT NULL CHECK (length(token) > 0),
    direction text NOT NULL CHECK (direction = 'both'),
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    detector_id text NOT NULL
        CHECK (detector_id IN ('swap_count_spike.v1', 'active_wallets_spike.v1')),
    result_revision bigint NOT NULL CHECK (result_revision > 0),
    input_digest text NOT NULL REFERENCES baseline_inputs(input_digest),
    policy_id text NOT NULL REFERENCES detector_policies(policy_id),
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch >= 0),
    status text NOT NULL CHECK (status IN (
        'scored', 'cold_start', 'missing', 'pending', 'insufficient_coverage')),
    triggered boolean NOT NULL,
    score numeric,
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    promoted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, token, direction, window_start, detector_id, result_revision),
    CHECK (NOT triggered OR status = 'scored'),
    CHECK ((status = 'scored') = (score IS NOT NULL)),
    CHECK (result->>'detector' = detector_id AND result->>'network' = network
           AND result->>'token' = token AND result->>'direction' = direction
           AND (result->>'window_start')::bigint = window_start
           AND result->>'input_digest' = input_digest AND result->>'policy_id' = policy_id
           AND (result->>'dependency_epoch')::bigint = dependency_epoch
           AND result->>'status' = status AND (result->>'triggered')::boolean = triggered
           AND (result->>'score')::numeric IS NOT DISTINCT FROM score
           AND jsonb_typeof(result->'explanation') = 'string')
);

CREATE OR REPLACE FUNCTION validate_detector_result() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    pin baseline_inputs%ROWTYPE;
    last_revision bigint;
BEGIN
    SELECT * INTO pin FROM baseline_inputs WHERE input_digest = NEW.input_digest;
    IF pin.network IS DISTINCT FROM NEW.network OR pin.token IS DISTINCT FROM NEW.token
       OR pin.direction IS DISTINCT FROM NEW.direction
       OR pin.window_start IS DISTINCT FROM NEW.window_start THEN
        RAISE EXCEPTION 'detector result does not belong to its pinned input';
    END IF;
    IF pin.policy_id <> NEW.policy_id OR pin.dependency_epoch <> NEW.dependency_epoch THEN
        RAISE EXCEPTION 'detector result policy/epoch differ from its pinned input';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM detector_policies p WHERE p.policy_id = NEW.policy_id
                   AND p.body->'detectors' ? NEW.detector_id) THEN
        RAISE EXCEPTION 'detector % is not part of policy %', NEW.detector_id, NEW.policy_id;
    END IF;
    SELECT max(result_revision) INTO last_revision FROM detector_results
    WHERE network = NEW.network AND token = NEW.token AND direction = NEW.direction
      AND window_start = NEW.window_start AND detector_id = NEW.detector_id;
    IF NEW.result_revision <> coalesce(last_revision, 0) + 1 THEN
        RAISE EXCEPTION 'detector result revisions are sequential';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS detector_results_validate ON detector_results;
CREATE TRIGGER detector_results_validate BEFORE INSERT ON detector_results
FOR EACH ROW EXECUTE FUNCTION validate_detector_result();
DROP TRIGGER IF EXISTS detector_results_immutable ON detector_results;
CREATE TRIGGER detector_results_immutable BEFORE UPDATE OR DELETE ON detector_results
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS detector_results_no_truncate ON detector_results;
CREATE TRIGGER detector_results_no_truncate BEFORE TRUNCATE ON detector_results
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 3. Current pointer per key. ``stale`` is set in the same transaction as any relevant
--    input/config change and cleared only by pointing at a newer result revision.
CREATE TABLE IF NOT EXISTS detector_current (
    network text NOT NULL,
    token text NOT NULL,
    direction text NOT NULL,
    window_start bigint NOT NULL,
    detector_id text NOT NULL,
    result_revision bigint NOT NULL,
    lookback_seconds bigint NOT NULL CHECK (lookback_seconds >= 300),
    stale boolean NOT NULL DEFAULT false,
    stale_epoch bigint,
    stale_reason text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, token, direction, window_start, detector_id),
    FOREIGN KEY (network, token, direction, window_start, detector_id, result_revision)
        REFERENCES detector_results(network, token, direction, window_start, detector_id,
                                    result_revision),
    CHECK (stale = (stale_reason IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS detector_current_window_idx
    ON detector_current(network, window_start);
CREATE INDEX IF NOT EXISTS detector_current_stale_idx
    ON detector_current(network, window_start, token) WHERE stale;

CREATE OR REPLACE FUNCTION guard_detector_current() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    old_epoch bigint;
    new_epoch bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'current detector results are never deleted';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.stale THEN
            RAISE EXCEPTION 'a new current detector result cannot start stale';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.result_revision = OLD.result_revision THEN
        IF OLD.stale AND NOT NEW.stale THEN
            RAISE EXCEPTION 'a stale detector result becomes current only by recomputation';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.result_revision < OLD.result_revision OR NEW.stale THEN
        RAISE EXCEPTION 'current detector result can only move to a newer, fresh revision';
    END IF;
    SELECT dependency_epoch INTO old_epoch FROM detector_results
    WHERE (network, token, direction, window_start, detector_id, result_revision)
        = (OLD.network, OLD.token, OLD.direction, OLD.window_start, OLD.detector_id,
           OLD.result_revision);
    SELECT dependency_epoch INTO new_epoch FROM detector_results
    WHERE (network, token, direction, window_start, detector_id, result_revision)
        = (NEW.network, NEW.token, NEW.direction, NEW.window_start, NEW.detector_id,
           NEW.result_revision);
    IF new_epoch < old_epoch THEN
        RAISE EXCEPTION 'a result of an older dependency epoch cannot become current';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS detector_current_guard ON detector_current;
CREATE TRIGGER detector_current_guard BEFORE INSERT OR UPDATE OR DELETE ON detector_current
FOR EACH ROW EXECUTE FUNCTION guard_detector_current();
DROP TRIGGER IF EXISTS detector_current_no_truncate ON detector_current;
CREATE TRIGGER detector_current_no_truncate BEFORE TRUNCATE ON detector_current
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 4. Alert state: an immutable, gap-free revision chain per key.
--    active    the current fresh result triggers;
--    pending   an active alert's inputs changed; awaiting recomputation (never served);
--    retracted the recomputed result is scored and no longer triggers;
--    inactive  the recomputed result is no longer scorable (missing/cold_start/coverage).
CREATE TABLE IF NOT EXISTS alert_revisions (
    network text NOT NULL,
    token text NOT NULL,
    direction text NOT NULL,
    window_start bigint NOT NULL,
    detector_id text NOT NULL,
    alert_revision bigint NOT NULL CHECK (alert_revision > 0),
    state text NOT NULL CHECK (state IN ('active', 'pending', 'retracted', 'inactive')),
    result_revision bigint NOT NULL,
    input_digest text NOT NULL REFERENCES baseline_inputs(input_digest),
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch >= 0),
    reason text NOT NULL CHECK (length(btrim(reason)) > 0),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, token, direction, window_start, detector_id, alert_revision),
    FOREIGN KEY (network, token, direction, window_start, detector_id, result_revision)
        REFERENCES detector_results(network, token, direction, window_start, detector_id,
                                    result_revision)
);

CREATE OR REPLACE FUNCTION validate_alert_revision() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    prev alert_revisions%ROWTYPE;
    res detector_results%ROWTYPE;
    cur detector_current%ROWTYPE;
BEGIN
    SELECT * INTO prev FROM alert_revisions
    WHERE network = NEW.network AND token = NEW.token AND direction = NEW.direction
      AND window_start = NEW.window_start AND detector_id = NEW.detector_id
    ORDER BY alert_revision DESC LIMIT 1;
    IF NEW.alert_revision <> coalesce(prev.alert_revision, 0) + 1 THEN
        RAISE EXCEPTION 'alert revisions are sequential';
    END IF;
    IF prev.alert_revision IS NOT NULL AND NEW.dependency_epoch < prev.dependency_epoch THEN
        RAISE EXCEPTION 'stale alert revision: dependency epoch % precedes %',
            NEW.dependency_epoch, prev.dependency_epoch;
    END IF;
    IF NOT ((prev.state IS NULL AND NEW.state = 'active')
            OR (prev.state = 'active' AND NEW.state IN ('active', 'pending', 'retracted',
                                                        'inactive'))
            OR (prev.state = 'pending' AND NEW.state IN ('active', 'retracted', 'inactive'))
            OR (prev.state IN ('retracted', 'inactive') AND NEW.state = 'active')) THEN
        RAISE EXCEPTION 'alert transition % -> % is not allowed', prev.state, NEW.state;
    END IF;
    SELECT * INTO res FROM detector_results
    WHERE (network, token, direction, window_start, detector_id, result_revision)
        = (NEW.network, NEW.token, NEW.direction, NEW.window_start, NEW.detector_id,
           NEW.result_revision);
    IF res.input_digest IS DISTINCT FROM NEW.input_digest THEN
        RAISE EXCEPTION 'alert revision names a different input than its result';
    END IF;
    IF NEW.state = 'pending' THEN
        IF prev.result_revision IS DISTINCT FROM NEW.result_revision THEN
            RAISE EXCEPTION 'a pending alert revision keeps the invalidated result';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO cur FROM detector_current
    WHERE network = NEW.network AND token = NEW.token AND direction = NEW.direction
      AND window_start = NEW.window_start AND detector_id = NEW.detector_id;
    IF cur.result_revision IS DISTINCT FROM NEW.result_revision OR cur.stale THEN
        RAISE EXCEPTION 'alert state may only follow the current, fresh detector result';
    END IF;
    IF res.dependency_epoch < coalesce(prev.dependency_epoch, 0) THEN
        RAISE EXCEPTION 'stale alert revision: result epoch precedes the alert history';
    END IF;
    IF (NEW.state = 'active') <> res.triggered
       OR (NEW.state = 'retracted' AND res.status <> 'scored')
       OR (NEW.state = 'inactive' AND res.status IN ('scored', 'pending')) THEN
        RAISE EXCEPTION 'alert state % does not match its result (%/%)', NEW.state,
            res.status, res.triggered;
    END IF;
    IF prev.state = 'active' AND NEW.state = 'active'
       AND NEW.result_revision <= prev.result_revision THEN
        RAISE EXCEPTION 'an active alert revision needs a newer result';
    END IF;
    -- A stale revision cannot re-open an alert: an input that a later, differently scored
    -- revision already superseded never becomes active again.
    IF NEW.state = 'active' AND EXISTS (
        SELECT 1 FROM alert_revisions a
        WHERE a.network = NEW.network AND a.token = NEW.token AND a.direction = NEW.direction
          AND a.window_start = NEW.window_start AND a.detector_id = NEW.detector_id
          AND a.input_digest = NEW.input_digest
          AND EXISTS (
              SELECT 1 FROM alert_revisions b
              WHERE b.network = a.network AND b.token = a.token AND b.direction = a.direction
                AND b.window_start = a.window_start AND b.detector_id = a.detector_id
                AND b.alert_revision > a.alert_revision AND b.state <> 'pending'
                AND b.input_digest <> NEW.input_digest)) THEN
        RAISE EXCEPTION 'a superseded input cannot re-open an alert';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS alert_revisions_validate ON alert_revisions;
CREATE TRIGGER alert_revisions_validate BEFORE INSERT ON alert_revisions
FOR EACH ROW EXECUTE FUNCTION validate_alert_revision();
DROP TRIGGER IF EXISTS alert_revisions_immutable ON alert_revisions;
CREATE TRIGGER alert_revisions_immutable BEFORE UPDATE OR DELETE ON alert_revisions
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS alert_revisions_no_truncate ON alert_revisions;
CREATE TRIGGER alert_revisions_no_truncate BEFORE TRUNCATE ON alert_revisions
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 5. Invalidation (spec 7.3 step 4): every relevant input/config change makes the current
--    results of the changed window and of its trailing-baseline dependents
--    [first, last + lookback] stale, and pends their active alerts, in the SAME transaction.
--    ``net``/``tok`` NULL mean every network/token; ``last_ws`` NULL means unbounded.
CREATE OR REPLACE FUNCTION invalidate_scores(net text, tok text, first_ws bigint,
                                             last_ws bigint, why text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    ep bigint;
    lo text;
    lf bigint;
    n integer;
BEGIN
    SELECT epoch INTO ep FROM dependency_epoch WHERE singleton;
    SELECT owner, fence INTO lo, lf FROM lease_state WHERE singleton;
    WITH s AS (
        UPDATE detector_current d
        SET stale = true, stale_epoch = ep, stale_reason = why, updated_at = clock_timestamp()
        WHERE NOT d.stale AND (net IS NULL OR d.network = net)
          AND (tok IS NULL OR d.token = tok) AND d.window_start >= first_ws
          AND (last_ws IS NULL OR d.window_start <= last_ws + d.lookback_seconds)
        RETURNING d.network, d.token, d.direction, d.window_start, d.detector_id)
    INSERT INTO alert_revisions(network, token, direction, window_start, detector_id,
                                alert_revision, state, result_revision, input_digest,
                                dependency_epoch, reason, lease_owner, lease_fence)
    SELECT s.network, s.token, s.direction, s.window_start, s.detector_id,
           a.alert_revision + 1, 'pending', a.result_revision, a.input_digest,
           greatest(coalesce(ep, 0), a.dependency_epoch), why,
           coalesce(lo, 'invalidation'), coalesce(lf, 1)
    FROM s JOIN LATERAL (
        SELECT * FROM alert_revisions x
        WHERE x.network = s.network AND x.token = s.token AND x.direction = s.direction
          AND x.window_start = s.window_start AND x.detector_id = s.detector_id
        ORDER BY x.alert_revision DESC LIMIT 1) a ON true
    WHERE a.state = 'active';
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$;

CREATE OR REPLACE FUNCTION invalidate_on_qualification() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM invalidate_scores(NEW.network, NULL, NEW.window_start, NEW.window_start,
                              'coverage_qualified');
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS action_window_qualification_invalidates ON action_window_qualification;
CREATE TRIGGER action_window_qualification_invalidates AFTER INSERT ON action_window_qualification
FOR EACH ROW EXECUTE FUNCTION invalidate_on_qualification();

CREATE OR REPLACE FUNCTION invalidate_on_promotion() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    w jsonb;
BEGIN
    FOR w IN SELECT value FROM jsonb_array_elements(NEW.windows) LOOP
        PERFORM invalidate_scores(w->>0, NULL, (w->>1)::bigint, (w->>1)::bigint,
                                  'features_promoted');
    END LOOP;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS repair_promotions_invalidate ON repair_promotions;
CREATE TRIGGER repair_promotions_invalidate AFTER INSERT ON repair_promotions
FOR EACH ROW EXECUTE FUNCTION invalidate_on_promotion();

CREATE OR REPLACE FUNCTION invalidate_on_dirty() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM invalidate_scores(NEW.network, NEW.token, NEW.window_start, NEW.window_start,
                              'dirty_' || NEW.role);
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS dirty_windows_invalidate ON dirty_windows;
CREATE TRIGGER dirty_windows_invalidate AFTER INSERT OR UPDATE ON dirty_windows
FOR EACH ROW WHEN (NEW.status = 'pending') EXECUTE FUNCTION invalidate_on_dirty();

CREATE OR REPLACE FUNCTION invalidate_on_new_features() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT DISTINCT network, window_start FROM changed_features LOOP
        PERFORM invalidate_scores(r.network, NULL, r.window_start, r.window_start,
                                  'features_replaced');
    END LOOP;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS served_features_invalidate_insert ON served_features;
CREATE TRIGGER served_features_invalidate_insert AFTER INSERT ON served_features
REFERENCING NEW TABLE AS changed_features
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_on_new_features();
DROP TRIGGER IF EXISTS served_features_invalidate_update ON served_features;
CREATE TRIGGER served_features_invalidate_update AFTER UPDATE ON served_features
REFERENCING NEW TABLE AS changed_features
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_on_new_features();
DROP TRIGGER IF EXISTS served_features_invalidate_delete ON served_features;
CREATE TRIGGER served_features_invalidate_delete AFTER DELETE ON served_features
REFERENCING OLD TABLE AS changed_features
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_on_new_features();

CREATE OR REPLACE FUNCTION invalidate_on_admission() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM invalidate_scores(NEW.network, NEW.token, NEW.admitted_window_start, NULL,
                              'universe_admitted');
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS token_universe_invalidate ON token_universe;
CREATE TRIGGER token_universe_invalidate AFTER INSERT ON token_universe
FOR EACH ROW EXECUTE FUNCTION invalidate_on_admission();

CREATE OR REPLACE FUNCTION invalidate_everything() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM invalidate_scores(NULL, NULL, 0, NULL, TG_ARGV[0]);
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS active_source_epoch_invalidate ON active_source_epoch;
CREATE TRIGGER active_source_epoch_invalidate AFTER UPDATE ON active_source_epoch
FOR EACH ROW WHEN (OLD.epoch IS DISTINCT FROM NEW.epoch)
EXECUTE FUNCTION invalidate_everything('source_epoch_changed');
DROP TRIGGER IF EXISTS active_detector_policy_invalidate ON active_detector_policy;
CREATE TRIGGER active_detector_policy_invalidate AFTER UPDATE ON active_detector_policy
FOR EACH ROW WHEN (OLD.policy_id IS DISTINCT FROM NEW.policy_id)
EXECUTE FUNCTION invalidate_everything('detector_policy_changed');
