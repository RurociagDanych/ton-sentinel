-- Exact qualified baseline grid and detector input contract.
--
-- Additive only: no earlier migration is edited and nothing is superseded. 005's
-- action_window_coverage keeps recording missing evidence (eligible_zero is always false
-- there); the evidence that may license a zero lives in action_window_qualification below,
-- which needs an explicit trace-completion bound and mapping. Rationale and decisions:
-- 

-- 1. Fixed detector policy identities. The v1 digest is computed by
--    ton_sentinel.contracts.DetectorPolicy (sha256 of the canonical JSON body) and pinned in
--    ton_sentinel.config.DETECTOR_POLICY_V1_ID; a unit/integration test asserts all agree.
CREATE TABLE IF NOT EXISTS detector_policies (
    policy_id text PRIMARY KEY CHECK (policy_id ~ '^sha256:[0-9a-f]{64}$'),
    version text NOT NULL UNIQUE CHECK (length(btrim(version)) > 0),
    body jsonb NOT NULL CHECK (jsonb_typeof(body) = 'object'
        AND body->>'version' = version
        AND (body->>'window_seconds')::bigint = 300
        AND (body->>'lookback_seconds')::bigint >= 300
        AND (body->>'lookback_seconds')::bigint % 300 = 0
        AND (body->>'min_prior_windows')::bigint >= 1
        AND body->>'direction' = 'both'),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO detector_policies(policy_id, version, body) VALUES (
    'sha256:34fc97d20f4907d727db53efd921830cdbb3ae17f877d67a1654d591db6830fc',
    'detector-policy.v1',
    '{"attribution_floor":"0.90","detectors":["swap_count_spike.v1","active_wallets_spike.v1"],"direction":"both","lookback_seconds":604800,"mad_multiplier":"1.4826","min_prior_windows":24,"scale_floor":"1","score_threshold":"6","swap_floor":10,"trader_floor":5,"version":"detector-policy.v1","window_seconds":300}'::jsonb
) ON CONFLICT (policy_id) DO NOTHING;

CREATE OR REPLACE FUNCTION reject_baseline_audit_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% rows are immutable', TG_TABLE_NAME;
END;
$$;
DROP TRIGGER IF EXISTS detector_policies_immutable ON detector_policies;
CREATE TRIGGER detector_policies_immutable BEFORE UPDATE OR DELETE ON detector_policies
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS detector_policies_no_truncate ON detector_policies;
CREATE TRIGGER detector_policies_no_truncate BEFORE TRUNCATE ON detector_policies
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 2. Versioned, data-derived active-token universe. A token is admitted, once, by the first
--    canonical repair promotion that carries an eligible swap for it; admission never moves,
--    so no later event can turn earlier windows into a zero history.
CREATE TABLE IF NOT EXISTS token_universe_version (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version bigint NOT NULL CHECK (version >= 0)
);
INSERT INTO token_universe_version(singleton, version) VALUES (true, 0)
ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION protect_token_universe_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.singleton AND OLD.singleton AND NEW.version = OLD.version + 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'token universe version only advances by exactly one';
END;
$$;
DROP TRIGGER IF EXISTS token_universe_version_monotonic ON token_universe_version;
CREATE TRIGGER token_universe_version_monotonic BEFORE UPDATE OR DELETE ON token_universe_version
FOR EACH ROW EXECUTE FUNCTION protect_token_universe_version();
DROP TRIGGER IF EXISTS token_universe_version_no_truncate ON token_universe_version;
CREATE TRIGGER token_universe_version_no_truncate BEFORE TRUNCATE ON token_universe_version
FOR EACH STATEMENT EXECUTE FUNCTION protect_token_universe_version();

CREATE TABLE IF NOT EXISTS token_universe (
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    token text NOT NULL CHECK (length(token) > 0),
    admitted_window_start bigint NOT NULL
        CHECK (admitted_window_start >= 0 AND admitted_window_start % 300 = 0),
    universe_version bigint NOT NULL UNIQUE CHECK (universe_version > 0),
    admitted_work_id uuid NOT NULL REFERENCES repair_promotions(work_id),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, token)
);
DROP TRIGGER IF EXISTS token_universe_immutable ON token_universe;
CREATE TRIGGER token_universe_immutable BEFORE UPDATE OR DELETE ON token_universe
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS token_universe_no_truncate ON token_universe;
CREATE TRIGGER token_universe_no_truncate BEFORE TRUNCATE ON token_universe
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 3. Qualified whole-window action coverage (spec 5.3): the only evidence that can make an
--    absent row a zero. Valid only while its source_epoch is the active source epoch.
--    Synthetic (fixture) evidence is bounded_observed, never certified.
CREATE TABLE IF NOT EXISTS action_window_qualification (
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    window_end bigint NOT NULL,
    mode text NOT NULL CHECK (mode IN ('bounded_observed', 'certified')),
    mapping_version text NOT NULL CHECK (length(btrim(mapping_version)) > 0),
    delta_seconds bigint NOT NULL CHECK (delta_seconds >= 0),
    trace_ranges jsonb NOT NULL
        CHECK (jsonb_typeof(trace_ranges) = 'array' AND jsonb_array_length(trace_ranges) > 0),
    source_epoch jsonb NOT NULL CHECK (jsonb_typeof(source_epoch) = 'object'),
    synthetic boolean NOT NULL,
    certification jsonb,
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch > 0),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, window_start),
    CHECK (window_end = window_start + 300),
    CHECK ((mode = 'certified') = (certification IS NOT NULL)),
    CHECK (certification IS NULL OR jsonb_typeof(certification) = 'object'),
    CHECK (NOT (synthetic AND mode = 'certified'))
);
DROP TRIGGER IF EXISTS action_window_qualification_immutable ON action_window_qualification;
CREATE TRIGGER action_window_qualification_immutable
BEFORE UPDATE OR DELETE ON action_window_qualification
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS action_window_qualification_no_truncate ON action_window_qualification;
CREATE TRIGGER action_window_qualification_no_truncate
BEFORE TRUNCATE ON action_window_qualification
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 4. Pinned detector baseline inputs. One row per (scored window, digest); cells are the exact
--    strictly prior grid, run-length encoded. The trigger rejects incomplete, cross-scope,
--    mis-stated or policy-inconsistent inputs; Python re-checks the digest.
CREATE TABLE IF NOT EXISTS baseline_inputs (
    input_digest text PRIMARY KEY CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    token text NOT NULL CHECK (length(token) > 0),
    direction text NOT NULL CHECK (direction = 'both'),
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    policy_id text NOT NULL REFERENCES detector_policies(policy_id),
    lookback_seconds bigint NOT NULL CHECK (lookback_seconds >= 300 AND lookback_seconds % 300 = 0),
    min_prior_windows integer NOT NULL CHECK (min_prior_windows >= 1),
    universe_version bigint NOT NULL CHECK (universe_version >= 0),
    admitted_window_start bigint
        CHECK (admitted_window_start >= 0 AND admitted_window_start % 300 = 0),
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch >= 0),
    active_epoch text,
    status text NOT NULL CHECK (status IN ('ready', 'cold_start', 'pending', 'missing')),
    eligible_count integer NOT NULL CHECK (eligible_count >= 0),
    certification text NOT NULL CHECK (certification IN (
        'certified', 'bounded_observed', 'synthetic_bounded_observed', 'none')),
    current_cell jsonb NOT NULL CHECK (jsonb_typeof(current_cell) = 'object'),
    cells jsonb NOT NULL CHECK (jsonb_typeof(cells) = 'array'),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    pinned_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS baseline_inputs_scored_idx
    ON baseline_inputs(network, token, window_start, dependency_epoch);

-- Returns NULL for a valid cell run, else the reason.
CREATE OR REPLACE FUNCTION baseline_cell_problem(c jsonb, net text, tok text, dir text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    st text;
    s bigint;
    e bigint;
    sw bigint;
    kn bigint;
    un bigint;
    has_values boolean;
    cov text;
    syn boolean;
    rev text;
BEGIN
    IF jsonb_typeof(c) IS DISTINCT FROM 'object' THEN
        RETURN 'baseline cell must be an object';
    END IF;
    IF (SELECT array_agg(k COLLATE "C" ORDER BY k COLLATE "C") FROM jsonb_object_keys(c) k)
       IS DISTINCT FROM ARRAY['coverage', 'direction', 'end', 'known_traders', 'network',
                              'revision', 'start', 'state', 'swaps', 'synthetic', 'token',
                              'unknown_count'] THEN
        RETURN 'baseline cell has unexpected fields';
    END IF;
    IF c->>'network' IS DISTINCT FROM net OR c->>'token' IS DISTINCT FROM tok
       OR c->>'direction' IS DISTINCT FROM dir THEN
        RETURN 'cross-scope baseline cell';
    END IF;
    IF jsonb_typeof(c->'start') <> 'number' OR jsonb_typeof(c->'end') <> 'number'
       OR jsonb_typeof(c->'synthetic') <> 'boolean' THEN
        RETURN 'baseline cell bounds/flags have wrong types';
    END IF;
    s := (c->>'start')::bigint;
    e := (c->>'end')::bigint;
    IF s < 0 OR s % 300 <> 0 OR e <= s OR e % 300 <> 0 THEN
        RETURN 'baseline cell is not whole five-minute windows';
    END IF;
    st := c->>'state';
    IF st IS NULL OR st NOT IN ('observed', 'zero', 'partial', 'provisional', 'missing',
                                'pre_universe') THEN
        RETURN 'unknown baseline cell state';
    END IF;
    IF e - s > 300 AND st NOT IN ('zero', 'missing', 'pre_universe') THEN
        RETURN 'only zero/missing/pre_universe cells may be run-length encoded';
    END IF;
    IF jsonb_typeof(c->'swaps') = 'null' AND jsonb_typeof(c->'known_traders') = 'null'
       AND jsonb_typeof(c->'unknown_count') = 'null' THEN
        has_values := false;
    ELSIF jsonb_typeof(c->'swaps') = 'number' AND jsonb_typeof(c->'known_traders') = 'number'
          AND jsonb_typeof(c->'unknown_count') = 'number' THEN
        sw := (c->>'swaps')::bigint;
        kn := (c->>'known_traders')::bigint;
        un := (c->>'unknown_count')::bigint;
        IF sw < 0 OR kn < 0 OR un < 0 OR kn + un > sw THEN
            RETURN 'invalid baseline cell counts';
        END IF;
        has_values := true;
    ELSE
        RETURN 'baseline cell counts must be all null or all numbers';
    END IF;
    cov := c->>'coverage';
    rev := c->>'revision';
    syn := (c->>'synthetic')::boolean;
    IF cov IS NOT NULL AND cov NOT IN ('bounded_observed', 'certified') THEN
        RETURN 'unknown baseline cell coverage';
    END IF;
    IF syn AND cov IS DISTINCT FROM 'bounded_observed' THEN
        RETURN 'synthetic coverage is bounded_observed only, never certified';
    END IF;
    IF (st IN ('observed', 'zero')) <> (cov IS NOT NULL) THEN
        RETURN 'only observed/zero cells carry qualified coverage';
    END IF;
    IF (st = 'observed' AND NOT (has_values AND sw >= 1 AND rev IS NOT NULL))
       OR (st = 'zero' AND NOT (has_values AND sw = 0 AND rev IS NOT NULL))
       OR (st = 'partial' AND NOT (has_values AND sw >= 1 AND rev IS NOT NULL))
       OR (st IN ('missing', 'pre_universe') AND (has_values OR rev IS NOT NULL)) THEN
        RETURN 'baseline cell values do not match its state';
    END IF;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION validate_baseline_input() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    pol jsonb;
    cell jsonb;
    problem text;
    cursor_at bigint;
    eligible bigint := 0;
    provisional boolean := false;
    considered integer := 0;
    any_synthetic boolean := false;
    all_certified boolean := true;
    cur_state text;
    want_status text;
    want_cert text;
BEGIN
    SELECT body INTO pol FROM detector_policies WHERE policy_id = NEW.policy_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'baseline input names an unregistered detector policy';
    END IF;
    IF (pol->>'lookback_seconds')::bigint <> NEW.lookback_seconds
       OR (pol->>'min_prior_windows')::bigint <> NEW.min_prior_windows
       OR pol->>'direction' <> NEW.direction THEN
        RAISE EXCEPTION 'baseline input does not match its detector policy';
    END IF;
    cursor_at := greatest(0, NEW.window_start - NEW.lookback_seconds);
    FOR cell IN SELECT e.value FROM jsonb_array_elements(NEW.cells) WITH ORDINALITY AS e(value, ord)
                ORDER BY e.ord LOOP
        problem := baseline_cell_problem(cell, NEW.network, NEW.token, NEW.direction);
        IF problem IS NOT NULL THEN
            RAISE EXCEPTION '%', problem;
        END IF;
        IF (cell->>'start')::bigint <> cursor_at THEN
            RAISE EXCEPTION 'incomplete baseline grid: gap or overlap at %', cursor_at;
        END IF;
        cursor_at := (cell->>'end')::bigint;
        IF cell->>'state' IN ('observed', 'zero') THEN
            eligible := eligible + ((cell->>'end')::bigint - (cell->>'start')::bigint) / 300;
            considered := considered + 1;
            any_synthetic := any_synthetic OR (cell->>'synthetic')::boolean;
            all_certified := all_certified AND cell->>'coverage' = 'certified';
        ELSIF cell->>'state' = 'provisional' THEN
            provisional := true;
        END IF;
    END LOOP;
    IF cursor_at <> NEW.window_start THEN
        RAISE EXCEPTION 'incomplete baseline grid: cells must end at the scored window';
    END IF;
    problem := baseline_cell_problem(NEW.current_cell, NEW.network, NEW.token, NEW.direction);
    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'current cell: %', problem;
    END IF;
    IF (NEW.current_cell->>'start')::bigint <> NEW.window_start
       OR (NEW.current_cell->>'end')::bigint <> NEW.window_start + 300 THEN
        RAISE EXCEPTION 'current cell is not the scored window';
    END IF;
    cur_state := NEW.current_cell->>'state';
    IF cur_state IN ('observed', 'zero') THEN
        considered := considered + 1;
        any_synthetic := any_synthetic OR (NEW.current_cell->>'synthetic')::boolean;
        all_certified := all_certified AND NEW.current_cell->>'coverage' = 'certified';
    END IF;
    want_status := CASE
        WHEN cur_state IN ('missing', 'partial', 'pre_universe') THEN 'missing'
        WHEN cur_state = 'provisional' OR provisional THEN 'pending'
        WHEN eligible < NEW.min_prior_windows THEN 'cold_start'
        ELSE 'ready' END;
    want_cert := CASE
        WHEN considered = 0 THEN 'none'
        WHEN any_synthetic THEN 'synthetic_bounded_observed'
        WHEN all_certified THEN 'certified'
        ELSE 'bounded_observed' END;
    IF NEW.eligible_count <> eligible THEN
        RAISE EXCEPTION 'baseline eligible count % does not match the cells (%)',
            NEW.eligible_count, eligible;
    END IF;
    IF NEW.status <> want_status THEN
        RAISE EXCEPTION 'baseline status % does not match the cells (%)', NEW.status, want_status;
    END IF;
    IF NEW.certification <> want_cert THEN
        RAISE EXCEPTION 'baseline certification % does not match the cells (%)',
            NEW.certification, want_cert;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS baseline_inputs_validate ON baseline_inputs;
CREATE TRIGGER baseline_inputs_validate BEFORE INSERT ON baseline_inputs
FOR EACH ROW EXECUTE FUNCTION validate_baseline_input();
DROP TRIGGER IF EXISTS baseline_inputs_immutable ON baseline_inputs;
CREATE TRIGGER baseline_inputs_immutable BEFORE UPDATE OR DELETE ON baseline_inputs
FOR EACH ROW EXECUTE FUNCTION reject_baseline_audit_change();
DROP TRIGGER IF EXISTS baseline_inputs_no_truncate ON baseline_inputs;
CREATE TRIGGER baseline_inputs_no_truncate BEFORE TRUNCATE ON baseline_inputs
FOR EACH STATEMENT EXECUTE FUNCTION reject_baseline_audit_change();

-- 5. baseline_dependents closure (dirty rows, role 'baseline_dependents'). A dependents
--    row may move pending -> done only when (a) its own target window is no longer pending and
--    (b) every scored window in [covers_start, covers_end) that was pinned before the change
--    has been re-pinned at or after the change's dependency epoch with a non-pending status.
--    Score promotion pins inputs only there, so a re-pin there is a recomputed score.
CREATE OR REPLACE FUNCTION guard_baseline_dependents_closure() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.role = 'baseline_dependents' AND OLD.status = 'pending' AND NEW.status = 'done' THEN
        IF EXISTS (
            SELECT 1 FROM dirty_windows t
            WHERE t.network = OLD.network AND t.token = OLD.token
              AND t.window_start = OLD.window_start AND t.role = 'target'
              AND t.status = 'pending') THEN
            RAISE EXCEPTION 'baseline dependents cannot close while their target is pending';
        END IF;
        IF EXISTS (
            SELECT 1 FROM baseline_inputs b
            WHERE b.network = OLD.network AND b.token = OLD.token
              AND b.window_start >= OLD.covers_start AND b.window_start < OLD.covers_end
              AND b.dependency_epoch < OLD.dependency_epoch
              AND NOT EXISTS (
                  SELECT 1 FROM baseline_inputs r
                  WHERE r.network = b.network AND r.token = b.token
                    AND r.window_start = b.window_start
                    AND r.dependency_epoch >= OLD.dependency_epoch
                    AND r.status <> 'pending')) THEN
            RAISE EXCEPTION 'baseline dependents cannot close before every stale dependent '
                            'score is recomputed';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS dirty_windows_dependents_closure ON dirty_windows;
CREATE TRIGGER dirty_windows_dependents_closure BEFORE UPDATE ON dirty_windows
FOR EACH ROW EXECUTE FUNCTION guard_baseline_dependents_closure();
