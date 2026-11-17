-- Whole-window admission against retained context and resource bounds.
-- Additive only; replays safely (every statement is idempotent).

-- 1. Retained-context horizons: the oldest window whose data of a class still survives.
--    Written by retention when it deletes, read by admission so that
--    actual surviving context is checked, not inferred from policy dates. A horizon only
--    moves forward and is never deleted: deleted data cannot come back.
CREATE TABLE IF NOT EXISTS retained_context_horizons (
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    data_class text NOT NULL CHECK (data_class IN ('canonical_payload', 'derived_features')),
    retained_from bigint NOT NULL CHECK (retained_from >= 0 AND retained_from % 300 = 0),
    advanced_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, data_class)
);

CREATE OR REPLACE FUNCTION retained_context_horizon_forward() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'retained context horizons are never deleted';
    END IF;
    IF NEW.network <> OLD.network OR NEW.data_class <> OLD.data_class
       OR NEW.retained_from < OLD.retained_from THEN
        RAISE EXCEPTION 'retained context horizon can only move forward';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS retained_context_horizon_forward ON retained_context_horizons;
CREATE TRIGGER retained_context_horizon_forward
BEFORE UPDATE OR DELETE ON retained_context_horizons
FOR EACH ROW EXECUTE FUNCTION retained_context_horizon_forward();

-- 2. Durable record of refused admissions: one immutable row per window of a refused work
--    unit. ``status`` is the window's missing/unrepairable outcome; ``reasons`` its own
--    reasons, or ["work_unit_refused"] when only another window of the unit failed.
CREATE TABLE IF NOT EXISTS window_admission_refusals (
    admission_digest text NOT NULL CHECK (admission_digest ~ '^sha256:[0-9a-f]{64}$'),
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    window_start bigint NOT NULL CHECK (window_start >= 0 AND window_start % 300 = 0),
    window_end bigint NOT NULL CHECK (window_end = window_start + 300),
    status text NOT NULL CHECK (status IN ('missing', 'unrepairable')),
    reasons jsonb NOT NULL CHECK (jsonb_typeof(reasons) = 'array'
                                  AND jsonb_array_length(reasons) > 0),
    work_windows jsonb NOT NULL CHECK (jsonb_typeof(work_windows) = 'array'
                                       AND jsonb_array_length(work_windows) > 0),
    decided_at bigint NOT NULL,
    event_cutoff bigint NOT NULL,
    repair_cutoff bigint NOT NULL,
    dependency_epoch bigint NOT NULL,
    usage jsonb NOT NULL,
    budget jsonb NOT NULL,
    lease_owner text NOT NULL,
    lease_fence bigint NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (admission_digest, network, window_start)
);
CREATE INDEX IF NOT EXISTS window_admission_refusals_window
    ON window_admission_refusals (network, window_start);

CREATE OR REPLACE FUNCTION window_admission_refusals_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'window admission refusals are immutable';
END $$;

DROP TRIGGER IF EXISTS window_admission_refusals_immutable ON window_admission_refusals;
CREATE TRIGGER window_admission_refusals_immutable
BEFORE UPDATE ON window_admission_refusals
FOR EACH ROW EXECUTE FUNCTION window_admission_refusals_immutable();
