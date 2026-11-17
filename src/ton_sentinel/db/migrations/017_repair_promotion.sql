-- One immutable row per promoted frozen repair. The primary key makes a duplicate
-- or racing promotion of the same frozen work a no-op for the loser.
CREATE TABLE IF NOT EXISTS repair_promotions (
    work_id uuid PRIMARY KEY,
    owner text NOT NULL CHECK (length(btrim(owner)) > 0),
    fence bigint NOT NULL CHECK (fence > 0),
    dependency_epoch bigint NOT NULL CHECK (dependency_epoch >= 0),
    input_digest text NOT NULL CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    windows jsonb NOT NULL CHECK (jsonb_typeof(windows) = 'array' AND jsonb_array_length(windows) > 0),
    promoted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION reject_repair_promotion_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'repair promotion records are immutable';
END;
$$;
DROP TRIGGER IF EXISTS repair_promotions_immutable ON repair_promotions;
CREATE TRIGGER repair_promotions_immutable BEFORE UPDATE OR DELETE ON repair_promotions
FOR EACH ROW EXECUTE FUNCTION reject_repair_promotion_change();
DROP TRIGGER IF EXISTS repair_promotions_no_truncate ON repair_promotions;
CREATE TRIGGER repair_promotions_no_truncate BEFORE TRUNCATE ON repair_promotions
FOR EACH STATEMENT EXECUTE FUNCTION reject_repair_promotion_change();
