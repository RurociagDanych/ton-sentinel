ALTER TABLE source_ranges
    ADD COLUMN IF NOT EXISTS scan_policy jsonb,
    ADD COLUMN IF NOT EXISTS next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    ADD COLUMN IF NOT EXISTS bytes_used bigint NOT NULL DEFAULT 0 CHECK (bytes_used >= 0),
    ADD COLUMN IF NOT EXISTS elapsed_ms bigint NOT NULL DEFAULT 0 CHECK (elapsed_ms >= 0),
    ADD COLUMN IF NOT EXISTS checkpoint_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN IF NOT EXISTS legacy_resume boolean;

-- NULL is the one-time migration sentinel. New and later rows are never adopted.
UPDATE source_ranges
SET legacy_resume = (status IN ('complete', 'accepted') AND scan_policy IS NULL)
WHERE legacy_resume IS NULL;
ALTER TABLE source_ranges ALTER COLUMN legacy_resume SET DEFAULT false;
ALTER TABLE source_ranges ALTER COLUMN legacy_resume SET NOT NULL;

CREATE TABLE IF NOT EXISTS scan_observation_receipts (
    range_id uuid NOT NULL,
    ordinal bigint NOT NULL CHECK (ordinal >= 0),
    payload_digest text NOT NULL CHECK (length(payload_digest) = 64),
    PRIMARY KEY (range_id, ordinal)
);

CREATE TABLE IF NOT EXISTS scan_page_attempts (
    range_id uuid NOT NULL,
    attempt_no bigint NOT NULL CHECK (attempt_no > 0),
    requested_offset bigint NOT NULL CHECK (requested_offset >= 0),
    PRIMARY KEY (range_id, attempt_no)
);
