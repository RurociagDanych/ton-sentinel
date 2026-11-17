CREATE TABLE IF NOT EXISTS scan_quarantine (
    range_id uuid NOT NULL REFERENCES source_ranges(range_id),
    ordinal bigint NOT NULL CHECK (ordinal >= 0),
    proposed_payload jsonb NOT NULL,
    reason text NOT NULL,
    epoch jsonb NOT NULL,
    start_inclusive bigint NOT NULL,
    end_exclusive bigint NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (range_id, ordinal),
    CHECK (start_inclusive < end_exclusive)
);