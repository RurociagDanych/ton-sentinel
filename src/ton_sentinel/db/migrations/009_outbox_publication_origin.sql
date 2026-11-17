CREATE TABLE IF NOT EXISTS outbox_publication_origin (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    origin_inclusive bigint NOT NULL CHECK (origin_inclusive >= 0),
    reconciled_frontier_end bigint CHECK (reconciled_frontier_end >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE outbox_publication_origin
    ADD COLUMN IF NOT EXISTS reconciled_frontier_end bigint
        CHECK (reconciled_frontier_end >= 0);
