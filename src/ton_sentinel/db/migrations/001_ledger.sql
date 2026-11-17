CREATE TABLE IF NOT EXISTS ingest_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    cursor bigint NOT NULL DEFAULT 0 CHECK (cursor >= 0)
);
INSERT INTO ingest_state(singleton, cursor) VALUES (true, 0) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS active_source_epoch (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch jsonb
);
INSERT INTO active_source_epoch(singleton, epoch) VALUES (true, NULL) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS lease_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    owner text NOT NULL,
    fence bigint NOT NULL CHECK (fence > 0),
    expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS source_ranges (
    range_id uuid PRIMARY KEY,
    start_inclusive bigint NOT NULL,
    end_exclusive bigint NOT NULL,
    epoch jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('walking', 'incomplete', 'complete', 'accepted')),
    page_count integer NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    action_count integer NOT NULL DEFAULT 0 CHECK (action_count >= 0),
    reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (start_inclusive < end_exclusive)
);

CREATE TABLE IF NOT EXISTS action_observations (
    range_id uuid NOT NULL REFERENCES source_ranges(range_id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    payload jsonb NOT NULL,
    PRIMARY KEY (range_id, ordinal)
);

CREATE TABLE IF NOT EXISTS coverage_ranges (
    range_id uuid PRIMARY KEY REFERENCES source_ranges(range_id),
    status text NOT NULL CHECK (status IN ('pending', 'bounded_observed')),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS canonical_actions (
    event_id text PRIMARY KEY,
    semantic_digest text NOT NULL,
    action jsonb NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS scan_membership (
    range_id uuid NOT NULL REFERENCES source_ranges(range_id),
    event_id text NOT NULL REFERENCES canonical_actions(event_id),
    PRIMARY KEY (range_id, event_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    event_id text PRIMARY KEY REFERENCES canonical_actions(event_id),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(created_at) WHERE published_at IS NULL;
