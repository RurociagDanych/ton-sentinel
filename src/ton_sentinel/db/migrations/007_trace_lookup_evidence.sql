CREATE TABLE IF NOT EXISTS trace_lookup_evidence (
    observation_id bigserial PRIMARY KEY,
    range_id uuid NOT NULL REFERENCES source_ranges(range_id),
    trace_id text NOT NULL CHECK (length(trace_id) BETWEEN 1 AND 512),
    epoch jsonb NOT NULL,
    start_inclusive bigint NOT NULL,
    end_exclusive bigint NOT NULL,
    state text NOT NULL CHECK (state IN ('classified_complete', 'unclassified', 'incomplete')),
    action_keys jsonb,
    global_coverage boolean NOT NULL DEFAULT false CHECK (global_coverage = false),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (start_inclusive < end_exclusive),
    CHECK (action_keys IS NULL OR jsonb_typeof(action_keys) = 'array'),
    CHECK ((state = 'classified_complete') = (action_keys IS NOT NULL))
);
