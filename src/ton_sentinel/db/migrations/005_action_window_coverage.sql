CREATE TABLE IF NOT EXISTS action_window_coverage (
    coverage_id uuid PRIMARY KEY,
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    epoch jsonb NOT NULL,
    window_start bigint NOT NULL CHECK (window_start >= 0),
    window_end bigint NOT NULL CHECK (window_end > window_start),
    status text NOT NULL CHECK (status = 'missing'),
    eligible_zero boolean NOT NULL CHECK (eligible_zero = false),
    reason text NOT NULL CHECK (reason IN (
        'no_action_time_scan_evidence',
        'trace_completion_mapping_unavailable'
    )),
    lineage jsonb NOT NULL CHECK (jsonb_typeof(lineage) = 'array'),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (network, epoch, window_start, window_end)
);
