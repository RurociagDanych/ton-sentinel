CREATE TABLE IF NOT EXISTS served_features (
    network text NOT NULL CHECK (network IN ('mainnet', 'testnet')),
    window_start bigint NOT NULL CHECK (window_start >= 0),
    window_end bigint NOT NULL CHECK (window_end = window_start + 300),
    token text NOT NULL CHECK (length(token) > 0),
    generation text NOT NULL CHECK (length(generation) > 0),
    swaps bigint NOT NULL CHECK (swaps >= 0),
    input_raw numeric(38, 0),
    output_raw numeric(38, 0),
    known_traders bigint NOT NULL CHECK (known_traders >= 0),
    unknown_count bigint NOT NULL CHECK (unknown_count >= 0),
    coverage_status text,
    status text NOT NULL,
    provisional boolean NOT NULL,
    write_fence bigint NOT NULL CHECK (write_fence > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (network, window_start, window_end, token)
);
CREATE INDEX IF NOT EXISTS served_features_window_idx
    ON served_features (network, window_start, window_end, token);
