-- Diagnostic stateful fast-path output (never authoritative) and bounded-state metrics.
CREATE TABLE IF NOT EXISTS stream_candidate_windows (
    environment_id text NOT NULL CHECK (length(environment_id) > 0),
    query_id text NOT NULL CHECK (length(query_id) > 0),
    pipeline_epoch text NOT NULL CHECK (length(pipeline_epoch) > 0),
    network text NOT NULL CHECK (length(network) > 0),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL CHECK (window_end > window_start),
    swaps bigint NOT NULL CHECK (swaps >= 0),
    diagnostic_only boolean NOT NULL DEFAULT true CHECK (diagnostic_only),
    batch_id bigint NOT NULL CHECK (batch_id >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment_id, query_id, pipeline_epoch, network, window_start)
);

CREATE TABLE IF NOT EXISTS stream_state_metrics (
    environment_id text NOT NULL CHECK (length(environment_id) > 0),
    query_id text NOT NULL CHECK (length(query_id) > 0),
    pipeline_epoch text NOT NULL CHECK (length(pipeline_epoch) > 0),
    batch_id bigint NOT NULL CHECK (batch_id >= 0),
    input_rows bigint NOT NULL CHECK (input_rows >= 0),
    state_rows bigint NOT NULL CHECK (state_rows >= 0),
    state_bytes bigint NOT NULL CHECK (state_bytes >= 0),
    watermark text,
    late_drops bigint NOT NULL CHECK (late_drops >= 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment_id, query_id, pipeline_epoch, batch_id)
);
