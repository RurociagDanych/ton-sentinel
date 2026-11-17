CREATE TABLE IF NOT EXISTS stream_batches (
    environment_id text NOT NULL CHECK (length(environment_id) > 0),
    query_id text NOT NULL CHECK (length(query_id) > 0),
    pipeline_epoch text NOT NULL CHECK (length(pipeline_epoch) > 0),
    batch_id bigint NOT NULL CHECK (batch_id >= 0),
    topic text NOT NULL CHECK (length(topic) > 0),
    topic_incarnation text NOT NULL CHECK (length(topic_incarnation) > 0),
    replay_epoch bigint NOT NULL CHECK (replay_epoch >= 0),
    offset_metadata jsonb NOT NULL,
    record_digest text NOT NULL CHECK (length(record_digest) = 64),
    record_count integer NOT NULL CHECK (record_count >= 0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (environment_id, query_id, pipeline_epoch, batch_id)
);
