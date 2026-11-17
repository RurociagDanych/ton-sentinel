ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS delivery_topic_incarnation text,
    ADD COLUMN IF NOT EXISTS replay_epoch bigint NOT NULL DEFAULT 0
        CHECK (replay_epoch >= 0);

CREATE TABLE IF NOT EXISTS outbox_delivery_config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    topic text NOT NULL CHECK (length(topic) BETWEEN 1 AND 249),
    topic_incarnation text NOT NULL CHECK (length(topic_incarnation) BETWEEN 1 AND 256),
    replay_epoch bigint NOT NULL CHECK (replay_epoch >= 0)
);

CREATE TABLE IF NOT EXISTS outbox_publication_frontier (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    end_exclusive bigint NOT NULL CHECK (end_exclusive >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
