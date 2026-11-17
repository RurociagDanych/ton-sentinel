-- Token-first reads (/v1/tokens, /v1/series). The primary key leads with the
-- window, so a per-token time range was unbounded without this index. Additive and idempotent.
CREATE INDEX IF NOT EXISTS served_features_token_idx
    ON served_features (network, token COLLATE "C", window_start);
