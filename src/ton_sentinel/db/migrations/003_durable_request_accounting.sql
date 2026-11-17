ALTER TABLE source_ranges
    ADD COLUMN IF NOT EXISTS request_count bigint NOT NULL DEFAULT 0
        CHECK (request_count >= 0);
