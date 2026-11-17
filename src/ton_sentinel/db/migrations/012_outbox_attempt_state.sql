DO $$
DECLARE
    had_publication_state boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'outbox'::regclass
          AND attname = 'publication_state'
          AND NOT attisdropped
    ) INTO had_publication_state;

    ALTER TABLE outbox
        ADD COLUMN IF NOT EXISTS publication_state text NOT NULL DEFAULT 'pending'
            CHECK (publication_state IN ('pending', 'attempted', 'ambiguous', 'acknowledged')),
        ADD COLUMN IF NOT EXISTS publication_attempts integer NOT NULL DEFAULT 0
            CHECK (publication_attempts >= 0),
        ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz,
        ADD COLUMN IF NOT EXISTS acknowledged_at timestamptz;

    -- Only the first upgrade from the legacy schema may infer acknowledgement
    -- from its historical published_at marker. A replay must not launder a
    -- partial marker created under the lifecycle schema.
    IF NOT had_publication_state THEN
        UPDATE outbox
        SET publication_state='acknowledged', acknowledged_at=published_at
        WHERE published_at IS NOT NULL;
    END IF;

    -- Clear malformed partial markers from older versions without inventing
    -- an acknowledgement. Keep attempted/ambiguous lifecycle state intact.
    UPDATE outbox
    SET publication_state=CASE WHEN publication_state='acknowledged'
                               THEN 'pending' ELSE publication_state END,
        published_at=NULL,
        acknowledged_at=NULL
    WHERE (publication_state='acknowledged'
           AND (published_at IS NULL OR acknowledged_at IS NULL))
       OR (publication_state<>'acknowledged'
           AND (published_at IS NOT NULL OR acknowledged_at IS NOT NULL));

    ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_publication_ack_consistent;
    ALTER TABLE outbox ADD CONSTRAINT outbox_publication_ack_consistent
        CHECK ((publication_state='acknowledged') = (published_at IS NOT NULL)
           AND (publication_state='acknowledged') = (acknowledged_at IS NOT NULL));
END
$$;

CREATE INDEX IF NOT EXISTS outbox_unacknowledged_order_idx
    ON outbox(created_at, event_id COLLATE "C")
    WHERE publication_state <> 'acknowledged';
