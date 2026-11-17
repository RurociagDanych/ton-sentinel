-- Validate a durable recovery cut and resume only from an exact
-- fenced match of it. Additive only; no earlier migration is replaced, so nothing is superseded.
--
-- One resumption per cut, recorded in the same transaction that releases the cut's pause. It
-- binds the handoff lease, a new consumer pipeline epoch, the consumer start offsets (exactly
-- the cut's end offsets E) and the resolution of every ambiguous outbox member of the cut:
-- [event_id, partition, offset] when its send is found on the retained log (deduplicated, never
-- resent) or [event_id, null, null] when it must be redelivered by its original event id.
-- Not cut state: written while the pause is still active, so it carries no pause guard.
CREATE TABLE IF NOT EXISTS recovery_resumptions (
    cut_id uuid PRIMARY KEY REFERENCES recovery_cuts(cut_id),
    pause_id bigint NOT NULL UNIQUE REFERENCES recovery_pauses(pause_id),
    recovery_epoch bigint NOT NULL CHECK (recovery_epoch > 0),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    pipeline_epoch text NOT NULL UNIQUE
        CHECK (length(btrim(pipeline_epoch)) > 0 AND length(pipeline_epoch) <= 256),
    start_offsets jsonb NOT NULL CHECK (jsonb_typeof(start_offsets) = 'object'),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
    resolutions jsonb NOT NULL CHECK (jsonb_typeof(resolutions) = 'array'),
    resumed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION recovery_resumption_admit() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE cut record; pause record; latest bigint; delivery record; generation_now bigint;
        live_members jsonb; expected jsonb; presented jsonb; bad integer;
BEGIN
    SELECT * INTO cut FROM recovery_cuts WHERE cut_id = NEW.cut_id;
    IF cut.cut_id IS NULL OR cut.pause_id <> NEW.pause_id
       OR cut.recovery_epoch <> NEW.recovery_epoch
       OR cut.manifest_digest <> NEW.manifest_digest
       OR cut.end_offsets IS DISTINCT FROM NEW.start_offsets THEN
        RAISE EXCEPTION 'recovery resume does not match its cut exactly';
    END IF;
    SELECT * INTO pause FROM recovery_pauses WHERE pause_id = cut.pause_id;
    SELECT max(recovery_epoch) INTO latest FROM recovery_pauses;
    IF pause.released_at IS NOT NULL OR latest IS DISTINCT FROM cut.recovery_epoch THEN
        RAISE EXCEPTION 'recovery resume refused: stale cut (pause released or newer epoch)';
    END IF;
    IF NOT recovery_lease_is_current(NEW.lease_owner, NEW.lease_fence)
       OR NEW.lease_fence <= cut.lease_fence THEN
        RAISE EXCEPTION 'recovery resume fencing token superseded or expired, or not a handoff '
                        'after the cut';
    END IF;
    IF EXISTS (SELECT 1 FROM stream_batches WHERE pipeline_epoch = NEW.pipeline_epoch) THEN
        RAISE EXCEPTION 'recovery resume pipeline epoch already used by a consumer lineage';
    END IF;
    SELECT topic, topic_incarnation, replay_epoch INTO delivery
    FROM outbox_delivery_authority WHERE singleton;
    SELECT generation INTO generation_now FROM canonical_generation WHERE singleton;
    SELECT coalesce(jsonb_agg(jsonb_build_array(event_id, accepted_generation, publication_state)
                              ORDER BY event_id COLLATE "C"), '[]'::jsonb)
      INTO live_members FROM outbox;
    IF (delivery.topic, delivery.topic_incarnation, delivery.replay_epoch)
          IS DISTINCT FROM (cut.topic, cut.topic_incarnation, cut.replay_epoch)
       OR generation_now IS DISTINCT FROM cut.canonical_generation
       OR live_members IS DISTINCT FROM cut.outbox_members THEN
        RAISE EXCEPTION 'recovery resume refused: stale cut (live state differs from the cut)';
    END IF;
    -- Every ambiguous member of the cut is resolved exactly once, and nothing else is.
    SELECT coalesce(jsonb_agg(m -> 0 ORDER BY (m ->> 0) COLLATE "C"), '[]'::jsonb) INTO expected
      FROM jsonb_array_elements(cut.manifest #> '{outbox,members}') m
     WHERE m ->> 3 = 'ambiguous';
    SELECT coalesce(jsonb_agg(r -> 0 ORDER BY (r ->> 0) COLLATE "C"), '[]'::jsonb),
           count(*) FILTER (WHERE CASE
                WHEN jsonb_typeof(r) <> 'array' THEN true
                WHEN jsonb_array_length(r) <> 3 OR jsonb_typeof(r -> 0) <> 'string' THEN true
                WHEN jsonb_typeof(r -> 1) = 'null' THEN jsonb_typeof(r -> 2) <> 'null'
                ELSE jsonb_typeof(r -> 1) <> 'number' OR jsonb_typeof(r -> 2) <> 'number' END)
      INTO presented, bad FROM jsonb_array_elements(NEW.resolutions) r;
    IF bad > 0 OR presented IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'recovery resume must resolve exactly the ambiguous members of its cut';
    END IF;
    NEW.resumed_at := clock_timestamp();
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS recovery_resumptions_admit ON recovery_resumptions;
CREATE TRIGGER recovery_resumptions_admit BEFORE INSERT ON recovery_resumptions
FOR EACH ROW EXECUTE FUNCTION recovery_resumption_admit();

CREATE OR REPLACE FUNCTION reject_recovery_resumption_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'recovery resumptions are immutable';
END $$;
DROP TRIGGER IF EXISTS recovery_resumptions_immutable ON recovery_resumptions;
CREATE TRIGGER recovery_resumptions_immutable BEFORE UPDATE OR DELETE ON recovery_resumptions
FOR EACH ROW EXECUTE FUNCTION reject_recovery_resumption_change();
DROP TRIGGER IF EXISTS recovery_resumptions_no_truncate ON recovery_resumptions;
CREATE TRIGGER recovery_resumptions_no_truncate BEFORE TRUNCATE ON recovery_resumptions
FOR EACH STATEMENT EXECUTE FUNCTION reject_recovery_resumption_change();
