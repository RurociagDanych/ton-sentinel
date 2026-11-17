-- Pause fenced
-- ingestion and record one durable, immutable snapshot/outbox/offset manifest per pause.
-- Additive only; no earlier migration is replaced, so nothing is superseded.

-- 1. A recovery pause: taken under the shared transition slot (lease fence), it allocates
--    a new recovery epoch and is only ever released, never changed or deleted.
CREATE TABLE IF NOT EXISTS recovery_pauses (
    pause_id bigserial PRIMARY KEY,
    recovery_epoch bigint NOT NULL UNIQUE CHECK (recovery_epoch > 0),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    paused_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    released_at timestamptz,
    release_owner text CHECK (release_owner IS NULL OR length(btrim(release_owner)) > 0),
    release_fence bigint CHECK (release_fence IS NULL OR release_fence > 0),
    CONSTRAINT recovery_pauses_release_complete CHECK (
        (released_at IS NULL) = (release_owner IS NULL)
        AND (released_at IS NULL) = (release_fence IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS recovery_pauses_one_active
    ON recovery_pauses ((true)) WHERE released_at IS NULL;

CREATE OR REPLACE FUNCTION recovery_lease_is_current(owner_ text, fence_ bigint) RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM lease_state WHERE singleton AND owner = owner_
                   AND fence = fence_ AND expires_at > clock_timestamp())
$$;

CREATE OR REPLACE FUNCTION recovery_pause_admit() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT recovery_lease_is_current(NEW.lease_owner, NEW.lease_fence) THEN
        RAISE EXCEPTION 'recovery pause fencing token superseded or expired';
    END IF;
    -- The database allocates the epoch; the caller holds the lease row lock, so this
    -- max+1 is serialized with every other fenced transition.
    SELECT coalesce(max(recovery_epoch), 0) + 1 INTO NEW.recovery_epoch FROM recovery_pauses;
    NEW.paused_at := clock_timestamp();
    NEW.released_at := NULL;
    NEW.release_owner := NULL;
    NEW.release_fence := NULL;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS recovery_pauses_admit ON recovery_pauses;
CREATE TRIGGER recovery_pauses_admit BEFORE INSERT ON recovery_pauses
FOR EACH ROW EXECUTE FUNCTION recovery_pause_admit();

CREATE OR REPLACE FUNCTION recovery_pause_release_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.released_at IS NULL AND NEW.released_at IS NOT NULL
       AND (NEW.pause_id, NEW.recovery_epoch, NEW.lease_owner, NEW.lease_fence, NEW.paused_at)
           IS NOT DISTINCT FROM
           (OLD.pause_id, OLD.recovery_epoch, OLD.lease_owner, OLD.lease_fence, OLD.paused_at)
    THEN
        IF NOT recovery_lease_is_current(NEW.release_owner, NEW.release_fence) THEN
            RAISE EXCEPTION 'recovery pause fencing token superseded or expired';
        END IF;
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'recovery pauses are only released, never changed or deleted';
END $$;
DROP TRIGGER IF EXISTS recovery_pauses_release_only ON recovery_pauses;
CREATE TRIGGER recovery_pauses_release_only BEFORE UPDATE OR DELETE ON recovery_pauses
FOR EACH ROW EXECUTE FUNCTION recovery_pause_release_only();
DROP TRIGGER IF EXISTS recovery_pauses_no_truncate ON recovery_pauses;
CREATE TRIGGER recovery_pauses_no_truncate BEFORE TRUNCATE ON recovery_pauses
FOR EACH STATEMENT EXECUTE FUNCTION recovery_pause_release_only();

-- 2. While a recovery pause is active, every writer of the cut's state (ingestion,
--    dispatcher, consumer callback, repair and retention promotion) is refused by the
--    database, not merely assumed idle. Row-level, so migration replays that touch no
--    row (and ON CONFLICT seeding of singletons, guarded for UPDATE/DELETE only) still run.
CREATE OR REPLACE FUNCTION refuse_write_during_recovery_pause() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE active bigint;
BEGIN
    SELECT recovery_epoch INTO active FROM recovery_pauses WHERE released_at IS NULL;
    IF active IS NOT NULL THEN
        RAISE EXCEPTION 'writes to % are paused for a recovery cut (recovery epoch %)',
            TG_TABLE_NAME, active;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS canonical_actions_recovery_pause_guard ON canonical_actions;
CREATE TRIGGER canonical_actions_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON canonical_actions
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS canonical_current_recovery_pause_guard ON canonical_current;
CREATE TRIGGER canonical_current_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON canonical_current
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS canonical_revisions_recovery_pause_guard ON canonical_revisions;
CREATE TRIGGER canonical_revisions_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON canonical_revisions
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS scan_membership_recovery_pause_guard ON scan_membership;
CREATE TRIGGER scan_membership_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON scan_membership
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS source_ranges_recovery_pause_guard ON source_ranges;
CREATE TRIGGER source_ranges_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON source_ranges
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS outbox_recovery_pause_guard ON outbox;
CREATE TRIGGER outbox_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON outbox
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS outbox_publication_frontier_recovery_pause_guard ON outbox_publication_frontier;
CREATE TRIGGER outbox_publication_frontier_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON outbox_publication_frontier
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS outbox_publication_origin_recovery_pause_guard ON outbox_publication_origin;
CREATE TRIGGER outbox_publication_origin_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON outbox_publication_origin
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS outbox_delivery_config_recovery_pause_guard ON outbox_delivery_config;
CREATE TRIGGER outbox_delivery_config_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON outbox_delivery_config
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS event_identity_guards_recovery_pause_guard ON event_identity_guards;
CREATE TRIGGER event_identity_guards_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON event_identity_guards
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS identity_guard_horizons_recovery_pause_guard ON identity_guard_horizons;
CREATE TRIGGER identity_guard_horizons_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON identity_guard_horizons
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS retained_context_horizons_recovery_pause_guard ON retained_context_horizons;
CREATE TRIGGER retained_context_horizons_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON retained_context_horizons
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS delivery_quarantine_recovery_pause_guard ON delivery_quarantine;
CREATE TRIGGER delivery_quarantine_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON delivery_quarantine
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS revision_quarantine_recovery_pause_guard ON revision_quarantine;
CREATE TRIGGER revision_quarantine_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON revision_quarantine
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS scan_quarantine_recovery_pause_guard ON scan_quarantine;
CREATE TRIGGER scan_quarantine_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON scan_quarantine
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS dirty_windows_recovery_pause_guard ON dirty_windows;
CREATE TRIGGER dirty_windows_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON dirty_windows
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS source_epoch_transitions_recovery_pause_guard ON source_epoch_transitions;
CREATE TRIGGER source_epoch_transitions_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON source_epoch_transitions
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS repair_promotions_recovery_pause_guard ON repair_promotions;
CREATE TRIGGER repair_promotions_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON repair_promotions
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS served_features_recovery_pause_guard ON served_features;
CREATE TRIGGER served_features_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON served_features
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS stream_batches_recovery_pause_guard ON stream_batches;
CREATE TRIGGER stream_batches_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON stream_batches
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS retention_runs_recovery_pause_guard ON retention_runs;
CREATE TRIGGER retention_runs_recovery_pause_guard BEFORE INSERT OR UPDATE OR DELETE ON retention_runs
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS active_source_epoch_recovery_pause_guard ON active_source_epoch;
CREATE TRIGGER active_source_epoch_recovery_pause_guard BEFORE UPDATE OR DELETE ON active_source_epoch
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS ingest_state_recovery_pause_guard ON ingest_state;
CREATE TRIGGER ingest_state_recovery_pause_guard BEFORE UPDATE OR DELETE ON ingest_state
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS dependency_epoch_recovery_pause_guard ON dependency_epoch;
CREATE TRIGGER dependency_epoch_recovery_pause_guard BEFORE UPDATE OR DELETE ON dependency_epoch
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS canonical_generation_recovery_pause_guard ON canonical_generation;
CREATE TRIGGER canonical_generation_recovery_pause_guard BEFORE UPDATE OR DELETE ON canonical_generation
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS outbox_delivery_authority_recovery_pause_guard ON outbox_delivery_authority;
CREATE TRIGGER outbox_delivery_authority_recovery_pause_guard BEFORE UPDATE OR DELETE ON outbox_delivery_authority
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS token_universe_version_recovery_pause_guard ON token_universe_version;
CREATE TRIGGER token_universe_version_recovery_pause_guard BEFORE UPDATE OR DELETE ON token_universe_version
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();
DROP TRIGGER IF EXISTS active_detector_policy_recovery_pause_guard ON active_detector_policy;
CREATE TRIGGER active_detector_policy_recovery_pause_guard BEFORE UPDATE OR DELETE ON active_detector_policy
FOR EACH ROW EXECUTE FUNCTION refuse_write_during_recovery_pause();

-- 3. The durable cut: one per pause, immutable, with the paired snapshot/outbox/offset
--    manifest. The database refuses incomplete, stale or mismatched metadata on insert.
CREATE TABLE IF NOT EXISTS recovery_cuts (
    cut_id uuid PRIMARY KEY,
    pause_id bigint NOT NULL UNIQUE REFERENCES recovery_pauses(pause_id),
    recovery_epoch bigint NOT NULL CHECK (recovery_epoch > 0),
    lease_owner text NOT NULL CHECK (length(btrim(lease_owner)) > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    canonical_generation bigint NOT NULL CHECK (canonical_generation >= 0),
    topic text NOT NULL CHECK (length(topic) > 0),
    topic_incarnation text NOT NULL CHECK (length(topic_incarnation) > 0),
    replay_epoch bigint NOT NULL CHECK (replay_epoch >= 0),
    kafka_cluster_id text NOT NULL CHECK (length(kafka_cluster_id) > 0),
    kafka_topic_id text NOT NULL CHECK (length(kafka_topic_id) > 0),
    end_offsets jsonb NOT NULL,
    outbox_members jsonb NOT NULL CHECK (jsonb_typeof(outbox_members) = 'array'),
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION recovery_cut_admit() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE pause record; delivery record; generation_now bigint; live_members jsonb;
        bad_keys integer; key_count integer; max_key integer;
BEGIN
    SELECT * INTO pause FROM recovery_pauses WHERE pause_id = NEW.pause_id;
    IF pause.pause_id IS NULL OR pause.released_at IS NOT NULL
       OR pause.recovery_epoch <> NEW.recovery_epoch THEN
        RAISE EXCEPTION 'recovery cut requires the active recovery pause of its epoch';
    END IF;
    IF NOT recovery_lease_is_current(NEW.lease_owner, NEW.lease_fence)
       OR NEW.lease_fence < pause.lease_fence THEN
        RAISE EXCEPTION 'recovery cut fencing token superseded or expired';
    END IF;
    SELECT topic, topic_incarnation, replay_epoch INTO delivery
    FROM outbox_delivery_authority WHERE singleton;
    IF delivery.topic IS NULL
       OR (delivery.topic, delivery.topic_incarnation, delivery.replay_epoch)
          IS DISTINCT FROM (NEW.topic, NEW.topic_incarnation, NEW.replay_epoch) THEN
        RAISE EXCEPTION 'recovery cut delivery identity mismatch';
    END IF;
    SELECT generation INTO generation_now FROM canonical_generation WHERE singleton;
    IF generation_now IS DISTINCT FROM NEW.canonical_generation THEN
        RAISE EXCEPTION 'recovery cut canonical generation mismatch';
    END IF;
    IF jsonb_typeof(NEW.end_offsets) <> 'object' OR NEW.end_offsets = '{}'::jsonb THEN
        RAISE EXCEPTION 'recovery cut offsets incomplete: no partition end offsets';
    END IF;
    SELECT count(*) FILTER (WHERE e.key !~ '^(0|[1-9][0-9]{0,8})$'
                              OR jsonb_typeof(e.value) <> 'number'
                              OR e.value::text !~ '^(0|[1-9][0-9]*)$'),
           count(*)
      INTO bad_keys, key_count FROM jsonb_each(NEW.end_offsets) e;
    IF bad_keys > 0 THEN
        RAISE EXCEPTION 'recovery cut offsets incomplete: invalid partition or offset';
    END IF;
    SELECT max(key::integer) INTO max_key FROM jsonb_object_keys(NEW.end_offsets) AS k(key);
    IF max_key + 1 <> key_count THEN
        RAISE EXCEPTION 'recovery cut offsets incomplete: partitions are not 0..n-1';
    END IF;
    SELECT coalesce(jsonb_agg(jsonb_build_array(event_id, accepted_generation, publication_state)
                              ORDER BY event_id COLLATE "C"), '[]'::jsonb)
      INTO live_members FROM outbox;
    IF live_members IS DISTINCT FROM NEW.outbox_members THEN
        RAISE EXCEPTION 'recovery cut outbox membership mismatch';
    END IF;
    IF NOT (NEW.manifest ?& ARRAY['snapshot', 'outbox', 'kafka', 'fence']) THEN
        RAISE EXCEPTION 'recovery cut manifest incomplete';
    END IF;
    IF NEW.manifest #> '{kafka,end_offsets}' IS DISTINCT FROM NEW.end_offsets
       OR NEW.manifest #> '{snapshot,canonical_generation}'
          IS DISTINCT FROM to_jsonb(NEW.canonical_generation)
       OR NEW.manifest #> '{fence,fence}' IS DISTINCT FROM to_jsonb(NEW.lease_fence) THEN
        RAISE EXCEPTION 'recovery cut manifest mismatch';
    END IF;
    NEW.captured_at := clock_timestamp();
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS recovery_cuts_admit ON recovery_cuts;
CREATE TRIGGER recovery_cuts_admit BEFORE INSERT ON recovery_cuts
FOR EACH ROW EXECUTE FUNCTION recovery_cut_admit();

CREATE OR REPLACE FUNCTION reject_recovery_cut_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'recovery cuts are immutable';
END $$;
DROP TRIGGER IF EXISTS recovery_cuts_immutable ON recovery_cuts;
CREATE TRIGGER recovery_cuts_immutable BEFORE UPDATE OR DELETE ON recovery_cuts
FOR EACH ROW EXECUTE FUNCTION reject_recovery_cut_change();
DROP TRIGGER IF EXISTS recovery_cuts_no_truncate ON recovery_cuts;
CREATE TRIGGER recovery_cuts_no_truncate BEFORE TRUNCATE ON recovery_cuts
FOR EACH STATEMENT EXECUTE FUNCTION reject_recovery_cut_change();
