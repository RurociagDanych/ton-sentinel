ALTER TABLE source_ranges ADD COLUMN IF NOT EXISTS corroborated boolean NOT NULL DEFAULT false;
ALTER TABLE source_ranges ALTER COLUMN corroborated SET DEFAULT false;

CREATE TABLE IF NOT EXISTS corroboration_groups (
    group_id uuid PRIMARY KEY,
    start_inclusive bigint NOT NULL,
    end_exclusive bigint NOT NULL,
    epoch jsonb NOT NULL,
    scan_policy jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('pending', 'corroborated', 'accepted')),
    walk_count integer NOT NULL DEFAULT 0 CHECK (walk_count BETWEEN 0 AND 3),
    accepted_range_id uuid REFERENCES source_ranges(range_id),
    request_count integer NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    page_count integer NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    action_count integer NOT NULL DEFAULT 0 CHECK (action_count >= 0),
    bytes_used bigint NOT NULL DEFAULT 0 CHECK (bytes_used >= 0),
    elapsed_ms bigint NOT NULL DEFAULT 0 CHECK (elapsed_ms >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (start_inclusive < end_exclusive)
);

CREATE TABLE IF NOT EXISTS corroboration_walks (
    group_id uuid NOT NULL REFERENCES corroboration_groups(group_id),
    walk_no integer NOT NULL CHECK (walk_no BETWEEN 1 AND 3),
    range_id uuid NOT NULL UNIQUE REFERENCES source_ranges(range_id),
    scan_policy jsonb NOT NULL,
    complete boolean NOT NULL DEFAULT false,
    key_set jsonb,
    PRIMARY KEY (group_id, walk_no),
    CHECK ((complete AND key_set IS NOT NULL) OR (NOT complete AND key_set IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS corroboration_one_range_per_walk
    ON corroboration_walks(range_id) WHERE complete;

-- Preserve only rows with verifiable persisted two-walk linkage; do not bless
-- historical standalone ranges merely because the former default was true.
UPDATE source_ranges r SET corroborated = EXISTS (
    SELECT 1 FROM corroboration_groups g
    JOIN corroboration_walks w ON w.group_id=g.group_id AND w.range_id=g.accepted_range_id
    WHERE g.accepted_range_id=r.range_id
      AND g.status IN ('corroborated','accepted') AND w.complete AND w.key_set IS NOT NULL
);

CREATE OR REPLACE FUNCTION enforce_corroboration_range_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.corroborated AND NOT EXISTS (
        SELECT 1 FROM corroboration_groups g
        JOIN corroboration_walks w ON w.group_id=g.group_id AND w.range_id=g.accepted_range_id
        JOIN corroboration_walks p ON p.group_id=w.group_id AND p.walk_no=w.walk_no-1
        JOIN source_ranges pr ON pr.range_id=p.range_id
        WHERE g.accepted_range_id=NEW.range_id
          AND g.status IN ('corroborated','accepted') AND w.complete AND p.complete
          AND w.key_set IS NOT NULL AND w.key_set=p.key_set
          AND NEW.status IN ('complete','accepted') AND pr.status IN ('complete','accepted')
          AND w.walk_no=g.walk_count AND w.walk_no BETWEEN 2 AND 3
    ) THEN
        RAISE EXCEPTION 'range cannot be corroborated without selected complete walk evidence';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS source_range_corroboration_gate ON source_ranges;
CREATE TRIGGER source_range_corroboration_gate
BEFORE INSERT OR UPDATE OF corroborated ON source_ranges
FOR EACH ROW EXECUTE FUNCTION enforce_corroboration_range_gate();

-- Terminal range state is written only after all promotion effects exist.
-- The deferred group check below makes the paired terminal transition atomic.
CREATE OR REPLACE FUNCTION enforce_source_range_acceptance() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected_keys jsonb; actual_keys jsonb;
BEGIN
    IF NEW.status<>'accepted' THEN
        IF TG_OP='UPDATE' AND OLD.status='accepted' THEN
            RAISE EXCEPTION 'accepted range status is terminal';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='INSERT' OR NOT NEW.corroborated THEN
        RAISE EXCEPTION 'accepted range requires atomic corroborated promotion';
    END IF;
    SELECT w.key_set INTO expected_keys
      FROM corroboration_groups g JOIN corroboration_walks w
        ON w.group_id=g.group_id AND w.range_id=g.accepted_range_id
      WHERE g.accepted_range_id=NEW.range_id
        AND g.status IN ('corroborated','accepted') AND w.complete;
    SELECT COALESCE(jsonb_agg(event_id ORDER BY event_id COLLATE "C"),'[]'::jsonb) INTO actual_keys
      FROM scan_membership WHERE range_id=NEW.range_id;
    IF expected_keys IS NULL OR expected_keys<>actual_keys
       OR NOT EXISTS (SELECT 1 FROM active_source_epoch WHERE singleton AND epoch=NEW.epoch)
       OR NOT EXISTS (SELECT 1 FROM ingest_state WHERE singleton AND cursor>=NEW.end_exclusive)
       OR EXISTS (
           SELECT 1 FROM scan_membership m JOIN canonical_actions c USING(event_id)
           LEFT JOIN outbox o USING(event_id) WHERE m.range_id=NEW.range_id
             AND (o.event_id IS NULL OR o.payload<>c.action)
       ) THEN
        RAISE EXCEPTION 'accepted range requires atomic canonical, membership, outbox and cursor effects';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS source_range_acceptance_gate ON source_ranges;
CREATE TRIGGER source_range_acceptance_gate
BEFORE INSERT OR UPDATE OF status ON source_ranges
FOR EACH ROW EXECUTE FUNCTION enforce_source_range_acceptance();

CREATE OR REPLACE FUNCTION enforce_accepted_range_group_commit() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status='accepted' AND NOT EXISTS (
        SELECT 1 FROM corroboration_groups WHERE accepted_range_id=NEW.range_id AND status='accepted'
    ) THEN
        RAISE EXCEPTION 'accepted range and group must commit atomically';
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS source_range_acceptance_commit ON source_ranges;
CREATE CONSTRAINT TRIGGER source_range_acceptance_commit
AFTER INSERT OR UPDATE OF status ON source_ranges DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_accepted_range_group_commit();

CREATE OR REPLACE FUNCTION enforce_corroboration_group_link() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND NEW.status<>OLD.status AND NOT (
        (OLD.status='pending' AND NEW.status='corroborated') OR
        (OLD.status='corroborated' AND NEW.status='accepted')
    ) THEN
        RAISE EXCEPTION 'invalid corroboration group state transition';
    END IF;
    IF TG_OP='INSERT' AND NEW.status<>'pending' THEN
        RAISE EXCEPTION 'corroboration groups must start pending';
    END IF;
    IF NEW.status='accepted' AND NOT EXISTS (
        SELECT 1 FROM source_ranges r WHERE r.range_id=NEW.accepted_range_id
          AND r.status='accepted' AND r.corroborated
    ) THEN
        RAISE EXCEPTION 'accepted group requires an accepted source range';
    END IF;
    IF NEW.status IN ('corroborated','accepted') AND NOT EXISTS (
        SELECT 1 FROM corroboration_walks w JOIN source_ranges r USING(range_id)
        JOIN corroboration_walks p ON p.group_id=w.group_id AND p.walk_no=w.walk_no-1
        JOIN source_ranges pr ON pr.range_id=p.range_id
        WHERE w.group_id=NEW.group_id AND w.range_id=NEW.accepted_range_id
          AND w.complete AND p.complete AND w.key_set IS NOT NULL AND w.key_set=p.key_set
          AND w.walk_no=NEW.walk_count AND w.walk_no BETWEEN 2 AND 3
          AND r.status IN ('complete','accepted') AND pr.status IN ('complete','accepted')
    ) THEN
        RAISE EXCEPTION 'group status requires a selected complete corroborated walk';
    END IF;
    IF TG_OP='UPDATE' AND
       (NEW.group_id,NEW.start_inclusive,NEW.end_exclusive,NEW.epoch,NEW.scan_policy)
       IS DISTINCT FROM
       (OLD.group_id,OLD.start_inclusive,OLD.end_exclusive,OLD.epoch,OLD.scan_policy) THEN
        RAISE EXCEPTION 'corroboration group manifest is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS corroboration_group_link_guard ON corroboration_groups;
CREATE TRIGGER corroboration_group_link_guard
BEFORE INSERT OR UPDATE ON corroboration_groups
FOR EACH ROW EXECUTE FUNCTION enforce_corroboration_group_link();

CREATE OR REPLACE FUNCTION enforce_corroboration_walk_link() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE g corroboration_groups%ROWTYPE; r source_ranges%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' AND (NEW.group_id,NEW.walk_no,NEW.range_id,NEW.scan_policy)
       IS DISTINCT FROM (OLD.group_id,OLD.walk_no,OLD.range_id,OLD.scan_policy) THEN
        RAISE EXCEPTION 'corroboration walk manifest is immutable';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.complete AND
       (NOT NEW.complete OR NEW.key_set IS DISTINCT FROM OLD.key_set) THEN
        RAISE EXCEPTION 'completed corroboration evidence is immutable';
    END IF;
    SELECT * INTO g FROM corroboration_groups WHERE group_id=NEW.group_id;
    SELECT * INTO r FROM source_ranges WHERE range_id=NEW.range_id;
    IF g.group_id IS NULL OR r.range_id IS NULL OR r.start_inclusive<>g.start_inclusive
       OR r.end_exclusive<>g.end_exclusive OR r.epoch<>g.epoch OR r.corroborated
       OR NEW.scan_policy<>r.scan_policy THEN
        RAISE EXCEPTION 'child walk does not match the persisted group manifest';
    END IF;
    IF NEW.complete AND (NEW.key_set IS NULL OR r.status<>'complete') THEN
        RAISE EXCEPTION 'complete walk requires a complete source range and key set';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS corroboration_walk_link_guard ON corroboration_walks;
CREATE TRIGGER corroboration_walk_link_guard
BEFORE INSERT OR UPDATE ON corroboration_walks
FOR EACH ROW EXECUTE FUNCTION enforce_corroboration_walk_link();

CREATE OR REPLACE FUNCTION protect_linked_range_manifest() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM corroboration_walks WHERE range_id=OLD.range_id)
       AND (NEW.start_inclusive,NEW.end_exclusive,NEW.epoch,NEW.scan_policy)
           IS DISTINCT FROM
           (OLD.start_inclusive,OLD.end_exclusive,OLD.epoch,OLD.scan_policy) THEN
        RAISE EXCEPTION 'linked source range manifest is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS source_range_manifest_guard ON source_ranges;
CREATE TRIGGER source_range_manifest_guard
BEFORE UPDATE OF start_inclusive,end_exclusive,epoch,scan_policy ON source_ranges
FOR EACH ROW EXECUTE FUNCTION protect_linked_range_manifest();
