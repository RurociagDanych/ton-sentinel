-- Canonical accepted generation.
-- 
--
-- One database-wide counter orders accepted canonical decisions. A new
-- canonical change receives counter+1 from a BEFORE INSERT trigger inside the
-- accepting transaction: rollback releases the value, the singleton row lock
-- serializes concurrent allocators, and committed generations are therefore
-- unique, gap-free and in commit order. The value is local decision ordering,
-- never proof of upstream chronology. Rows accepted before this migration keep
-- a NULL (unknown) generation; none is fabricated retroactively.

CREATE TABLE IF NOT EXISTS canonical_generation (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation bigint NOT NULL CHECK (generation >= 0)
);
INSERT INTO canonical_generation(singleton, generation) VALUES (true, 0)
ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION protect_canonical_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.singleton AND OLD.singleton
       AND NEW.generation = OLD.generation + 1 THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'canonical generation only advances by exactly one';
END;
$$;
DROP TRIGGER IF EXISTS canonical_generation_monotonic ON canonical_generation;
CREATE TRIGGER canonical_generation_monotonic
BEFORE UPDATE OR DELETE ON canonical_generation
FOR EACH ROW EXECUTE FUNCTION protect_canonical_generation();
DROP TRIGGER IF EXISTS canonical_generation_no_truncate ON canonical_generation;
CREATE TRIGGER canonical_generation_no_truncate
BEFORE TRUNCATE ON canonical_generation
FOR EACH STATEMENT EXECUTE FUNCTION protect_canonical_generation();

ALTER TABLE canonical_actions
    ADD COLUMN IF NOT EXISTS accepted_generation bigint CHECK (accepted_generation > 0);
CREATE UNIQUE INDEX IF NOT EXISTS canonical_actions_accepted_generation_key
    ON canonical_actions(accepted_generation);
ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS accepted_generation bigint CHECK (accepted_generation > 0);

CREATE OR REPLACE FUNCTION allocate_canonical_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.accepted_generation IS NOT NULL THEN
        RAISE EXCEPTION 'canonical generation is allocated by the database only';
    END IF;
    UPDATE canonical_generation SET generation = generation + 1
    WHERE singleton RETURNING generation INTO NEW.accepted_generation;
    IF NEW.accepted_generation IS NULL THEN
        RAISE EXCEPTION 'canonical generation state is not initialized';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS canonical_actions_allocate_generation ON canonical_actions;
CREATE TRIGGER canonical_actions_allocate_generation
BEFORE INSERT ON canonical_actions
FOR EACH ROW EXECUTE FUNCTION allocate_canonical_generation();

CREATE OR REPLACE FUNCTION protect_accepted_generation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.accepted_generation IS DISTINCT FROM OLD.accepted_generation THEN
        RAISE EXCEPTION 'accepted canonical generation is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS canonical_actions_generation_immutable ON canonical_actions;
CREATE TRIGGER canonical_actions_generation_immutable
BEFORE UPDATE ON canonical_actions
FOR EACH ROW EXECUTE FUNCTION protect_accepted_generation();
DROP TRIGGER IF EXISTS outbox_generation_immutable ON outbox;
CREATE TRIGGER outbox_generation_immutable
BEFORE UPDATE ON outbox
FOR EACH ROW EXECUTE FUNCTION protect_accepted_generation();

-- An outbox intent references exactly its canonical change's generation; a
-- generation-bearing change cannot be downgraded to a generation-less intent.
CREATE OR REPLACE FUNCTION check_outbox_generation_reference() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    canonical bigint;
BEGIN
    SELECT accepted_generation INTO canonical
    FROM canonical_actions WHERE event_id = NEW.event_id;
    IF canonical IS DISTINCT FROM NEW.accepted_generation THEN
        RAISE EXCEPTION 'outbox generation must equal its canonical generation';
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS outbox_generation_reference ON outbox;
CREATE TRIGGER outbox_generation_reference
AFTER INSERT ON outbox
FOR EACH ROW EXECUTE FUNCTION check_outbox_generation_reference();

-- Supersede 015's version-1-only CHECK (file unchanged). Every row is wholly
-- unbound legacy, a pre-generation version-1 envelope, or a version-2 envelope
-- that exactly equals its own identity, incarnation, epoch, generation and action.
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_delivery_envelope_consistent;
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_delivery_envelope_generation_consistent;
ALTER TABLE outbox ADD CONSTRAINT outbox_delivery_envelope_generation_consistent CHECK (
    (delivery_topic IS NULL AND delivery_topic_incarnation IS NULL
     AND delivery_envelope IS NULL AND accepted_generation IS NULL)
    OR (delivery_topic IS NOT NULL AND delivery_topic_incarnation IS NOT NULL
        AND accepted_generation IS NULL
        AND delivery_envelope = jsonb_build_object(
            'envelope_version', 1, 'event_id', event_id,
            'topic_incarnation', delivery_topic_incarnation,
            'replay_epoch', replay_epoch, 'action', payload))
    OR (delivery_topic IS NOT NULL AND delivery_topic_incarnation IS NOT NULL
        AND accepted_generation IS NOT NULL
        AND delivery_envelope = jsonb_build_object(
            'envelope_version', 2, 'event_id', event_id,
            'topic_incarnation', delivery_topic_incarnation,
            'replay_epoch', replay_epoch,
            'accepted_generation', accepted_generation, 'action', payload))
);

-- migrate() skips files recorded here; replaying 015 would re-add its
-- version-1-only CHECK and reject every generation-bearing intent.
CREATE TABLE IF NOT EXISTS schema_migration_supersessions (
    migration text PRIMARY KEY,
    superseded_by text NOT NULL
);
INSERT INTO schema_migration_supersessions(migration, superseded_by)
VALUES ('015_outbox_envelope_check.sql', '016_canonical_generation.sql')
ON CONFLICT (migration) DO NOTHING;
