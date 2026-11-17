CREATE TABLE IF NOT EXISTS outbox_delivery_authority (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    topic text NOT NULL CHECK (length(topic) BETWEEN 1 AND 249),
    topic_incarnation text NOT NULL CHECK (length(topic_incarnation) BETWEEN 1 AND 256),
    replay_epoch bigint NOT NULL CHECK (replay_epoch >= 0)
);

DO $$
DECLARE
    identity_count bigint;
BEGIN
    SELECT count(*) INTO identity_count
    FROM (
        SELECT topic, topic_incarnation, replay_epoch FROM outbox_delivery_authority
        UNION
        SELECT topic, topic_incarnation, replay_epoch FROM outbox_delivery_config
        UNION
        SELECT delivery_topic, delivery_topic_incarnation, replay_epoch
        FROM outbox
        WHERE delivery_topic IS NOT NULL OR delivery_topic_incarnation IS NOT NULL
           OR delivery_envelope IS NOT NULL
    ) identities;
    IF identity_count > 1 THEN
        RAISE EXCEPTION 'conflicting outbox delivery authority evidence';
    END IF;
END
$$;

INSERT INTO outbox_delivery_authority(singleton, topic, topic_incarnation, replay_epoch)
SELECT true, topic, topic_incarnation, replay_epoch
FROM (
    SELECT topic, topic_incarnation, replay_epoch FROM outbox_delivery_config
    UNION
    SELECT delivery_topic, delivery_topic_incarnation, replay_epoch
    FROM outbox
    WHERE delivery_topic IS NOT NULL OR delivery_topic_incarnation IS NOT NULL
       OR delivery_envelope IS NOT NULL
) evidence
LIMIT 1
ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION protect_outbox_delivery_authority() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'outbox delivery authority is immutable';
END;
$$;

DROP TRIGGER IF EXISTS outbox_delivery_authority_immutable ON outbox_delivery_authority;
CREATE TRIGGER outbox_delivery_authority_immutable
BEFORE UPDATE OR DELETE ON outbox_delivery_authority
FOR EACH ROW EXECUTE FUNCTION protect_outbox_delivery_authority();

DROP TRIGGER IF EXISTS outbox_delivery_authority_no_truncate ON outbox_delivery_authority;
CREATE TRIGGER outbox_delivery_authority_no_truncate
BEFORE TRUNCATE ON outbox_delivery_authority
FOR EACH STATEMENT EXECUTE FUNCTION protect_outbox_delivery_authority();
