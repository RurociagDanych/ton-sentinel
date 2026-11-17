ALTER TABLE outbox
    ADD COLUMN IF NOT EXISTS delivery_topic text,
    ADD COLUMN IF NOT EXISTS delivery_envelope jsonb;

-- Legacy/unconfigured intents stay unbound: migration cannot prove which
-- delivery contract existed at their acceptance boundary.
CREATE OR REPLACE FUNCTION protect_outbox_delivery_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.event_id, NEW.payload, NEW.delivery_topic,
           NEW.delivery_topic_incarnation, NEW.replay_epoch, NEW.delivery_envelope)
       IS DISTINCT FROM
       ROW(OLD.event_id, OLD.payload, OLD.delivery_topic,
           OLD.delivery_topic_incarnation, OLD.replay_epoch, OLD.delivery_envelope)
    THEN
        RAISE EXCEPTION 'accepted outbox delivery binding is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS outbox_delivery_binding_immutable ON outbox;
CREATE TRIGGER outbox_delivery_binding_immutable
BEFORE UPDATE ON outbox
FOR EACH ROW EXECUTE FUNCTION protect_outbox_delivery_binding();
