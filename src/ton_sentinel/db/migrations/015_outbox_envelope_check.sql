-- Database-level validation of the persisted delivery contract: an outbox row is
-- either wholly unbound (legacy) or carries a version-1 envelope that exactly
-- equals its own identity, incarnation, replay epoch and action columns.
ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_delivery_envelope_consistent;
ALTER TABLE outbox ADD CONSTRAINT outbox_delivery_envelope_consistent CHECK (
    (delivery_topic IS NULL AND delivery_topic_incarnation IS NULL
     AND delivery_envelope IS NULL)
    OR (delivery_topic IS NOT NULL AND delivery_topic_incarnation IS NOT NULL
        AND delivery_envelope = jsonb_build_object(
            'envelope_version', 1, 'event_id', event_id,
            'topic_incarnation', delivery_topic_incarnation,
            'replay_epoch', replay_epoch, 'action', payload))
);
