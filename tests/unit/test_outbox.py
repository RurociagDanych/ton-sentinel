import pytest

from ton_sentinel.outbox import configure_delivery, publish_pending


def test_delivery_configuration_validates_identity_before_database_access():
    class NoConnection:
        def execute(self, *args):
            raise AssertionError("invalid config must fail before database access")

    with pytest.raises(ValueError):
        configure_delivery(NoConnection(), topic="", topic_incarnation="inc")
    with pytest.raises(ValueError):
        configure_delivery(NoConnection(), topic="actions", topic_incarnation="inc", replay_epoch=True)


def test_publish_limit_is_bounded_before_database_access():
    class NoConnection:
        def execute(self, *args):
            raise AssertionError("invalid limit must fail before database access")

    with pytest.raises(ValueError):
        publish_pending(NoConnection(), None, None, limit=0)


@pytest.mark.parametrize("max_retries", [-1, 11, True, 1.5])
def test_publish_retry_budget_is_bounded_before_database_access(max_retries):
    class NoConnection:
        def execute(self, *args):
            raise AssertionError("invalid retry budget must fail before database access")

    with pytest.raises(ValueError, match="max_retries"):
        publish_pending(NoConnection(), None, None, max_retries=max_retries)


@pytest.mark.parametrize("config", [None, {}, {"acks": 0}, {"acks": 1},
                                    {"acks": "all"}, {"acks": True}])
def test_publish_rejects_unsafe_or_absent_ack_config_before_side_effects(config):
    class NoConnection:
        def execute(self, *args):
            raise AssertionError("unsafe producer must fail before database access")

    class NoSend:
        def __init__(self):
            self.config = config

        def send(self, *args, **kwargs):
            raise AssertionError("unsafe producer must fail before Kafka send")

    with pytest.raises(ValueError, match="acks='all'"):
        publish_pending(NoConnection(), NoSend(), object())


def test_publish_accepts_kafka_python_normalized_all_ack_value():
    class NoConnection:
        info = type("Info", (), {"transaction_status": type("Status", (), {"name": "IDLE"})()})()

        def execute(self, *args):
            raise AssertionError("database is reached only after producer guard")

    class Producer:
        config = {"acks": -1}

    with pytest.raises(AssertionError, match="database is reached"):
        publish_pending(NoConnection(), Producer(), object())


def _envelope(**over):
    from ton_sentinel.outbox import build_delivery_envelope
    base = dict(event_id="e1", topic_incarnation="inc", replay_epoch=3,
                action={"epoch": {"network": "testnet"}, "x": 1})
    base.update(over)
    return build_delivery_envelope(**base)


def test_envelope_is_versioned_and_carries_identity_incarnation_epoch_and_action():
    assert _envelope() == {
        "envelope_version": 1, "event_id": "e1", "topic_incarnation": "inc",
        "replay_epoch": 3, "action": {"epoch": {"network": "testnet"}, "x": 1}}


@pytest.mark.parametrize("over", [
    {"event_id": ""}, {"event_id": 1}, {"topic_incarnation": ""},
    {"replay_epoch": -1}, {"replay_epoch": True}, {"action": []}])
def test_envelope_builder_rejects_invalid_identity_fields(over):
    with pytest.raises(ValueError):
        _envelope(**over)


@pytest.mark.parametrize("mutate", [
    lambda e: e.update(envelope_version=2), lambda e: e.update(event_id="other"),
    lambda e: e.update(topic_incarnation="x"), lambda e: e.update(replay_epoch=4),
    lambda e: e.update(action={"y": 2}), lambda e: e.update(extra=1),
    lambda e: e.pop("action")])
def test_envelope_validation_rejects_any_drift_from_persisted_columns(mutate):
    from ton_sentinel.outbox import validate_delivery_envelope
    stored = _envelope()
    validate_delivery_envelope(stored, event_id="e1", topic_incarnation="inc",
                               replay_epoch=3, action={"epoch": {"network": "testnet"}, "x": 1})
    drifted = dict(stored)
    mutate(drifted)
    with pytest.raises(ValueError, match="envelope"):
        validate_delivery_envelope(drifted, event_id="e1", topic_incarnation="inc",
                                   replay_epoch=3, action={"epoch": {"network": "testnet"}, "x": 1})


# Canonical accepted generation

def test_generation_envelope_is_version_2_and_carries_accepted_generation():
    assert _envelope(accepted_generation=5) == {
        "envelope_version": 2, "event_id": "e1", "topic_incarnation": "inc",
        "replay_epoch": 3, "accepted_generation": 5,
        "action": {"epoch": {"network": "testnet"}, "x": 1}}


@pytest.mark.parametrize("generation", [0, -1, True, 1.5, "1"])
def test_generation_envelope_rejects_non_positive_or_non_integer_generation(generation):
    with pytest.raises(ValueError):
        _envelope(accepted_generation=generation)


@pytest.mark.parametrize("mutate", [
    lambda e: e.update(accepted_generation=6), lambda e: e.pop("accepted_generation"),
    lambda e: e.update(envelope_version=1), lambda e: e.update(accepted_generation=True)])
def test_generation_envelope_validation_rejects_generation_drift_or_downgrade(mutate):
    from ton_sentinel.outbox import validate_delivery_envelope
    fields = dict(event_id="e1", topic_incarnation="inc", replay_epoch=3,
                  action={"epoch": {"network": "testnet"}, "x": 1})
    stored = _envelope(accepted_generation=5)
    validate_delivery_envelope(stored, accepted_generation=5, **fields)
    drifted = dict(stored)
    mutate(drifted)
    with pytest.raises(ValueError, match="envelope"):
        validate_delivery_envelope(drifted, accepted_generation=5, **fields)


def test_legacy_envelope_cannot_be_validated_as_generation_bearing():
    from ton_sentinel.outbox import validate_delivery_envelope
    with pytest.raises(ValueError, match="envelope"):
        validate_delivery_envelope(_envelope(), event_id="e1", topic_incarnation="inc",
                                   replay_epoch=3, accepted_generation=1,
                                   action={"epoch": {"network": "testnet"}, "x": 1})


def test_broker_envelope_shape_accepts_only_v1_or_generation_bearing_v2():
    from ton_sentinel.envelope import broker_envelope_version
    assert broker_envelope_version(_envelope()) == 1
    assert broker_envelope_version(_envelope(accepted_generation=2)) == 2
    for bad in ({**_envelope(), "accepted_generation": 2},
                {**_envelope(accepted_generation=2), "envelope_version": 1},
                {**_envelope(accepted_generation=2), "accepted_generation": 0},
                {**_envelope(accepted_generation=2), "accepted_generation": True},
                {**_envelope(), "envelope_version": 3}, [], None):
        with pytest.raises(ValueError):
            broker_envelope_version(bad)


def test_migrate_skips_migrations_recorded_as_superseded():
    import importlib
    migrate_module = importlib.import_module("ton_sentinel.db.migrate")

    class Recorder:
        def __init__(self):
            self.executed = []

        def execute(self, sql, params=None):
            self.executed.append(sql)

            class Result:
                def fetchone(self):
                    return ("schema_migration_supersessions",)

                def fetchall(self):
                    return [("015_outbox_envelope_check.sql",)]
            return Result()

        def commit(self):
            pass

    recorder = Recorder()
    migrate_module.migrate(recorder)
    files = sorted(migrate_module.Path(migrate_module.__file__).with_name("migrations")
                   .glob("[0-9][0-9][0-9]_*.sql"))
    executed = set(recorder.executed)
    assert (files[0].read_text() in executed)
    check015 = next(path for path in files if path.name == "015_outbox_envelope_check.sql")
    assert check015.read_text() not in executed
    assert any(path.name.startswith("016_") and path.read_text() in executed for path in files)
