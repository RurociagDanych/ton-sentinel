import json

import pytest

from ton_sentinel.db.stream_batches import bind_checkpoint, checkpoint_identity


def _identity(**changes):
    identity = {
        "environment_id": "local-test",
        "query_id": "canonical-features",
        "pipeline_epoch": "epoch-a",
        "topic": "canonical",
        "topic_incarnation": "incarnation-a",
        "replay_epoch": 0,
    }
    identity.update(changes)
    return checkpoint_identity(**identity)


def test_checkpoint_identity_is_bound_once_and_mismatch_fails_closed(tmp_path):
    checkpoint = tmp_path / "checkpoint"
    original = _identity()

    bind_checkpoint(checkpoint, original)
    bind_checkpoint(checkpoint, original)

    assert json.loads((checkpoint / "_ton_sentinel_identity.json").read_text()) == original
    with pytest.raises(ValueError, match="identity mismatch"):
        bind_checkpoint(checkpoint, _identity(replay_epoch=1))
    with pytest.raises(ValueError, match="identity mismatch"):
        bind_checkpoint(checkpoint, _identity(topic_incarnation="incarnation-b"))
    with pytest.raises(ValueError, match="identity mismatch"):
        bind_checkpoint(checkpoint, _identity(pipeline_epoch="epoch-b"))


def test_checkpoint_without_identity_cannot_adopt_existing_spark_state(tmp_path):
    checkpoint = tmp_path / "populated"
    checkpoint.mkdir()
    (checkpoint / "offsets").mkdir()

    with pytest.raises(ValueError, match="unverified populated checkpoint"):
        bind_checkpoint(checkpoint, _identity())
    assert not (checkpoint / "_ton_sentinel_identity.json").exists()


@pytest.mark.parametrize("replay_epoch", [False, 0.0])
def test_checkpoint_rejects_malformed_persisted_replay_epoch_without_mutation(
    tmp_path, replay_epoch
):
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    identity_path = checkpoint / "_ton_sentinel_identity.json"
    malformed = _identity()
    malformed["replay_epoch"] = replay_epoch
    original_bytes = (json.dumps(malformed) + "\n").encode()
    identity_path.write_bytes(original_bytes)

    with pytest.raises(ValueError, match="identity"):
        bind_checkpoint(checkpoint, _identity())

    assert identity_path.read_bytes() == original_bytes
    assert sorted(path.name for path in checkpoint.iterdir()) == [identity_path.name]


@pytest.mark.parametrize("malformed", [
    '{"environment_id":"local-test","environment_id":"local-test",'
    '"query_id":"canonical-features","pipeline_epoch":"epoch-a",'
    '"topic":"canonical","topic_incarnation":"incarnation-a","replay_epoch":0}\n',
    '{"environment_id":"local-test","query_id":"canonical-features",'
    '"pipeline_epoch":"epoch-a","topic":"canonical",'
    '"topic_incarnation":"incarnation-a","replay_epoch":0,"extra":1}\n',
    '{"environment_id":"local-test","query_id":"canonical-features",'
    '"pipeline_epoch":"epoch-a","topic":"canonical",'
    '"topic_incarnation":"incarnation-a"}\n',
])
def test_checkpoint_rejects_malformed_persisted_schema_without_mutation(tmp_path, malformed):
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    identity_path = checkpoint / "_ton_sentinel_identity.json"
    original_bytes = malformed.encode()
    identity_path.write_bytes(original_bytes)

    with pytest.raises(ValueError, match="identity"):
        bind_checkpoint(checkpoint, _identity())

    assert identity_path.read_bytes() == original_bytes
    assert sorted(path.name for path in checkpoint.iterdir()) == [identity_path.name]


@pytest.mark.parametrize("changes", [
    {"replay_epoch": False},
    {"replay_epoch": 0.0},
    {"unexpected": "field"},
])
def test_invalid_caller_identity_fails_before_checkpoint_creation(tmp_path, changes):
    checkpoint = tmp_path / "not-created"
    identity = _identity()
    identity.update(changes)

    with pytest.raises(ValueError, match="identity"):
        bind_checkpoint(checkpoint, identity)

    assert not checkpoint.exists()


def test_checkpoint_identity_never_accepts_blank_or_secret_locator_as_identity(tmp_path):
    with pytest.raises(ValueError, match="environment_id"):
        _identity(environment_id=" ")
    with pytest.raises(ValueError, match="replay_epoch"):
        _identity(replay_epoch=-1)