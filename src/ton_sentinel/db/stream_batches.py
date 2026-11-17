"""Durable local checkpoint identity for the canonical streaming query."""
from __future__ import annotations

import json
import os
from pathlib import Path


_IDENTITY_FILE = "_ton_sentinel_identity.json"
_IDENTITY_FIELDS = frozenset({
    "environment_id", "query_id", "pipeline_epoch", "topic",
    "topic_incarnation", "replay_epoch",
})


def checkpoint_identity(*, environment_id: str, query_id: str, pipeline_epoch: str,
                         topic: str, topic_incarnation: str, replay_epoch: int) -> dict:
    identity = {
        "environment_id": environment_id,
        "query_id": query_id,
        "pipeline_epoch": pipeline_epoch,
        "topic": topic,
        "topic_incarnation": topic_incarnation,
        "replay_epoch": replay_epoch,
    }
    for name, value in identity.items():
        if name == "replay_epoch":
            if type(value) is not int or value < 0:
                raise ValueError("replay_epoch must be a nonnegative integer")
        elif type(value) is not str or not value.strip() or len(value) > 256:
            raise ValueError(f"{name} must be explicit nonempty text of at most 256 characters")
    return identity


def _validated_identity(identity: object) -> dict:
    if type(identity) is not dict or identity.keys() != _IDENTITY_FIELDS:
        raise ValueError("checkpoint identity has an invalid schema")
    try:
        return checkpoint_identity(**identity)
    except (TypeError, ValueError) as exc:
        raise ValueError("checkpoint identity has invalid values") from exc


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate checkpoint identity field")
        result[key] = value
    return result


def bind_checkpoint(checkpoint: Path, identity: dict) -> None:
    """Bind an empty local checkpoint, or fail closed on any identity mismatch."""
    identity = _validated_identity(identity)
    checkpoint.mkdir(parents=True, exist_ok=True)
    identity_path = checkpoint / _IDENTITY_FILE
    expected = json.dumps(identity, sort_keys=True, separators=(",", ":")) + "\n"
    if identity_path.exists():
        try:
            actual = json.loads(
                identity_path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object
            )
            actual = _validated_identity(actual)
        except (OSError, ValueError) as exc:
            raise ValueError("checkpoint identity is unreadable; refusing reuse") from exc
        if actual != identity:
            raise ValueError("checkpoint identity mismatch; refusing reuse")
        return
    if any(checkpoint.iterdir()):
        raise ValueError("unverified populated checkpoint has no identity; refusing reuse")
    try:
        descriptor = os.open(identity_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # Another local process won the atomic create; verify its complete value.
        bind_checkpoint(checkpoint, identity)
        return
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(expected)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        identity_path.unlink(missing_ok=True)
        raise