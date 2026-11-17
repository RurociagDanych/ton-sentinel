"""Versioned delivery envelope persisted with each canonical change.

Version 2 carries the canonical accepted generation. Version 1 is the
pre-generation contract: it is still validated for intents bound before
migration 016 but is never written for a new acceptance.
"""
import json


LEGACY_ENVELOPE_VERSION = 1
ENVELOPE_VERSION = 2
_V1_FIELDS = frozenset({"envelope_version", "event_id", "topic_incarnation",
                        "replay_epoch", "action"})
_V2_FIELDS = _V1_FIELDS | {"accepted_generation"}


def _valid_generation(value) -> bool:
    return type(value) is int and value > 0


def build_delivery_envelope(*, event_id: str, topic_incarnation: str,
                            replay_epoch: int, action: dict,
                            accepted_generation: int | None = None) -> dict:
    """Return the envelope persisted with a canonical change.

    With an accepted generation the envelope is version 2; without one it is
    the legacy version-1 shape, used only to validate pre-generation intents.
    """
    if (type(event_id) is not str or not event_id
            or type(topic_incarnation) is not str or not topic_incarnation
            or type(replay_epoch) is not int or replay_epoch < 0
            or type(action) is not dict
            or (accepted_generation is not None
                and not _valid_generation(accepted_generation))):
        raise ValueError("invalid delivery envelope fields")
    envelope = {"envelope_version": LEGACY_ENVELOPE_VERSION, "event_id": event_id,
                "topic_incarnation": topic_incarnation, "replay_epoch": replay_epoch,
                "action": action}
    if accepted_generation is not None:
        envelope["envelope_version"] = ENVELOPE_VERSION
        envelope["accepted_generation"] = accepted_generation
    return envelope


def validate_delivery_envelope(envelope, *, event_id: str, topic_incarnation: str,
                               replay_epoch: int, action: dict,
                               accepted_generation: int | None = None) -> None:
    expected = build_delivery_envelope(
        event_id=event_id, topic_incarnation=topic_incarnation,
        replay_epoch=replay_epoch, action=action,
        accepted_generation=accepted_generation)
    if json.dumps(envelope, sort_keys=True) != json.dumps(expected, sort_keys=True):
        raise ValueError("invalid acceptance-bound delivery envelope")


def broker_envelope_version(envelope) -> int:
    """Return 1 or 2 for a structurally valid broker envelope; raise otherwise."""
    if type(envelope) is not dict:
        raise ValueError("broker envelope must be an object")
    version = envelope.get("envelope_version")
    if type(version) is int and version == LEGACY_ENVELOPE_VERSION and set(envelope) == _V1_FIELDS:
        return LEGACY_ENVELOPE_VERSION
    if (type(version) is int and version == ENVELOPE_VERSION and set(envelope) == _V2_FIELDS
            and _valid_generation(envelope["accepted_generation"])):
        return ENVELOPE_VERSION
    raise ValueError("broker envelope fields do not match publication contract")
