"""Pure contract tests for revision authorization (no database)."""
import json

import pytest

from ton_sentinel.contracts import (
    Change, RevisionPolicy, RevisionRejected, SourceEpoch, WindowKey,
    parse_authorization,
)

EPOCH = SourceEpoch("testnet", "s", "f", "b", "p")
import base64


def _id(n):
    return base64.b64encode(bytes([n]) * 32).decode("ascii")


def _event(n):
    return "ton-sentinel:event:v1:" + json.dumps(["testnet", _id(1), _id(n)],
                                                 separators=(",", ":"))


EVENT, EVENT2 = _event(2), _event(3)


def auth(**over):
    base = {
        "contract": "source_monotonic_revision", "authority_id": "source-owner",
        "epoch": json.loads(EPOCH.to_json()),
        "grants": [{"event_id": EVENT, "action": "revise", "digest": "sha256:" + "a" * 64,
                    "expected_generation": 3, "source_revision": 2,
                    "previous_source_revision": 1}],
    }
    base.update(over)
    return base


def test_valid_authorization_parses_to_typed_frozen_value():
    parsed = parse_authorization(auth())
    assert parsed.contract == "source_monotonic_revision"
    assert parsed.epoch == EPOCH
    grant = parsed.grants[EVENT]
    assert (grant.action, grant.expected_generation, grant.source_revision) == ("revise", 3, 2)
    with pytest.raises(Exception):
        parsed.contract = "owner_resolution"


@pytest.mark.parametrize("mutate", [
    lambda a: a.update(contract="anything"),
    lambda a: a.update(authority_id=" "),
    lambda a: a.update(unknown=1),
    lambda a: a.update(epoch={"network": "testnet"}),
    lambda a: a.update(grants=[]),
    lambda a: a.update(grants=a["grants"] * 2),
    lambda a: a["grants"][0].update(action="delete"),
    lambda a: a["grants"][0].update(source_revision=True),
    lambda a: a["grants"][0].update(source_revision=0),
    lambda a: a["grants"][0].update(expected_generation="3"),
    lambda a: a["grants"][0].pop("source_revision"),
    lambda a: a["grants"][0].pop("digest"),
    lambda a: a["grants"][0].update(extra=1),
    lambda a: a.update(trace_sets={"t": ["b", "a", "a"]}),
    lambda a: a.update(trace_sets={"t": "ab"}),
])
def test_malformed_authorization_fails_closed(mutate):
    raw = auth()
    mutate(raw)
    with pytest.raises(RevisionRejected):
        parse_authorization(raw)


def test_add_grant_has_no_expected_generation_and_retract_has_no_digest():
    raw = auth(contract="owner_resolution", grants=[
        {"event_id": EVENT, "action": "add", "digest": "sha256:" + "b" * 64},
        {"event_id": EVENT2, "action": "retract", "expected_generation": None},
    ])
    parsed = parse_authorization(raw)
    assert parsed.grants[EVENT].expected_generation is None
    assert parsed.grants[EVENT2].digest is None
    raw["grants"][0]["expected_generation"] = 1
    with pytest.raises(RevisionRejected):
        parse_authorization(raw)


def test_trace_sets_are_normalized_sorted_unique_tuples():
    parsed = parse_authorization(auth(trace_sets={"t": ["b", "a"]}))
    assert parsed.trace_sets == {"t": ("a", "b")}


def test_none_authorization_is_none():
    assert parse_authorization(None) is None


def test_window_key_math_and_baseline_dependent_span():
    key = WindowKey("testnet", "TON", 600)
    assert key.window_end == 900
    policy = RevisionPolicy(baseline_lookback_seconds=3600)
    # Windows whose trailing baseline contains [600,900): starts in (600, 600+3600].
    assert key.dependents_span(policy) == (900, 4500)
    for bad in (-1, 7, 1.5):
        with pytest.raises(ValueError):
            WindowKey("testnet", "TON", bad)
    with pytest.raises(ValueError):
        RevisionPolicy(baseline_lookback_seconds=100)


def test_change_requires_known_kind():
    Change("quarantine", EVENT, "unauthorized_change", None, (), ())
    with pytest.raises(ValueError):
        Change("deleted", EVENT, "x", None, (), ())
