"""Unit contract: frozen repair inputs are immutable, tamper-evident and fenced."""

import dataclasses
from datetime import datetime, timezone

import pytest

from ton_sentinel.contracts import Lease
from ton_sentinel.db.serving import WindowKey
from ton_sentinel.db.work import FrozenWork, freeze_work, promote

LEASE = Lease("repair", 7, datetime(2030, 1, 1, tzinfo=timezone.utc))
TARGET = WindowKey("testnet", 0, 300)


class _NoDatabase:
    def __getattr__(self, name):
        raise AssertionError(f"database touched: {name}")


def _frozen(**changes):
    base = dict(owner="repair", fence=7, epoch=3, targets=(TARGET,),
                inputs=(("e1", 4, "sha256:a", '{"x":1}'),), coverage=(), active_epoch=None,
                baseline=(), dirty=(), lookback=7 * 86400)
    base.update(changes)
    return FrozenWork.create(**base)


def test_frozen_work_is_immutable_and_holds_only_tuples():
    work = _frozen()
    with pytest.raises(dataclasses.FrozenInstanceError):
        work.epoch = 4
    with pytest.raises(dataclasses.FrozenInstanceError):
        work.inputs = ()
    for name in ("targets", "inputs", "coverage", "baseline", "dirty"):
        assert type(getattr(work, name)) is tuple


def test_digest_is_deterministic_and_changes_with_any_pinned_input():
    assert _frozen().input_digest == _frozen().input_digest
    assert _frozen().work_id != _frozen().work_id
    changed = [
        _frozen(epoch=4), _frozen(fence=8),
        _frozen(inputs=(("e1", 5, "sha256:a", '{"x":1}'),)),
        _frozen(inputs=(("e1", 4, "sha256:b", '{"x":1}'),)),
        _frozen(inputs=()), _frozen(coverage=(("c",),)), _frozen(active_epoch={"a": 1}),
        _frozen(baseline=(("b",),)), _frozen(dirty=(("testnet", "TON", 0, "target", 1),)),
    ]
    digests = {w.input_digest for w in changed} | {_frozen().input_digest}
    assert len(digests) == len(changed) + 1


def test_tampered_or_forged_digest_is_refused():
    work = _frozen()
    with pytest.raises(ValueError, match="digest"):
        dataclasses.replace(work, epoch=work.epoch + 1)
    with pytest.raises(ValueError, match="digest"):
        dataclasses.replace(work, input_digest="sha256:" + "0" * 64)


@pytest.mark.parametrize("targets", [
    [], [("testnet", 0, 300)], [WindowKey("testnet", 0, 300), WindowKey("testnet", 0, 300)],
    [WindowKey("testnet", 0, 301)], [WindowKey("devnet", 0, 300)]])
def test_freeze_rejects_bad_targets_before_touching_the_database(targets):
    with pytest.raises(ValueError):
        freeze_work(_NoDatabase(), LEASE, targets)


def test_promote_returns_false_for_a_different_lease_without_touching_the_database():
    other = Lease("repair", 8, LEASE.expires_at)
    assert promote(_NoDatabase(), other, _frozen(), []) is False
    assert promote(_NoDatabase(), Lease("intruder", 7, LEASE.expires_at), _frozen(), []) is False


def test_promote_refuses_features_outside_the_frozen_windows_before_the_database():
    row = {"network": "testnet", "window_start": 300, "window_end": 600, "token": "A",
           "generation": "g", "swaps": 1, "input_raw": 1, "output_raw": None, "known_traders": 0,
           "unknown_count": 1, "coverage_status": None, "status": "provisional",
           "provisional": True}
    with pytest.raises(ValueError, match="frozen"):
        promote(_NoDatabase(), LEASE, _frozen(), [row])
