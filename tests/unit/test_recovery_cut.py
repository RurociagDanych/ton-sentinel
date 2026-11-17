"""Pure recovery-cut rules (pause, consistent cut metadata, claim refusal).

Real PostgreSQL/Kafka behaviour is covered by tests/integration/test_recovery.py.
"""
import copy
import re
from pathlib import Path

import pytest

from ton_sentinel import recovery
from ton_sentinel.db.ledger import IngestionPaused, refuse_if_paused
from ton_sentinel.recovery import (RecoveryCutRejected, capture_recovery_cut,
                                   classify_publication_state, claim_recovery_point,
                                   manifest_digest, normalize_kafka_sample,
                                   pause_for_recovery, recovery_point_reasons,
                                   release_recovery_pause, validate_manifest)

MIGRATION = (Path(recovery.__file__).parent / "db" / "migrations" / "026_recovery_cut.sql")


def sample(**over):
    value = {"cluster_id": "cluster-a", "topic": "actions", "topic_id": "tid-1",
             "partitions": {0: {"start": 0, "end": 3}, 1: {"start": 1, "end": 5}}}
    value.update(over)
    return value


def manifest():
    return {
        "version": 1, "pause_id": 4, "recovery_epoch": 2,
        "fence": {"owner": "recovery", "fence": 9},
        "snapshot": {"canonical_generation": 3,
                     "contributions": [["e1", "testnet", 1, "d1"], ["e2", "testnet", 2, "d2"]],
                     "tables": {"canonical_actions": {"rows": 2, "digest": "a" * 64}}},
        "outbox": {"members": [["e1", 1, "acknowledged", "acknowledged"],
                               ["e2", 2, "attempted", "ambiguous"]],
                   "counts": {"acknowledged": 1, "ambiguous": 1, "pending": 0},
                   "frontier": {"origin_inclusive": 0, "end_exclusive": 20}},
        "kafka": {"cluster_id": "cluster-a", "topic": "actions", "topic_id": "tid-1",
                  "topic_incarnation": "inc-1", "replay_epoch": 0,
                  "end_offsets": {"0": 3, "1": 5}, "log_start_offsets": {"0": 0, "1": 1}},
    }


def cut_row(m):
    return {"pause_id": m["pause_id"], "recovery_epoch": m["recovery_epoch"],
            "lease_owner": m["fence"]["owner"], "lease_fence": m["fence"]["fence"],
            "canonical_generation": m["snapshot"]["canonical_generation"],
            "topic": "actions", "topic_incarnation": "inc-1", "replay_epoch": 0,
            "kafka_cluster_id": "cluster-a", "kafka_topic_id": "tid-1",
            "end_offsets": {"0": 3, "1": 5}}


def live(m, **over):
    value = {"pause_active": True, "latest_epoch": m["recovery_epoch"],
             "snapshot": copy.deepcopy(m["snapshot"]),
             "outbox": copy.deepcopy(m["outbox"]),
             "delivery": ("actions", "inc-1", 0),
             "kafka": normalize_kafka_sample(sample(), topic="actions")}
    value.update(over)
    return value


def test_kafka_sample_normalizes_contiguous_partitions_to_json_keys():
    assert normalize_kafka_sample(sample(), topic="actions") == {
        "cluster_id": "cluster-a", "topic": "actions", "topic_id": "tid-1",
        "partitions": {"0": {"start": 0, "end": 3}, "1": {"start": 1, "end": 5}}}


@pytest.mark.parametrize("bad, words", [
    (sample(cluster_id=""), "cluster"),
    (sample(topic_id=None), "topic id"),
    (sample(topic="other"), "topic"),
    (sample(partitions={}), "partition"),
    (sample(partitions={1: {"start": 0, "end": 1}}), "partition"),
    (sample(partitions={0: {"start": 0, "end": -1}}), "offset"),
    (sample(partitions={0: {"start": 4, "end": 3}}), "offset"),
    (sample(partitions={0: {"start": 0, "end": True}}), "offset"),
    (sample(partitions={0: {"end": 3}}), "offset"),
    ("not a mapping", "sample"),
])
def test_kafka_sample_refuses_incomplete_or_inconsistent_offsets(bad, words):
    with pytest.raises(RecoveryCutRejected, match=words):
        normalize_kafka_sample(bad, topic="actions")


@pytest.mark.parametrize("state, expected", [
    ("pending", "pending"), ("attempted", "ambiguous"), ("ambiguous", "ambiguous"),
    ("acknowledged", "acknowledged")])
def test_outbox_state_classification_never_fabricates_acknowledgement(state, expected):
    assert classify_publication_state(state) == expected


def test_unknown_outbox_state_is_refused():
    with pytest.raises(RecoveryCutRejected, match="publication state"):
        classify_publication_state("published")


def test_manifest_digest_is_order_independent_and_content_sensitive():
    m = manifest()
    reordered = dict(reversed(list(m.items())))
    assert manifest_digest(m) == manifest_digest(reordered)
    assert re.fullmatch(r"[0-9a-f]{64}", manifest_digest(m))
    changed = manifest()
    changed["kafka"]["end_offsets"]["1"] = 6
    assert manifest_digest(changed) != manifest_digest(m)
    changed = manifest()
    changed["outbox"]["members"][1][2] = "pending"
    assert manifest_digest(changed) != manifest_digest(m)


def test_validate_manifest_accepts_complete_matching_digest():
    m = manifest()
    validate_manifest(m, manifest_digest(m))


@pytest.mark.parametrize("section", ["snapshot", "outbox", "kafka", "fence"])
def test_validate_manifest_refuses_missing_component(section):
    m = manifest()
    del m[section]
    with pytest.raises(RecoveryCutRejected, match="incomplete"):
        validate_manifest(m, manifest_digest(m))


def test_validate_manifest_refuses_missing_partition_offsets_and_digest_mismatch():
    m = manifest()
    m["kafka"]["end_offsets"] = {}
    with pytest.raises(RecoveryCutRejected, match="incomplete"):
        validate_manifest(m, manifest_digest(m))
    m = manifest()
    with pytest.raises(RecoveryCutRejected, match="incomplete"):
        validate_manifest(m, "0" * 64)


def test_recovery_point_reasons_empty_for_matching_paused_cut():
    m = manifest()
    assert recovery_point_reasons(cut_row(m), m, manifest_digest(m), live(m)) == ()


def test_recovery_point_reasons_refuse_stale_cuts():
    m = manifest()
    digest = manifest_digest(m)
    reasons = recovery_point_reasons(cut_row(m), m, digest, live(m, pause_active=False))
    assert any(r.startswith("stale:") and "released" in r for r in reasons)
    reasons = recovery_point_reasons(cut_row(m), m, digest, live(m, latest_epoch=3))
    assert any(r.startswith("stale:") and "newer" in r for r in reasons)
    moved = live(m)
    moved["snapshot"]["canonical_generation"] = 4
    reasons = recovery_point_reasons(cut_row(m), m, digest, moved)
    assert any(r.startswith("stale:") and "snapshot" in r for r in reasons)
    moved = live(m)
    moved["outbox"]["members"][1][2] = "acknowledged"
    reasons = recovery_point_reasons(cut_row(m), m, digest, moved)
    assert any(r.startswith("stale:") and "outbox" in r for r in reasons)


def test_recovery_point_reasons_refuse_mismatched_metadata():
    m = manifest()
    digest = manifest_digest(m)
    row = cut_row(m)
    row["lease_fence"] = 8
    assert any(r.startswith("mismatched:") for r in
               recovery_point_reasons(row, m, digest, live(m)))
    assert any("delivery" in r for r in recovery_point_reasons(
        cut_row(m), m, digest, live(m, delivery=("actions", "inc-2", 0))))
    for over, words in [({"cluster_id": "cluster-b"}, "cluster"),
                        ({"topic_id": "tid-2"}, "topic id"),
                        ({"partitions": {0: {"start": 0, "end": 3}}}, "partition"),
                        ({"partitions": {0: {"start": 4, "end": 9}, 1: {"start": 1, "end": 5}}},
                         "offset"),
                        ({"partitions": {0: {"start": 0, "end": 2}, 1: {"start": 1, "end": 5}}},
                         "offset")]:
        current = live(m, kafka=normalize_kafka_sample(sample(**over), topic="actions"))
        reasons = recovery_point_reasons(cut_row(m), m, digest, current)
        assert any(r.startswith("mismatched:") and words in r for r in reasons), (over, reasons)


def test_recovery_point_reasons_refuse_incomplete_manifest():
    m = manifest()
    reasons = recovery_point_reasons(cut_row(m), m, "f" * 64, live(m))
    assert any(r.startswith("incomplete:") for r in reasons)


class NoConnection:
    def execute(self, *args, **kwargs):
        raise AssertionError("invalid input must fail before database access")


def test_entry_points_validate_inputs_before_database_access():
    with pytest.raises(ValueError, match="owner"):
        pause_for_recovery(NoConnection(), " ")
    with pytest.raises(ValueError, match="lease"):
        release_recovery_pause(NoConnection(), None, 1)
    with pytest.raises(ValueError, match="lease"):
        capture_recovery_cut(NoConnection(), None, 1, lambda topic: sample())
    with pytest.raises(ValueError, match="cut id"):
        claim_recovery_point(NoConnection(), "not-a-uuid", lambda topic: sample())


def test_refuse_if_paused_honours_an_active_recovery_pause():
    class Result:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Paused:
        def execute(self, sql, *args):
            if "recovery_pauses" in sql:
                return Result((7, "recovery-owner"))
            return Result(None)

    with pytest.raises(IngestionPaused, match="recovery cut"):
        refuse_if_paused(Paused())


def test_every_snapshot_table_is_write_guarded_by_migration_026():
    text = MIGRATION.read_text()
    guarded = set(re.findall(r"CREATE TRIGGER \w+_recovery_pause_guard\s+BEFORE [A-Z ]+ ON (\w+)",
                             text))
    assert set(recovery.SNAPSHOT_TABLES) <= guarded
    assert set(recovery.GUARDED_TABLES) == guarded
