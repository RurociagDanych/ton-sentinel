"""Pure rules for validating a recovery cut and resuming only from an exact match."""

import dataclasses
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ton_sentinel import recovery
from ton_sentinel.contracts import Lease
from ton_sentinel.recovery import (RecoveryCutRejected, RecoveryPoint, RecoveryResumption,
                                   classify_resume_records, resume_from_recovery_cut,
                                   resume_refusals, resumed_records, spark_starting_offsets)

MIGRATION = (Path(recovery.__file__).parent / "db" / "migrations" / "027_recovery_resume.sql")
ENVELOPES = {"e2": {"event_id": "e2", "v": 2}, "e3": {"event_id": "e3", "v": 3}}


def manifest():
    return {
        "version": 1, "pause_id": 4, "recovery_epoch": 2,
        "fence": {"owner": "recovery", "fence": 9},
        "snapshot": {"canonical_generation": 3, "contributions": [], "tables": {}},
        "outbox": {"members": [["e1", 1, "acknowledged", "acknowledged"],
                               ["e2", 2, "attempted", "ambiguous"],
                               ["e3", 2, "ambiguous", "ambiguous"],
                               ["e4", 3, "pending", "pending"]],
                   "counts": {"acknowledged": 1, "ambiguous": 2, "pending": 1},
                   "frontier": {"origin_inclusive": 0, "end_exclusive": 20}},
        "kafka": {"cluster_id": "c", "topic": "actions", "topic_id": "t",
                  "topic_incarnation": "inc-1", "replay_epoch": 0,
                  "end_offsets": {"0": 2, "1": 1}, "log_start_offsets": {"0": 0, "1": 0}},
    }


def record(partition, offset, key, value):
    if isinstance(value, dict):
        value = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return {"partition": partition, "offset": offset,
            "key": None if key is None else key.encode(),
            "value": None if value is None else value.encode()}


def point(**over):
    value = RecoveryPoint("8d4f2b3e-58a5-4a39-9a0e-0d1b1d6c2a10", 2, 3, "actions", "inc-1",
                          "t", {0: 2, 1: 1}, "a" * 64)
    return dataclasses.replace(value, **over)


def lease(fence=10):
    return Lease("resumer", fence, datetime(2026, 11, 13, tzinfo=timezone.utc))



def test_ambiguous_send_that_landed_after_the_cut_is_deduplicated_not_redelivered():
    records = [record(0, 0, "e1", {"event_id": "e1"}), record(0, 1, "e1", {"event_id": "e1"}),
               record(1, 1, "e2", ENVELOPES["e2"])]
    resolutions, duplicates, reasons = classify_resume_records(manifest(), ENVELOPES, records)
    assert reasons == ()
    assert resolutions == {"e2": (1, 1), "e3": None}
    assert duplicates == ()


def test_ambiguous_send_that_landed_before_the_cut_is_also_deduplicated():
    records = [record(0, 0, "e3", ENVELOPES["e3"])]
    resolutions, _, reasons = classify_resume_records(manifest(), ENVELOPES, records)
    assert reasons == () and resolutions == {"e2": None, "e3": (0, 0)}


def test_repeated_late_sends_designate_the_first_copy_and_report_the_rest():
    records = [record(1, 3, "e2", ENVELOPES["e2"]), record(1, 1, "e2", ENVELOPES["e2"]),
               record(1, 2, "e2", ENVELOPES["e2"])]
    resolutions, duplicates, reasons = classify_resume_records(manifest(), ENVELOPES, records)
    assert reasons == ()
    assert resolutions["e2"] == (1, 1) and duplicates == ((1, 2), (1, 3))


@pytest.mark.parametrize("bad, words", [
    (record(1, 1, "stray", {"event_id": "stray"}), "not an ambiguous member"),
    (record(1, 1, "e1", {"event_id": "e1"}), "not an ambiguous member"),
    (record(1, 1, "e4", {"event_id": "e4"}), "not an ambiguous member"),
    (record(1, 1, "e2", {"event_id": "e2", "v": 99}), "persisted envelope"),
    (record(1, 1, "e2", "{not json"), "persisted envelope"),
    (record(1, 1, None, None), "not an ambiguous member"),
    (record(2, 0, "e2", ENVELOPES["e2"]), "partition"),
])
def test_unexpected_post_cut_record_refuses_resume(bad, words):
    _, _, reasons = classify_resume_records(manifest(), ENVELOPES, [bad])
    assert any(r.startswith("mismatched:") and words in r for r in reasons), reasons


def test_pre_cut_history_that_is_not_ambiguous_is_left_alone():
    records = [record(0, 0, "e1", {"event_id": "e1"}), record(0, 1, "old", "x")]
    _, _, reasons = classify_resume_records(manifest(), ENVELOPES, records)
    assert reasons == ()


def test_missing_persisted_envelope_for_an_ambiguous_member_is_incomplete():
    _, _, reasons = classify_resume_records(manifest(), {"e2": ENVELOPES["e2"]}, [])
    assert any(r.startswith("incomplete:") and "e3" in r for r in reasons)



def test_exact_fenced_match_with_a_fresh_pipeline_epoch_is_accepted():
    assert resume_refusals(point(), point(), cut_fence=9, lease_fence=10,
                           pipeline_epoch_used=False) == ()


@pytest.mark.parametrize("field, value", [
    ("cut_id", "1d4f2b3e-58a5-4a39-9a0e-0d1b1d6c2a10"), ("recovery_epoch", 1),
    ("canonical_generation", 4), ("topic", "other"), ("topic_incarnation", "inc-2"),
    ("topic_id", "t2"), ("end_offsets", {0: 2, 1: 0}), ("end_offsets", {0: 2}),
    ("digest", "b" * 64)])
def test_any_difference_from_the_durable_cut_refuses_resume(field, value):
    reasons = resume_refusals(point(), point(**{field: value}), cut_fence=9, lease_fence=10,
                              pipeline_epoch_used=False)
    assert any(r.startswith("mismatched:") and "presented recovery point" in r
               for r in reasons), reasons


def test_resume_needs_a_handoff_lease_newer_than_the_cut_fence():
    for fence in (8, 9):
        reasons = resume_refusals(point(), point(), cut_fence=9, lease_fence=fence,
                                  pipeline_epoch_used=False)
        assert any(r.startswith("stale:") and "handoff" in r for r in reasons)


def test_resume_refuses_a_reused_pipeline_epoch():
    reasons = resume_refusals(point(), point(), cut_fence=9, lease_fence=10,
                              pipeline_epoch_used=True)
    assert any(r.startswith("stale:") and "pipeline epoch" in r for r in reasons)


def test_presented_point_must_be_a_recovery_point():
    reasons = resume_refusals(point(), {"cut_id": point().cut_id}, cut_fence=9,
                              lease_fence=10, pipeline_epoch_used=False)
    assert any(r.startswith("incomplete:") for r in reasons)



def resumption(resolutions):
    return RecoveryResumption(point().cut_id, 4, 2, "pe-2", lease(), "actions", {0: 2, 1: 1},
                              resolutions, (), "a" * 64)


def test_resumed_records_start_at_the_cut_and_apply_each_event_once():
    records = [record(0, 0, "e1", "x"), record(0, 1, "e3", ENVELOPES["e3"]),
               record(1, 0, "e9", "x"),
               record(1, 1, "e2", ENVELOPES["e2"]),
               record(1, 2, "e2", ENVELOPES["e2"]),
               record(0, 2, "e3", ENVELOPES["e3"]),
               record(0, 3, "e4", "x"), record(0, 4, "e4", "x"),
               record(1, 3, "e5", "x")]
    kept = resumed_records(resumption({"e2": (1, 1), "e3": (0, 1)}), records)
    assert [(r["partition"], r["offset"]) for r in kept] == [(0, 3), (1, 1), (1, 3)]
    assert sorted(r["key"].decode() for r in kept) == ["e2", "e4", "e5"]


def test_spark_starting_offsets_are_the_exact_cut_end_offsets():
    assert json.loads(spark_starting_offsets(resumption({}))) == {"actions": {"0": 2, "1": 1}}



class NoConnection:
    def execute(self, *args, **kwargs):
        raise AssertionError("invalid input must fail before database access")


def test_resume_validates_inputs_before_database_access():
    reader = lambda topic: {}                                     # noqa: E731
    records = lambda topic, start, end: []                        # noqa: E731
    cases = [((None, point(), reader, records), {"pipeline_epoch": "pe"}, "lease"),
             ((lease(), {"cut_id": "x"}, reader, records), {"pipeline_epoch": "pe"}, "point"),
             ((lease(), point(), None, records), {"pipeline_epoch": "pe"}, "Kafka offset"),
             ((lease(), point(), reader, None), {"pipeline_epoch": "pe"}, "record reader"),
             ((lease(), point(), reader, records), {"pipeline_epoch": " "}, "pipeline epoch"),
             ((lease(), point(), reader, records), {"pipeline_epoch": "pe", "max_records": 0},
              "max_records")]
    for args, kwargs, words in cases:
        with pytest.raises(ValueError, match=words):
            resume_from_recovery_cut(NoConnection(), *args, **kwargs)


def test_resume_refusal_is_a_recovery_cut_rejection():
    assert issubclass(RecoveryCutRejected, RuntimeError)


def test_migration_027_records_resumptions_immutably_and_checks_them_in_the_database():
    text = MIGRATION.read_text()
    assert "CREATE TABLE IF NOT EXISTS recovery_resumptions" in text
    assert re.search(r"BEFORE UPDATE OR DELETE ON recovery_resumptions", text)
    assert re.search(r"BEFORE TRUNCATE ON recovery_resumptions", text)
    for words in ("does not match its cut", "pipeline epoch already used",
                  "not a handoff", "stale cut", "ambiguous members"):
        assert words in text, words
    # Resumption metadata is not cut state: it is written while the pause is still active.
    assert "recovery_resumptions" not in recovery.GUARDED_TABLES
