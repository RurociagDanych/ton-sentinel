"""Pure-logic bounds for the stateful fast path (Spark itself is exercised by integration)."""
import pytest

from ton_sentinel.spark import stream


def _policy(**over):
    base = {"watermark_seconds": 60, "max_state_rows": 100, "max_partitions": 2}
    base.update(over)
    return base


def test_policy_defaults_to_five_minute_windows_and_is_immutable():
    policy = stream.fast_path_policy(_policy())
    assert policy.window_seconds == 300
    assert (policy.watermark_seconds, policy.max_state_rows, policy.max_partitions) == (60, 100, 2)
    with pytest.raises(Exception):
        policy.max_state_rows = 5


@pytest.mark.parametrize("bad", [
    {"watermark_seconds": 0}, {"watermark_seconds": 86_401}, {"watermark_seconds": True},
    {"max_state_rows": 0}, {"max_state_rows": 1_000_001}, {"max_state_rows": "9"},
    {"max_partitions": 0}, {"max_partitions": 65}, {"window_seconds": 60},
])
def test_policy_rejects_unbounded_or_invalid_values(bad):
    with pytest.raises(ValueError):
        stream.fast_path_policy(_policy(**bad))


def test_policy_rejects_missing_and_unknown_keys():
    with pytest.raises(ValueError):
        stream.fast_path_policy({"watermark_seconds": 60})
    with pytest.raises(ValueError):
        stream.fast_path_policy(_policy(extra=1))


def _progress(*rows, watermark="2026-01-01T00:10:00.000Z", late=0):
    return {"batchId": 3, "numInputRows": 4,
            "eventTime": {"watermark": watermark},
            "stateOperators": [{"numRowsTotal": r, "memoryUsedBytes": 10 * r,
                                "numRowsDroppedByWatermark": late} for r in rows]}


def test_state_bound_allows_exactly_the_limit_and_refuses_above():
    policy = stream.fast_path_policy(_policy(max_state_rows=5))
    stream.enforce_state_bound(_progress(3, 2), policy)       # 5 total rows == limit
    with pytest.raises(stream.StateBoundExceeded):
        stream.enforce_state_bound(_progress(3, 3), policy)   # 6 > limit


def test_state_bound_requires_state_operators():
    policy = stream.fast_path_policy(_policy())
    with pytest.raises(stream.StateBoundExceeded):
        stream.enforce_state_bound({"batchId": 1, "stateOperators": []}, policy)


def test_summarize_progress_reports_rows_bytes_watermark_and_late_drops():
    summary = stream.summarize_progress(_progress(3, 2, late=7))
    assert summary == {"batch_id": 3, "input_rows": 4, "state_rows": 5, "state_bytes": 50,
                       "watermark": "2026-01-01T00:10:00.000Z", "late_drops": 14}


def test_partition_bound_is_checked_against_session_setting():
    policy = stream.fast_path_policy(_policy(max_partitions=2))
    stream.check_partitions(2, policy)
    with pytest.raises(ValueError):
        stream.check_partitions(3, policy)


class _FakeBatch:
    def __init__(self, rows):
        self.rows, self.limit_arg = rows, None

    def limit(self, n):
        self.limit_arg = n
        return self

    def collect(self):
        return self.rows[:self.limit_arg]


def test_candidate_batch_cap_default_is_the_documented_1000():
    assert stream._MAX_BATCH_WINDOWS == 1_000


def test_collect_candidate_rows_at_the_cap_is_accepted_and_reads_only_cap_plus_one():
    batch = _FakeBatch(list(range(5000)))
    with pytest.raises(ValueError, match="window cap"):
        stream.collect_candidate_rows(batch)          # 5000 > default 1000
    assert batch.limit_arg == stream._MAX_BATCH_WINDOWS + 1   # never collects more than cap+1
    exact = _FakeBatch(list(range(stream._MAX_BATCH_WINDOWS)))
    assert stream.collect_candidate_rows(exact) == list(range(stream._MAX_BATCH_WINDOWS))
    over = _FakeBatch(list(range(stream._MAX_BATCH_WINDOWS + 1)))
    with pytest.raises(ValueError, match="window cap"):
        stream.collect_candidate_rows(over)


def test_collect_candidate_rows_rejects_a_non_positive_cap():
    with pytest.raises(ValueError):
        stream.collect_candidate_rows(_FakeBatch([]), cap=0)
