"""Offline tests for the local envelope workload harness pure functions (no services)."""

import json
import random

import pytest

from ton_sentinel import workload as w

LEVEL = {"name": "level-1", "events_per_second": 2, "payload_bytes": 2000, "active_tokens": 500,
         "active_wallets": 100000, "zero_grid_rows": 4320000, "scan_membership": 7200,
         "reconciliation_per_hour": 4, "corrections_per_hour": 72, "baseline_fanout": 2016,
         "maintenance_seconds": 600, "backlog_events": 3600, "outage_seconds": 1800,
         "latency_seconds": 900, "catchup_seconds": 1800, "run_seconds": 5100}


def test_phase_plan_derives_steady_time_and_cadences_from_level():
    plan = w.phase_plan(LEVEL)
    assert plan["steady_seconds"] == 5100 - 1800 - 1800 - 600 == 900
    assert plan["zero_grid_windows_per_token"] == 8640
    assert plan["reconciliation_interval_seconds"] == 900
    assert plan["correction_interval_seconds"] == 50
    assert plan["retention_expiry_rows"] == 500 * 288
    assert plan["max_wall_seconds"] > LEVEL["run_seconds"]


@pytest.mark.parametrize("change", [
    {"run_seconds": 4200},
    {"zero_grid_rows": 4320001},
    {"events_per_second": 0},
])
def test_phase_plan_rejects_inconsistent_levels(change):
    with pytest.raises(ValueError):
        w.phase_plan(dict(LEVEL, **change))


def test_event_payload_is_exactly_payload_bytes_and_parseable():
    rng = random.Random(7)
    for seq in (0, 1, 999999):
        raw = w.event_payload(seq, LEVEL, "2026-11-11T12:00:00.000000+00:00", rng)
        assert len(raw) == LEVEL["payload_bytes"]
        event = json.loads(raw)
        assert event["id"] == f"a{seq:012d}"
        assert 0 <= event["token_id"] < LEVEL["active_tokens"]
        assert 0 <= event["wallet_id"] < LEVEL["active_wallets"]


def test_due_events_follow_the_level_rate():
    assert w.due_events(0, 2) == 0
    assert w.due_events(0.49, 2) == 0
    assert w.due_events(10.0, 2) == 20
    assert w.due_events(1800, 4) == 7200


def raw_sample(mono, *, usage=0, k3s_usage=0, mem=100, peak=200, kernel=50, storage=True, file_key=True):
    containers = {}
    for name in ("app", "kafka", "postgres"):
        entry = {"memory_current": mem, "memory_peak": peak, "anon": mem // 2,
                 "usage_usec": usage, "oom_kill": 0, "nr_throttled": 0, "throttled_usec": 0}
        if file_key:
            entry["file"] = mem // 4
        containers[name] = entry
    sample = {"mono": mono, "utc": "2026-11-11T12:00:00+00:00", "containers": containers,
              "k3s": {"scope_memory_current": 1000, "excluded_memory_current": 400,
                      "scope_usage_usec": k3s_usage, "excluded_usage_usec": 0, "oom_kill": 0},
              "host_kernel_bytes": kernel}
    if storage:
        sample["storage"] = {"workspace": {"total_bytes": 100, "free_bytes": 55},
                             "postgres": {"total_bytes": 100, "free_bytes": 50},
                             "kafka": {"total_bytes": 100, "free_bytes": 60}}
    return sample


def harness_result():
    return {"events": 9000, "input_bytes": 18000000,
            "kafka": {"produced_events": 9000, "send_errors": 0},
            "spark": {"canonical_batches": 40, "feature_batches": 40, "checkpoint_bytes": 123456,
                      "state_rows_max": 900, "state_memory_bytes_max": 1000},
            "pg": {"wal_dir_bytes": 10, "wal_generated_bytes": 20, "index_bytes": 30, "vacuum_seconds": 1.5},
            "zero_grid": {"rows_materialized": 4320000},
            "retention": {"expired_feature_rows": 144000},
            "corrections": {"applied": 30, "baseline_rows_read_max": 2016, "baseline_rows_read_min": 2016},
            "backlog": {"events": 3600, "catchup_seconds": 95.0, "caught_up": True},
            "latency": {"normal_operation_max_seconds": 150.0},
            "maintenance": {"serialized": True, "seconds": 80.0}}


def test_coverage_is_derived_only_from_present_evidence():
    raw = [raw_sample(0), raw_sample(10)]
    assert w.derive_coverage(harness_result(), raw, LEVEL) == sorted(w.COVERAGE)
    weaker = harness_result()
    weaker["zero_grid"]["rows_materialized"] = 1000
    weaker["maintenance"]["serialized"] = False
    got = w.derive_coverage(weaker, [raw_sample(0, file_key=False), raw_sample(10)], LEVEL)
    assert "retention_zero_grid_repair" not in got
    assert "serialized_maintenance" not in got
    assert "page_cache" not in got


@pytest.mark.parametrize("low", [2015, None, "missing"])
def test_repair_coverage_needs_every_correction_to_read_the_full_baseline(low):
    # Level-2 run 20261111T182826920753Z: one unmaterialized grid window -> every
    # correction read 2015 of 2016 baseline windows. One full read is not enough.
    harness = harness_result()
    if low == "missing":
        del harness["corrections"]["baseline_rows_read_min"]
    else:
        harness["corrections"]["baseline_rows_read_min"] = low
    got = w.derive_coverage(harness, [raw_sample(0), raw_sample(10)], LEVEL)
    assert "retention_zero_grid_repair" not in got
    assert len(got) == len(w.COVERAGE) - 1


@pytest.mark.parametrize("drop", [("k3s",), ("host_kernel_bytes",)])
def test_host_kernel_k3s_coverage_fails_closed_when_one_sample_lacks_it(drop):
    raw = [raw_sample(0), raw_sample(5), raw_sample(10)]
    for key in drop:
        del raw[1][key]
    assert "host_kernel_k3s" not in w.derive_coverage(harness_result(), raw, LEVEL)


@pytest.mark.parametrize("name,key,category", [
    ("kafka", "anon", "broker_cache_native"), ("kafka", "memory_current", "broker_cache_native"),
    ("app", "memory_current", "spark_driver_executor_python"), ("postgres", "file", "page_cache")])
def test_container_coverage_fails_closed_when_one_sample_lacks_a_field(name, key, category):
    raw = [raw_sample(0), raw_sample(5), raw_sample(10)]
    del raw[1]["containers"][name][key]
    assert category not in w.derive_coverage(harness_result(), raw, LEVEL)


def test_zero_fill_covers_every_window_from_the_frontier_to_the_emitted_window():
    # grid ends at 3000 (last seeded window starts 2700); the producer started late,
    # so the first emitted window is 3300 and window 3000 had no events at all.
    fill, frontier = w.zero_fill_starts(3000, [3300])
    assert fill == [3000, 3300] and frontier == 3600
    fill, frontier = w.zero_fill_starts(frontier, [3600, 3900])
    assert fill == [3600, 3900] and frontier == 4200
    fill, frontier = w.zero_fill_starts(frontier, [])
    assert fill == [] and frontier == 4200
    fill, frontier = w.zero_fill_starts(frontier, [3900, 4800])
    assert fill == [3900, 4200, 4500, 4800] and frontier == 5100


@pytest.mark.parametrize("frontier,starts", [(3001, [3300]), (3000, [3301]), (3000.0, [3300])])
def test_zero_fill_refuses_unaligned_windows(frontier, starts):
    with pytest.raises(ValueError):
        w.zero_fill_starts(frontier, starts)


def test_zero_grid_contiguity_is_measured_not_assumed():
    ok = w.grid_contiguity(distinct_windows=9, rows=9 * 500, min_epoch=0, max_epoch=8 * 300, tokens=500)
    assert ok == {"distinct_windows": 9, "expected_windows": 9, "rows": 4500, "expected_rows": 4500,
                  "missing_windows": 0, "contiguous": True}
    gap = w.grid_contiguity(distinct_windows=8, rows=8 * 500, min_epoch=0, max_epoch=8 * 300, tokens=500)
    assert gap["missing_windows"] == 1 and gap["contiguous"] is False
    partial = w.grid_contiguity(distinct_windows=9, rows=9 * 500 - 1, min_epoch=0, max_epoch=8 * 300, tokens=500)
    assert partial["contiguous"] is False


def test_summarize_level_computes_cpu_footprint_disk_and_objectives():
    raw = [raw_sample(0, usage=0, k3s_usage=0, mem=100, kernel=50),
           raw_sample(100, usage=10_000_000, k3s_usage=5_000_000, mem=300, kernel=70)]
    capacity = {"effective_cores": 8.0, "source": "sched_getaffinity + cgroup v2 cpu.max"}
    s = w.summarize_level(raw, harness_result(), level=LEVEL, target_id="local-x86_64-single-host",
                          architecture="x86_64", capacity=capacity, provenance="cmd")
    assert s["level"] == "level-1" and s["synthetic"] is False
    assert s["elapsed_seconds"] == 100
    # three containers x 10 s + k3s system 5 s
    assert s["measured_cpu_seconds"] == pytest.approx(35.0)
    assert s["measured_cpu_capacity_cores"] == 8.0
    # sampled max: 3*300 + (1000-400) + 70 = 1570; peak bound: 3*200 + 600 + 70 = 1270
    assert s["full_host_footprint_bytes"] == 1570
    assert s["data_free_fraction"] == 0.5
    assert s["oom_events"] == 0
    assert s["events"] == 9000 and s["input_bytes"] == 18000000
    assert s["measured_maintenance_seconds"] == 80.0
    assert s["measured_catchup_seconds"] == 95.0
    assert s["measured_latency_seconds"] == 150.0
    assert s["measured_backlog_events"] == 3600
    assert s["zero_grid_rows_materialized"] == 4320000
    assert s["coverage"] == sorted(w.COVERAGE)
    assert s["cpu_measurement_scope"].strip() and s["cpu_measurement_provenance"] == "cmd"
    assert "no live TON" in s["workload_data_origin"]


def test_summarize_level_refuses_too_few_samples_or_missing_storage():
    with pytest.raises(ValueError):
        w.summarize_level([raw_sample(0)], harness_result(), level=LEVEL, target_id="t",
                          architecture="x86_64", capacity={"effective_cores": 8.0}, provenance="p")
    with pytest.raises(ValueError):
        w.summarize_level([raw_sample(0, storage=False), raw_sample(1, storage=False)], harness_result(),
                          level=LEVEL, target_id="t", architecture="x86_64",
                          capacity={"effective_cores": 8.0}, provenance="p")


def test_k3s_system_usage_excludes_foreign_workload_pods():
    assert w.k3s_system(1410, 508) == 902
    with pytest.raises(ValueError):
        w.k3s_system(100, 101)


def test_latency_split_excludes_outage_affected_windows():
    marks = {"outage_start": 1000.0, "catchup_end": 3000.0}
    rows = [(400.0, 790.0),
            (2000.0, 3100.0),
            (3000.0, 3400.0)]
    lat = w.latency_summary(rows, marks)
    assert lat["normal_operation_max_seconds"] == pytest.approx(100.0)
    assert lat["normal_operation_windows"] == 1 + 1
    assert lat["outage_affected_max_seconds"] == pytest.approx(3100.0 - 2300.0)
    assert lat["outage_affected_windows"] == 1


def _snapshot_without_leaf_cpu(tmp_path, controllers):
    from scripts.measure_feasibility import host_snapshot
    from tests.integration.test_feasibility import cgroup_fixture
    proc_cgroup, mountinfo, meminfo = cgroup_fixture(tmp_path, [("max", 0), (1_000_000_000, 0), ("max", 0)])
    job = tmp_path / "cg" / "tenant" / "job"
    (job / "cpu.max").unlink()
    (tmp_path / "cg" / "tenant" / "cpu.max").write_text("150000 100000\n")
    if controllers is not None:
        (job / "cgroup.controllers").write_text(controllers + "\n")
    return host_snapshot(tmp_path, 1, 1, cpu_budget_cores=0.1, meminfo_path=meminfo,
                         cgroup_path=proc_cgroup, mountinfo_path=mountinfo)


def test_host_snapshot_cpu_controller_not_enabled_means_no_limit_at_that_level(tmp_path):
    # cgroup v2: cpu.max exists only where the cpu controller is enabled; the
    # parent's quota still applies and must be observed.
    result = _snapshot_without_leaf_cpu(tmp_path, "memory pids")
    cpu = result["observations"]["cpu"]
    assert cpu is not None, result["reasons"]
    assert cpu["limits"][0]["interface"] == "cpu_controller_not_enabled"
    assert cpu["effective_cores"] == min(1.5, cpu["affinity_cores"])


@pytest.mark.parametrize("controllers", [None, "cpu memory pids"])
def test_host_snapshot_missing_cpu_max_with_controller_enabled_still_blocks(tmp_path, controllers):
    result = _snapshot_without_leaf_cpu(tmp_path, controllers)
    assert result["observations"]["cpu"] is None
    assert result["admission_status"] == "BLOCKED"
