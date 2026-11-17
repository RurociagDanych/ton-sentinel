import json
import math
import os
import platform
from pathlib import Path
import subprocess
import sys
import pytest
from ton_sentinel.feasibility import run_probe
from scripts.measure_feasibility import measure

# These tests exercise evidence classification without services.
def test_unknown_resource_evidence_blocks():
    assert measure([], {}).status == "BLOCKED"

@pytest.mark.integration
def test_real_roundtrip_restart(probe_settings):
    result = run_probe(probe_settings)
    fixture = probe_settings["fixture"]
    assert result["pg_rows"] == fixture["expected_rows"]
    assert result["restart_rows"] == result["pg_rows"]
    assert result["state_operator_count"] >= 2
    assert result["watermark"] > fixture["base_time"]
    assert result["late_dropped"] > 0
    assert result["idle_watermark_before"] == result["idle_watermark_after"]
    assert result["idle_rows"] == result["pg_rows"]
    assert result["restored_open_state_rows"] > 0
    assert fixture["missing_token"] not in {r[0] for r in result["pg_rows"]}
    assert result["correctness_checks"] == "true"
    assert result["state_store_provider"].endswith("HDFSBackedStateStoreProvider")
    Path(probe_settings["evidence"] + "-readback.json").write_text(json.dumps(result, indent=2))


def test_incomplete_resource_evidence_blocks():
    assert measure([{}], {"status": "BLOCKED"}).status == "BLOCKED"


def resource_case():
    envelope = {"status": "PASS", "synthetic": True,
                "owner_workload_agreement_supplied": True, "target_id": "synthetic-only",
                "levels": ["low", "high"], "ram_budget_bytes": 6000000000,
                "required_minimum_free_data_volume_fraction": 0.20,
                "maintenance_and_retention_measured": True}
    samples = []
    for level in envelope["levels"]:
        sample = {"target_id": "synthetic-only", "synthetic": True, "architecture": "aarch64",
                  "level": level, "full_host_footprint_bytes": 4500000000,
                  "data_free_fraction": 0.25, "oom_events": 0,
                  "coverage": ["host_kernel_k3s", "broker_cache_native", "spark_driver_executor_python",
                               "state_checkpoint_spill", "pg_wal_index_vacuum", "page_cache",
                               "retention_zero_grid_repair", "backlog_catchup", "serialized_maintenance"],
                  "elapsed_seconds": 60, "input_bytes": 1000, "events": 10}
        sample.update({"measured_cpu_seconds": 30,
                       "measured_cpu_capacity_cores": 2,
                       "cpu_measurement_scope": "explicit synthetic interval measurement",
                       "cpu_measurement_provenance": "synthetic fixture control; not host/workload evidence"})
        samples.append(sample)
    return samples, envelope


def test_unreviewed_actual_resource_evidence_cannot_qualify():
    samples, envelope = resource_case()
    envelope["synthetic"] = False
    for sample in samples:
        sample["synthetic"] = False
    result = measure(samples, envelope)
    assert result.status == "BLOCKED"
    assert result.actual_fit_eligible is False


@pytest.mark.parametrize("samples,envelope", [(None, {}), ([], None), ({}, {}), ([], [])])
def test_malformed_evidence_container_blocks(samples, envelope):
    assert measure(samples, envelope).status == "BLOCKED"


def test_complete_synthetic_resource_control():
    samples, envelope = resource_case()
    footprint = measure(samples, envelope)
    assert footprint.status == "PASS"
    assert footprint.synthetic is True  # never promote synthetic evidence to actual fit


def test_local_actual_measurement_passes_without_final_target_eligibility():
    samples, envelope = resource_case()
    envelope["synthetic"] = False
    for sample in samples:
        sample["synthetic"] = False
        sample["architecture"] = platform.machine()
    result = measure(samples, envelope, mode="local", cpu_budget_cores=1)
    assert result.status == "PASS"
    assert result.synthetic is False
    assert result.actual_fit_eligible is False


@pytest.mark.parametrize("change", ["wrong_arch", "over_budget", "missing_budget", "missing_coverage"])
def test_local_measurement_still_fails_closed(change):
    samples, envelope = resource_case()
    for sample in samples:
        sample["architecture"] = platform.machine()
    if change == "wrong_arch":
        samples[0]["architecture"] = "unavailable-architecture"
    elif change == "over_budget":
        samples[0]["full_host_footprint_bytes"] = envelope["ram_budget_bytes"] + 1
    elif change == "missing_budget":
        envelope.pop("ram_budget_bytes")
    else:
        samples[0]["coverage"].pop()
    assert measure(samples, envelope, mode="local", cpu_budget_cores=1).status != "PASS"


def test_local_measurement_blocks_missing_samples():
    _, envelope = resource_case()
    assert measure([], envelope, mode="local").status == "BLOCKED"


def local_resource_case():
    samples, envelope = resource_case()
    for sample in samples:
        sample["architecture"] = platform.machine()
    return samples, envelope


@pytest.mark.parametrize("field,value", [
    ("measured_cpu_seconds", True), ("measured_cpu_seconds", float("inf")),
    ("measured_cpu_seconds", float("nan")), ("measured_cpu_seconds", -1),
    ("measured_cpu_seconds", "30"), ("measured_cpu_capacity_cores", True),
    ("measured_cpu_capacity_cores", float("inf")),
    ("measured_cpu_capacity_cores", float("nan")), ("measured_cpu_capacity_cores", 0),
    ("measured_cpu_capacity_cores", -1),
    ("cpu_measurement_scope", ""), ("cpu_measurement_provenance", None),
])
def test_local_cpu_evidence_invalid_values_block(field, value):
    samples, envelope = local_resource_case()
    assert measure(samples, envelope, mode="local", cpu_budget_cores=1).status == "PASS"
    samples[0][field] = value
    assert measure(samples, envelope, mode="local", cpu_budget_cores=1).status == "BLOCKED"


def test_local_cpu_evidence_impossible_consumption_blocks():
    samples, envelope = local_resource_case()
    samples[0]["measured_cpu_seconds"] = 121
    assert measure(samples, envelope, mode="local", cpu_budget_cores=1).status == "BLOCKED"


@pytest.mark.parametrize("invalid", ["seconds", "capacity", "interval", "budget"])
@pytest.mark.parametrize("failure", ["oom", "ram", "disk", "cpu"])
def test_invalid_cpu_evidence_blocks_even_with_observed_failure(invalid, failure):
    samples, envelope = local_resource_case()
    cpu_budget = 1
    if invalid == "seconds":
        samples[0]["measured_cpu_seconds"] = True
    elif invalid == "capacity":
        samples[0]["measured_cpu_capacity_cores"] = 0
    elif invalid == "interval":
        samples[0]["measured_cpu_seconds"] = 121
    else:
        cpu_budget = None
    if failure == "oom":
        samples[1]["oom_events"] = 1
        reason = "observed OOM"
    elif failure == "ram":
        samples[1]["full_host_footprint_bytes"] = envelope["ram_budget_bytes"] + 1
        reason = "full footprint exceeds residual RAM"
    elif failure == "disk":
        samples[1]["data_free_fraction"] = 0
        reason = "insufficient data-volume safety headroom"
    else:
        samples[1]["measured_cpu_seconds"] = 90
        reason = "average CPU consumption exceeds caller CPU budget"
    result = measure(samples, envelope, mode="local", cpu_budget_cores=cpu_budget)
    assert result.status == "BLOCKED"
    if invalid != "budget" or failure != "cpu":
        assert reason in result.reasons


def test_local_cpu_budget_above_measured_capacity_fails():
    samples, envelope = local_resource_case()
    assert measure(samples, envelope, mode="local", cpu_budget_cores=3).status == "FAIL"


def test_local_cpu_requires_caller_budget():
    samples, envelope = local_resource_case()
    assert measure(samples, envelope, mode="local").status == "BLOCKED"


def test_cli_local_mode_classifies_explicitly_synthetic_cpu_evidence(tmp_path):
    samples, envelope = local_resource_case()
    (tmp_path / "envelope.json").write_text(json.dumps(envelope))
    (tmp_path / "probe-resources.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
    proc = subprocess.run(
        [sys.executable, "-B", "scripts/measure_feasibility.py", "--input", str(tmp_path),
         "--envelope", str(tmp_path / "envelope.json"), "--mode", "local",
         "--cpu-budget-cores", "1"],
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode == 0, proc.stderr
    output = json.loads(proc.stdout)
    persisted = json.loads((tmp_path / "resources.json").read_text())
    assert output == persisted
    assert output["status"] == "PASS"
    assert output["synthetic"] is True
    assert output["actual_fit_eligible"] is False
    assert output["mode"] == "local"
    assert "not final-target qualification" in output["scope"]
    assert "average" in output["cpu_classification_limitations"][0]


@pytest.mark.parametrize("case,expected_status,expected_exit", [
    ("missing", "BLOCKED", 2), ("over_budget", "FAIL", 2),
])
def test_cli_local_cpu_evidence_blocks_missing_and_fails_over_budget(
        tmp_path, case, expected_status, expected_exit):
    samples, envelope = local_resource_case()
    if case == "missing":
        samples[0].pop("measured_cpu_seconds")
    else:
        samples[0]["measured_cpu_seconds"] = 90  # 1.5 average cores > 1-core budget
    (tmp_path / "envelope.json").write_text(json.dumps(envelope))
    (tmp_path / "probe-resources.jsonl").write_text(
        "".join(json.dumps(sample) + "\n" for sample in samples))
    proc = subprocess.run(
        [sys.executable, "-B", "scripts/measure_feasibility.py", "--input", str(tmp_path),
         "--envelope", str(tmp_path / "envelope.json"), "--mode", "local",
         "--cpu-budget-cores", "1"], capture_output=True, text=True, timeout=10)
    assert proc.returncode == expected_exit, proc.stderr
    output = json.loads(proc.stdout)
    assert output == json.loads((tmp_path / "resources.json").read_text())
    assert output["status"] == expected_status
    assert output["synthetic"] is True


def test_cli_default_target_mode_keeps_strict_final_target_gate(tmp_path):
    samples, envelope = resource_case()
    envelope["synthetic"] = False
    for sample in samples:
        sample["synthetic"] = False
    (tmp_path / "envelope.json").write_text(json.dumps(envelope))
    (tmp_path / "probe-resources.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
    proc = subprocess.run(
        [sys.executable, "-B", "scripts/measure_feasibility.py", "--input", str(tmp_path),
         "--envelope", str(tmp_path / "envelope.json")],
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode == 2
    output = json.loads(proc.stdout)
    assert output["status"] == "BLOCKED"
    assert output["actual_fit_eligible"] is False
    assert output["mode"] == "target"


def test_host_snapshot_cli_emits_real_machine_readable_observations(tmp_path):
    artifact = tmp_path / "host-snapshot.json"
    proc = subprocess.run(
        [sys.executable, "-B", "scripts/measure_feasibility.py", "--host-snapshot",
         "--filesystem", str(tmp_path), "--memory-budget-bytes", "1",
         "--free-disk-safety-floor-bytes", "1", "--cpu-budget-cores", "0.01",
         "--output", str(artifact)],
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode in (0, 2), proc.stderr
    output = json.loads(proc.stdout)
    assert output == json.loads(artifact.read_text())
    assert output["admission_status"] in ("PASS", "BLOCKED")
    assert output["observations"]["memory"]["total_bytes"] > 0
    assert output["observations"]["filesystem"]["total_bytes"] > 0
    assert output["architecture"] == platform.machine()
    assert output["observed_at_utc"].endswith("+00:00")
    assert "cgroup" in output["limitations"][0]
    assert output["budgets"]["minimum_available_memory_bytes"] == 1


def test_host_snapshot_blocks_missing_observation_or_budget(tmp_path):
    from scripts.measure_feasibility import host_snapshot
    result = host_snapshot(tmp_path, None, 1, cpu_budget_cores=0.01,
                           meminfo_path=tmp_path / "absent-meminfo")
    assert result["admission_status"] == "BLOCKED"
    assert result["observations"]["memory"] is None
    assert any("budget required" in reason for reason in result["reasons"])


def test_host_snapshot_fails_when_supplied_floor_exceeds_observation(tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 1024 kB\nMemAvailable: 512 kB\n")
    proc_cgroup, mountinfo, _ = cgroup_fixture(tmp_path, [("max", 0), ("max", 0), (1_000_000, 0)])
    from scripts.measure_feasibility import host_snapshot
    result = host_snapshot(tmp_path, 1, 2**63 - 1, cpu_budget_cores=0.01, meminfo_path=meminfo,
                           cgroup_path=proc_cgroup, mountinfo_path=mountinfo)
    assert result["admission_status"] == "FAIL"
    assert any("below supplied safety floor" in reason for reason in result["reasons"])


def cgroup_fixture(tmp_path, limits, *, membership="/tenant/job"):
    root = tmp_path / "cg"
    (root / "tenant" / "job").mkdir(parents=True)
    paths = [root / "tenant" / "job", root / "tenant", root]
    for path, (limit, current) in zip(paths, limits):
        (path / "memory.max").write_text(f"{limit}\n")
        (path / "memory.current").write_text(f"{current}\n")
        (path / "cpu.max").write_text("max 100000\n")
    proc_cgroup = tmp_path / "proc-cgroup"
    proc_cgroup.write_text(f"0::{membership}\n")
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(f"1 0 0:1 / {root} rw - cgroup2 cgroup rw\n")
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 1048576 kB\nMemAvailable: 800000 kB\n")
    return proc_cgroup, mountinfo, meminfo


def snapshot_from_fixture(tmp_path, limits, *, budget=100, membership="/tenant/job", cpu_budget=0.01):
    from scripts.measure_feasibility import host_snapshot
    proc_cgroup, mountinfo, meminfo = cgroup_fixture(tmp_path, limits, membership=membership)
    return host_snapshot(tmp_path, budget, 1, cpu_budget_cores=cpu_budget, meminfo_path=meminfo,
                         cgroup_path=proc_cgroup, mountinfo_path=mountinfo)


def test_host_snapshot_tight_cgroup_fails_despite_host_available(tmp_path):
    result = snapshot_from_fixture(tmp_path, [(50, 0), (1000, 0), ("max", 0)])
    assert result["admission_status"] == "FAIL"
    cg = result["observations"]["cgroup_memory"]
    assert cg["effective_available_bytes"] == 50
    assert any("available memory below" in reason for reason in result["reasons"])


def test_host_snapshot_unlimited_leaf_uses_constrained_parent(tmp_path):
    result = snapshot_from_fixture(tmp_path, [("max", 2), (150, 100), ("max", 0)], budget=40)
    cg = result["observations"]["cgroup_memory"]
    assert result["admission_status"] == "PASS"
    assert cg["effective_headroom_bytes"] == 50
    assert cg["effective_available_bytes"] == 50
    assert cg["limits"][0]["limit_bytes"] is None


def test_host_snapshot_all_observed_ancestors_unbounded_blocks(tmp_path):
    result = snapshot_from_fixture(tmp_path, [("max", 2), ("max", 3), ("max", 4)])
    assert result["admission_status"] == "BLOCKED"
    assert result["observations"]["cgroup_memory"] is None
    assert any("no finite observable" in reason for reason in result["reasons"])


@pytest.mark.parametrize("quota,budget,status", [
    ("50000 100000", 0.5, "PASS"),
    ("50000 100000", 0.50001, "FAIL"),
    ("max 100000", 0.5, "PASS"),
    ("0 100000", 0.1, "BLOCKED"),
    ("50000 0", 0.1, "BLOCKED"),
    ("broken", 0.1, "BLOCKED"),
    ("9" * 400 + " 100000", 0.1, "BLOCKED"),
    ("1 " + "9" * 400, 0.1, "BLOCKED"),
])
def test_host_snapshot_cpu_quota_parsing_and_budget_boundary(tmp_path, quota, budget, status):
    cgroup_fixture(tmp_path, [("max", 0), ("max", 0), (1_000_000_000, 0)])
    (tmp_path / "cg" / "tenant" / "job" / "cpu.max").write_text(quota + "\n")
    from scripts.measure_feasibility import host_snapshot
    result = host_snapshot(tmp_path, 1, 1, cpu_budget_cores=budget,
                           meminfo_path=tmp_path / "meminfo", cgroup_path=tmp_path / "proc-cgroup",
                           mountinfo_path=tmp_path / "mountinfo")
    assert result["admission_status"] == status
    if status != "BLOCKED":
        expected = 0.5 if quota == "50000 100000" else result["observations"]["cpu"]["affinity_cores"]
        assert result["observations"]["cpu"]["effective_cores"] == expected
    if status == "FAIL":
        assert any("CPU capacity below" in reason for reason in result["reasons"])


@pytest.mark.parametrize("budget", [None, 0, -1, float("inf"), float("nan"), True, 10**400])
def test_host_snapshot_requires_positive_finite_non_boolean_cpu_budget(tmp_path, budget):
    result = snapshot_from_fixture(tmp_path, [("max", 0), ("max", 0), ("max", 0)],
                                   cpu_budget=budget)
    assert result["admission_status"] == "BLOCKED"
    assert any("CPU budget required" in reason for reason in result["reasons"])


def test_host_snapshot_missing_descendant_cpu_interface_blocks(tmp_path):
    proc_cgroup, mountinfo, meminfo = cgroup_fixture(
        tmp_path, [("max", 0), ("max", 0), (1_000_000_000, 0)])
    (tmp_path / "cg" / "tenant" / "job" / "cpu.max").unlink()
    from scripts.measure_feasibility import host_snapshot
    result = host_snapshot(tmp_path, 1, 1, cpu_budget_cores=0.1, meminfo_path=meminfo,
                           cgroup_path=proc_cgroup, mountinfo_path=mountinfo)
    assert result["admission_status"] == "BLOCKED"
    assert result["observations"]["cpu"] is None
    assert any("CPU capacity observation unavailable" in reason for reason in result["reasons"])


@pytest.mark.parametrize("finite", [True, False])
def test_host_snapshot_root_without_memory_interface(tmp_path, finite):
    from scripts.measure_feasibility import host_snapshot
    proc_cgroup, mountinfo, meminfo = cgroup_fixture(
        tmp_path, [(150 if finite else "max", 100), ("max", 0), ("max", 0)])
    root = tmp_path / "cg"
    (root / "memory.max").unlink()
    (root / "memory.current").unlink()
    result = host_snapshot(tmp_path, 40, 1, cpu_budget_cores=0.01, meminfo_path=meminfo,
                           cgroup_path=proc_cgroup, mountinfo_path=mountinfo)
    assert result["admission_status"] == ("PASS" if finite else "BLOCKED")
    if finite:
        cg = result["observations"]["cgroup_memory"]
        assert cg["effective_available_bytes"] == 50
        assert cg["limits"][-1]["interface"] == "absent_at_visible_root"
    else:
        assert any("no finite observable" in reason for reason in result["reasons"])


@pytest.mark.parametrize("level,missing", [
    ("", ("memory.max",)), ("", ("memory.current",)),
    ("tenant", ("memory.max", "memory.current")),
])
def test_host_snapshot_missing_nonroot_or_partial_root_interface_blocks(tmp_path, level, missing):
    from scripts.measure_feasibility import host_snapshot
    proc_cgroup, mountinfo, meminfo = cgroup_fixture(
        tmp_path, [(150, 100), ("max", 0), ("max", 0)])
    for name in missing:
        (tmp_path / "cg" / level / name).unlink()
    result = host_snapshot(tmp_path, 40, 1, cpu_budget_cores=0.01, meminfo_path=meminfo,
                           cgroup_path=proc_cgroup, mountinfo_path=mountinfo)
    assert result["admission_status"] == "BLOCKED"
    assert result["observations"]["cgroup_memory"] is None


def test_host_snapshot_usage_at_or_above_limit_has_zero_headroom(tmp_path):
    result = snapshot_from_fixture(tmp_path, [(10, 11), ("max", 0), ("max", 0)])
    assert result["admission_status"] == "FAIL"
    assert result["observations"]["cgroup_memory"]["effective_headroom_bytes"] == 0


@pytest.mark.parametrize("case", ["malformed_limit", "missing_usage", "missing_mount"])
def test_host_snapshot_invalid_or_unobservable_cgroup_blocks(tmp_path, case):
    from scripts.measure_feasibility import host_snapshot
    proc_cgroup, mountinfo, meminfo = cgroup_fixture(
        tmp_path, [("bad" if case == "malformed_limit" else 10, 0),
                   ("max", 0), ("max", 0)])
    leaf = tmp_path / "cg" / "tenant" / "job"
    if case == "missing_usage":
        (leaf / "memory.current").unlink()
    if case == "missing_mount":
        mountinfo.write_text("1 0 0:1 / /other rw - cgroup cgroup rw\n")
    result = host_snapshot(tmp_path, 1, 1, cpu_budget_cores=0.01, meminfo_path=meminfo,
                           cgroup_path=proc_cgroup, mountinfo_path=mountinfo)
    assert result["admission_status"] == "BLOCKED"
    assert result["observations"]["cgroup_memory"] is None


def test_host_snapshot_cli_reports_real_cgroup_observation(tmp_path):
    artifact = tmp_path / "live-snapshot.json"
    proc = subprocess.run(
        [sys.executable, "-B", "scripts/measure_feasibility.py", "--host-snapshot",
         "--filesystem", str(tmp_path), "--memory-budget-bytes", "1",
         "--free-disk-safety-floor-bytes", "1", "--cpu-budget-cores", "0.01",
         "--output", str(artifact)],
        capture_output=True, text=True, timeout=10)
    assert proc.returncode in (0, 2), proc.stderr
    output = json.loads(proc.stdout)
    assert output == json.loads(artifact.read_text())
    assert output["admission_status"] in ("PASS", "FAIL", "BLOCKED")
    assert output["observations"]["cgroup_memory"] is None or output["observations"]["cgroup_memory"]["effective_available_bytes"] >= 0
    assert output["provenance"]["cgroup_memory_source"].startswith("cgroup v2")


@pytest.mark.parametrize("change", ["missing_level", "x86", "nan", "bool", "oom", "disk", "ram", "wrong_target", "missing_category", "unagreed", "mixed_synthetic"])
def test_resource_boundaries_fail_closed(change):
    samples, envelope = resource_case()
    if change == "missing_level": samples.pop()
    elif change == "x86": samples[0]["architecture"] = "x86_64"
    elif change == "nan": samples[0]["full_host_footprint_bytes"] = math.nan
    elif change == "bool": samples[0]["full_host_footprint_bytes"] = True
    elif change == "oom": samples[0]["oom_events"] = 1
    elif change == "disk": samples[0]["data_free_fraction"] = 0.19
    elif change == "ram": samples[0]["full_host_footprint_bytes"] = 6500000000
    elif change == "wrong_target": samples[0]["target_id"] = "other"
    elif change == "missing_category": samples[0]["coverage"].pop()
    elif change == "unagreed": envelope["owner_workload_agreement_supplied"] = False
    elif change == "mixed_synthetic": samples[0]["synthetic"] = False
    assert measure(samples, envelope).status != "PASS"


import importlib.machinery
import importlib.util


def runner_module():
    loader = importlib.machinery.SourceFileLoader("local_check", "scripts/local-check")
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


@pytest.mark.parametrize("env,endpoint", [({}, "tcp://127.0.0.1:2375"), ({}, "ssh://host"), ({"DOCKER_HOST":"tcp://remote:2375"}, "unix:///var/run/docker.sock"), ({"DOCKER_CONTEXT":"remote"}, "unix:///var/run/docker.sock"), ({"KUBECONFIG":"other"}, "unix:///var/run/docker.sock")])
def test_remote_context_refused_before_client(env, endpoint):
    with pytest.raises(ValueError, match="BLOCKED"):
        runner_module().check_local_context(env, endpoint)


@pytest.mark.parametrize("host,image", [("x86_64","arm64"), ("aarch64","amd64"), ("aarch64","unknown")])
def test_native_arm64_rejects_emulated_or_unknown_image(host, image):
    with pytest.raises(ValueError, match="BLOCKED"):
        runner_module().require_native_arm64(host, image)


def test_native_arm64_pair_positive_control():
    assert runner_module().require_native_arm64("aarch64", "arm64") is None


def test_local_context_positive_control():
    assert runner_module().check_local_context({}, "unix:///var/run/docker.sock") is None


@pytest.mark.integration
def test_pg_commit_checkpoint_replay(probe_settings):
    probe_settings["crash_after_sink_commit"] = True
    result = run_probe(probe_settings)
    assert result["crash_exit"] == 91
    assert result["crash_checkpoint_committed"] is False
    assert result["crash_offset_written"] is True
    assert result["crash_batch_id"] == result["replay_first_batch_id"]
    assert result["crash_rows"] == probe_settings["fixture"]["expected_rows"]
    assert result["replayed_rows"] == result["crash_rows"]
    assert result["pg_rows"] == probe_settings["fixture"]["expected_rows"]
    Path(probe_settings["evidence"] + "-crash-readback.json").write_text(json.dumps(result, indent=2))


@pytest.mark.integration
def test_final_artifact_lane():
    # Compatibility coverage, not manufactured behavioral RED for vendor code.
    from pyspark.sql import SparkSession
    jars = list(Path("/opt/spark/jars").glob("*.jar"))
    assert not any(p.name == "lz4-java-1.8.0.jar" for p in jars)
    assert len([p for p in jars if "lz4-java" in p.name]) == 1
    assert len([p for p in jars if "kafka-clients" in p.name]) == 1
    assert len([p for p in jars if "hadoop-client-api" in p.name]) == 1
    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    try:
        assert spark.version == "4.0.5"
        assert spark._jvm.org.apache.kafka.common.utils.AppInfoParser.getVersion() == "3.9.2"
        jdbc = (spark.read.format("jdbc")
                .option("url", "jdbc:postgresql://postgres:5432/feasibility")
                .option("query", "SELECT 7::bigint AS fixture_value")
                .option("user", "probe")
                .option("password", os.environ["TON_SENTINEL_PG_PASSWORD"])
                .option("driver", "org.postgresql.Driver").load())
        assert [r.fixture_value for r in jdbc.collect()] == [7]
        native = {"lz4": spark._jvm.net.jpountz.lz4.LZ4Factory.fastestInstance().toString(),
                  "snappy": spark._jvm.org.xerial.snappy.Snappy.getNativeLibraryVersion(),
                  "state_codec": spark.conf.get("spark.sql.streaming.stateStore.compression.codec")}
        Path("/evidence/native-codecs.json").write_text(json.dumps(native, indent=2))
    finally:
        spark.stop()


# Bounded local stack envelope (pure classifier) ---------------
from scripts.measure_feasibility import evaluate_stack, STACK_BUDGET

DIGEST = "sha256:" + "a" * 64


def stack_case():
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine(), platform.machine())
    containers = {}
    for name, budget in STACK_BUDGET["services"].items():
        containers[name] = {
            "image": f"example/{name}@{DIGEST}", "image_architecture": arch,
            "memory_usage_bytes": budget["max_memory_bytes"] // 2,
            "memory_limit_bytes": budget["max_memory_bytes"],
            "cpu_limit_cores": budget["max_cpu_cores"],
            "cpu_percent": 10.0, "data_bytes": 1024 if budget["max_data_bytes"] else 0}
    return {"synthetic": True, "host_architecture": platform.machine(),
            "containers": containers}


def test_stack_within_budget_passes_and_is_never_live():
    result = evaluate_stack(stack_case(), STACK_BUDGET)
    assert result["status"] == "PASS", result
    assert result["live"] is False and result["target_qualified"] is False


@pytest.mark.parametrize("change,status", [
    ("missing_service", "BLOCKED"), ("missing_measure", "BLOCKED"),
    ("nan_measure", "BLOCKED"), ("bool_measure", "BLOCKED"),
    ("unpinned", "BLOCKED"), ("tag_only", "BLOCKED"),
    ("wrong_arch", "BLOCKED"), ("unsupported_host", "BLOCKED"),
    ("unexpected_service", "BLOCKED"), ("empty_budget", "BLOCKED"),
    ("mem_over", "FAIL"), ("limit_over", "FAIL"), ("limit_missing", "FAIL"),
    ("cpu_over", "FAIL"), ("data_over", "FAIL"), ("total_over", "FAIL"),
])
def test_stack_fails_closed(change, status):
    obs, budget = stack_case(), json.loads(json.dumps(STACK_BUDGET))
    c = obs["containers"]
    first = next(iter(c))
    sb = budget["services"][first]
    if change == "missing_service": del c[first]
    elif change == "missing_measure": del c[first]["memory_usage_bytes"]
    elif change == "nan_measure": c[first]["cpu_percent"] = float("nan")
    elif change == "bool_measure": c[first]["data_bytes"] = True
    elif change == "unpinned": c[first]["image"] = "example/x:latest@"
    elif change == "tag_only": c[first]["image"] = "example/x:1.0"
    elif change == "wrong_arch": c[first]["image_architecture"] = "riscv64"
    elif change == "unsupported_host": obs["host_architecture"] = "riscv64"
    elif change == "unexpected_service": c["extra"] = dict(c[first])
    elif change == "empty_budget": budget = {}
    elif change == "mem_over": c[first]["memory_usage_bytes"] = sb["max_memory_bytes"] + 1
    elif change == "limit_over": c[first]["memory_limit_bytes"] = sb["max_memory_bytes"] + 1
    elif change == "limit_missing": c[first]["memory_limit_bytes"] = 0
    elif change == "cpu_over": c[first]["cpu_limit_cores"] = sb["max_cpu_cores"] + 1
    elif change == "data_over":
        k = next(n for n, b in budget["services"].items() if b["max_data_bytes"])
        c[k]["data_bytes"] = budget["services"][k]["max_data_bytes"] + 1
    elif change == "total_over":
        budget["total_memory_bytes"] = 1
        for v in c.values(): v["memory_usage_bytes"] = 2
    assert evaluate_stack(obs, budget)["status"] == status


# Host-only: the fixture runner executes this file INSIDE the app container, which has no docker CLI or socket,
# so the stack CLI cannot observe the compose project there. It is verified on the host instead.
@pytest.mark.skipif(__import__("shutil").which("docker") is None,
                    reason="docker CLI unavailable inside the app container; run on the host")
def test_stack_cli_measures_real_running_stack(tmp_path):
    out = tmp_path / "stack.json"
    proc = subprocess.run([sys.executable, "scripts/measure_feasibility.py", "--stack",
                           "ton-sentinel-local", "--output", str(out)],
                          capture_output=True, text=True)
    data = json.loads(out.read_text())
    assert proc.returncode == (0 if data["status"] == "PASS" else 2), proc.stderr
    assert data["live"] is False and data["target_qualified"] is False
    assert set(data["observation"]["containers"]) == set(STACK_BUDGET["services"])
    assert data["status"] == "PASS", data["reasons"]


def test_stack_cli_blocks_when_project_absent(tmp_path):
    out = tmp_path / "stack.json"
    proc = subprocess.run([sys.executable, "scripts/measure_feasibility.py", "--stack",
                           "no-such-project-xyz", "--output", str(out)],
                          capture_output=True, text=True)
    assert proc.returncode == 2
    assert json.loads(out.read_text())["status"] == "BLOCKED"


# Unknown/empty/absent evidence is never fit evidence (fail closed).
@pytest.mark.parametrize("mode", ["target", "local"])
def test_empty_samples_with_passing_envelope_never_pass(mode):
    _, envelope = resource_case()
    result = measure([], envelope, mode=mode, cpu_budget_cores=1)
    assert result.status != "PASS"
    assert result.actual_fit_eligible is False


def _cli(tmp_path, envelope_path, *extra):
    return subprocess.run(
        [sys.executable, "-B", "scripts/measure_feasibility.py", "--input", str(tmp_path),
         "--envelope", str(envelope_path), *extra],
        capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize("case", ["empty_metrics", "missing_envelope", "malformed_envelope",
                                  "malformed_metrics", "non_object_envelope"])
def test_cli_unusable_evidence_blocks_with_exit_2(tmp_path, case):
    _, envelope = resource_case()
    envelope_path = tmp_path / "envelope.json"
    envelope_path.write_text(json.dumps(envelope))
    if case == "empty_metrics":
        (tmp_path / "probe-resources.jsonl").write_text("")
    elif case == "missing_envelope":
        envelope_path = tmp_path / "absent-acceptance.json"
    elif case == "malformed_envelope":
        envelope_path.write_text("{not json")
    elif case == "non_object_envelope":
        envelope_path.write_text("[]")
    else:
        (tmp_path / "probe-resources.jsonl").write_text("{broken\n")
    proc = _cli(tmp_path, envelope_path, "--mode", "local", "--cpu-budget-cores", "1")
    assert proc.returncode == 2, proc.stderr
    output = json.loads(proc.stdout)
    assert output["status"] == "BLOCKED"
    assert output["actual_fit_eligible"] is False
    assert output["reasons"]
    assert json.loads((tmp_path / "resources.json").read_text()) == output
