#!/usr/bin/env python3
"""Classify supplied footprint evidence; missing/partial evidence cannot mean fit.

Synthetic controls are explicitly labeled and never actual fit eligibility. This is
not an entitlement evaluator, a measured workload envelope, or ARM qualification.
"""
from __future__ import annotations
import argparse
import json
import math
import platform
import os
import re
import subprocess
from datetime import datetime, timezone
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

COVERAGE = frozenset({"host_kernel_k3s", "broker_cache_native", "spark_driver_executor_python",
                      "state_checkpoint_spill", "pg_wal_index_vacuum", "page_cache",
                      "retention_zero_grid_repair", "backlog_catchup", "serialized_maintenance"})

TBD_OWNER = "TBD-OWNER"
WORKLOAD_FIELDS = tuple("events_per_second payload_bytes active_tokens active_wallets zero_grid_rows "
                        "scan_membership reconciliation_per_hour corrections_per_hour baseline_fanout "
                        "maintenance_seconds backlog_events outage_seconds latency_seconds "
                        "catchup_seconds run_seconds".split())


def _tbd_paths(value, path="$"):
    if value == TBD_OWNER:
        yield path
    elif isinstance(value, dict):
        for key, item in value.items():
            if key == TBD_OWNER:
                yield f"{path}.<key>"
            yield from _tbd_paths(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from _tbd_paths(item, f"{path}[{index}]")


def envelope_problems(envelope: dict) -> list[str]:
    """Fail-closed envelope schema check: any TBD-OWNER value or malformed workload block."""
    if not isinstance(envelope, dict):
        return ["envelope is not an object"]
    problems = []
    tbd = list(_tbd_paths(envelope))
    if tbd:
        problems.append(f"envelope contains {len(tbd)} unresolved TBD-OWNER value(s): "
                        + ", ".join(tbd[:8]) + (", ..." if len(tbd) > 8 else ""))
    if "workload" not in envelope:
        return problems
    workload = envelope["workload"]
    if not isinstance(workload, dict):
        return problems + ["workload block is not an object"]
    fraction = workload.get("safety_free_fraction")
    if fraction != TBD_OWNER and not (_number(fraction, 0.20) and fraction < 1):
        problems.append("workload safety_free_fraction must be a finite number in [0.20, 1)")
    levels = workload.get("levels")
    if not isinstance(levels, list) or len(levels) != 2 or not all(isinstance(v, dict) for v in levels):
        return problems + ["workload requires exactly two level objects"]
    names = [v.get("name") for v in levels]
    if not all(isinstance(n, str) and n for n in names) or len(set(names)) != 2:
        problems.append("workload requires two distinct non-empty level names")
    elif envelope.get("levels") != names:
        problems.append("workload levels names must equal envelope levels")
    for level in levels:
        for key in WORKLOAD_FIELDS:
            if key not in level:
                problems.append(f"workload level {level.get('name')!r} missing {key}")
            elif level[key] != TBD_OWNER and not _number(level[key], 0) or level.get(key) == 0:
                problems.append(f"workload level {level.get('name')!r} {key} must be a positive finite number")
    return problems


@dataclass(frozen=True)
class Footprint:
    status: str
    reasons: tuple[str, ...] = ()
    synthetic: bool = False
    actual_fit_eligible: bool = False
    agreement_basis: str = "none"


DELEGATED_AUTHORITY = "owner_standing_delegation"
# measured field -> (envelope level objective, comparison): "max" means measured <= objective
# (exceeding FAILs); "min" means measured >= envelope value (short of it BLOCKs: not measured at level).
MEASURED_OBJECTIVES = {
    "measured_maintenance_seconds": ("maintenance_seconds", "max"),
    "measured_catchup_seconds": ("catchup_seconds", "max"),
    "measured_latency_seconds": ("latency_seconds", "max"),
    "measured_backlog_events": ("backlog_events", "min"),
    "zero_grid_rows_materialized": ("zero_grid_rows", "min"),
}


def agreement_basis(envelope: dict) -> str:
    """Owner-signed agreement, or a heavy-delegate decision under the owner's standing delegation.

    A delegated decision is never an owner signature: it must say owner_signed=false explicitly.
    """
    if envelope.get("owner_workload_agreement_supplied") is True:
        return "owner_signed"
    decision = envelope.get("workload_decision")
    if (isinstance(decision, dict)
            and decision.get("authority") == DELEGATED_AUTHORITY
            and decision.get("owner_signed") is False
            and decision.get("decision") in ("accepted", "accepted_with_revisions")
            and isinstance(decision.get("decided_by"), str) and decision["decided_by"].strip()
            and isinstance(decision.get("rationale"), str) and decision["rationale"].strip()
            and isinstance(decision.get("decision_date"), str)
            and re.fullmatch(r"\d{4}-\d{2}-\d{2}", decision["decision_date"])):
        return "heavy_delegate_standing_delegation"
    return "none"


def objective_reasons(sample: dict, envelope: dict) -> tuple[list[str], bool]:
    """Compare a sample's measured objectives with its envelope level; (reasons, failed)."""
    workload = envelope.get("workload")
    if not isinstance(workload, dict) or not isinstance(workload.get("levels"), list):
        return [], False
    level = next((v for v in workload["levels"] if isinstance(v, dict) and v.get("name") == sample.get("level")), None)
    if level is None:
        return [], False
    reasons, failed = [], False
    for field, (objective, kind) in MEASURED_OBJECTIVES.items():
        measured, target = sample.get(field), level.get(objective)
        if not _number(measured) or not _number(target):
            reasons.append(f"missing or invalid measured objective {field}")
        elif kind == "max" and measured > target:
            failed = True
            reasons.append(f"{field} exceeds envelope {objective}")
        elif kind == "min" and measured < target:
            reasons.append(f"{field} below envelope {objective}")
    return reasons, failed


def _number(value, minimum=0):
    # Deliberate measurement bound; compare integers before any float conversion.
    return (type(value) in (int, float) and minimum <= value <= 2**63 - 1
            and (type(value) is int or math.isfinite(value)))


def measure(samples: list[dict], envelope: dict, *, mode: str = "target",
            cpu_budget_cores: int | float | None = None) -> Footprint:
    reasons = []
    if not isinstance(envelope, dict) or not isinstance(samples, list):
        return Footprint("BLOCKED", ("malformed evidence container",))
    if mode not in {"local", "target"}:
        return Footprint("BLOCKED", ("unknown classification mode",))
    synthetic = envelope.get("synthetic") is True
    if mode == "target" and not synthetic:
        reasons.append("actual fit requires native artifact review; bounded local classifier cannot qualify deployment")
    if not samples or not envelope:
        return Footprint("BLOCKED", ("missing resource samples or approved envelope",), synthetic)
    cpu_blocked = mode == "local" and not _number(cpu_budget_cores, 0)
    if cpu_blocked:
        reasons.append("local classification requires a finite nonnegative caller cpu_budget_cores")
    # Local-first: no cloud entitlement status is required or consulted.
    basis = agreement_basis(envelope)
    if (envelope.get("status") != "PASS" or basis == "none"
            or envelope.get("maintenance_and_retention_measured") is not True):
        reasons.append("envelope status, owner-agreed workload, or measured retention/maintenance absent")
    reasons.extend(envelope_problems(envelope))
    levels = envelope.get("levels")
    if not isinstance(levels, list) or len(levels) != 2 or not all(isinstance(v, str) and v for v in levels) or len(set(levels)) != 2:
        reasons.append("two distinct workload levels required")
        levels = []
    budget = envelope.get("ram_budget_bytes")
    disk_floor = envelope.get("required_minimum_free_data_volume_fraction")
    if not _number(budget, 1) or not _number(disk_floor, 0.20) or disk_floor > 1:
        reasons.append("invalid RAM budget or disk safety floor")
    observed = set()
    failed = False
    for sample in samples:
        if not isinstance(sample, dict):
            reasons.append("malformed sample")
            cpu_blocked |= mode == "local"
            continue
        expected_arch = platform.machine() if mode == "local" else "aarch64"
        if (sample.get("architecture") != expected_arch or not envelope.get("target_id")
                or sample.get("target_id") != envelope.get("target_id")
                or type(sample.get("synthetic")) is not bool or sample["synthetic"] != synthetic):
            reasons.append(f"{mode} architecture, target, or provenance mismatch")
        observed.add(sample.get("level") if isinstance(sample.get("level"), str) else None)
        coverage = sample.get("coverage")
        if not isinstance(coverage, list) or not all(isinstance(v, str) for v in coverage) or set(coverage) != COVERAGE:
            reasons.append("incomplete full footprint/workset measurement categories")
        if mode == "local":
            cpu_seconds = sample.get("measured_cpu_seconds")
            capacity = sample.get("measured_cpu_capacity_cores")
            scope = sample.get("cpu_measurement_scope")
            provenance = sample.get("cpu_measurement_provenance")
            if (not _number(cpu_seconds) or not _number(capacity) or capacity <= 0
                    or not isinstance(scope, str) or not scope.strip()
                    or not isinstance(provenance, str) or not provenance.strip()):
                reasons.append("missing or invalid CPU measurement, capacity, scope, or provenance")
                cpu_blocked = True
            elif (not _number(sample.get("elapsed_seconds")) or sample["elapsed_seconds"] <= 0
                  or cpu_seconds > sample["elapsed_seconds"] * capacity):
                reasons.append("impossible CPU seconds for elapsed interval and measured capacity")
                cpu_blocked = True
            elif _number(cpu_budget_cores, 0):
                cpu_budget = cast(float, cpu_budget_cores)
                cpu_capacity = cast(float, capacity)
                if cpu_budget > cpu_capacity:
                    failed = True
                    reasons.append("requested CPU budget exceeds measured CPU capacity")
                if cpu_seconds / sample["elapsed_seconds"] > cpu_budget:
                    failed = True
                    reasons.append("average CPU consumption exceeds caller CPU budget")
        objective, objective_failed = objective_reasons(sample, envelope)
        reasons.extend(objective)
        failed |= objective_failed
        numeric = ("full_host_footprint_bytes", "data_free_fraction", "oom_events", "elapsed_seconds", "input_bytes", "events")
        if not all(_number(sample.get(k)) for k in numeric):
            reasons.append("missing/nonfinite/bool resource measurement")
            continue
        if sample["elapsed_seconds"] <= 0 or sample["events"] <= 0 or sample["input_bytes"] <= 0 or sample["full_host_footprint_bytes"] <= 0 or sample["data_free_fraction"] > 1:
            reasons.append("empty or invalid measured workload")
        if sample["oom_events"] > 0:
            failed = True
            reasons.append("observed OOM")
        if _number(budget, 1) and sample["full_host_footprint_bytes"] > budget:
            failed = True
            reasons.append("full footprint exceeds residual RAM")
        if _number(disk_floor, 0.20) and sample["data_free_fraction"] < disk_floor:
            failed = True
            reasons.append("insufficient data-volume safety headroom")
    if set(levels) != observed:
        reasons.append("missing or unexpected workload level")
    # Invalid CPU evidence prevents classification; retain observed failures in reasons.
    status = "BLOCKED" if cpu_blocked else "FAIL" if failed else "BLOCKED" if reasons else "PASS"
    return Footprint(status, tuple(dict.fromkeys(reasons)), synthetic,
                     status == "PASS" and not synthetic and mode == "target", basis)


def host_snapshot(filesystem: Path, memory_budget_bytes: int | None,
                  free_disk_safety_floor_bytes: int | None, *,
                  cpu_budget_cores: int | float | None = None,
                  meminfo_path: Path = Path("/proc/meminfo"),
                  cgroup_path: Path = Path("/proc/self/cgroup"),
                  mountinfo_path: Path = Path("/proc/self/mountinfo")) -> dict:
    """Observe visible Linux CPU/memory capacity and a selected filesystem."""
    reasons = []
    memory = None
    filesystem_observation = None
    try:
        fields = {}
        for line in meminfo_path.read_text().splitlines():
            key, value = line.split(":", 1)
            parts = value.split()
            if len(parts) != 2 or parts[1] != "kB":
                continue
            fields[key] = int(parts[0]) * 1024
        total = fields.get("MemTotal")
        available = fields.get("MemAvailable")
        if not total or available is None or available > total:
            raise ValueError("missing or invalid MemTotal/MemAvailable")
        memory = {"source": str(meminfo_path), "total_bytes": total,
                  "available_bytes": available}
    except (OSError, ValueError, TypeError):
        reasons.append("Linux memory observation missing or invalid")
    cgroup = None
    membership = None
    mount = None
    try:
        membership = None
        for line in cgroup_path.read_text().splitlines():
            fields = line.split(":", 2)
            if len(fields) == 3 and fields[0] == "0" and fields[1] == "":
                membership = Path(fields[2])
                break
        if membership is None or not membership.is_absolute() or ".." in membership.parts:
            raise ValueError("unified cgroup membership missing or unsafe")
        mount = None
        for line in mountinfo_path.read_text().splitlines():
            before, separator, after = line.partition(" - ")
            if not separator:
                continue
            left, right = before.split(), after.split()
            if len(left) < 5 or len(right) < 3 or right[0] != "cgroup2":
                continue
            root, mountpoint = Path(left[3]), Path(left[4])
            if not root.is_absolute() or not mountpoint.is_absolute() or ".." in root.parts or ".." in mountpoint.parts:
                raise ValueError("unsafe cgroup2 mount path")
            try:
                relative = membership.relative_to(root)
            except ValueError:
                continue
            mount = (mountpoint, relative)
            break
        if mount is None:
            raise ValueError("cgroup2 memory controller mount unavailable (cgroup v1 unsupported)")
        mountpoint, relative = mount
        leaf = mountpoint / relative
        if not leaf.is_dir():
            raise ValueError("current cgroup directory unavailable")
        observed_limits = []
        cursor = leaf
        while True:
            limit_path = cursor / "memory.max"
            current_path = cursor / "memory.current"
            try:
                limit_text = limit_path.read_text().strip()
                current_text = current_path.read_text().strip()
            except FileNotFoundError:
                # A cgroup2 hierarchy root has no memory accounting interface.
                # Missing interfaces in descendants or partial pairs are errors.
                if cursor == mountpoint and not limit_path.exists() and not current_path.exists():
                    observed_limits.append({"path": str(cursor), "limit_bytes": None,
                                            "current_bytes": None, "remaining_bytes": None,
                                            "interface": "absent_at_visible_root"})
                    break
                raise
            if not current_text.isdecimal() or int(current_text) > 2**63 - 1:
                raise ValueError(f"invalid memory.current at {cursor}")
            current = int(current_text)
            if limit_text == "max":
                observed_limits.append({"path": str(cursor), "limit_bytes": None,
                                        "current_bytes": current, "remaining_bytes": None})
            elif limit_text.isdecimal() and int(limit_text) <= 2**63 - 1:
                limit = int(limit_text)
                observed_limits.append({"path": str(cursor), "limit_bytes": limit,
                                        "current_bytes": current,
                                        "remaining_bytes": max(0, limit - current)})
            else:
                raise ValueError(f"invalid memory.max at {cursor}")
            if cursor == mountpoint:
                break
            parent = cursor.parent
            if parent == cursor or not parent.is_relative_to(mountpoint):
                raise ValueError("cgroup hierarchy escaped mounted controller")
            cursor = parent
        finite_remaining = [row["remaining_bytes"] for row in observed_limits
                            if row["remaining_bytes"] is not None]
        if not finite_remaining:
            raise ValueError("no finite observable cgroup memory limit")
        cgroup = {"source": "cgroup v2 memory.max/memory.current",
                  "membership": str(membership), "mountpoint": str(mountpoint),
                  "limits": observed_limits,
                  "effective_headroom_bytes": min(finite_remaining)}
    except (OSError, ValueError, TypeError) as exc:
        reasons.append(f"cgroup memory observation unavailable or invalid: {exc}")
    cpu = None
    try:
        affinity = os.sched_getaffinity(0)
        if not affinity or any(type(core) is not int or core < 0 for core in affinity):
            raise ValueError("invalid CPU affinity set")
        if membership is None or mount is None:
            # Resolve membership and the cgroup2 mount independently of the memory
            # parse result; both controllers share the same visible hierarchy.
            membership = None
            for line in cgroup_path.read_text().splitlines():
                fields = line.split(":", 2)
                if len(fields) == 3 and fields[0] == "0" and fields[1] == "":
                    membership = Path(fields[2])
                    break
            if membership is None or not membership.is_absolute() or ".." in membership.parts:
                raise ValueError("unified cgroup membership missing or unsafe")
            mount = None
            for line in mountinfo_path.read_text().splitlines():
                before, separator, after = line.partition(" - ")
                if not separator:
                    continue
                left, right = before.split(), after.split()
                if len(left) < 5 or len(right) < 3 or right[0] != "cgroup2":
                    continue
                root, mountpoint = Path(left[3]), Path(left[4])
                if not root.is_absolute() or not mountpoint.is_absolute() or ".." in root.parts or ".." in mountpoint.parts:
                    raise ValueError("unsafe cgroup2 mount path")
                try:
                    relative = membership.relative_to(root)
                except ValueError:
                    continue
                mount = (mountpoint, relative)
                break
        if mount is None:
            raise ValueError("cgroup v2 CPU controller mount unavailable")
        mountpoint, relative = mount
        cursor = mountpoint / relative
        if not cursor.is_dir():
            raise ValueError("current cgroup directory unavailable")
        limits = []
        while True:
            quota_path = cursor / "cpu.max"
            try:
                quota_text = quota_path.read_text().strip()
            except FileNotFoundError:
                if cursor == mountpoint and not quota_path.exists():
                    limits.append({"path": str(cursor), "quota_cores": None,
                                   "interface": "absent_at_visible_root"})
                    break
                controllers_path = cursor / "cgroup.controllers"
                if (controllers_path.is_file()
                        and "cpu" not in controllers_path.read_text().split()):
                    # cgroup v2 exposes cpu.max only where the cpu controller is
                    # enabled; no bandwidth limit at this level, ancestors still apply.
                    limits.append({"path": str(cursor), "quota_cores": None,
                                   "interface": "cpu_controller_not_enabled"})
                    cursor = cursor.parent
                    if not cursor.is_relative_to(mountpoint):
                        raise ValueError("cgroup hierarchy escaped mounted controller")
                    continue
                raise
            fields = quota_text.split()
            if len(fields) != 2 or not fields[1].isdecimal() or int(fields[1]) <= 0:
                raise ValueError(f"invalid cpu.max at {cursor}")
            period = int(fields[1])
            if fields[0] == "max":
                quota_cores = None
            elif fields[0].isdecimal() and int(fields[0]) > 0:
                quota_cores = int(fields[0]) / period
                if not math.isfinite(quota_cores) or quota_cores <= 0:
                    raise ValueError(f"unrepresentable cpu.max quota at {cursor}")
            else:
                raise ValueError(f"invalid cpu.max quota at {cursor}")
            limits.append({"path": str(cursor), "quota_cores": quota_cores})
            if cursor == mountpoint:
                break
            cursor = cursor.parent
            if not cursor.is_relative_to(mountpoint):
                raise ValueError("cgroup hierarchy escaped mounted controller")
        quota_values = [row["quota_cores"] for row in limits if row["quota_cores"] is not None]
        effective = min([float(len(affinity)), *quota_values])
        cpu = {"source": "sched_getaffinity + cgroup v2 cpu.max", "affinity_cpu_ids": sorted(affinity),
               "affinity_cores": len(affinity), "limits": limits,
               "effective_cores": effective,
               "limit_visibility": "visible hierarchy only; external ancestors may be omitted"}
    except (OSError, ValueError, TypeError, AttributeError, OverflowError) as exc:
        reasons.append(f"CPU capacity observation unavailable or invalid: {exc}")
    try:
        stat = os.statvfs(filesystem)
        free_bytes = stat.f_bavail * stat.f_frsize
        total_bytes = stat.f_blocks * stat.f_frsize
        if stat.f_frsize <= 0 or total_bytes <= 0 or free_bytes > total_bytes:
            raise ValueError("invalid filesystem capacity")
        filesystem_observation = {"selected_path": str(filesystem.resolve()),
                                  "total_bytes": total_bytes,
                                  "available_to_unprivileged_bytes": free_bytes}
    except (OSError, ValueError):
        reasons.append("selected filesystem observation missing or invalid")
    for value, label in ((memory_budget_bytes, "memory budget"),
                         (free_disk_safety_floor_bytes, "free-disk safety floor")):
        if type(value) is not int or value <= 0 or value > 2**63 - 1:
            reasons.append(f"explicit positive finite {label} required")
    try:
        valid_cpu_budget = (type(cpu_budget_cores) in (int, float)
                            and math.isfinite(cast(float, cpu_budget_cores))
                            and cast(float, cpu_budget_cores) > 0)
    except OverflowError:
        valid_cpu_budget = False
    if not valid_cpu_budget:
        reasons.append("explicit positive finite CPU budget required")
    if cpu is not None and valid_cpu_budget:
        if cast(float, cpu_budget_cores) > cpu["effective_cores"]:
            reasons.append("effective CPU capacity below supplied CPU budget")
    if memory is not None and cgroup is not None:
        cgroup["effective_available_bytes"] = min(memory["available_bytes"], cgroup["effective_headroom_bytes"])
    if memory is not None and cgroup is not None and type(memory_budget_bytes) is int and memory_budget_bytes > 0:
        if cgroup["effective_available_bytes"] < memory_budget_bytes:
            reasons.append("available memory below supplied memory budget")
    if (filesystem_observation is not None and type(free_disk_safety_floor_bytes) is int
            and free_disk_safety_floor_bytes > 0
            and filesystem_observation["available_to_unprivileged_bytes"] < free_disk_safety_floor_bytes):
        reasons.append("available filesystem capacity below supplied safety floor")
    return {
        "mode": "host_snapshot",
        "admission_status": "BLOCKED" if any("required" in r or "missing or invalid" in r or "unavailable or invalid" in r for r in reasons)
                            else "FAIL" if reasons else "PASS",
        "reasons": reasons,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "architecture": platform.machine(),
        "observations": {"memory": memory, "cgroup_memory": cgroup, "cpu": cpu, "filesystem": filesystem_observation},
        "budgets": {"minimum_available_memory_bytes": memory_budget_bytes,
                    "minimum_available_filesystem_bytes": free_disk_safety_floor_bytes,
                    "cpu_budget_cores": cpu_budget_cores},
        "provenance": {"memory_source": str(meminfo_path), "cgroup_memory_source": "cgroup v2 membership + mountinfo + memory.max/current", "filesystem_source": "statvfs",
                       "filesystem_path": str(filesystem.resolve()) if filesystem_observation else str(filesystem)},
        "limitations": ["Linux procfs and filesystem values are as visible to this process; they may be container/cgroup/mount scoped, not physical-host capacity.",
                        "Snapshot only: no workload footprint, component coverage, retention, maintenance, or sustained capacity measurement.",
                        "The visible cgroup hierarchy may omit limits imposed by ancestors outside the mounted namespace; only observed ancestors are accounted for.",
                        "Budgets are caller-supplied admission thresholds, not approved envelopes or target qualification."],
    }


MIB = 2**20
# Documented finite local budget; mirrors deploy/compose.yaml limits. Local only.
STACK_BUDGET = {
    "scope": "local fixture stack budget; not final-target or live qualification",
    "total_memory_bytes": 4608 * MIB,
    "services": {
        "postgres": {"max_memory_bytes": 512 * MIB, "max_cpu_cores": 0.5, "max_data_bytes": 1024 * MIB},
        "kafka": {"max_memory_bytes": 1024 * MIB, "max_cpu_cores": 0.5, "max_data_bytes": 512 * MIB},
        "app": {"max_memory_bytes": 3072 * MIB, "max_cpu_cores": 1.0, "max_data_bytes": 512 * MIB},
    },
}
_ARCH = {"x86_64": "amd64", "aarch64": "arm64"}
_PINNED = re.compile(r"@sha256:[0-9a-f]{64}$")
# Data directories measured inside each container (bytes via du).
_DATA_DIRS = {"postgres": "/var/lib/postgresql", "kafka": "/var/lib/kafka/data", "app": "/runtime/tmp"}


def evaluate_stack(observation, budget) -> dict:
    """Classify an observed container footprint against a finite budget (fail-closed)."""
    blocked, failed = [], []
    result = {"mode": "stack", "live": False, "target_qualified": False,
              "synthetic": bool(isinstance(observation, dict) and observation.get("synthetic") is True),
              "scope": "local container footprint snapshot; no sustained/peak, live, or target qualification",
              "observation": observation, "budget": budget}
    services = budget.get("services") if isinstance(budget, dict) else None
    containers = observation.get("containers") if isinstance(observation, dict) else None
    if (not isinstance(services, dict) or not services or not _number(budget.get("total_memory_bytes"), 1)
            or not isinstance(containers, dict) or not containers):
        result.update(status="BLOCKED", reasons=["missing or malformed observation or finite budget"])
        return result
    host = observation.get("host_architecture")
    if host not in _ARCH:
        blocked.append(f"unsupported host architecture: {host!r}")
    for extra in sorted(set(containers) - set(services)):
        blocked.append(f"unbudgeted service observed: {extra}")
    total = 0
    for name, sb in services.items():
        c = containers.get(name)
        if not isinstance(c, dict):
            blocked.append(f"{name}: no measurement"); continue
        if not all(_number(c.get(k)) for k in ("memory_usage_bytes", "memory_limit_bytes", "cpu_limit_cores",
                                                "cpu_percent", "data_bytes")):
            blocked.append(f"{name}: missing/nonfinite/bool measurement"); continue
        if not (isinstance(c.get("image"), str) and _PINNED.search(c["image"])):
            blocked.append(f"{name}: image not pinned by sha256 digest")
        if host in _ARCH and c.get("image_architecture") != _ARCH[host]:
            blocked.append(f"{name}: image architecture {c.get('image_architecture')!r} != host {_ARCH[host]}")
        if c["memory_limit_bytes"] <= 0 or c["cpu_limit_cores"] <= 0:
            failed.append(f"{name}: unbounded memory or CPU limit")
        if c["memory_limit_bytes"] > sb["max_memory_bytes"]:
            failed.append(f"{name}: memory limit exceeds budget")
        if c["cpu_limit_cores"] > sb["max_cpu_cores"]:
            failed.append(f"{name}: CPU limit exceeds budget")
        if c["memory_usage_bytes"] > sb["max_memory_bytes"]:
            failed.append(f"{name}: memory usage exceeds budget")
        if c["data_bytes"] > sb["max_data_bytes"]:
            failed.append(f"{name}: data volume exceeds budget")
        total += c["memory_usage_bytes"]
    if total > budget["total_memory_bytes"]:
        failed.append("total memory usage exceeds budget")
    result["total_memory_usage_bytes"] = total
    result["status"] = "BLOCKED" if blocked else "FAIL" if failed else "PASS"
    result["reasons"] = blocked + failed
    return result


def _docker(*args):
    out = subprocess.run(["docker", "--context", "default", *args], capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        raise ValueError(f"docker {args[0]} failed: {out.stderr.strip()[:200]}")
    return out.stdout


def _bytes(text):
    m = re.fullmatch(r"([0-9.]+)\s*([KMGT]?i?B)", text.strip())
    if not m: raise ValueError(f"unparseable size {text!r}")
    unit = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40,
            "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4}[m.group(2)]
    return int(float(m.group(1)) * unit)


def collect_stack(project: str) -> dict:
    """Read the local compose project's real containers via the local docker CLI."""
    obs = {"synthetic": False, "host_architecture": platform.machine(), "project": project,
           "source": "docker inspect/stats/exec (local default context)",
           "observed_at_utc": datetime.now(timezone.utc).isoformat(), "containers": {}}
    ids = _docker("ps", "-q", "--filter", f"label=com.docker.compose.project={project}").split()
    for cid in ids:
        info = json.loads(_docker("inspect", cid))[0]
        service = info["Config"]["Labels"].get("com.docker.compose.service")
        stats = json.loads(_docker("stats", "--no-stream", "--format", "{{json .}}", cid))
        usage = _bytes(stats["MemUsage"].split("/")[0])
        host = info["HostConfig"]
        image = json.loads(_docker("image", "inspect", info["Image"]))[0]
        entry = {"image": info["Config"]["Image"], "image_architecture": image.get("Architecture"),
                 "memory_usage_bytes": usage, "memory_limit_bytes": host.get("Memory", 0),
                 "cpu_limit_cores": host.get("NanoCpus", 0) / 1e9,
                 "cpu_percent": float(stats["CPUPerc"].rstrip("%"))}
        try:
            du = _docker("exec", cid, "du", "-sk", _DATA_DIRS.get(service, "/nonexistent")).split()[0]
            entry["data_bytes"] = int(du) * 1024  # allocated KiB; apparent size is inflated by sparse Kafka index files
        except (ValueError, IndexError, subprocess.SubprocessError):
            pass  # absent data_bytes => evaluate_stack BLOCKED
        obs["containers"][service] = entry
    return obs


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--host-snapshot", action="store_true", help="observe current Linux CPU/memory and one filesystem")
    modes.add_argument("--stack", metavar="COMPOSE_PROJECT", help="measure the running local compose project against STACK_BUDGET")
    modes.add_argument("--input", type=Path, help="directory containing supplied resource JSONL")
    parser.add_argument("--envelope", type=Path)
    parser.add_argument("--filesystem", type=Path)
    parser.add_argument("--memory-budget-bytes", type=int)
    parser.add_argument("--free-disk-safety-floor-bytes", type=int)
    parser.add_argument("--cpu-budget-cores", type=float,
                        help="caller-supplied local CPU budget in cores (required for local mode)")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--mode", choices=("local", "target"), default="target",
                        help="local classifies measured evidence on this host; target retains strict qualification gates")
    args = parser.parse_args()
    if args.stack:
        if args.output is None:
            parser.error("--stack requires --output")
        try:
            result = evaluate_stack(collect_stack(args.stack), STACK_BUDGET)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
            result = {"mode": "stack", "status": "BLOCKED", "live": False, "target_qualified": False,
                      "reasons": [f"measurement failed: {exc}"]}
        else:
            if not result["observation"]["containers"]:
                result.update(status="BLOCKED", reasons=["no running containers for project"])
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
        return 0 if result["status"] == "PASS" else 2
    if args.host_snapshot:
        if args.filesystem is None or args.output is None:
            parser.error("--host-snapshot requires --filesystem and --output")
        result = host_snapshot(args.filesystem, args.memory_budget_bytes,
                               args.free_disk_safety_floor_bytes,
                               cpu_budget_cores=args.cpu_budget_cores)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
        return 0 if result["admission_status"] == "PASS" else 2
    if args.envelope is None:
        parser.error("--input requires --envelope")
    try:
        envelope = json.loads(args.envelope.read_text())
        samples = []
        for path in sorted(args.input.glob("*-resources.jsonl")):
            samples.extend(json.loads(line) for line in path.read_text().splitlines() if line)
        if not isinstance(envelope, dict):
            raise ValueError("envelope is not a JSON object")
        result = asdict(measure(samples, envelope, mode=args.mode,
                                cpu_budget_cores=args.cpu_budget_cores))
    except (OSError, ValueError) as exc:  # unreadable/malformed evidence never means fit
        result = asdict(Footprint("BLOCKED", (f"unusable evidence: {type(exc).__name__}: {exc}",)))
    result["mode"] = args.mode
    result["scope"] = ("supplied local evidence classification only; not final-target qualification"
                       if args.mode == "local" else
                       "supplied evidence classification only; target qualification requires independent review")
    if args.mode == "local":
        result["cpu_classification_limitations"] = [
            "CPU consumption is an average over each supplied elapsed interval, not a peak or sustained-capacity measurement.",
            "This is evidence classification only; it does not qualify execution authorization or sustained/peak CPU capacity."]
    (args.input / "resources.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" and (args.mode == "local" or result["actual_fit_eligible"]) else 2

if __name__ == "__main__":
    raise SystemExit(main())
