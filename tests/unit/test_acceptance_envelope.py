"""Workload-envelope schema for measurement: TBD-OWNER values fail closed."""
import copy
import json
import re
from pathlib import Path

import pytest

from scripts.measure_feasibility import WORKLOAD_FIELDS, envelope_problems, measure
from tests.integration.test_feasibility import resource_case

# Tracked envelope (moved out of the git-ignored docs/ tree so a clean clone runs this test).
ACCEPTANCE = Path(__file__).resolve().parents[2] / "deploy" / "workload" / "acceptance.json"
OWNER = "TBD-OWNER"


def concrete_workload():
    level = {key: 1 for key in WORKLOAD_FIELDS}
    return {"safety_free_fraction": 0.20,
            "levels": [dict(level, name="low"), dict(level, name="high", events_per_second=2)]}


MEASURED_OBJECTIVES = {"measured_maintenance_seconds": 1, "measured_catchup_seconds": 1,
                       "measured_latency_seconds": 1, "measured_backlog_events": 1,
                       "zero_grid_rows_materialized": 1}


def case():
    samples, envelope = resource_case()
    envelope["workload"] = concrete_workload()
    for sample in samples:
        sample.update(MEASURED_OBJECTIVES)
    return samples, envelope


def test_complete_concrete_envelope_has_no_problems_and_still_passes():
    samples, envelope = case()
    assert envelope_problems(envelope) == []
    assert measure(samples, envelope).status == "PASS"


@pytest.mark.parametrize("where", ["top", "level_field", "level_name"])
def test_tbd_owner_anywhere_blocks_even_with_passing_samples(where):
    samples, envelope = case()
    if where == "top":
        envelope["target_id"] = OWNER
        for s in samples:
            s["target_id"] = OWNER
    elif where == "level_field":
        envelope["workload"]["levels"][1]["payload_bytes"] = OWNER
    else:
        envelope["workload"]["levels"][1]["name"] = OWNER
        envelope["levels"][1] = OWNER
        samples[1]["level"] = OWNER
    result = measure(samples, envelope)
    assert result.status == "BLOCKED"
    assert any("TBD-OWNER" in r for r in result.reasons)
    assert result.actual_fit_eligible is False


def test_tbd_owner_blocks_in_local_mode():
    samples, envelope = case()
    envelope["workload"]["levels"][0]["outage_seconds"] = OWNER
    result = measure(samples, envelope, mode="local", cpu_budget_cores=1)
    assert result.status == "BLOCKED"
    assert any("TBD-OWNER" in r for r in result.reasons)


@pytest.mark.parametrize("mutate", [
    lambda w: w["levels"][0].pop("latency_seconds"),
    lambda w: w["levels"][0].update(events_per_second=0),
    lambda w: w["levels"][0].update(events_per_second=True),
    lambda w: w["levels"][0].update(events_per_second="5"),
    lambda w: w["levels"][0].update(events_per_second=float("nan")),
    lambda w: w["levels"].pop(),
    lambda w: w["levels"][1].update(name="low"),
    lambda w: w.update(safety_free_fraction=0.19),
    lambda w: w.pop("safety_free_fraction"),
    lambda w: w.update(levels="low,high"),
])
def test_malformed_workload_block_is_a_problem_and_blocks(mutate):
    samples, envelope = case()
    mutate(envelope["workload"])
    assert envelope_problems(envelope)
    assert measure(samples, envelope).status == "BLOCKED"


def test_workload_level_names_must_match_envelope_levels():
    samples, envelope = case()
    envelope["workload"]["levels"][1]["name"] = "other"
    assert any("levels" in p for p in envelope_problems(envelope))
    assert measure(samples, envelope).status == "BLOCKED"


def test_envelope_without_workload_block_is_unchanged():
    _, envelope = resource_case()
    assert envelope_problems(envelope) == []


DECIDED_FIELDS = tuple(k for k in WORKLOAD_FIELDS if k != "baseline_fanout")


def committed():
    return json.loads(ACCEPTANCE.read_text())


def test_committed_envelope_has_concrete_proposed_values_and_no_tbd():
    envelope = committed()
    assert envelope_problems(envelope) == []
    assert OWNER not in json.dumps(envelope)
    assert isinstance(envelope["target_id"], str) and envelope["target_id"].strip()
    levels = envelope["workload"]["levels"]
    assert [lv["name"] for lv in levels] == envelope["levels"] == ["level-1", "level-2"]
    for level in levels:
        for key in WORKLOAD_FIELDS:
            assert type(level[key]) in (int, float) and level[key] > 0, key


def test_committed_envelope_is_not_owner_signed_and_controls_never_pass():
    envelope = committed()
    # The owner did not sign; a delegate decided under the owner's standing delegation.
    assert envelope["owner_workload_agreement_supplied"] is True
    assert "standing delegation" in envelope["owner_agreement_source"]
    agreement = envelope["proposal_provenance"]["owner_agreement"]
    assert agreement["owner_agreed"] is False and "NOT signed" in agreement["statement"]
    samples, _ = resource_case()  # synthetic aarch64 controls without measured objectives
    result = measure(samples, copy.deepcopy(envelope))
    assert result.status in ("BLOCKED", "FAIL") and result.actual_fit_eligible is False


def test_every_decided_value_has_matching_provenance():
    envelope = committed()
    prov = envelope["proposal_provenance"]
    target = prov["target_id"]
    assert target["value"] == envelope["target_id"]
    assert target["rationale"].strip() and target["source"].strip()
    for level in envelope["workload"]["levels"]:
        entries = prov["levels"][level["name"]]
        assert set(DECIDED_FIELDS) <= set(entries)
        for key in DECIDED_FIELDS:
            entry = entries[key]
            assert entry["value"] == level[key], (level["name"], key)
            assert entry["rationale"].strip() and entry["source"].strip(), key
            assert entry["unit"].strip(), key


def test_decided_values_are_internally_consistent():
    envelope = committed()
    d = envelope["derived"]
    one, two = envelope["workload"]["levels"]
    for lv in (one, two):
        assert lv["backlog_events"] == lv["events_per_second"] * lv["outage_seconds"]
        assert lv["zero_grid_rows"] == lv["active_tokens"] * d["derived_retention_windows_per_token"]["value"]
        assert lv["scan_membership"] == lv["events_per_second"] * d["reconciliation_range_seconds"]["value"]
        assert lv["baseline_fanout"] == d["baseline_lookback_windows_per_token"]["value"]
        assert lv["latency_seconds"] > d["window_seconds"]["value"]
        assert lv["catchup_seconds"] <= lv["outage_seconds"] * 2
        # Cold-start history is seeded at full retained cardinality (zero grid), so the wall-clock
        # run is steady state + outage + catch-up + maintenance with >= 3 steady windows.
        steady = lv["run_seconds"] - lv["outage_seconds"] - lv["catchup_seconds"] - lv["maintenance_seconds"]
        assert steady >= 3 * d["window_seconds"]["value"]
        assert lv["zero_grid_rows"] // lv["active_tokens"] >= d["baseline_lookback_windows_per_token"]["value"]
        # 24 h of broker ingress fits the single swap-topic partition budget.
        assert lv["events_per_second"] * lv["payload_bytes"] * 86400 <= d["kafka_swap_topic_budget_bytes"]["value"]
    # Level 2 is about twice level 1 for every scaled field (backlog is a product of two doubled terms).
    for key in DECIDED_FIELDS:
        ratio = two[key] / one[key]
        same = {"outage_seconds", "catchup_seconds", "run_seconds"}  # level-2 keeps these timing bounds; backlog = events x outage
        expected = 1 if key in same else 2
        assert ratio == expected, (key, ratio)


def test_budgets_are_local_ceilings_without_cloud_wording():
    envelope = committed()
    assert envelope["ram_budget_bytes"] == 6000000000 and envelope["cpu_budget_cores"] == 2
    d = envelope["derived"]
    assert d["ram_budget_ceiling_bytes"]["value"] == 6000000000
    assert d["cpu_budget_ceiling_cores"]["value"] == 2
    assert d["cpu_budget_ceiling_cores"]["unit"] == "local CPU cores"
    assert "local" in d["ram_budget_ceiling_bytes"]["formula"] and "not cloud" in d["ram_budget_ceiling_bytes"]["formula"]
    assert "g1_status" not in envelope
    flat = json.dumps(envelope)
    for forbidden in (r"\bOCI\b", r"\bOCPU\b", r"(?i)always[ -]free", r"\bG1\b", r"\bG5\b", r"(?i)oracle"):
        assert not re.search(forbidden, flat), forbidden


def test_measure_does_not_require_cloud_entitlement_status():
    samples, envelope = case()
    assert "g1_status" not in envelope
    result = measure(samples, envelope)
    assert result.status == "PASS", result.reasons
    envelope["owner_workload_agreement_supplied"] = False
    result = measure(samples, envelope)
    assert result.status == "BLOCKED"
    assert not any("G1" in r for r in result.reasons)


def test_committed_derived_constants_match_their_formulas():
    d = json.loads(ACCEPTANCE.read_text())["derived"]
    window = d["window_seconds"]["value"]
    assert window == 5 * 60
    assert d["windows_per_day"]["value"] == 86400 // window == 288
    assert d["baseline_lookback_windows_per_token"]["value"] == 7 * 86400 // window == 2016
    assert d["baseline_min_eligible_windows"]["value"] == 24
    assert d["baseline_min_history_seconds"]["value"] == 24 * window == 7200
    assert d["canonical_retention_windows_per_token"]["value"] == 7 * 288
    assert d["derived_retention_windows_per_token"]["value"] == 30 * 288
    assert d["reconciliation_walks_per_attempt"] == {"min_agreeing": 2, "max": 3, **{
        k: v for k, v in d["reconciliation_walks_per_attempt"].items() if k not in ("min_agreeing", "max")}}
    assert d["detectors"]["value"] == 2
    assert all(v.get("source") for v in d.values() if isinstance(v, dict))


def delegated_decision():
    return {"decision": "accepted_with_revisions", "decided_by": "heavy delegate",
            "authority": "owner_standing_delegation", "owner_signed": False,
            "decision_date": "2026-11-11", "rationale": "see workload decision record"}


def test_delegated_decision_counts_as_workload_agreement_without_owner_signature():
    samples, envelope = case()
    envelope["owner_workload_agreement_supplied"] = False
    assert measure(samples, envelope).status == "BLOCKED"
    envelope["workload_decision"] = delegated_decision()
    result = measure(samples, envelope)
    assert result.status == "PASS", result.reasons
    assert result.agreement_basis == "heavy_delegate_standing_delegation"
    envelope["owner_workload_agreement_supplied"] = True
    envelope.pop("workload_decision")
    assert measure(samples, envelope).agreement_basis == "owner_signed"


@pytest.mark.parametrize("mutate", [
    lambda d: d.update(owner_signed=True),
    lambda d: d.update(authority="self"),
    lambda d: d.update(decision_date="11/11/2026"),
    lambda d: d.update(decided_by=" "),
    lambda d: d.pop("rationale"),
    lambda d: d.update(decision="proposed"),
])
def test_invalid_delegated_decision_blocks(mutate):
    samples, envelope = case()
    envelope["owner_workload_agreement_supplied"] = False
    decision = delegated_decision()
    mutate(decision)
    envelope["workload_decision"] = decision
    result = measure(samples, envelope)
    assert result.status == "BLOCKED"
    assert any("agreed workload" in r for r in result.reasons)


@pytest.mark.parametrize("field", sorted(MEASURED_OBJECTIVES))
@pytest.mark.parametrize("value", [None, True, float("nan"), "1"])
def test_measured_objectives_required_when_workload_present(field, value):
    samples, envelope = case()
    if value is None:
        samples[0].pop(field)
    else:
        samples[0][field] = value
    result = measure(samples, envelope)
    assert result.status == "BLOCKED"
    assert any("measured objective" in r for r in result.reasons)


@pytest.mark.parametrize("field,objective", [
    ("measured_maintenance_seconds", "maintenance_seconds"),
    ("measured_catchup_seconds", "catchup_seconds"),
    ("measured_latency_seconds", "latency_seconds"),
])
def test_measured_objective_exceeding_envelope_fails(field, objective):
    samples, envelope = case()
    samples[1][field] = envelope["workload"]["levels"][1][objective] + 1
    result = measure(samples, envelope)
    assert result.status == "FAIL"
    assert any(objective in r for r in result.reasons)


@pytest.mark.parametrize("field", ["measured_backlog_events", "zero_grid_rows_materialized"])
def test_backlog_or_zero_grid_below_envelope_blocks(field):
    samples, envelope = case()
    envelope["workload"]["levels"][0]["backlog_events"] = 5
    envelope["workload"]["levels"][0]["zero_grid_rows"] = 5
    for s in samples:
        s["measured_backlog_events"] = 5
        s["zero_grid_rows_materialized"] = 5
    assert measure(samples, envelope).status == "PASS"
    samples[0][field] = 4
    result = measure(samples, envelope)
    assert result.status == "BLOCKED"
    assert any("below envelope" in r for r in result.reasons)


def test_committed_envelope_records_heavy_delegate_decision():
    envelope = committed()
    decision = envelope["workload_decision"]
    assert decision["authority"] == "owner_standing_delegation"
    assert decision["owner_signed"] is False
    assert decision["decision_date"] == "2026-11-11"
    assert "heavy delegate" in decision["decided_by"]
    assert decision["decision"] in ("accepted", "accepted_with_revisions")
    assert decision["rationale"].strip() and (ACCEPTANCE.parents[2] / decision["record"]).is_file()
    assert envelope["ram_budget_bytes"] == 6000000000 and envelope["cpu_budget_cores"] == 2
    review = decision["field_review"]
    for level in envelope["workload"]["levels"]:
        for key in DECIDED_FIELDS:
            entry = review[level["name"]][key]
            assert entry["verdict"] in ("accepted", "revised"), (level["name"], key)
            assert entry["value"] == level[key]
            if entry["verdict"] == "revised":
                assert entry["previous_value"] != entry["value"] and entry["rationale"].strip()


def test_measured_flag_requires_existing_measurement_evidence():
    envelope = committed()
    if envelope["maintenance_and_retention_measured"] is not True:
        assert envelope["status"] != "PASS"
        return
    paths = envelope["maintenance_and_retention_evidence"]
    assert {json.loads((ACCEPTANCE.parents[2] / p).read_text().splitlines()[0])["level"] for p in paths} == set(envelope["levels"])
    for p in paths:
        sample = json.loads((ACCEPTANCE.parents[2] / p).read_text().splitlines()[0])
        assert sample["synthetic"] is False
        assert type(sample["measured_maintenance_seconds"]) in (int, float)
        assert sample["zero_grid_rows_materialized"] >= next(
            lv["zero_grid_rows"] for lv in envelope["workload"]["levels"] if lv["name"] == sample["level"])
