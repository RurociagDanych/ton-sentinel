"""Exact qualified baseline grid and detector input contract (pure contract tests).

The hand-authored oracle is ``tests/fixtures/truth/expected.json`` -> ``baseline_grid``; the
expected states there are written from spec section 8.1, never derived from product code.
"""
import dataclasses
import json
from pathlib import Path

import pytest

from ton_sentinel import config
from ton_sentinel.contracts import (
    ELIGIBLE_GRID_STATES, GRID_STATES, BaselineInput, DetectorPolicy, GridCell, baseline_span,
    grid_cell_state,
)

ORACLE = json.loads((Path(__file__).parents[1] / "fixtures" / "truth" / "expected.json")
                    .read_text())["baseline_grid"]
W = 300
POLICY = config.DETECTOR_POLICY_V1


def cell(start, state, token="jetton:A", network="testnet", direction="both", synthetic=True,
         coverage="bounded_observed", **values):
    defaults = {
        "observed": dict(swaps=3, known_traders=3, unknown_count=0, revision="feature:g:1:w"),
        "zero": dict(swaps=0, known_traders=0, unknown_count=0, revision="qualification:m1"),
        "partial": dict(swaps=3, known_traders=1, unknown_count=2, revision="feature:g:1:w"),
        "provisional": dict(swaps=None, known_traders=None, unknown_count=None, revision=None),
        "missing": dict(swaps=None, known_traders=None, unknown_count=None, revision=None),
        "pre_universe": dict(swaps=None, known_traders=None, unknown_count=None, revision=None),
    }[state]
    defaults.update(values)
    if state in ("observed", "zero"):
        cov, syn = coverage, synthetic
    else:
        cov, syn = None, False
    return GridCell(network=network, token=token, direction=direction, window_start=start,
                    state=state, coverage=cov, synthetic=syn, **defaults)


def grid(scored, states, **kw):
    """Cells of the 7-day span before ``scored`` (clipped at 0); ``states``: index -> state."""
    first = max(0, scored - 7 * 86400) // W
    return tuple(cell(i * W, states.get(i, "missing"), **kw) for i in range(first, scored // W))


def make(scored=60 * W, states=None, current="observed", **overrides):
    states = {} if states is None else states
    args = dict(network="testnet", token="jetton:A", window_start=scored, policy=POLICY,
                universe_version=1, admitted_window_start=0, dependency_epoch=3,
                active_epoch='{"network":"testnet"}',
                current=cell(scored, current) if isinstance(current, str) else current,
                cells=grid(scored, states))
    args.update(overrides)
    return BaselineInput.create(**args)


# ------------------------------------------------------------------------- detector policy
def test_v1_policy_is_the_spec_default_and_has_a_fixed_identity():
    p = POLICY
    assert (p.version, p.window_seconds, p.lookback_seconds, p.min_prior_windows) == (
        "detector-policy.v1", 300, 7 * 86400, 24)
    assert (p.score_threshold, p.swap_floor, p.trader_floor, p.attribution_floor) == (
        "6", 10, 5, "0.90")
    assert (p.mad_multiplier, p.scale_floor, p.direction) == ("1.4826", "1", "both")
    assert p.detectors == ("swap_count_spike.v1", "active_wallets_spike.v1")
    assert p.identity.startswith("sha256:") and len(p.identity) == 71
    assert p.identity == DetectorPolicy().identity == config.DETECTOR_POLICY_V1_ID
    assert json.loads(p.to_json())["min_prior_windows"] == 24


@pytest.mark.parametrize("field,value", [
    ("min_prior_windows", 23), ("score_threshold", "6.0"), ("swap_floor", 11),
    ("attribution_floor", "0.9"), ("lookback_seconds", 6 * 86400), ("mad_multiplier", "1.5"),
])
def test_any_policy_change_changes_the_identity(field, value):
    changed = dataclasses.replace(POLICY, **{field: value})
    assert changed.identity != POLICY.identity


@pytest.mark.parametrize("field,value", [
    ("min_prior_windows", 0), ("min_prior_windows", True), ("swap_floor", 1.0),
    ("score_threshold", 6.0), ("score_threshold", "nan"), ("attribution_floor", "1.5"),
    ("lookback_seconds", 301), ("window_seconds", 60), ("direction", "input"),
    ("detectors", ("swap_count_spike.v1",)), ("mad_multiplier", "0"), ("scale_floor", "0"),
])
def test_policy_rejects_invalid_or_unsupported_values(field, value):
    with pytest.raises(ValueError):
        dataclasses.replace(POLICY, **{field: value})


def test_config_parses_policy_strictly_and_defaults_to_v1():
    assert config.detector_policy() is POLICY
    assert config.detector_policy(json.loads(POLICY.to_json())) == POLICY
    raw = json.loads(POLICY.to_json())
    for bad in ({**raw, "extra": 1}, {k: v for k, v in raw.items() if k != "swap_floor"},
                {**raw, "swap_floor": 10.0}, {**raw, "detectors": "swap_count_spike.v1"}, []):
        with pytest.raises(ValueError):
            config.detector_policy(bad)


# ------------------------------------------------------------------------- cell states
def oracle_rows():
    for entry in ORACLE["entries"]:
        for i in range(entry["first"], entry["last"] + 1):
            yield i, entry["setup"], entry["expected"]


def classifier_facts(i, setup):
    """Map hand-authored setup facts to the classifier's inputs (no product logic)."""
    row = setup.get("direct_write_after_promotion") or setup.get("promoted_row")
    action = setup.get("canonical_action")
    return dict(
        feature_swaps=None if row is None else row["swaps"],
        canonical=("promoted_row" in setup or "promoted_other_token" in setup)
        and "direct_write_after_promotion" not in setup,
        qualified=setup.get("qualified", False),
        pending=setup.get("pending_target", False),
        touched=action is not None and ORACLE["token"] in action,
        in_universe=i >= ORACLE["admission_index"])


def test_cell_states_match_the_hand_authored_oracle():
    rows = list(oracle_rows())
    assert [i for i, _, _ in rows] == list(range(ORACLE["scored_index"] + 1))
    assert {state for _, _, state in rows} == set(GRID_STATES)
    for i, setup, expected in rows:
        assert grid_cell_state(**classifier_facts(i, setup)) == expected, i


def test_absent_row_is_zero_only_with_qualified_coverage_inside_the_universe():
    base = dict(feature_swaps=None, canonical=False, qualified=True, pending=False,
                touched=False, in_universe=True)
    assert grid_cell_state(**base) == "zero"                                  # never seen
    assert grid_cell_state(**{**base, "qualified": False}) == "missing"       # gap, not zero
    assert grid_cell_state(**{**base, "in_universe": False}) == "pre_universe"
    assert grid_cell_state(**{**base, "touched": True}) == "provisional"     # awaiting repair
    assert grid_cell_state(**{**base, "touched": True, "canonical": True}) == "zero"
    assert grid_cell_state(**{**base, "pending": True}) == "provisional"


def test_rows_need_canonical_closure_and_coverage_to_be_eligible():
    base = dict(feature_swaps=4, canonical=True, qualified=True, pending=False,
                touched=True, in_universe=True)
    assert grid_cell_state(**base) == "observed"
    assert grid_cell_state(**{**base, "qualified": False}) == "partial"
    assert grid_cell_state(**{**base, "canonical": False}) == "provisional"
    assert grid_cell_state(**{**base, "pending": True}) == "provisional"
    assert grid_cell_state(**{**base, "feature_swaps": 0}) == "zero"
    # an observed row before the token's admission is real data, never pre_universe
    assert grid_cell_state(**{**base, "in_universe": False}) == "observed"
    assert ELIGIBLE_GRID_STATES == ("observed", "zero")


@pytest.mark.parametrize("state,values", [
    ("observed", dict(swaps=0)), ("observed", dict(known_traders=3, unknown_count=1)),
    ("zero", dict(swaps=1)), ("zero", dict(revision=None)),
    ("missing", dict(swaps=0, known_traders=0, unknown_count=0)),
    ("partial", dict(swaps=None, known_traders=None, unknown_count=None)),
    ("pre_universe", dict(swaps=0, known_traders=0, unknown_count=0)),
    ("observed", dict(swaps=True)),
])
def test_cell_values_must_match_their_state(state, values):
    with pytest.raises(ValueError):
        cell(0, state, **values)


def test_zero_and_observed_need_coverage_and_others_carry_none():
    with pytest.raises(ValueError):
        GridCell("testnet", "jetton:A", "both", 0, "zero", 0, 0, 0, "q", None, False)
    with pytest.raises(ValueError):
        GridCell("testnet", "jetton:A", "both", 0, "missing", None, None, None, None,
                 "bounded_observed", False)
    with pytest.raises(ValueError):       # a fixture is never certified
        GridCell("testnet", "jetton:A", "both", 0, "zero", 0, 0, 0, "q", "certified", True)
    with pytest.raises(ValueError):
        GridCell("testnet", "jetton:A", "both", 1, "missing", None, None, None, None, None, False)
    with pytest.raises(ValueError):
        GridCell("testnet", "jetton:A", "input", 0, "missing", None, None, None, None, None,
                 False)


# ------------------------------------------------------------------------ baseline input
def test_span_is_strictly_prior_and_exactly_the_lookback():
    assert baseline_span(POLICY, 10 * 86400) == (10 * 86400 - 7 * 86400, 10 * 86400)
    assert baseline_span(POLICY, 600) == (0, 600)                # clipped at time zero
    scored = 9 * 86400
    full = make(scored=scored)
    assert len(full.cells) == 2016
    assert full.cells[0].window_start == scored - 7 * 86400
    assert full.cells[-1].window_start == scored - W


def test_23_prior_eligible_windows_are_cold_start_and_24_are_ready():
    zeros23 = {i: "zero" for i in range(23)}
    assert make(states=zeros23).status == "cold_start"
    assert make(states=zeros23).eligible_count == 23
    ready = make(states={**zeros23, 40: "observed"})
    assert (ready.status, ready.eligible_count) == ("ready", 24)


def test_status_precedence_missing_then_pending_then_cold_start():
    eligible = {i: "zero" for i in range(30)}
    assert make(states=eligible, current="missing").status == "missing"
    assert make(states=eligible, current="partial").status == "missing"
    assert make(states=eligible, current="pre_universe").status == "missing"
    assert make(states=eligible, current="provisional").status == "pending"
    assert make(states={**eligible, 45: "provisional"}).status == "pending"
    assert make(states={**eligible, 45: "provisional"}, current="missing").status == "missing"
    assert make(states=eligible, current="zero").status == "ready"


def test_incomplete_or_cross_scope_inputs_are_rejected():
    good = grid(60 * W, {})
    current = cell(60 * W, "observed")
    for cells in (good[1:], good[:-1], good[:10] + good[11:], good + (cell(60 * W, "zero"),),
                  good[:5] + (good[5],) + good[5:], tuple(reversed(good)),
                  good[:-1] + (cell(59 * W, "zero", token="jetton:B"),),
                  good[:-1] + (cell(59 * W, "zero", network="mainnet"),)):
        with pytest.raises(ValueError):
            make(cells=cells, current=current)
    for bad_current in (cell(59 * W, "observed"), cell(60 * W, "observed", token="jetton:B")):
        with pytest.raises(ValueError):
            make(current=bad_current)
    real = make()
    fields = {f.name: getattr(real, f.name) for f in dataclasses.fields(BaselineInput)}
    for name, value in (("lookback_seconds", 86400), ("direction", "input"),
                        ("policy_id", "not-a-digest"), ("min_prior_windows", 0)):
        with pytest.raises(ValueError):
            BaselineInput(**{**fields, name: value})


def test_derived_fields_cannot_be_forged():
    real = make(states={i: "zero" for i in range(30)})
    fields = {f.name: getattr(real, f.name) for f in dataclasses.fields(BaselineInput)}
    for name, value in (("status", "cold_start"), ("eligible_count", 31),
                        ("certification", "certified"), ("digest", "sha256:" + "1" * 64)):
        with pytest.raises(ValueError):
            BaselineInput(**{**fields, name: value})


def test_digest_is_reproducible_and_pins_every_cell():
    states = {i: "zero" for i in range(30)}
    a, b = make(states=states), make(states=states)
    assert a.digest == b.digest and a == b
    assert make(states={**states, 31: "observed"}).digest != a.digest
    assert make(states=states, dependency_epoch=4).digest != a.digest
    assert make(states=states, universe_version=2).digest != a.digest
    assert make(states=states, policy=dataclasses.replace(POLICY, swap_floor=11)).digest \
        != a.digest


def test_record_round_trip_uses_exact_runs():
    states = {**{i: "zero" for i in range(10, 40)}, 41: "observed", 42: "observed"}
    states.update({i: "pre_universe" for i in range(10)})
    original = make(states=states, admitted_window_start=3000)
    record = original.to_record()
    runs = record["cells"]
    assert [(r["start"], r["end"], r["state"]) for r in runs] == [
        (0, 3000, "pre_universe"), (3000, 12000, "zero"), (12000, 12300, "missing"),
        (12300, 12600, "observed"), (12600, 12900, "observed"), (12900, 18000, "missing")]
    assert all({r["network"], r["token"], r["direction"]} == {"testnet", "jetton:A", "both"}
               for r in runs)
    assert BaselineInput.from_record(record) == original
    assert json.loads(json.dumps(record)) == record              # JSON-safe


def test_fixtures_are_never_silently_certified():
    states = {i: "zero" for i in range(30)}
    assert make(states=states).certification == "synthetic_bounded_observed"
    certified = tuple(cell(i * W, "zero", synthetic=False, coverage="certified")
                      for i in range(60))
    real = make(cells=certified, current=cell(60 * W, "observed", synthetic=False,
                                              coverage="certified"))
    assert real.certification == "certified"
    mixed = certified[:-1] + (cell(59 * W, "zero", synthetic=False),)
    assert make(cells=mixed, current=cell(60 * W, "observed", synthetic=False,
                                          coverage="certified")).certification \
        == "bounded_observed"
    assert make(states={}, current="missing").certification == "none"


def test_eligible_values_exclude_every_non_eligible_state():
    states = {0: "observed", 1: "zero", 2: "partial", 3: "provisional", 4: "missing",
              5: "pre_universe"}
    values = make(states=states).eligible_cells()
    assert [(c.window_start, c.state) for c in values] == [(0, "observed"), (300, "zero")]


# --------------------------------------------------------------------- coverage qualification
def test_qualification_needs_contiguous_trace_ranges_through_window_end_plus_delta():
    ok = config.qualify_action_window(
        window_start=600, delta_seconds=120, trace_ranges=[[600, 800], [800, 1020]],
        mode="bounded_observed", synthetic=True, certification=None)
    assert ok["trace_ranges"] == [[600, 800], [800, 1020]] and ok["window_end"] == 900
    bad = [
        dict(trace_ranges=[[600, 800], [801, 1020]]),        # gap
        dict(trace_ranges=[[600, 1019]]),                    # short of e + delta
        dict(trace_ranges=[[601, 1020]]),                    # starts after s
        dict(trace_ranges=[]), dict(delta_seconds=-1), dict(delta_seconds=True),
        dict(window_start=601), dict(mode="certified"),      # synthetic never certified
        dict(mode="unknown"), dict(mode="certified", synthetic=False),   # no certification
        dict(synthetic=None),
    ]
    for override in bad:
        args = dict(window_start=600, delta_seconds=120, trace_ranges=[[600, 1020]],
                    mode="bounded_observed", synthetic=True, certification=None)
        args.update(override)
        with pytest.raises(ValueError):
            config.qualify_action_window(**args)
    certified = config.qualify_action_window(
        window_start=600, delta_seconds=0, trace_ranges=[[600, 900]], mode="certified",
        synthetic=False, certification={"contract": "source-certified"})
    assert certified["mode"] == "certified"
