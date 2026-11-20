import math
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ton_sentinel.detectors import score_features
from tests.oracle import expected_detector_result


NOW = 1_800_000


def feature(start, *, swaps=0, known=0, unknown=0, coverage="bounded_observed",
            eligible_zero=True, generation="g1", network="mainnet", token="T"):
    return {
        "network": network, "token": token, "window_start": start,
        "window_end": start + 300, "generation": generation, "swaps": swaps,
        "known_traders": known, "unknown_count": unknown,
        "coverage_status": coverage, "eligible_zero": eligible_zero,
        "status": "provisional", "provisional": True,
    }


def zeros(count=24):
    return [feature(NOW - (count - i) * 300, eligible_zero=True) for i in range(count)]


class DetectorTests(unittest.TestCase):
    def assert_result_close(self, actual, expected, path="result"):
        self.assertEqual(type(actual), type(expected), path)
        if type(actual) is dict:
            self.assertEqual(set(actual), set(expected), path)
            for key in actual:
                self.assert_result_close(actual[key], expected[key], f"{path}.{key}")
        elif type(actual) is list:
            self.assertEqual(len(actual), len(expected), path)
            for index, (left, right) in enumerate(zip(actual, expected)):
                self.assert_result_close(left, right, f"{path}[{index}]")
        elif type(actual) is float:
            self.assertTrue(math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-9),
                            f"{path}: {actual} != {expected}")
        else:
            self.assertEqual(actual, expected, path)

    def test_independent_oracle_matches_complete_provisional_results(self):
        steady = [feature(NOW - (24 - i) * 300,
                          swaps=4 + (i % 3), known=3 + (i % 2), unknown=0,
                          eligible_zero=False, generation=f"base-{i}") for i in range(24)]
        correction = [dict(row, generation=f"corrected-{i}", swaps=row["swaps"] + (1 if i > 12 else 0))
                      for i, row in enumerate(steady)]
        scenarios = [
            ("ordinary", feature(NOW, swaps=7, known=4, eligible_zero=False), steady, None),
            ("spike", feature(NOW, swaps=100, known=40, eligible_zero=False), steady,
             {"score_threshold": 5.0}),
            ("correction", feature(NOW, swaps=9, known=5, eligible_zero=False), correction, None),
            ("cold-start", feature(NOW, swaps=9, known=5, eligible_zero=False), steady[:23], None),
            ("thresholds", feature(NOW, swaps=10, known=5, unknown=0, eligible_zero=False),
             [feature(NOW - (24 - i) * 300, swaps=4, known=0, eligible_zero=False)
              for i in range(24)], {"score_threshold": 6.0, "attribution_floor": 1.0}),
            ("coverage", feature(NOW, coverage="missing", eligible_zero=False), steady, None),
        ]
        for label, current, history, config in scenarios:
            with self.subTest(scenario=label):
                expected = expected_detector_result(current, history, config)
                actual = score_features(current, history, config)
                self.assert_result_close(actual, expected)

    def test_cold_start_then_constant_zero_count_spike_scores_ten(self):
        history = zeros(23)
        result = score_features(feature(NOW, swaps=10, known=5, unknown=0, eligible_zero=False), history)
        self.assertEqual(result["count"]["status"], "cold_start")
        result = score_features(feature(NOW, swaps=10, known=5, unknown=0, eligible_zero=False), zeros())
        self.assertEqual(result["count"]["score"], 10.0)
        self.assertTrue(result["count"]["triggered"])

    def test_exact_score_and_swap_floor_thresholds(self):
        baseline = [feature(NOW - (24 - i) * 300, swaps=4, eligible_zero=False) for i in range(24)]
        result = score_features(feature(NOW, swaps=10, known=9, unknown=1, eligible_zero=False), baseline)
        self.assertEqual(result["count"]["score"], 6.0)
        self.assertTrue(result["count"]["triggered"])
        below = score_features(feature(NOW, swaps=9, known=5, unknown=0, eligible_zero=False), baseline)
        self.assertEqual(below["count"]["score"], 5.0)
        self.assertFalse(below["count"]["triggered"])

    def test_unique_trader_score_floor_and_attribution_threshold(self):
        baseline = [feature(NOW - (24 - i) * 300, swaps=0, eligible_zero=True) for i in range(24)]
        exact = score_features(feature(NOW, swaps=10, known=5, unknown=0, eligible_zero=False), baseline,
                               {"score_threshold": 5.0})
        self.assertEqual(exact["unique_traders"]["score"], 5.0)
        self.assertTrue(exact["unique_traders"]["triggered"])
        covered = score_features(feature(NOW, swaps=10, known=9, unknown=1, eligible_zero=False), baseline)
        self.assertEqual(covered["unique_traders"]["attribution"], 0.9)
        self.assertTrue(covered["unique_traders"]["triggered"])
        low = score_features(feature(NOW, swaps=10, known=8, unknown=2, eligible_zero=False), baseline)
        self.assertEqual(low["unique_traders"]["status"], "insufficient_coverage")
        self.assertFalse(low["unique_traders"]["triggered"])

    def test_covered_zero_is_valid_but_missing_coverage_is_not_zero(self):
        baseline = zeros()
        scored = score_features(feature(NOW, eligible_zero=True), baseline)
        self.assertEqual(scored["count"]["status"], "scored")
        self.assertEqual(scored["count"]["score"], 0.0)
        missing = score_features(feature(NOW, coverage="missing", eligible_zero=False), baseline)
        self.assertEqual(missing["count"]["status"], "missing")
        self.assertIsNone(missing["count"]["score"])

    def test_baseline_is_same_asset_strictly_prior_and_within_seven_days(self):
        selected = zeros()
        excluded = [
            feature(NOW + 300, swaps=999, eligible_zero=False),
            feature(NOW - 7 * 86400 - 300, swaps=999, eligible_zero=False),
            feature(NOW - 300, swaps=999, eligible_zero=False, token="OTHER"),
        ]
        result = score_features(feature(NOW, swaps=10, known=5, eligible_zero=False), selected + excluded)
        self.assertEqual(result["count"]["baseline_count"], 24)
        self.assertEqual([k["window_start"] for k in result["count"]["baseline_keys"]],
                         [row["window_start"] for row in selected])

    def test_duplicate_baseline_key_rejected(self):
        history = zeros()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            score_features(feature(NOW), history + [history[0]])

    def test_positive_spark_output_shape_needs_no_eligible_zero_field(self):
        spark_row = {
            "network": "mainnet", "window_start": NOW, "window_end": NOW + 300,
            "token": "T", "generation": "g1", "swaps": 100,
            "input_raw": "1", "output_raw": None, "known_traders": 5,
            "unknown_count": 1, "coverage_status": "bounded_observed",
            "status": "provisional", "provisional": True,
        }
        result = score_features(spark_row, zeros())
        self.assertEqual(result["count"]["observed_value"], 100)
        self.assertEqual(result["unique_traders"]["attribution"], 0.99)

    def test_attribution_counts_attributed_events_not_distinct_wallets(self):
        result = score_features(
            feature(NOW, swaps=100, known=5, unknown=1, eligible_zero=False), zeros())
        self.assertEqual(result["unique_traders"]["attribution"], 0.99)

    def test_unaligned_window_and_unknown_coverage_status_are_rejected(self):
        unaligned = feature(NOW + 1)
        with self.assertRaisesRegex(ValueError, "align"):
            score_features(unaligned, [])
        unknown = feature(NOW, coverage="arbitrary-status")
        with self.assertRaisesRegex(ValueError, "recognized bounded status"):
            score_features(unknown, [])

    def test_wallet_baseline_excludes_low_attribution_rows_independently(self):
        history = [feature(NOW - (24 - i) * 300, swaps=100, known=5,
                           unknown=20, eligible_zero=False) for i in range(24)]
        result = score_features(feature(NOW, swaps=100, known=50, unknown=0,
                                        eligible_zero=False), history)
        self.assertEqual(result["count"]["baseline_count"], 24)
        self.assertEqual(result["count"]["status"], "scored")
        self.assertEqual(result["unique_traders"]["baseline_count"], 0)
        self.assertEqual(result["unique_traders"]["baseline_keys"], [])
        self.assertEqual(result["unique_traders"]["status"], "cold_start")
        self.assertFalse(result["unique_traders"]["triggered"])

    def test_rejects_boolean_and_nonfinite_counts_or_configuration(self):
        with self.assertRaisesRegex(ValueError, "swaps"):
            score_features(feature(NOW, swaps=True), zeros())
        bad = feature(NOW)
        bad["unknown_count"] = math.nan
        with self.assertRaisesRegex(ValueError, "unknown_count"):
            score_features(bad, zeros())
        with self.assertRaisesRegex(ValueError, "finite"):
            score_features(feature(NOW), zeros(), {"score_threshold": math.nan})

    def test_unrepresentable_counts_and_intermediate_scores_fail_closed(self):
        enormous = 10 ** 1000
        with self.assertRaisesRegex(ValueError, "detector score"):
            score_features(feature(NOW, swaps=enormous, known=enormous,
                                   eligible_zero=False), zeros())
        history = [feature(NOW - (24 - i) * 300, swaps=enormous,
                           known=enormous, eligible_zero=False) for i in range(24)]
        with self.assertRaisesRegex(ValueError, "baseline median"):
            score_features(feature(NOW, swaps=enormous, known=enormous, eligible_zero=False), history)

        varied = [feature(NOW - (24 - i) * 300, swaps=1 if i % 2 == 0 else 5,
                          known=1 if i % 2 == 0 else 5, eligible_zero=False) for i in range(24)]
        with self.assertRaisesRegex(ValueError, "score scale"):
            score_features(feature(NOW, swaps=100, known=100, eligible_zero=False), varied,
                           {"mad_multiplier": 1e308})

    def test_cli_reports_numeric_and_file_input_errors_without_traceback(self):
        module = Path(__file__).resolve().parents[2] / "src/ton_sentinel/detectors.py"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            payload = {"current": feature(NOW), "history": zeros(),
                       "config": {"score_threshold": 10 ** 1000}}
            path.write_text(json.dumps(payload), encoding="utf-8")
            result = subprocess.run([sys.executable, str(module), str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("error: cannot score input:", result.stderr)
            self.assertNotIn("Traceback", result.stderr)

            missing = subprocess.run([sys.executable, str(module), str(Path(directory) / "missing.json")],
                                     capture_output=True, text=True)
            self.assertEqual(missing.returncode, 2)
            self.assertIn("cannot score input:", missing.stderr)
            self.assertNotIn("Traceback", missing.stderr)

    def test_cli_loads_custom_policy_and_fails_closed_on_bad_policy_file(self):
        module = Path(__file__).resolve().parents[2] / "src/ton_sentinel/detectors.py"
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "input.json"
            config_path = Path(directory) / "policy.json"
            payload = {"current": feature(NOW, swaps=10, known=5, eligible_zero=False),
                       "history": zeros()}
            policy = {"score_threshold": 5.0, "minimum_baseline_windows": 20}
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            config_path.write_text(json.dumps(policy), encoding="utf-8")
            completed = subprocess.run([sys.executable, str(module), str(input_path),
                                        "--config", str(config_path)], capture_output=True, text=True)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            output = json.loads(completed.stdout)
            self.assertEqual(output["config"]["score_threshold"], 5.0)
            self.assertEqual(output["config"]["minimum_baseline_windows"], 20)
            self.assert_result_close(output, expected_detector_result(payload["current"], payload["history"], policy))

            for invalid in ({"score_threshold": -1}, None, [], True):
                config_path.write_text(json.dumps(invalid), encoding="utf-8")
                rejected = subprocess.run([sys.executable, str(module), str(input_path),
                                           "--config", str(config_path)], capture_output=True, text=True)
                self.assertEqual(rejected.returncode, 2, repr(invalid))
                self.assertIn("cannot score input:", rejected.stderr)
                self.assertNotIn("Traceback", rejected.stderr)

            config_path.write_text('{"score_threshold":', encoding="utf-8")
            malformed = subprocess.run([sys.executable, str(module), str(input_path),
                                        "--config", str(config_path)], capture_output=True, text=True)
            self.assertEqual(malformed.returncode, 2)
            self.assertIn("cannot score input:", malformed.stderr)
            self.assertNotIn("Traceback", malformed.stderr)



import dataclasses as _dc

import pytest as _pytest

from ton_sentinel import config as _config
from ton_sentinel.contracts import BaselineInput as _BI, GridCell as _Cell
from ton_sentinel.detectors import score_baseline
from tests.oracle import expected_detector_outcome

_W = 300
_V1 = _config.DETECTOR_POLICY_V1


def _cell(start, state, swaps=None, known=None, unknown=None, *, coverage="bounded_observed",
          synthetic=True, token="jetton:A"):
    if state in ("observed", "zero", "partial"):
        swaps = {"zero": 0}.get(state, 3 if swaps is None else swaps)
        known = swaps if known is None else known
        unknown = 0 if unknown is None else unknown
        revision = f"feature:g:{start}"
    else:
        swaps = known = unknown = revision = None
    eligible = state in ("observed", "zero")
    return _Cell("testnet", token, "both", start, state, swaps, known, unknown, revision,
                 coverage if eligible else None, synthetic if eligible else False)


def _input(scored, cells, current, policy=_V1, epoch=7):
    first = max(0, scored - policy.lookback_seconds)
    by_start = {c.window_start: c for c in cells}
    grid = tuple(by_start.get(s, _cell(s, "missing")) for s in range(first, scored, _W))
    return _BI.create(network="testnet", token="jetton:A", window_start=scored, policy=policy,
                      universe_version=1, admitted_window_start=0, dependency_epoch=epoch,
                      active_epoch='{"network":"testnet"}', current=current, cells=grid)


def _zeros(scored, count):
    return [_cell(scored - (count - i) * _W, "zero") for i in range(count)]


def _varied(scored, count=24):
    return [_cell(scored - (count - i) * _W, "observed", 4 + i % 3, 4 + i % 3)
            for i in range(count)]


S = 3000 * _W


def _scenarios():
    yield "ready_spike_both_active", _input(S, _zeros(S, 24), _cell(S, "observed", 12, 6))
    yield "cold_start_23", _input(S, _zeros(S, 23), _cell(S, "observed", 12, 6))
    yield "exact_score_six_and_floors", _input(S, _zeros(S, 24), _cell(S, "observed", 6, 6))
    yield "count_floor_9_wallet_floor_4", _input(S, _zeros(S, 24), _cell(S, "observed", 9, 4))
    yield "count_floor_10_wallet_floor_5", _input(S, _zeros(S, 24), _cell(S, "observed", 10, 5))
    yield "varied_mad_nonzero", _input(S, _varied(S), _cell(S, "observed", 14, 14))
    yield "varied_even_median", _input(S, [_cell(S - (24 - i) * _W, "observed", 1 + i % 2,
                                                 1 + i % 2) for i in range(24)],
                                       _cell(S, "observed", 30, 30))
    yield "current_missing", _input(S, _zeros(S, 24), _cell(S, "missing"))
    yield "current_partial", _input(S, _zeros(S, 24), _cell(S, "partial", 12, 6))
    yield "current_pre_universe", _input(S, _zeros(S, 24), _cell(S, "pre_universe"))
    yield "current_provisional", _input(S, _zeros(S, 24), _cell(S, "provisional"))
    yield "baseline_provisional", _input(S, _zeros(S, 24) + [_cell(S - 30 * _W, "provisional")],
                                         _cell(S, "observed", 12, 6))
    yield "attribution_0_8_insufficient", _input(S, _zeros(S, 24),
                                                 _cell(S, "observed", 10, 6, 2))
    yield "attribution_exact_0_9", _input(S, _zeros(S, 24), _cell(S, "observed", 10, 9, 1))
    yield "wallet_baseline_drops_low_attribution", _input(
        S, [_cell(S - (24 - i) * _W, "observed", 10, 1, 5) for i in range(24)],
        _cell(S, "observed", 40, 30))
    yield "zero_current_scores_zero", _input(S, _zeros(S, 24), _cell(S, "zero"))
    yield "clipped_lookback_24", _input(24 * _W, _zeros(24 * _W, 24),
                                        _cell(24 * _W, "observed", 11, 11))
    yield "certified_coverage", _input(
        S, [_dc.replace(c, coverage="certified", synthetic=False) for c in _zeros(S, 24)],
        _dc.replace(_cell(S, "observed", 12, 6), coverage="certified", synthetic=False))
    yield "partial_and_missing_cells_excluded", _input(
        S, _zeros(S, 24) + [_cell(S - 40 * _W, "partial", 50, 50), _cell(S - 41 * _W, "missing")],
        _cell(S, "observed", 12, 6))


def _oracle(baseline, policy=_V1):
    record = json.loads(json.dumps(baseline.to_record()))
    return expected_detector_outcome(record, json.loads(policy.to_json()))


@_pytest.mark.parametrize("label,baseline", list(_scenarios()), ids=lambda v: v if
                          isinstance(v, str) else "")
def test_both_detectors_match_the_independent_oracle_exactly(label, baseline):
    got = score_baseline(baseline, _V1)
    assert type(got) is tuple and len(got) == 2
    assert list(got) == _oracle(baseline)


def test_hand_computed_spike_scores_and_triggers():
    count, wallets = score_baseline(_input(S, _zeros(S, 24), _cell(S, "observed", 12, 6)), _V1)
    assert (count["detector"], count["status"], count["score"], count["triggered"]) == (
        "swap_count_spike.v1", "scored", "12", True)
    assert (count["median"], count["mad"], count["scale"], count["sample_count"]) == (
        "0", "0", "1", 24)
    assert count["sample_windows"] == [[S - 24 * _W, S]]
    assert (wallets["detector"], wallets["score"], wallets["attribution"],
            wallets["triggered"]) == ("active_wallets_spike.v1", "6", "1", True)
    # varied baseline 4,5,6 repeating: median 5, MAD 1, scale 1.4826, (14-5)/1.4826
    varied, _ = score_baseline(_input(S, _varied(S), _cell(S, "observed", 14, 14)), _V1)
    assert (varied["median"], varied["mad"], varied["scale"]) == ("5", "1", "1.4826")
    assert varied["score"] == "6.070416835"
    assert varied["triggered"] is True


def test_boundaries_threshold_floors_and_cold_start():
    exact, wallet6 = score_baseline(_input(S, _zeros(S, 24), _cell(S, "observed", 6, 6)), _V1)
    assert (exact["score"], exact["reasons"], exact["triggered"]) == (
        "6", ["below_swap_floor"], False)
    assert (wallet6["score"], wallet6["triggered"]) == ("6", True)
    below, wallet4 = score_baseline(_input(S, _zeros(S, 24), _cell(S, "observed", 9, 4)), _V1)
    assert below["reasons"] == ["below_swap_floor"] and below["score"] == "9"
    assert wallet4["reasons"] == ["below_trader_floor", "below_score_threshold"]
    at, wallet5 = score_baseline(_input(S, _zeros(S, 24), _cell(S, "observed", 10, 5)), _V1)
    assert at["triggered"] is True and wallet5["reasons"] == ["below_score_threshold"]
    cold, cold_w = score_baseline(_input(S, _zeros(S, 23), _cell(S, "observed", 12, 6)), _V1)
    assert (cold["status"], cold["score"], cold["sample_count"]) == ("cold_start", None, 23)
    assert cold_w["status"] == "cold_start" and not cold["triggered"]


def test_baseline_is_strictly_prior_and_current_never_enters_the_sample():
    baseline = _input(S, _zeros(S, 24), _cell(S, "observed", 12, 6))
    for result in score_baseline(baseline, _V1):
        assert all(end <= S for _, end in result["sample_windows"])
        assert result["observed"] in (12, 6)


def test_missing_and_pending_carry_no_score_and_name_the_reason():
    missing, _ = score_baseline(_input(S, _zeros(S, 24), _cell(S, "missing")), _V1)
    assert (missing["status"], missing["score"], missing["observed"], missing["reasons"]) == (
        "missing", None, None, ["current_missing"])
    pending, _ = score_baseline(_input(S, _zeros(S, 24) + [_cell(S - 30 * _W, "provisional")],
                                       _cell(S, "observed", 12, 6)), _V1)
    assert (pending["status"], pending["reasons"], pending["triggered"]) == (
        "pending", ["baseline_provisional"], False)
    low, low_w = score_baseline(_input(S, _zeros(S, 24), _cell(S, "observed", 10, 6, 2)), _V1)
    assert low["triggered"] is True
    assert (low_w["status"], low_w["attribution"], low_w["triggered"]) == (
        "insufficient_coverage", "0.8", False)


def test_explanation_reproduces_from_the_stored_pinned_record():
    baseline = _input(S, _varied(S), _cell(S, "observed", 14, 14))
    first = score_baseline(baseline, _V1)
    stored = json.loads(json.dumps(baseline.to_record()))
    again = score_baseline(_BI.from_record(stored), _V1)
    assert again == first
    for result in first:
        text = result["explanation"]
        assert text.startswith(result["detector"] + " testnet/jetton:A")
        for value in (result["median"], result["mad"], result["scale"], result["score"],
                      baseline.digest, str(result["observed"])):
            assert value in text
        assert text.endswith("; input " + baseline.digest)


def test_score_baseline_refuses_inputs_of_another_policy():
    other = _dc.replace(_V1, score_threshold="5")
    baseline = _input(S, _zeros(S, 24), _cell(S, "observed", 12, 6))
    with _pytest.raises(ValueError, match="policy"):
        score_baseline(baseline, other)
    with _pytest.raises(ValueError):
        score_baseline(baseline.to_record(), _V1)
    changed = _input(S, _zeros(S, 24), _cell(S, "observed", 12, 6), policy=other)
    count, _ = score_baseline(changed, other)
    assert count["policy_id"] == other.identity and count["threshold"] == "5"
    assert list(score_baseline(changed, other)) == _oracle(changed, other)


if __name__ == "__main__":
    unittest.main()
