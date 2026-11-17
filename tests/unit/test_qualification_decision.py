"""Stale/cross-target evidence rejection and the offline decision artifact.

Synthetic inputs only; a 'locally_verified' decision is evaluator consistency, never
ARM64/live/security/load qualification of any target.
"""
from copy import deepcopy
import json
import subprocess
import sys

import pytest

from test_qualification_report import CHECKS, QUALIFY, ROOT, evaluate, supplied_report

DIMENSIONS = ('target', 'build', 'config', 'workload')
SCRIPT = str(ROOT / 'scripts/qualify')


def decide(report, requested=None):
    requested = report['identity'] if requested is None else requested
    result = QUALIFY['qualify'](report, {}, requested)
    return QUALIFY['decision_artifact'](result, requested)


def run_cli(tmp_path, report, requested=None, out=None):
    path = tmp_path / 'report.json'
    path.write_text(json.dumps(report))
    args = [sys.executable, '-B', SCRIPT, 'report', '--inputs', str(path)]
    for field, value in (report['identity'] if requested is None else requested).items():
        args += ['--' + field, value]
    if out is not None:
        args += ['--decision-out', str(out)]
    return subprocess.run(args, capture_output=True, text=True)


# ---- stale / cross-target evidence: all rejected dimensions reported together ----

def test_every_stale_gate_and_dimension_is_reported_in_one_evaluation():
    report = supplied_report()
    report['gates']['arm64']['identity'].update(build='old-build', config='old-config')
    report['gates']['load']['identity']['target'] = 'other-target'
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    text = ' | '.join(result.reasons)
    assert 'arm64: missing or invalid evidence (evidence identity mismatch: build, config)' in text
    assert 'load: missing or invalid evidence (evidence identity mismatch: target)' in text


def test_stale_identity_is_not_hidden_by_another_defect_in_the_same_gate():
    report = supplied_report()
    gate = report['gates']['source']
    gate['identity']['build'] = 'old-build'
    gate['checks']['approved_route'] = False
    result = evaluate(report)
    text = ' | '.join(result.reasons)
    assert 'evidence identity mismatch: build' in text
    assert 'approved_route' in text


def test_stale_identity_is_not_hidden_by_synthetic_marking_in_the_same_gate():
    report = supplied_report()
    gate = report['gates']['security']
    gate['identity']['workload'] = 'old-workload'
    gate['synthetic'] = True
    text = ' | '.join(evaluate(report).reasons)
    assert 'evidence identity mismatch: workload' in text


@pytest.mark.parametrize('field', DIMENSIONS)
def test_report_for_another_target_is_rejected_for_requested_evaluation(field):
    report = supplied_report()
    requested = deepcopy(report['identity'])
    requested[field] = 'requested-' + field
    result = QUALIFY['qualify'](report, {}, requested)
    assert result.status == 'BLOCKED'
    assert result.reasons == ('requested identity mismatch: ' + field,)
    assert all(row['status'] == 'BLOCKED' for row in result.services)


# ---- decision artifact content ----

def test_decision_for_complete_consistent_evidence_is_locally_verified_without_qualification_claims():
    decision = decide(supplied_report())
    assert decision['decision'] == 'locally_verified'
    assert decision['evaluator_status'] == 'PASS'
    assert decision['requested_identity'] == supplied_report()['identity']
    assert decision['rejected_dimensions'] == []
    assert decision['reasons'] == []
    assert [row['gate'] for row in decision['gates']] == list(CHECKS)
    assert all(row['status'] == 'PASS' for row in decision['gates'])
    assert decision['claims'] == {'actual_qualification': False, 'arm64': False,
                                  'live_source': False, 'security': False,
                                  'load': False, 'k3s_pvc': False}
    assert decision['scope'] == 'supplied_evidence_aggregation_only'
    assert 'not' in decision['limitations'][0].lower()


def test_decision_distinguishes_blocked_incomplete_and_locally_verified():
    missing = supplied_report()
    del missing['gates']['security']
    failed = supplied_report()
    failed['gates']['load']['levels'][0]['oom_count'] = 1
    stale = supplied_report()
    stale['gates']['arm64']['identity']['build'] = 'old-build'
    cross_target = supplied_report()
    kinds = {
        'verified': decide(supplied_report())['decision'],
        'missing_gate': decide(missing)['decision'],
        'measured_failure': decide(failed)['decision'],
        'stale_gate': decide(stale)['decision'],
        'cross_target': decide(cross_target, dict(cross_target['identity'], target='x'))['decision'],
        'no_request': decide(supplied_report(), {})['decision'],
        'not_a_report': QUALIFY['decision_artifact'](
            QUALIFY['qualify']({}, {}, supplied_report()['identity']), supplied_report()['identity'])['decision'],
    }
    assert kinds == {'verified': 'locally_verified', 'missing_gate': 'incomplete',
                     'measured_failure': 'blocked', 'stale_gate': 'blocked',
                     'cross_target': 'blocked', 'no_request': 'blocked',
                     'not_a_report': 'blocked'}


def test_decision_lists_each_rejected_dimension_with_scope():
    report = supplied_report()
    report['gates']['arm64']['identity'].update(build='old-build', config='old-config')
    report['gates']['load']['identity']['target'] = 'other-target'
    decision = decide(report)
    assert decision['rejected_dimensions'] == [
        {'scope': 'arm64', 'dimension': 'build'},
        {'scope': 'arm64', 'dimension': 'config'},
        {'scope': 'load', 'dimension': 'target'}]
    assert {row['gate']: row['status'] for row in decision['gates']}['arm64'] == 'BLOCKED'


def test_decision_lists_requested_identity_mismatches():
    report = supplied_report()
    requested = dict(report['identity'], target='t2', workload='w2')
    decision = decide(report, requested)
    assert decision['rejected_dimensions'] == [{'scope': 'requested', 'dimension': 'target'},
                                               {'scope': 'requested', 'dimension': 'workload'}]
    assert decision['requested_identity'] == requested
    assert len(decision['reasons']) == 2


def test_decision_lists_missing_gates():
    report = supplied_report()
    del report['gates']['security']
    del report['gates']['recovery']
    decision = decide(report)
    assert decision['missing_gates'] == ['recovery', 'security']
    assert decision['decision'] == 'incomplete'


def test_decision_is_deterministic_and_has_no_clock_or_host_data():
    report = supplied_report()
    report['gates']['arm64']['identity']['build'] = 'old-build'
    first = json.dumps(decide(report), sort_keys=True)
    second = json.dumps(decide(deepcopy(report)), sort_keys=True)
    assert first == second
    for forbidden in ('time', 'date', 'host', 'user', 'path'):
        assert forbidden not in set(decide(report))


# ---- reviewability: CLI output and written file ----

def test_cli_writes_canonical_decision_file_matching_stdout(tmp_path):
    out = tmp_path / 'decision.json'
    result = run_cli(tmp_path, supplied_report(), out=out)
    assert result.returncode == 0, result.stderr
    stdout = json.loads(result.stdout)
    raw = out.read_text()
    written = json.loads(raw)
    assert written == stdout['decision']
    assert written['decision'] == 'locally_verified'
    assert raw == json.dumps(written, sort_keys=True, indent=2, allow_nan=False) + '\n'


def test_cli_decision_for_stale_evidence_exits_nonzero_and_is_blocked(tmp_path):
    report = supplied_report()
    report['gates']['deployment']['identity']['config'] = 'old-config'
    out = tmp_path / 'decision.json'
    result = run_cli(tmp_path, report, out=out)
    assert result.returncode == 2
    written = json.loads(out.read_text())
    assert written['decision'] == 'blocked'
    assert written['rejected_dimensions'] == [{'scope': 'deployment', 'dimension': 'config'}]
    assert written['claims']['actual_qualification'] is False


def test_cli_cross_target_request_never_writes_a_verified_decision(tmp_path):
    out = tmp_path / 'decision.json'
    other = dict(supplied_report()['identity'], target='another-target')
    result = run_cli(tmp_path, supplied_report(), requested=other, out=out)
    assert result.returncode == 2
    assert json.loads(out.read_text())['decision'] == 'blocked'


def test_cli_decision_out_is_reported_in_stdout_without_it_too(tmp_path):
    result = run_cli(tmp_path, supplied_report())
    assert json.loads(result.stdout)['decision']['decision'] == 'locally_verified'


def test_cli_decision_out_rejected_outside_report_mode(tmp_path):
    result = subprocess.run([sys.executable, '-B', SCRIPT, 'arm64', '--decision-out',
                             str(tmp_path / 'd.json')], capture_output=True, text=True)
    assert result.returncode == 2
    assert not (tmp_path / 'd.json').exists()


def test_cli_decision_out_refuses_to_overwrite_existing_file(tmp_path):
    out = tmp_path / 'decision.json'
    out.write_text('keep')
    result = run_cli(tmp_path, supplied_report(), out=out)
    assert result.returncode == 2
    assert out.read_text() == 'keep'
