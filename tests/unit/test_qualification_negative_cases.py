"""Negative cases for the offline qualification evaluator."""

from copy import deepcopy
import json
import socket
import subprocess
import sys
from typing import Any

import pytest

from test_qualification_report import CHECKS, QUALIFY, ROOT, evaluate, supplied_report


def reasons_text(result) -> str:
    return ' | '.join(result.reasons)



AUTHORITY_GATES = ['arm64', 'source', 'authorization', 'deployment']


@pytest.mark.parametrize('gate', AUTHORITY_GATES)
def test_missing_authority_gate_is_blocked_with_named_reason(gate):
    report = supplied_report()
    del report['gates'][gate]
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert any(r.startswith(gate + ': missing or invalid evidence') for r in result.reasons)
    assert {'gate': gate, 'status': 'BLOCKED'} in [dict(row) for row in result.services]


@pytest.mark.parametrize('gate,check', [(g, c) for g in AUTHORITY_GATES for c in CHECKS[g]])
def test_each_authority_check_false_or_absent_is_rejected_naming_the_check(gate, check):
    for mutate in ('false', 'absent'):
        report = supplied_report()
        if mutate == 'false':
            report['gates'][gate]['checks'][check] = False
        else:
            del report['gates'][gate]['checks'][check]
        result = evaluate(report)
        assert result.status != 'PASS'
        assert gate + ': missing or invalid evidence (missing/unsupported/failed check: ' + check in reasons_text(result)


@pytest.mark.parametrize('gate', AUTHORITY_GATES)
def test_authority_gate_without_evidence_refs_is_rejected(gate):
    report = supplied_report()
    report['gates'][gate]['evidence_refs'] = []
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert gate + ': missing or invalid evidence (actual evidence references required)' in reasons_text(result)




def test_authority_hold_cli_modes_refuse_without_network(tmp_path):
    auth = tmp_path / 'auth.json'
    auth.write_text(json.dumps({'approved': True}))
    for mode in ('arm64', 'live-source'):
        done = subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/qualify'), mode,
                               '--authorization', str(auth)], capture_output=True, text=True)
        assert done.returncode != 0
        assert 'No network/client initialized' in done.stderr



LEVEL_FAILS = [
    ('oom_count', 1, 'oom_count'),
    ('eviction_count', 3, 'eviction_count'),
    ('memory_free_fraction', '-0.01', 'insufficient memory_free_fraction'),
    ('cpu_free_fraction', '-0.01', 'insufficient cpu_free_fraction'),
    ('disk_free_fraction', '0.1999', 'insufficient disk_free_fraction'),
]


@pytest.mark.parametrize('index', [0, 1])
@pytest.mark.parametrize('field,value,reason', LEVEL_FAILS)
def test_resource_failure_at_either_level_fails_naming_level_and_dimension(index, field, value, reason):
    report = supplied_report()
    level = report['gates']['load']['levels'][index]
    level[field] = value
    result = evaluate(report)
    assert result.status == 'FAIL'
    assert 'load/' + level['id'] + ': ' + reason in result.reasons
    assert {'gate': 'load', 'status': 'FAIL'} in [dict(row) for row in result.services]


@pytest.mark.parametrize('field', ['memory_free_fraction', 'cpu_free_fraction', 'disk_free_fraction'])
def test_exhausted_resource_headroom_is_not_pass(field):
    report = supplied_report()
    report['gates']['load']['levels'][0][field] = '0'
    assert evaluate(report).status == 'FAIL'


@pytest.mark.parametrize('field', ['rss_bytes', 'cpu_free_fraction', 'memory_free_fraction',
                                   'disk_free_fraction', 'oom_count', 'eviction_count'])
def test_missing_resource_measurement_is_not_pass(field):
    report = supplied_report()
    del report['gates']['load']['levels'][1][field]
    assert evaluate(report).status != 'PASS'


@pytest.mark.parametrize('value', ['0', '-1', 'NaN', 'Infinity', '1e3', 1024, None, ''])
def test_unmeasured_or_nonpositive_rss_is_not_pass(value):
    report = supplied_report()
    report['gates']['load']['levels'][0]['rss_bytes'] = value
    assert evaluate(report).status != 'PASS'


@pytest.mark.parametrize('value', ['1.0001', '2', '1e0', 0.5])
def test_free_fraction_above_one_or_non_string_is_not_pass(value):
    report = supplied_report()
    report['gates']['load']['levels'][0]['memory_free_fraction'] = value
    assert evaluate(report).status != 'PASS'


def test_one_load_level_or_duplicated_level_is_not_pass():
    report = supplied_report()
    report['gates']['load']['levels'] = report['gates']['load']['levels'][:1]
    assert evaluate(report).status == 'BLOCKED'
    report = supplied_report()
    report['gates']['load']['levels'][1]['id'] = report['gates']['load']['levels'][0]['id']
    assert evaluate(report).status == 'BLOCKED'


def test_failed_resource_level_is_not_hidden_by_passing_supplied_verdict():
    report = supplied_report()
    assert report['gates']['load']['status'] == 'PASS'
    report['gates']['load']['levels'][0]['oom_count'] = 1
    assert evaluate(report).status == 'FAIL'


def test_catchup_resource_shortfall_fails():
    for key, value in (('processed_per_second', '10'), ('processed_per_second', '9'),
                       ('elapsed_seconds', '61')):
        report = supplied_report()
        report['gates']['catchup']['measurements'][key] = value
        assert evaluate(report).status == 'FAIL', (key, value)




def test_supplied_resource_gate_verdicts_fail_closed():
    for gate in ('load', 'retention', 'maintenance', 'catchup'):
        for verdict, expected in (('FAIL', 'FAIL'), ('BLOCKED', 'BLOCKED')):
            report = supplied_report()
            report['gates'][gate]['status'] = verdict
            result = evaluate(report)
            assert result.status == expected, (gate, verdict)
            assert gate + ': supplied verdict ' + verdict in result.reasons



DETECTOR_CHECKS = CHECKS['detectors']


@pytest.mark.parametrize('check', DETECTOR_CHECKS)
def test_each_detector_support_or_eligibility_check_is_load_bearing(check):
    for mutate in ('false', 'absent', 'string_true', 'one'):
        report = supplied_report()
        checks = report['gates']['detectors']['checks']
        if mutate == 'false':
            checks[check] = False
        elif mutate == 'absent':
            del checks[check]
        elif mutate == 'string_true':
            checks[check] = 'true'
        else:
            checks[check] = 1
        result = evaluate(report)
        assert result.status == 'BLOCKED', (check, mutate)
        assert 'detectors: missing or invalid evidence (missing/unsupported/failed check: ' + check in reasons_text(result)


def test_missing_detectors_gate_is_blocked():
    report = supplied_report()
    del report['gates']['detectors']
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert any(r.startswith('detectors: ') for r in result.reasons)


def test_acceptance_gate_requires_both_detectors_eligible():
    report = supplied_report()
    report['gates']['acceptance']['checks']['both_detectors_eligible'] = False
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert 'both_detectors_eligible' in reasons_text(result)


@pytest.mark.parametrize('name', ['second_detector', 'wallet2', 'detectors2', 'Detectors', 'liquidity'])
def test_unsupported_extra_detector_gate_is_unsupported_evidence(name):
    report = supplied_report()
    report['gates'][name] = deepcopy(report['gates']['detectors'])
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert result.reasons == ('report: missing or invalid schema, identities, workload or mandatory policy',)


@pytest.mark.parametrize('extra', ['third_supported', 'second_detector_supported', 'second_detector_eligible'])
def test_unsupported_extra_detector_check_does_not_substitute_for_a_missing_one(extra):
    report = supplied_report()
    checks = report['gates']['detectors']['checks']
    del checks['wallet_supported']
    checks[extra] = True
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert 'missing/unsupported/failed check: wallet_supported' in reasons_text(result)


@pytest.mark.parametrize('field,value', [('synthetic', True), ('evidence_class', 'fixture'),
                                         ('evidence_class', 'synthetic'), ('evidence_class', 'local')])
def test_detector_fixture_evidence_presented_as_actual_is_rejected(field, value):
    report = supplied_report()
    report['gates']['detectors'][field] = value
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert 'detectors: missing or invalid evidence (synthetic/fixture/local evidence is not actual)' in result.reasons
    assert result.synthetic is (field == 'synthetic')


def test_detector_gate_bound_to_other_identity_is_stale():
    report = supplied_report()
    report['gates']['detectors']['identity']['workload'] = 'other-workload'
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert 'detectors: missing or invalid evidence (evidence identity mismatch: workload)' in result.reasons



NEGATIVES = {
    'authority': lambda r: r['gates'].pop('authorization'),
    'memory': lambda r: r['gates']['load']['levels'][0].update(oom_count=1),
    'cpu': lambda r: r['gates']['load']['levels'][0].update(cpu_free_fraction='-0.5'),
    'disk': lambda r: r['gates']['load']['levels'][0].update(disk_free_fraction='0.05'),
    'retention': lambda r: r['gates']['retention'].update(status='FAIL'),
    'detector': lambda r: r['gates']['detectors']['checks'].update(wallet_supported=False),
}


@pytest.mark.parametrize('kind', sorted(NEGATIVES))
def test_cli_exits_nonzero_with_rejection_and_never_claims_qualification(tmp_path, kind):
    report: dict[str, Any] = supplied_report()
    NEGATIVES[kind](report)
    path = tmp_path / 'report.json'
    path.write_text(json.dumps(report))
    args = [sys.executable, '-B', str(ROOT / 'scripts/qualify'), 'report', '--inputs', str(path)]
    for field, value in report['identity'].items():
        args += ['--' + field, value]
    done = subprocess.run(args, capture_output=True, text=True)
    payload = json.loads(done.stdout)
    assert done.returncode == 2
    assert payload['status'] in ('BLOCKED', 'FAIL')
    assert payload['reasons']
    assert payload['claims_actual_qualification'] is False


@pytest.mark.parametrize('kind', sorted(NEGATIVES))
def test_negative_evaluation_opens_no_sockets_and_does_not_mutate_input(monkeypatch, kind):
    attempts = []

    def forbid(*args, **kwargs):
        attempts.append(args)
        raise AssertionError('network attempt')
    for name in ('socket', 'create_connection', 'getaddrinfo', 'gethostbyname'):
        monkeypatch.setattr(socket, name, forbid)
    report: dict[str, Any] = supplied_report()
    NEGATIVES[kind](report)
    before = deepcopy(report)
    assert evaluate(report).status != 'PASS'
    assert report == before
    assert attempts == []


def test_evaluator_has_no_request_counter_state_to_reset():
    source = (ROOT / 'scripts/qualify').read_text()
    for token in ('request_count', 'requests_made', 'counter', 'reset_'):
        assert token not in source, token
    assert 'qualify' in QUALIFY
