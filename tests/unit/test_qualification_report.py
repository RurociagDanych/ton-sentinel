"""Synthetic aggregation inputs only: these tests do not qualify any deployment."""
from copy import deepcopy
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from typing import Any, Callable, cast

import pytest

ROOT = Path(__file__).resolve().parents[2]
QUALIFY = runpy.run_path(str(ROOT / 'scripts/qualify'))


def evaluate(report, required=None) -> Any:
    evaluator = QUALIFY.get('qualify')
    assert callable(evaluator), 'offline qualification evaluator is missing'
    identity = report.get('identity') if isinstance(report, dict) else None
    return cast(Callable[..., Any], evaluator)(report, {} if required is None else required,
                                                 identity)


def test_missing_arm_or_source_evidence_cannot_pass():
    result = evaluate({})
    assert result.status == 'BLOCKED'
    assert result.reasons


CHECKS = {
    'arm64': ['native_aarch64', 'host_and_pod_measurements'],
    'source': ['approved_route', 'both_dexes_all_tokens_both_directions',
               'independent_trace_truth', 'unresolved_traces', 'budgets_honored'],
    'acceptance': ['frozen_two_level_envelope', 'both_detectors_eligible'],
    'authorization': ['exact_target', 'independent_g2_g5_review', 'within_budgets'],
    'deployment': ['actual_k3s_readback', 'immutable_build', 'disabled_services'],
    'load': ['steady_state', 'corrections_zero_grid_baseline', 'retained_footprint_projection',
             'rss_cpu_state_checkpoint_spill', 'pg_wal_broker_disk_io', 'query_source_budgets'],
    'retention': ['measured_full_retention', 'deletion_overshoot'],
    'maintenance': ['serialized_repair_retention_vacuum', 'workspace_growth'],
    'catchup': ['exceeds_incoming', 'within_objective'],
    'recovery': ['pod_restart', 'broker_restart', 'pg_restart', 'authorized_vm_reboot',
                 'promised_rows_preserved'],
    'security': ['allowed_and_denied_vantages', 'ipv6', 'tls_auth_rbac_netpol',
                 'sanitized_secret_scan'],
    'detectors': ['volume_supported', 'volume_eligible', 'wallet_supported', 'wallet_eligible'],
}


def supplied_report() -> dict[str, Any]:
    """Invented test values, NOT measurements or actual qualification evidence.

    Simulates a supplied actual-evidence envelope to exercise aggregation only.
    Nothing in this module is a qualification artifact or an authenticated claim.
    """
    identity = dict(target='test-only-target', build='test-only-build',
                    config='test-only-config', workload='test-only-workload')
    gates: dict[str, Any] = {name: dict(status='PASS', synthetic=False, evidence_class='actual',
                        identity=deepcopy(identity), evidence_refs=['test-only-not-an-artifact'],
                        checks=dict.fromkeys(checks, True)) for name, checks in CHECKS.items()}
    gates['load']['levels'] = [dict(id=name, oom_count=0, eviction_count=0,
                                   rss_bytes='1024', cpu_free_fraction='0.3',
                                   memory_free_fraction='0.3', disk_free_fraction='0.3')
                              for name in ['test-base', 'test-high']]
    gates['catchup']['measurements'] = dict(incoming_per_second='10',
                                          processed_per_second='20',
                                          elapsed_seconds='30', objective_seconds='60')
    return dict(schema_version=1, synthetic=False, identity=identity,
                workload_levels=['test-base', 'test-high'], gates=gates)


def test_complete_consistent_supplied_evidence_passes_aggregation_only():
    result = evaluate(supplied_report())
    assert result.status == 'PASS'
    assert not result.reasons
    assert result.availability == 'supplied_evidence_only_not_authorization'


def test_missing_requested_identity_is_blocked():
    result = cast(Callable[..., Any], QUALIFY['qualify'])(supplied_report(), {}, None)
    assert result.status == 'BLOCKED'
    assert result.reasons == tuple('requested identity missing: ' + field
                                   for field in ('target', 'build', 'config', 'workload'))


@pytest.mark.parametrize('field', ['target', 'build', 'config', 'workload'])
def test_same_report_is_blocked_for_a_different_requested_identity(field):
    report = supplied_report()
    requested = deepcopy(report['identity'])
    requested[field] = 'other-requested-' + field
    result = cast(Callable[..., Any], QUALIFY['qualify'])(report, {}, requested)
    assert result.status == 'BLOCKED'
    assert result.reasons == ('requested identity mismatch: ' + field,)
    assert all(row['status'] == 'BLOCKED' for row in result.services)


def test_all_requested_identity_mismatches_are_reported_together():
    report = supplied_report()
    requested = {field: 'other-' + field for field in report['identity']}
    result = cast(Callable[..., Any], QUALIFY['qualify'])(report, {}, requested)
    assert result.reasons == tuple('requested identity mismatch: ' + field
                                   for field in ('target', 'build', 'config', 'workload'))


@pytest.mark.parametrize('gate', CHECKS)
def test_every_mandatory_gate_is_required(gate):
    report = supplied_report()
    del report['gates'][gate]
    assert evaluate(report).status == 'BLOCKED'


@pytest.mark.parametrize('required', [{'source': False}, {'arm64': False},
                                      {'unknown': True}, [], {'load': 1}])
def test_cannot_disable_or_misspell_mandatory_gates(required):
    assert evaluate(supplied_report(), required).status == 'BLOCKED'


@pytest.mark.parametrize('mutation', ['fixture', 'top_fixture', 'identity', 'missing_check',
                                     'smoke', 'unsupported_wallet', 'failed', 'missing_refs',
                                     'local_evidence_class', 'invalid_level', 'headroom_nan'])
def test_invalid_or_incomplete_supplied_evidence_never_passes(mutation):
    report = supplied_report()
    if mutation == 'fixture':
        report['gates']['source']['synthetic'] = True
    elif mutation == 'top_fixture':
        report['synthetic'] = True
    elif mutation == 'identity':
        report['gates']['arm64']['identity']['build'] = 'old-build'
    elif mutation == 'missing_check':
        del report['gates']['security']['checks']['ipv6']
    elif mutation == 'smoke':
        report['gates'] = {'load': report['gates']['load']}
    elif mutation == 'unsupported_wallet':
        report['gates']['detectors']['checks']['wallet_supported'] = False
    elif mutation == 'failed':
        report['gates']['source']['status'] = 'FAIL'
    elif mutation == 'missing_refs':
        report['gates']['source']['evidence_refs'] = []
    elif mutation == 'local_evidence_class':
        report['gates']['deployment']['evidence_class'] = 'local'
    elif mutation == 'invalid_level':
        report['gates']['load']['levels'][1]['id'] = 'unagreed'
    else:
        report['gates']['load']['levels'][0]['disk_free_fraction'] = 'NaN'
    assert evaluate(report).status != 'PASS'


@pytest.mark.parametrize('field,value', [('oom_count', 1), ('eviction_count', 1),
                                        ('cpu_free_fraction', '-0.1'),
                                        ('memory_free_fraction', '-0.1'),
                                        ('disk_free_fraction', '0.19')])
def test_oom_or_insufficient_headroom_fails(field, value):
    report = supplied_report()
    report['gates']['load']['levels'][0][field] = value
    assert evaluate(report).status == 'FAIL'


def cli(tmp_path, raw, requested=None):
    path = tmp_path / 'report.json'
    path.write_bytes(raw)
    args = [sys.executable, '-B', str(ROOT / 'scripts/qualify'), 'report', '--inputs', str(path)]
    for field, value in (supplied_report()['identity'] if requested is None else requested).items():
        args.extend(['--' + field, value])
    return subprocess.run(args, capture_output=True, text=True)


@pytest.mark.parametrize('kind,exit_code,status', [('complete', 0, 'PASS'),
                                                  ('failed', 2, 'FAIL'),
                                                  ('missing', 2, 'BLOCKED'),
                                                  ('malformed', 2, 'BLOCKED'),
                                                  ('duplicate', 2, 'BLOCKED'),
                                                  ('oversized', 2, 'BLOCKED'),
                                                  ('nonfinite', 2, 'BLOCKED')])
def test_report_cli(tmp_path, kind, exit_code, status):
    failed = supplied_report()
    failed['gates']['source']['status'] = 'FAIL'
    raw = {'complete': json.dumps(supplied_report()).encode(), 'missing': b'{}',
           'failed': json.dumps(failed).encode(),
           'malformed': b'{', 'duplicate': b'{"schema_version":1,"schema_version":1}',
           'oversized': b' ' * (1024 * 1024 + 1), 'nonfinite': b'{"x":NaN}'}[kind]
    result = cli(tmp_path, raw)
    assert result.returncode == exit_code, result.stderr
    payload = json.loads(result.stdout)
    assert payload['status'] == status
    assert payload['scope'] == 'supplied_evidence_aggregation_only'
    assert payload['result_kind'] == 'consistency_only'
    assert payload['claims_actual_qualification'] is False
    assert bool(payload['reasons']) == (exit_code != 0)


def test_report_cli_requires_explicit_expected_identity(tmp_path):
    path = tmp_path / 'report.json'
    path.write_text(json.dumps(supplied_report()))
    result = subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/qualify'),
                             'report', '--inputs', str(path)], capture_output=True, text=True)
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload['status'] == 'BLOCKED'
    assert payload['reasons'] == ['requested identity missing: ' + field
                                  for field in ('target', 'build', 'config', 'workload')]


def test_report_cli_reports_each_requested_identity_mismatch(tmp_path):
    identity = {field: 'other-' + field for field in supplied_report()['identity']}
    result = cli(tmp_path, json.dumps(supplied_report()).encode(), identity)
    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload['status'] == 'BLOCKED'
    assert payload['reasons'] == ['requested identity mismatch: ' + field
                                  for field in ('target', 'build', 'config', 'workload')]
    assert payload['availability'] == 'supplied_evidence_only_not_authorization'


@pytest.mark.parametrize('mode', ['arm64', 'live-source'])
def test_live_modes_still_refuse_without_reading_authorization(tmp_path, mode):
    result = subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/qualify'), mode,
                             '--authorization', str(tmp_path / 'absent')],
                            capture_output=True, text=True)
    assert result.returncode == 2
    assert 'No network/client initialized' in result.stderr


@pytest.mark.parametrize('field,value', [('processed_per_second', '5'),
                                        ('elapsed_seconds', '61')])
def test_catchup_measurements_must_support_claims(field, value):
    report = supplied_report()
    report['gates']['catchup']['measurements'][field] = value
    assert evaluate(report).status == 'FAIL'


@pytest.mark.parametrize('arguments', [[], ['--inputs', 'not-an-existing-report']])
def test_report_cli_missing_file_or_option_is_machine_readable(arguments):
    result = subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/qualify'),
                             'report', *arguments], capture_output=True, text=True)
    assert result.returncode == 2
    assert json.loads(result.stdout)['status'] == 'BLOCKED'


def test_malformed_nested_evidence_is_blocked():
    for name in CHECKS:
        report = supplied_report()
        report['gates'][name] = []
        assert evaluate(report).status == 'BLOCKED'


def test_report_result_rejects_fifo_without_blocking(tmp_path):
    path = tmp_path / 'report-pipe'
    os.mkfifo(path)
    result = QUALIFY['report_result'](path, supplied_report()['identity'])
    assert result.status == 'BLOCKED'


# ---- target-bound evidence and fail-closed aggregation -------------

IDENTITY_DIMENSIONS = ('target', 'build', 'config', 'workload')


def leaf_paths(node, prefix=()):
    """Every dict key path and list index path of the supplied report."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield prefix + (key,)
            yield from leaf_paths(value, prefix + (key,))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield prefix + (index,)
            yield from leaf_paths(value, prefix + (index,))


def lookup(report, path):
    node = report
    for part in path:
        node = node[part]
    return node


def set_path(report, path, value, delete=False):
    node = report
    for part in path[:-1]:
        node = node[part]
    if delete:
        del node[path[-1]]
    else:
        node[path[-1]] = value


def test_every_supplied_field_is_load_bearing_deleting_or_corrupting_never_passes():
    """No default PASS: removing or type-corrupting any field cannot keep a PASS."""
    paths = list(leaf_paths(supplied_report()))
    assert len(paths) > 100
    # Only the identity-free, purely informational list entries may be absent.
    survivors = []
    for path in paths:
        for corruption in ('delete', None, 12345, '', [], {}):
            report = supplied_report()
            if corruption == 'delete':
                if isinstance(path[-1], int):
                    continue  # list element removal is covered by the replacement cases
                set_path(report, path, None, delete=True)
            else:
                if corruption == lookup(report, path):
                    continue  # a no-op replacement is not a corruption
                set_path(report, path, corruption)
            if evaluate(report).status == 'PASS':
                survivors.append((path, repr(corruption)))
    assert survivors == []


@pytest.mark.parametrize('gate', CHECKS)
@pytest.mark.parametrize('field', IDENTITY_DIMENSIONS)
def test_gate_evidence_bound_to_a_different_target_identity_is_stale_and_not_pass(gate, field):
    report = supplied_report()
    report['gates'][gate]['identity'][field] = 'previous-' + field
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert any(reason.startswith(gate + ':') and 'identity' in reason for reason in result.reasons)
    assert {row['gate']: row['status'] for row in result.services}[gate] == 'BLOCKED'


@pytest.mark.parametrize('gate', CHECKS)
def test_gate_without_any_identity_binding_is_not_pass(gate):
    report = supplied_report()
    del report['gates'][gate]['identity']
    assert evaluate(report).status == 'BLOCKED'


@pytest.mark.parametrize('evidence_class', ['fixture', 'synthetic', 'local', 'simulated',
                                            'ACTUAL', None, True])
def test_non_actual_evidence_class_is_not_pass(evidence_class):
    report = supplied_report()
    report['gates']['deployment']['evidence_class'] = evidence_class
    assert evaluate(report).status == 'BLOCKED'


@pytest.mark.parametrize('version', [0, 2, '1', True, 1.0, None])
def test_unsupported_schema_version_is_not_pass(version):
    report = supplied_report()
    report['schema_version'] = version
    assert evaluate(report).status == 'BLOCKED'


def test_unknown_extra_gate_is_unsupported_evidence_not_pass():
    report = supplied_report()
    report['gates']['invented'] = deepcopy(report['gates']['source'])
    assert evaluate(report).status == 'BLOCKED'


def test_synthetic_report_is_visibly_synthetic_and_never_actual_pass():
    report = supplied_report()
    report['synthetic'] = True
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert result.synthetic is True
    cli_payload = json.loads(subprocess.run(
        [sys.executable, '-B', str(ROOT / 'scripts/qualify'), 'report', '--inputs', '/dev/null'],
        capture_output=True, text=True).stdout)
    assert cli_payload['claims_actual_qualification'] is False


def test_synthetic_single_gate_marks_result_synthetic_and_blocks():
    report = supplied_report()
    report['gates']['arm64']['synthetic'] = True
    result = evaluate(report)
    assert result.status == 'BLOCKED'
    assert result.synthetic is True


def test_worst_status_wins_fail_over_blocked_over_pass():
    report = supplied_report()
    report['gates']['source']['status'] = 'BLOCKED'
    assert evaluate(report).status == 'BLOCKED'
    report['gates']['load']['levels'][0]['oom_count'] = 1
    assert evaluate(report).status == 'FAIL'
    report['gates']['security']['status'] = 'PASS-ish'
    assert evaluate(report).status == 'FAIL'


@pytest.mark.parametrize('requested', [
    {}, {'target': 't'}, {'target': 't', 'build': 'b', 'config': 'c'},
    {'target': 't', 'build': 'b', 'config': 'c', 'workload': ''},
    {'target': 't', 'build': 'b', 'config': 'c', 'workload': ' '},
    {'target': 't', 'build': 'b', 'config': 'c', 'workload': 7},
    {'target': 't', 'build': 'b', 'config': 'c', 'workload': 'w', 'extra': 'x'},
    [], 'target', 0])
def test_incomplete_or_malformed_requested_identity_is_never_pass(requested):
    result = QUALIFY['qualify'](supplied_report(), {}, requested)
    assert result.status == 'BLOCKED'
    assert result.reasons


def test_evaluation_never_opens_network_clients_or_evidence_references(monkeypatch):
    import socket
    import builtins
    attempts = []

    def forbid(*args, **kwargs):
        attempts.append(args)
        raise AssertionError('network or file access attempted by evaluator')

    for name in ('connect', 'connect_ex', 'sendto'):
        monkeypatch.setattr(socket.socket, name, forbid)
    monkeypatch.setattr(socket, 'getaddrinfo', forbid)
    monkeypatch.setattr(socket, 'create_connection', forbid)
    real_open = builtins.open
    monkeypatch.setattr(builtins, 'open', lambda *a, **k: forbid(*a, **k))
    report = supplied_report()
    for gate in report['gates'].values():
        gate['evidence_refs'] = ['https://example.invalid/evidence', '/etc/passwd']
    result = evaluate(report)
    monkeypatch.setattr(builtins, 'open', real_open)
    assert result.status == 'PASS'
    assert attempts == []


def test_evaluator_source_imports_no_network_client_modules():
    source = (ROOT / 'scripts/qualify').read_text()
    for forbidden in ('import socket', 'import requests', 'import urllib', 'import http',
                      'import httpx', 'import boto', 'import kubernetes'):
        assert forbidden not in source


def test_cli_report_with_other_target_exits_nonzero_for_every_dimension(tmp_path):
    for field in IDENTITY_DIMENSIONS:
        requested = dict(supplied_report()['identity'])
        requested[field] = 'moved-' + field
        result = cli(tmp_path, json.dumps(supplied_report()).encode(), requested)
        assert result.returncode == 2
        assert json.loads(result.stdout)['status'] == 'BLOCKED'


def test_cli_partial_identity_options_are_blocked_not_defaulted(tmp_path):
    path = tmp_path / 'report.json'
    path.write_text(json.dumps(supplied_report()))
    result = subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/qualify'), 'report',
                             '--inputs', str(path), '--target', 'test-only-target'],
                            capture_output=True, text=True)
    assert result.returncode == 2
    assert json.loads(result.stdout)['status'] == 'BLOCKED'
