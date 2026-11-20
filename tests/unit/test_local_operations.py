"""Offline regressions for operational safety; no Docker transport."""

import importlib.machinery
import importlib.util
import json
import subprocess
import sys
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'scripts'


def runner():
    loader = importlib.machinery.SourceFileLoader('operations_runner', str(ROOT / 'scripts/local-check'))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


@pytest.mark.parametrize('field', ['ram_budget_bytes', 'full_host_footprint_bytes'])
def test_r5_huge_integer_structured_cli_refusal(tmp_path, field):
    envelope = {'synthetic': True, 'ram_budget_bytes': 1}
    sample = {'full_host_footprint_bytes': 1}
    (envelope if field == 'ram_budget_bytes' else sample)[field] = 10**1000
    (tmp_path / 'envelope.json').write_text(json.dumps(envelope))
    (tmp_path / 'probe-resources.jsonl').write_text(json.dumps(sample)+'\n')
    result = subprocess.run([sys.executable, '-B', str(ROOT/'scripts/measure_feasibility.py'),
                             '--input', str(tmp_path), '--envelope', str(tmp_path/'envelope.json')],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stdout)
    assert payload['status'] == 'BLOCKED'
    assert payload['actual_fit_eligible'] is False
    assert 'OverflowError' not in result.stderr


@pytest.mark.parametrize('run_name', ['__main__', 'historical_import'])
def test_r3_historical_launcher_never_calls_docker(monkeypatch, run_name):
    def forbidden(*args, **kwargs):
        pytest.fail('historical helper attempted a subprocess before admission')
    monkeypatch.setattr(subprocess, 'check_output', forbidden)
    monkeypatch.setattr(subprocess, 'run', forbidden)
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    with pytest.raises(SystemExit, match='BLOCKED: historical'):
        runpy.run_path(str(EVIDENCE/'run_diagnostics.py'), run_name=run_name)


@pytest.mark.parametrize('failure', ['timeout', 'sampling', 'interrupt', 'disk'])
def test_r1_workload_stopped_and_client_reaped_on_every_failure(tmp_path, monkeypatch, failure):
    from unittest.mock import Mock
    from contextlib import nullcontext
    m = runner()
    monkeypatch.setattr(m, 'storage_fixture', lambda prefix: nullcontext(('helper', {})))
    def disk_failure(helper):
        raise ValueError('BLOCKED: storage runtime floor')
    monkeypatch.setattr(m, 'measure_storage', disk_failure)
    monkeypatch.setattr(m, 'ROOT', tmp_path)
    monkeypatch.setattr(m, 'evidence_directory', lambda: tmp_path)
    monkeypatch.setattr(sys, 'argv', ['local-check', 'unit'])
    monkeypatch.setattr(m.os, 'environ', {})
    calls = []
    container = {'Id': 'app-id', 'Config': {'Labels': {
        'com.docker.compose.project': m.PROJECT,
        'com.docker.compose.service': 'app'}},
        'HostConfig': {}, 'State': {'Running': False}, 'Mounts': []}
    def command(args, **kwargs):
        calls.append(args)
        if 'context' in args: return 'unix:///var/run/docker.sock'
        if 'network' in args: return '[{"Internal":true}]'
        if 'ps' in args: return 'app-id'
        if 'inspect' in args: return json.dumps([container])
        if 'stats' in args:
            if failure == 'disk': return '{}'
            if failure == 'interrupt': raise KeyboardInterrupt()
            raise RuntimeError('sampling failed')
        return ''
    monkeypatch.setattr(m, 'command', command)
    proc = Mock()
    proc.poll.return_value = None
    proc.wait.return_value = -15
    monkeypatch.setattr(m.subprocess, 'Popen', Mock(return_value=proc))
    monkeypatch.setattr(m.time, 'monotonic', lambda: 1000 if proc.poll.called and failure == 'timeout' else 0)
    try:
        m.main()
    except (TimeoutError, RuntimeError, KeyboardInterrupt, ValueError):
        pass
    assert any('stop' in c and ('app' in c or 'app-id' in c) for c in calls), calls
    assert proc.wait.called, 'exec client was never reaped'
    assert any('inspect' in c for c in calls[calls.index(next(c for c in calls if 'stop' in c))+1:]), 'missing stopped readback'


def test_r4_startup_deadline_and_cleanup(tmp_path, monkeypatch):
    from contextlib import nullcontext
    m = runner()
    monkeypatch.setattr(m, 'storage_fixture', lambda prefix: nullcontext(('helper', {})))
    def disk_failure(helper):
        raise ValueError('BLOCKED: storage runtime floor')
    monkeypatch.setattr(m, 'measure_storage', disk_failure)
    monkeypatch.setattr(m, 'ROOT', tmp_path)
    monkeypatch.setattr(m, 'evidence_directory', lambda: tmp_path)
    monkeypatch.setattr(sys, 'argv', ['local-check', 'unit'])
    monkeypatch.setattr(m.os, 'environ', {})
    calls = []
    container = {'Id': 'app-id', 'Config': {'Labels': {
        'com.docker.compose.project': m.PROJECT, 'com.docker.compose.service': 'app'}},
        'State': {'Running': False}}
    def command(args, **kwargs):
        calls.append((args, kwargs))
        if 'context' in args: return 'unix:///var/run/docker.sock'
        if 'up' in args:
            assert kwargs.get('timeout', 60) > 180, 'startup subprocess expires before readiness'
            raise subprocess.TimeoutExpired(args, kwargs['timeout'], output='partial startup')
        if 'ps' in args: return 'app-id'
        if 'inspect' in args: return json.dumps([container])
        return ''
    monkeypatch.setattr(m, 'command', command)
    with pytest.raises(subprocess.TimeoutExpired):
        m.main()
    assert any('stop' in c for c, _ in calls)
    errors = list(tmp_path.rglob('*.error.json'))
    assert errors and json.loads(errors[0].read_text())['error_type'] == 'TimeoutExpired'


def test_r4_command_accepts_explicit_deadline(monkeypatch):
    from unittest.mock import Mock
    m = runner()
    run = Mock(return_value=Mock(stdout='ready'))
    monkeypatch.setattr(m.subprocess, 'run', run)
    assert m.command(['fixture'], timeout=210) == 'ready'
    assert run.call_args.kwargs['timeout'] == 210


@pytest.mark.parametrize('bad', ['workspace', 'postgres', 'kafka', 'missing', 'zero', 'bool'])
def test_r2_each_actual_filesystem_fails_closed(monkeypatch, bad):
    m = runner()
    records = {name: {'total_bytes': 100*2**30, 'free_bytes': 40*2**30,
                      'device': i, 'path': '/'+name}
               for i, name in enumerate(['workspace', 'postgres', 'kafka'])}
    if bad in records: records[bad]['free_bytes'] = 1
    elif bad == 'missing': records.pop('kafka')
    elif bad == 'zero': records['postgres']['total_bytes'] = 0
    elif bad == 'bool': records['postgres']['free_bytes'] = True
    monkeypatch.setattr(m, 'command', lambda *a, **kw: json.dumps(records))
    with pytest.raises(ValueError, match='BLOCKED: storage'):
        m.measure_storage('helper-id')


def test_r2_measurements_keep_actual_volume_identity(monkeypatch):
    m = runner()
    records = {name: {'total_bytes': (i+10)*2**30, 'free_bytes': 5*2**30,
                      'device': i, 'path': '/'+name}
               for i, name in enumerate(['workspace', 'postgres', 'kafka'])}
    monkeypatch.setattr(m, 'command', lambda *a, **kw: json.dumps(records))
    assert m.measure_storage('helper-id') == records


def test_r2_repeated_attempts_bounded_and_concurrent_refused(tmp_path):
    m = runner()
    with m.admission(tmp_path, 'first'):
        with pytest.raises(ValueError, match='BLOCKED: concurrent'):
            with m.admission(tmp_path, 'overlap'): pass
    # Failure/exception must consume its reservation; no automatic retention deletion.
    for i in range(m.MAX_RUNS - 1):
        with pytest.raises(RuntimeError):
            with m.admission(tmp_path, f'run{i}'):
                raise RuntimeError('failed run')
    with pytest.raises(ValueError, match='BLOCKED: run budget'):
        with m.admission(tmp_path, 'extra'): pass
    assert len(json.loads((tmp_path/'run-admission.json').read_text())['attempts']) == m.MAX_RUNS


def test_r1_reap_escalates_and_waits_for_stubborn_client(monkeypatch):
    from unittest.mock import Mock
    m = runner()
    proc = Mock()
    proc.poll.return_value = None
    proc.wait.side_effect = [subprocess.TimeoutExpired('exec', 10), -9]
    m.reap(proc)
    proc.terminate.assert_called_once()
    proc.kill.assert_called_once()
    assert proc.wait.call_count == 2


def test_r1_cleanup_refuses_unowned_target(tmp_path, monkeypatch):
    m = runner()
    calls = []
    def command(argv, **kwargs):
        calls.append(argv)
        return json.dumps([{'Id': 'foreign', 'Config': {'Labels': {
            'com.docker.compose.project': 'other', 'com.docker.compose.service': 'app'}}}])
    monkeypatch.setattr(m, 'command', command)
    with pytest.raises(ValueError, match='ownership mismatch'):
        m.stop_owned(['foreign'], tmp_path/'probe')
    assert not any('stop' in argv for argv in calls)


def test_r2_runtime_growth_and_retained_bound(tmp_path, monkeypatch):
    m = runner()
    monkeypatch.setattr(m, 'ROOT', tmp_path)
    monkeypatch.setattr(m, 'evidence_directory', lambda: tmp_path)
    baseline = {name: {'free_bytes': 4*2**30} for name in ['workspace', 'postgres', 'kafka']}
    changed = {name: {'free_bytes': rec['free_bytes']} for name, rec in baseline.items()}
    m.storage_guard(changed, baseline)
    changed['kafka']['free_bytes'] -= m.MAX_RUN_GROWTH + 1
    with pytest.raises(ValueError, match='run growth'):
        m.storage_guard(changed, baseline)
    directory = tmp_path/'.cache/runtime'
    directory.mkdir(parents=True)
    # Sparse local test file: exercise the bound without allocating 256 MiB RAM/disk.
    with (directory/'large').open('wb') as handle:
        handle.truncate(m.MAX_RETAINED_WORKSPACE + 1)
    with pytest.raises(ValueError, match='retained workspace'):
        m.storage_guard(baseline, baseline)


def test_r2_foreign_volume_refused_before_helper_create(tmp_path, monkeypatch):
    m = runner()
    calls = []
    def command(argv, **kwargs):
        calls.append(argv)
        return json.dumps([{'Name': m.PROJECT+'_pgdata', 'Driver': 'local', 'Options': None,
                            'Labels': {'com.docker.compose.project': 'foreign'}}])
    monkeypatch.setattr(m, 'command', command)
    with pytest.raises(ValueError, match='volume ownership'):
        with m.storage_fixture(tmp_path/'probe'): pass
    assert not any('create' in argv or 'start' in argv for argv in calls)


def test_heavy_delegate_budget_extension_is_ten_attempts():
    m = runner()
    assert m.MAX_RUNS >= 10


def test_workload_mode_uses_larger_but_finite_run_growth(tmp_path, monkeypatch):
    m = runner()
    monkeypatch.setattr(m, 'ROOT', tmp_path)
    baseline = {name: {'free_bytes': 10*2**30} for name in ['workspace', 'postgres', 'kafka']}
    changed = {name: {'free_bytes': rec['free_bytes']} for name, rec in baseline.items()}
    changed['postgres']['free_bytes'] -= m.MAX_RUN_GROWTH + 1
    with pytest.raises(ValueError, match='run growth'):
        m.storage_guard(changed, baseline)
    m.storage_guard(changed, baseline, growth=m.MAX_WORKLOAD_RUN_GROWTH)
    assert m.MAX_RUN_GROWTH < m.MAX_WORKLOAD_RUN_GROWTH <= 4 * 2**30
    changed['postgres']['free_bytes'] = baseline['postgres']['free_bytes'] - m.MAX_WORKLOAD_RUN_GROWTH - 1
    with pytest.raises(ValueError, match='run growth'):
        m.storage_guard(changed, baseline, growth=m.MAX_WORKLOAD_RUN_GROWTH)


def test_workload_level_is_validated_before_any_attempt_is_reserved(tmp_path, monkeypatch):
    m = runner()
    (tmp_path / 'run-admission.json').write_text(json.dumps({'policy': 'p', 'attempts': []}))
    monkeypatch.setattr(m, 'evidence_directory', lambda: tmp_path)
    monkeypatch.setattr(m.os, 'environ', {})
    monkeypatch.setattr(m, 'command', lambda *a, **kw: 'unix:///var/run/docker.sock')
    monkeypatch.setattr(sys, 'argv', ['local-check', 'workload', '--level', 'level-9'])
    with pytest.raises(ValueError, match='BLOCKED: unknown workload level'):
        m.main()
    assert json.loads((tmp_path / 'run-admission.json').read_text())['attempts'] == []


def test_k3s_pod_exclusion_reads_only_non_system_namespaces():
    m = runner()
    pods = {'items': [
        {'metadata': {'namespace': 'kube-system', 'uid': 'a'}},
        {'metadata': {'namespace': 'sl-ton-sentinel', 'uid': 'b-1'}},
    ]}
    assert m.foreign_pod_uids(pods) == ['b-1']
