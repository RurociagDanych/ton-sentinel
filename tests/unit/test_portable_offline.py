"""Portable local tooling selection and fail-closed runtime handoff."""
import json
import runpy
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]


def qualification():
    return runpy.run_path(str(ROOT / 'scripts/qualify'))


@pytest.mark.parametrize('override', [False, True])
def test_static_uses_selected_helm(monkeypatch, tmp_path, override):
    helm = tmp_path / 'helm'
    helm.write_text('#!/bin/sh\nexit 0\n')
    helm.chmod(0o755)
    monkeypatch.setenv('PATH', str(tmp_path))
    monkeypatch.delenv('TON_SENTINEL_HELM', raising=False)
    if override:
        monkeypatch.setenv('TON_SENTINEL_HELM', str(helm))
        monkeypatch.setenv('PATH', '')
    calls = []
    monkeypatch.setattr(subprocess, 'run', lambda args, **kwargs:
                        calls.append((args, kwargs)) or subprocess.CompletedProcess(args, 0))
    assert qualification()['static_checks']() == 0
    helm_calls = [args for args, _ in calls if args and args[0] == str(helm)]
    assert [args[1] for args in helm_calls] == ['lint', 'template']
    assert all(options['env']['TON_SENTINEL_HELM'] == str(helm) for _, options in calls)


def test_static_pytest_selects_offline_tests_across_directories(monkeypatch, tmp_path):
    monkeypatch.setattr('shutil.which', lambda name: '/preinstalled/helm')
    # An inherited service-only selection must not escape the static boundary.
    monkeypatch.setenv('PYTEST_ADDOPTS', '-m integration')
    calls = []
    real_run = subprocess.run
    monkeypatch.setattr(subprocess, 'run', lambda args, **kwargs:
                        calls.append((args, kwargs)) or subprocess.CompletedProcess(args, 0))
    assert qualification()['static_checks']() == 0
    command, options = calls[-1]
    assert command == [sys.executable, '-m', 'pytest', '-q']
    assert options['env']['TON_SENTINEL_STATIC_ONLY'] == '1'
    (tmp_path / 'conftest.py').write_text((ROOT / 'tests/conftest.py').read_text())
    (tmp_path / 'pytest.ini').write_text(
        '[pytest]\nmarkers =\n integration: service\n k3s: service\n')
    for directory in ('unit', 'integration'):
        target = tmp_path / 'tests' / directory
        target.mkdir(parents=True)
        (target / f'test_{directory}_offline.py').write_text('def test_offline():\n    assert True\n')
    (tmp_path / 'tests/integration/test_service.py').write_text(
        'import pytest\n'
        '@pytest.mark.integration\n'
        'def test_integration():\n    pytest.fail("service body executed")\n'
        '@pytest.mark.k3s\n'
        'def test_k3s():\n    pytest.fail("service body executed")\n')
    result = real_run(command, cwd=tmp_path, env=options['env'],
                      capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert '2 passed, 2 deselected' in result.stdout


def test_missing_helm_refuses_before_subprocess(monkeypatch):
    monkeypatch.setenv('PATH', '')
    monkeypatch.delenv('TON_SENTINEL_HELM', raising=False)
    monkeypatch.setattr(subprocess, 'run', lambda *a, **kw: pytest.fail('unexpected subprocess'))
    assert qualification()['static_checks']() == 127


def test_runtime_requires_retained_ledger_before_docker(tmp_path, monkeypatch):
    from tests.unit.test_local_operations import runner
    module = runner()
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(sys, 'argv', ['local-check', 'unit'])
    monkeypatch.setattr(module, 'command', lambda *a, **kw: pytest.fail('Docker before retained ledger'))
    with pytest.raises(ValueError, match='retained admission ledger'):
        module.main()


def test_runtime_reuses_legacy_ledger_without_reset(tmp_path, monkeypatch):
    from tests.unit.test_local_operations import runner
    module = runner()
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    legacy = tmp_path / 'docs/research/implementation-evidence/task-02/fix-round-1'
    legacy.mkdir(parents=True)
    ledger = legacy / 'run-admission.json'
    ledger.write_text(json.dumps({'attempts': [f'prior{i}' for i in range(module.MAX_RUNS)]}))
    before = ledger.read_bytes()
    assert module.evidence_directory() == legacy
    with pytest.raises(ValueError, match='run budget exhausted'):
        with module.admission(module.evidence_directory(), 'third'):
            pytest.fail('exhausted ledger admitted')
    assert ledger.read_bytes() == before


def test_relocated_fixture_evidence_counts_toward_storage_limit(tmp_path, monkeypatch):
    from tests.unit.test_local_operations import runner
    module = runner()
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    evidence = tmp_path / '.cache/fixture-evidence'
    evidence.mkdir(parents=True)
    with (evidence / 'large').open('wb') as handle:
        handle.truncate(module.MAX_RETAINED_WORKSPACE + 1)
    baseline = {name: {'free_bytes': 4 * 2**30}
                for name in ('workspace', 'postgres', 'kafka')}
    with pytest.raises(ValueError, match='retained workspace'):
        module.storage_guard(baseline, baseline)
