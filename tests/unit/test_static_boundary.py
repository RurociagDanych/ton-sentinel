"""Service-marked tests must not start clients in offline qualification."""

from pathlib import Path
import os
import runpy
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('marker', ['integration', 'k3s'])
def test_static_guard_refuses_service_tests(monkeypatch, marker):
    monkeypatch.setenv('TON_SENTINEL_STATIC_ONLY', '1')
    hook = runpy.run_path(str(ROOT / 'tests/conftest.py')).get('pytest_runtest_setup')
    assert callable(hook), 'missing offline service execution boundary'
    item = SimpleNamespace(get_closest_marker=lambda name: object() if name == marker else None)
    with pytest.raises(pytest.fail.Exception, match='BLOCKED: offline static qualification'):
        hook(item)


def test_static_guard_allows_pure_tests(monkeypatch):
    monkeypatch.setenv('TON_SENTINEL_STATIC_ONLY', '1')
    hook = runpy.run_path(str(ROOT / 'tests/conftest.py')).get('pytest_runtest_setup')
    assert callable(hook), 'missing offline service execution boundary'
    hook(SimpleNamespace(get_closest_marker=lambda name: None))


def test_static_guard_does_not_change_authorized_service_runner(monkeypatch):
    monkeypatch.delenv('TON_SENTINEL_STATIC_ONLY', raising=False)
    hook = runpy.run_path(str(ROOT / 'tests/conftest.py')).get('pytest_runtest_setup')
    assert callable(hook), 'missing offline service execution boundary'
    hook(SimpleNamespace(get_closest_marker=lambda name: object()))


@pytest.mark.parametrize('marker', ['integration', 'k3s'])
def test_explicit_service_selection_refuses_before_fixture_but_remains_available(tmp_path, marker):
    (tmp_path / 'conftest.py').write_text((ROOT / 'tests/conftest.py').read_text())
    (tmp_path / 'pytest.ini').write_text(f'[pytest]\nmarkers =\n {marker}: service\n')
    touched = tmp_path / 'fixture-ran'
    (tmp_path / 'test_service.py').write_text(
        'import pytest\nfrom pathlib import Path\n'
        '@pytest.fixture\ndef service():\n'
        f'    Path({str(touched)!r}).write_text("local control only")\n'
        f'@pytest.mark.{marker}\ndef test_service(service):\n    assert True\n')
    env = dict(os.environ, TON_SENTINEL_STATIC_ONLY='1', PYTEST_ADDOPTS='')
    command = [sys.executable, '-m', 'pytest', '-q', '-m', marker]
    refused = subprocess.run(command, cwd=tmp_path, env=env,
                             capture_output=True, text=True, check=False)
    assert refused.returncode == 1, refused.stdout + refused.stderr
    assert 'BLOCKED: offline static qualification' in refused.stdout
    assert not touched.exists(), 'service fixture ran before refusal'
    del env['TON_SENTINEL_STATIC_ONLY']
    allowed = subprocess.run(command, cwd=tmp_path, env=env,
                             capture_output=True, text=True, check=False)
    assert allowed.returncode == 0, allowed.stdout + allowed.stderr
    assert '1 passed' in allowed.stdout
    assert touched.exists(), 'explicit non-static selection did not run the local control'
