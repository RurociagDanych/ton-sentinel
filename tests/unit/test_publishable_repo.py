"""Publishable-repository guards: a clean clone must hold everything the tests need."""

import ast
import importlib.machinery
import importlib.util
import json
import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / 'deploy/helm/ton-sentinel'
LOCAL_VALUES = ROOT / 'deploy/helm/local/values-local-k3s.yaml'
ENVELOPE = ROOT / 'deploy/workload/acceptance.json'
PG_VAR = 'TON_SENTINEL_PG_PASSWORD'
IGNORED_PATH = re.compile(r'^(\.\./)*\.env$')


def _string_constants(tree):
    """Non-docstring string constants with their parent node."""
    docstrings = set()
    for node in ast.walk(tree):
        body = getattr(node, 'body', None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr) \
                and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
            docstrings.add(id(body[0].value))
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            if isinstance(child, ast.Constant) and isinstance(child.value, str) \
                    and id(child) not in docstrings:
                yield child, parent


def _under_tmp(parent):
    return (isinstance(parent, ast.BinOp) and isinstance(parent.op, ast.Div)
            and isinstance(parent.left, ast.Name) and parent.left.id == 'tmp_path')


def test_no_test_reads_dotenv():
    offenders = []
    for path in sorted((ROOT / 'tests').rglob('*.py')):
        tree = ast.parse(path.read_text(), str(path))
        for node, parent in _string_constants(tree):
            # A comparison (for example a .gitignore line check) is not a path read.
            if IGNORED_PATH.match(node.value) and not _under_tmp(parent) \
                    and not isinstance(parent, ast.Compare):
                offenders.append(f'{path.relative_to(ROOT)}:{node.lineno}')
    assert offenders == []


def test_local_helm_values_live_outside_the_chart():
    assert LOCAL_VALUES.is_file()
    assert not (CHART / 'values-local-k3s.yaml').exists()
    values = yaml.safe_load(LOCAL_VALUES.read_text())
    assert values['local']['enabled'] is True
    assert 'password' not in LOCAL_VALUES.read_text().lower()


def _runner():
    loader = importlib.machinery.SourceFileLoader('publishable_runner', str(ROOT / 'scripts/local-check'))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_acceptance_envelope_and_its_measurements_are_tracked():
    envelope = json.loads(ENVELOPE.read_text())
    paths = envelope['maintenance_and_retention_evidence']
    assert paths
    paths = paths + [envelope['workload_decision']['record']]
    assert all(not IGNORED_PATH.match(p) and (ROOT / p).is_file() for p in paths)
    assert _runner().ENVELOPE == ENVELOPE


def _strings(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)
    elif isinstance(value, str):
        yield value


@pytest.mark.parametrize('fixture', sorted(p.relative_to(ROOT).as_posix()
                                            for p in (ROOT / 'tests/fixtures').rglob('*.json')))
def test_fixtures_carry_no_urls_or_hosts(fixture):
    found = [s for s in _strings(json.loads((ROOT / fixture).read_text())) if '://' in s]
    assert len(found) == 0, f'{len(found)} URL-bearing strings in {fixture}'


def _interpolates(value, var):
    return re.fullmatch(r'\$\{' + var + r'(:\?[^}]*)?\}', str(value)) is not None


def test_compose_reads_the_database_password_from_the_environment():
    compose = yaml.safe_load((ROOT / 'deploy/compose.yaml').read_text())
    assert _interpolates(compose['services']['postgres']['environment']['POSTGRES_PASSWORD'], PG_VAR)
    assert _interpolates(compose['services']['app']['environment'][PG_VAR], PG_VAR)


LITERAL_SECRET = [
    re.compile(r'password=(?!\{|\$)[^\s"\'{}]+'),
    re.compile(r'POSTGRES_PASSWORD\s*[:=]\s*(?![\'"]?\$\{)[^\s#]+'),
    re.compile(r'postgres(ql)?://[^:/\s@]+:(?!\$\{|\{)[^@\s]+@'),
    re.compile(r'\.option\("password",\s*"'),
]


@pytest.mark.parametrize('relative', ['deploy/compose.yaml', 'src/ton_sentinel/feasibility.py',
                                      'src/ton_sentinel/workload.py', 'scripts/local-stack-check',
                                      'tests/integration/test_feasibility.py'])
def test_no_literal_database_password(relative):
    text = (ROOT / relative).read_text()
    hits = [rx.pattern for rx in LITERAL_SECRET if rx.search(text)]
    assert hits == []


def test_local_dsn_comes_from_the_environment(monkeypatch):
    from ton_sentinel import feasibility
    monkeypatch.delenv(PG_VAR, raising=False)
    with pytest.raises(RuntimeError, match=PG_VAR):
        feasibility.local_dsn()
    monkeypatch.setenv(PG_VAR, 'example-placeholder')
    dsn = feasibility.local_dsn()
    assert 'password=example-placeholder' in dsn and 'user=probe' in dsn and 'dbname=feasibility' in dsn


def test_env_example_has_placeholders_only():
    text = (ROOT / '.env.example').read_text()
    values = dict(line.split('=', 1) for line in text.splitlines()
                  if line.strip() and not line.lstrip().startswith('#'))
    assert {PG_VAR, 'TONCENTER_API_KEY', 'TONCENTER_BASE_URL'} <= set(values)
    for key, value in values.items():
        assert re.search(r'change-me|example', value), key
    gitignore = (ROOT / '.gitignore').read_text().splitlines()
    assert '.env' in gitignore and '!.env.example' in gitignore
    assert not any(line.strip().lstrip('/').startswith('docs') for line in gitignore)


def test_docs_directory_is_tracked():
    import subprocess
    tracked = subprocess.run(['git', 'ls-files', '--', 'docs'], cwd=ROOT, capture_output=True,
                             text=True, check=True).stdout.split()
    assert {'docs/OPERATIONS.md', 'docs/SECURITY.md'} <= set(tracked)


def test_mit_license_and_pyproject_field():
    text = (ROOT / 'LICENSE').read_text()
    assert text.startswith('MIT License')
    assert 'Permission is hereby granted, free of charge' in text
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']
    assert project['license'] == 'MIT'


def test_readme_is_short_honest_and_free_of_counts():
    text = (ROOT / 'README.md').read_text()
    for heading in ('## Architecture', '## Quick start', '## Limitations', '## License'):
        assert heading in text
    for word in ('STON.fi', 'DeDust', 'PostgreSQL', 'Kafka', 'Spark', 'Prometheus', 'Grafana',
                 '.env.example', 'MIT'):
        assert word in text, word
    assert not re.search(r'\b\d[\d,]*\s+(passed|tests)\b', text)
    assert text.count('/v1/') <= 2
    for target in re.findall(r'\]\((docs/[^)#\s]+)', text):
        assert (ROOT / target).is_file(), target
    assert not re.search('[ąćęłńóśźżĄĆĘŁŃÓŚŹŻ]', text)
    assert len(text.splitlines()) <= 150


def test_local_check_takes_only_the_database_password_from_dotenv(tmp_path):
    runner = _runner()
    env_file = tmp_path / 'local.env'
    env_file.write_text('TONCENTER_API_KEY=example-key\n' + PG_VAR + '=example-pg\n')
    environ = {}
    runner.load_pg_password(environ, env_file)
    assert environ == {PG_VAR: 'example-pg'}
    kept = {PG_VAR: 'example-already-set'}
    runner.load_pg_password(kept, env_file)
    assert kept == {PG_VAR: 'example-already-set'}
    absent = {}
    runner.load_pg_password(absent, tmp_path / 'absent.env')
    assert absent == {}


# Runtime fallbacks that tolerate a missing path (optional historical ledger) or build one in tmp.
_DOCS_REF_ALLOWED = {'scripts/local-check', 'tests/unit/test_portable_offline.py',
                     'tests/unit/test_publishable_repo.py'}


def test_no_tracked_file_references_ignored_docs_trees():
    import subprocess
    files = subprocess.run(['git', 'ls-files', '--', 'src', 'tests', 'scripts', 'deploy', '.github',
                            'pyproject.toml', 'README.md'], cwd=ROOT, capture_output=True,
                           text=True, check=True).stdout.split('\n')
    pattern = re.compile(rb'docs/(research|superpowers)')
    offenders = [f for f in files if f and f not in _DOCS_REF_ALLOWED
                 and (ROOT / f).is_file() and pattern.search((ROOT / f).read_bytes())]
    assert offenders == []


def test_default_compose_mounts_no_ignored_docs_path():
    doc = yaml.safe_load((ROOT / 'deploy/compose.yaml').read_text())
    for service in doc['services'].values():
        for volume in service.get('volumes', []):
            assert 'docs/' not in str(volume), volume
