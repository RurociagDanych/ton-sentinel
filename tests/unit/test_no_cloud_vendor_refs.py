"""Owner decision 2026-11-13: the project is local-first and the cloud target is not chosen."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
SCOPES = ('src', 'scripts', 'deploy', 'tests')
TOP_FILES = ('README.md', 'pyproject.toml')
# This guard and the envelope guard name the forbidden terms on purpose.
GUARDS = {Path(__file__).resolve(), (ROOT / 'tests/unit/test_acceptance_envelope.py').resolve()}
FORBIDDEN = re.compile(
    r'(?i)(?<![a-z0-9])oci(?![a-z0-9])|oracle[ _-]?cloud|docs\.oracle\.com|always[ _-]?free'
    r'|free[ _-]?tier|(?<![a-z0-9])ocpus?(?![a-z0-9])|VM\.Standard\.A1')
SKIP_DIRS = {'__pycache__', '.pytest_cache', 'node_modules'}


def candidate_files():
    for scope in SCOPES:
        for path in sorted((ROOT / scope).rglob('*')):
            if path.is_file() and not SKIP_DIRS & set(path.parts) and path.resolve() not in GUARDS:
                yield path
    for name in TOP_FILES:
        yield ROOT / name


def test_removed_cloud_artifacts_are_gone():
    for rel in ('infra', 'scripts/free_tier_gate.py', 'tests/fixtures/entitlements'):
        assert not (ROOT / rel).exists(), rel


def test_no_dropped_cloud_vendor_reference_in_product_tests_or_deploy():
    hits = []
    for path in candidate_files():
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        for number, line in enumerate(text.splitlines(), 1):
            if FORBIDDEN.search(line):
                hits.append(f'{path.relative_to(ROOT)}:{number}: {line.strip()[:120]}')
    assert hits == []


def test_pytest_markers_have_no_cloud_opt_in():
    text = (ROOT / 'pyproject.toml').read_text()
    match = re.search(r'markers = \[(.*?)\]', text, re.S)
    assert match is not None
    assert set(re.findall(r'"(\w+):', match.group(1))) == {'integration', 'k3s'}
