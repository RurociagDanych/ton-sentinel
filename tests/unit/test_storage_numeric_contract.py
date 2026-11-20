"""Exercise the real offline chart admission, not a Python validator."""

import json
import shutil
import subprocess

import pytest
import yaml

from tests.unit.test_deployment_policy import CHART, HELM, fixture_values


NUMERIC_FIELDS = [
    ('dataVolumeBytes',),
    ('reserveBytes',),
    *[('writersGiB', name) for name in
      ('postgres', 'kafka', 'checkpoints', 'scratch', 'metrics', 'k3s')],
    *[('kafka', name) for name in
      ('segmentBytes', 'overshootSegmentsPerPartition', 'metadataReserveBytes')],
    *[('kafka', 'topics', index, name) for index in range(4)
      for name in ('partitions', 'budgetPerPartitionBytes')],
]


def render_contract(tmp_path, field=None, mutation=None, skip_schema=False):
    chart = tmp_path / 'chart'
    shutil.copytree(CHART, chart)
    path = chart / 'storage-budget.json'
    budget = json.loads(path.read_text())
    if field is not None:
        parent = budget
        for key in field[:-1]:
            parent = parent[key]
        original = parent[field[-1]]
        parent[field[-1]] = {
            'fraction': original + 0.9,
            'rounded-fraction': original,
            'string': str(original),
            'boolean': True,
            'null': None,
            'overflow': 2**64 + original,
            'negative': -original,
        }[mutation]
    raw = json.dumps(budget)
    if mutation == 'rounded-fraction':
        # A decimal smaller than float64 precision must not disappear at JSON decode.
        # Rewrite only the chosen value via a unique string sentinel.
        parent[field[-1]] = 'P3_NUMERIC_SENTINEL'
        raw = json.dumps(budget).replace('"P3_NUMERIC_SENTINEL"',
                                        f'{original}.000000000000000001')
    path.write_text(raw)
    if skip_schema:
        (chart / 'values.schema.json').unlink()
    values = tmp_path / 'fixture.yaml'
    values.write_text(yaml.safe_dump(fixture_values()))
    return subprocess.run([HELM, 'template', 'p3', str(chart), '-f', str(values)],
                          capture_output=True, text=True, check=False)


@pytest.mark.parametrize('field', NUMERIC_FIELDS, ids=lambda x: '.'.join(map(str, x)))
@pytest.mark.parametrize('mutation', ['fraction', 'rounded-fraction', 'string',
                                     'boolean', 'null', 'overflow', 'negative'])
def test_p3_rejects_non_integer_contract(tmp_path, field, mutation):
    result = render_contract(tmp_path, field, mutation)
    assert result.returncode != 0, f'Accepted {field}: {mutation}'
    assert 'BLOCKED: storage numeric contract' in result.stderr
    assert result.stdout.strip() == ''


@pytest.mark.parametrize('skip_schema', [False, True])
def test_p3_valid_integer_contract_stays_inert(tmp_path, skip_schema):
    result = render_contract(tmp_path, skip_schema=skip_schema)
    assert result.returncode == 0, result.stderr
    objects = [obj for obj in yaml.safe_load_all(result.stdout) if obj]
    workloads = [obj for obj in objects if obj['kind'] in ('Deployment', 'StatefulSet')]
    assert workloads
    assert all(obj['spec']['replicas'] == 0 for obj in workloads)
    assert all(obj['spec']['suspend'] for obj in objects if obj['kind'] == 'CronJob')


@pytest.mark.parametrize('field', [('writersGiB', 'postgres'),
                                   ('kafka', 'topics', 0, 'partitions')])
def test_p3_fraction_refused_without_values_schema(tmp_path, field):
    result = render_contract(tmp_path, field, 'fraction', skip_schema=True)
    assert result.returncode != 0
    assert 'BLOCKED: storage numeric contract' in result.stderr
    assert result.stdout.strip() == ''
