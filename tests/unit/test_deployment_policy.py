"""Offline policy only: synthetic render inputs are NOT G2/G5 attestations."""
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import socket
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / 'deploy/helm/ton-sentinel'
HELM = os.environ.get('TON_SENTINEL_HELM') or shutil.which('helm') or 'helm'


def render(tmp_path, **updates):
    values = yaml.safe_load((CHART / 'values.yaml').read_text())
    values.update(updates)
    path = tmp_path / 'values.yaml'
    path.write_text(yaml.safe_dump(values))
    result = subprocess.run([HELM, 'template', 'sentinel', str(CHART), '-f', str(path)],
                            capture_output=True, text=True, check=False)
    return result


def fixture_values():
    # Deliberately non-deployable registry namespace and synthetic content digests.
    return {'enabled': True, 'admission': {'g2EvidenceId': 'fixture-only-not-a-real-gate',
            'allocatableMiB': 10000, 'osReserveMiB': 2048, 'nodeName': 'fixture-node'},
            'images': {name: 'invalid.example/fixture/' + name + '@sha256:' + '1' * 64
                       for name in ['app', 'kafka', 'postgres', 'spark']},
            'existingSecret': 'fixture-existing-secret'}


def objects(tmp_path):
    result = render(tmp_path, **fixture_values())
    assert result.returncode == 0, result.stderr
    return [x for x in yaml.safe_load_all(result.stdout) if x]


def pod_specs(objects):
    for obj in objects:
        if obj['kind'] in ('Deployment', 'StatefulSet'):
            yield obj['spec']['template']['spec']
        elif obj['kind'] == 'CronJob':
            yield obj['spec']['jobTemplate']['spec']['template']['spec']


@pytest.mark.parametrize('node_name', ['fixture-node', 'alternate-offline-node'])
def test_every_pod_stays_on_the_preloaded_arm_node(tmp_path, node_name):
    values = fixture_values()
    values['admission']['nodeName'] = node_name
    result = render(tmp_path, **values)
    assert result.returncode == 0, result.stderr
    objs = [obj for obj in yaml.safe_load_all(result.stdout) if obj]
    pods = list(pod_specs(objs))
    assert len(pods) == 7  # PG, Kafka, four apps, suspended maintenance.
    for pod in pods:
        assert pod.get('nodeSelector') == {
            'kubernetes.io/hostname': node_name, 'kubernetes.io/arch': 'arm64'}
        for container in pod['containers']:
            assert container.get('imagePullPolicy') == 'Never'
    maintenance = next(obj for obj in objs if obj['kind'] == 'CronJob')
    assert maintenance['spec']['suspend'] is True
    assert maintenance['spec']['jobTemplate']['spec']['template']['spec'][
        'containers'][0]['command'] == ['/bin/false']


def test_private_services(tmp_path):
    objs = objects(tmp_path)
    for obj in objs:
        if obj['kind'] == 'Service':
            assert obj['spec']['type'] == 'ClusterIP'
            assert 'externalIPs' not in obj['spec']
        assert obj['kind'] not in ('Ingress', 'Secret', 'LoadBalancer')
    for spec in pod_specs(objs):
        assert not spec.get('hostNetwork', False)
        assert not spec.get('hostPID', False)
        assert spec['automountServiceAccountToken'] is False
        for container in spec['containers']:
            security = container['securityContext']
            assert security['allowPrivilegeEscalation'] is False
            assert security['readOnlyRootFilesystem'] is True
            assert security['capabilities']['drop'] == ['ALL']
            assert '@sha256:' in container['image']
            assert set(container['resources']) == {'limits', 'requests'}
    policies = [x for x in objs if x['kind'] == 'NetworkPolicy']
    deny = next(x for x in policies if x['metadata']['name'].endswith('default-deny'))
    assert deny['spec']['podSelector'] == {}
    assert deny['spec']['policyTypes'] == ['Ingress', 'Egress']
    assert all('0.0.0.0/0' not in json.dumps(x) and '::/0' not in json.dumps(x) for x in policies)


def test_owned_objects_not_duplicated(tmp_path):
    objs = objects(tmp_path)
    keys = [(x['kind'], x['metadata'].get('namespace'), x['metadata']['name']) for x in objs]
    assert len(keys) == len(set(keys))
    assert not any(x['kind'] in ('Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding') for x in objs)


def test_disabled_defaults_and_admission(tmp_path):
    result = render(tmp_path)
    assert result.returncode == 0, result.stderr
    assert list(filter(None, yaml.safe_load_all(result.stdout))) == []
    assert render(tmp_path, enabled=True).returncode != 0
    bad = fixture_values()
    bad['images']['app'] = 'example/app:latest'
    assert render(tmp_path, **bad).returncode != 0
    bad = fixture_values()
    bad['admission']['allocatableMiB'] = 3000
    assert render(tmp_path, **bad).returncode != 0


def test_storage_mounts_and_retention(tmp_path):
    objs = objects(tmp_path)
    pvs = [x for x in objs if x['kind'] == 'PersistentVolume']
    assert pvs and all(x['spec']['persistentVolumeReclaimPolicy'] == 'Retain' for x in pvs)
    assert all(x['spec']['local']['path'].startswith('/srv/ton-sentinel/') for x in pvs)
    assert all('nodeAffinity' in x['spec'] for x in pvs)
    assert sum(int(x['spec']['capacity']['storage'].removesuffix('Gi')) for x in pvs) <= 100
    pg = next(x for x in objs if x['kind'] == 'StatefulSet' and x['metadata']['name'].endswith('postgres'))
    c = pg['spec']['template']['spec']['containers'][0]
    assert {'name': 'PGDATA', 'value': '/var/lib/postgresql/data/pgdata'} in c['env']
    assert any(v['mountPath'] == '/var/lib/postgresql/data' for v in c['volumeMounts'])
    kafka = next(x for x in objs if x['kind'] == 'StatefulSet' and x['metadata']['name'].endswith('kafka'))
    env = {x['name']: x.get('value') for x in kafka['spec']['template']['spec']['containers'][0]['env']}
    assert int(env['KAFKA_LOG_RETENTION_BYTES']) <= 8 * 1024**3
    assert env['KAFKA_LOG_RETENTION_HOURS'] == '24'
    assert env['KAFKA_LOG_SEGMENT_BYTES'] == '134217728'
    cron = next(x for x in objs if x['kind'] == 'CronJob')
    assert cron['spec']['concurrencyPolicy'] == 'Forbid'
    assert cron['spec']['suspend'] is True  # maintenance entrypoint not yet implemented


@pytest.mark.parametrize('mode', ['arm64', 'live-source'])
@pytest.mark.parametrize('authorization', [None, '{}', '{"mode":"arm64","target":"anything","budget":1}'])
def test_qualification_requires_authorization(tmp_path, monkeypatch, mode, authorization):
    calls = []
    def denied(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError('outbound/client/subprocess before admission')
    monkeypatch.setattr(socket, 'socket', denied)
    monkeypatch.setattr(socket, 'create_connection', denied)
    monkeypatch.setattr(subprocess, 'run', denied)
    args = ['qualify', mode]
    if authorization is not None:
        path = tmp_path / 'authorization.json'
        path.write_text(authorization)
        args += ['--authorization', str(path)]
    monkeypatch.setattr(sys, 'argv', args)
    with pytest.raises(SystemExit) as outcome:
        runpy.run_path(str(ROOT / 'scripts/qualify'), run_name='__main__')
    assert outcome.value.code == 2
    assert calls == []


@pytest.mark.parametrize('failed_step', [None, 1, 2, 3])
def test_static_qualification_runs_only_local_checks(monkeypatch, failed_step):
    monkeypatch.setattr(shutil, 'which', lambda name: '/preinstalled/helm')
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        assert kwargs['cwd'] == ROOT
        env = kwargs['env']
        assert env['CHECKPOINT_DISABLE'] == '1'
        assert env['TON_SENTINEL_STATIC_ONLY'] == '1'
        assert not any(key.startswith('TF_CLI_ARGS') for key in env)
        assert 'TF_CLI_CONFIG_FILE' not in env
        assert 'TF_IN_AUTOMATION' not in env
        assert command[0] != 'terraform'
        return subprocess.CompletedProcess(command, 7 if len(calls) == failed_step else 0)

    monkeypatch.setattr(subprocess, 'run', run)
    monkeypatch.setattr(socket, 'socket', lambda *a, **k: pytest.fail('network client'))
    monkeypatch.setattr(sys, 'argv', ['qualify', 'static'])
    with pytest.raises(SystemExit) as outcome:
        runpy.run_path(str(ROOT / 'scripts/qualify'), run_name='__main__')
    assert outcome.value.code == (7 if failed_step else 0)
    if failed_step:
        assert len(calls) == failed_step
    else:
        assert calls == [
            ['/preinstalled/helm', 'lint', 'deploy/helm/ton-sentinel', '--strict'],
            ['/preinstalled/helm', 'template', 'sentinel', 'deploy/helm/ton-sentinel'],
            [sys.executable, '-m', 'pytest', '-q', 'tests/unit/test_deployment_policy.py'],
            [sys.executable, '-m', 'pytest', '-q'],
        ]
        assert all('apply' not in command and 'plan' not in command for command in calls)



def test_static_qualification_has_no_cloud_infrastructure_option(monkeypatch, capsys):
    """Static mode is local-only: no Terraform step or cloud-specific opt-in flag exists."""
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: pytest.fail('subprocess for --help'))
    monkeypatch.setattr(sys, 'argv', ['qualify', '--help'])
    with pytest.raises(SystemExit) as outcome:
        runpy.run_path(str(ROOT / 'scripts/qualify'), run_name='__main__')
    assert outcome.value.code == 0
    options = set(re.findall(r'(?m)^\s+(?:-h, )?(--[a-z-]+)', capsys.readouterr().out))
    assert options == {'--help', '--authorization', '--decision-out', '--inputs',
                       '--target', '--build', '--config', '--workload'}
    assert not (ROOT / 'infra').exists()
    assert 'terraform' not in (ROOT / 'scripts/qualify').read_text().lower()


@pytest.mark.parametrize('component,files,passwords', [
    ('postgres', {'postgres.crt', 'postgres.key'}, {'postgres-password'}),
    ('kafka', {'kafka.keystore.jks', 'kafka.truststore.jks'},
     {'kafka-keystore-password', 'kafka-truststore-password'}),
])
def test_review_r1_exact_secret_projection(tmp_path, component, files, passwords):
    objs = objects(tmp_path)
    obj = next(x for x in objs if x['kind'] == 'StatefulSet'
               and x['metadata']['name'].endswith(component))
    spec = obj['spec']['template']['spec']
    secret = next(v['secret'] for v in spec['volumes'] if v['name'] == 'tls')
    assert secret['items'] == [{'key': key, 'path': key} for key in sorted(files)]
    assert secret['defaultMode'] == 0o440
    unrelated = {'postgres.crt', 'postgres.key', 'kafka.keystore.jks',
                 'kafka.truststore.jks', 'postgres-password', 'database-dsn',
                 'source-api-key', 'kafka-keystore-password', 'kafka-truststore-password'} - files
    projected = {item['key'] for volume in spec['volumes'] if 'secret' in volume
                 for item in volume['secret']['items']}
    assert projected == files
    assert projected.isdisjoint(unrelated)
    refs = {env['valueFrom']['secretKeyRef']['key']
            for c in spec['containers'] for env in c['env'] if 'valueFrom' in env}
    assert refs == passwords
    assert refs.isdisjoint(unrelated - passwords)
    assert not any('envFrom' in c for c in spec['containers'])
    assert all(x['kind'] != 'Secret' for x in objs)


@pytest.mark.parametrize('skip_schema', [False, True])
def test_review_r2_no_runtime_replica_escape(tmp_path, skip_schema):
    import shutil
    chart = tmp_path / 'chart'
    shutil.copytree(CHART, chart)
    if skip_schema:
        (chart / 'values.schema.json').unlink()
    values = fixture_values() | {'replicas': 1}
    path = tmp_path / 'active.yaml'
    path.write_text(yaml.safe_dump(values))
    result = subprocess.run([HELM, 'template', 'held', str(chart), '-f', str(path)],
                            capture_output=True, text=True)
    assert result.returncode != 0
    if skip_schema:
        assert 'BLOCKED: offline storage enforcement hold' in result.stderr
    else:
        assert "at '/replicas': maximum: got 1, want 0" in result.stderr


def test_review_r2_storage_design_and_render_consistency(tmp_path):
    assert (CHART / 'storage-budget.json').is_file(), 'Missing enforceable storage contract'
    policy = json.loads((CHART / 'storage-budget.json').read_text())
    assert policy['mode'] == 'offline-hold-separate-filesystems-required'
    assert policy['dataVolumeBytes'] == 150_000_000_000
    assert policy['reserveBytes'] >= 16 * 1024**3
    writers = policy['writersGiB']
    assert set(writers) == {'postgres', 'kafka', 'checkpoints', 'scratch', 'metrics', 'k3s'}
    assert sum(writers.values()) * 1024**3 + policy['reserveBytes'] <= policy['dataVolumeBytes']
    kafka = policy['kafka']
    assert {t['name'] for t in kafka['topics']} == {
        'ton.pool_state.v1', 'ton.swap_events.v1', '__consumer_offsets', '__transaction_state'}
    assert all(t['partitions'] == 1 for t in kafka['topics'])
    aggregate = sum(t['partitions'] * (t['budgetPerPartitionBytes'] +
                    kafka['segmentBytes'] * kafka['overshootSegmentsPerPartition'])
                    for t in kafka['topics']) + kafka['metadataReserveBytes']
    assert aggregate <= writers['kafka'] * 1024**3
    objs = objects(tmp_path)
    for pv in (x for x in objs if x['kind'] == 'PersistentVolume'):
        writer = pv['spec']['local']['path'].split('/')[-1]
        assert pv['spec']['capacity']['storage'] == f'{writers[writer]}Gi'
    obj = next(x for x in objs if x['kind'] == 'StatefulSet'
               and x['metadata']['name'].endswith('kafka'))
    env = {e['name']: e.get('value') for e in obj['spec']['template']['spec']['containers'][0]['env']}
    assert env['KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS'] == '1'
    assert env['KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS'] == '1'
    assert int(env['KAFKA_LOG_SEGMENT_BYTES']) == kafka['segmentBytes']
    assert int(env['KAFKA_LOG_RETENTION_BYTES']) == kafka['topics'][0]['budgetPerPartitionBytes']
    assert all(x['spec']['replicas'] == 0 for x in objs if x['kind'] in ('StatefulSet', 'Deployment'))


def test_review_r2_retention_is_independent_of_topic_order(tmp_path):
    import shutil
    chart = tmp_path / 'chart'
    shutil.copytree(CHART, chart)
    path = chart / 'storage-budget.json'
    policy = json.loads(path.read_text())
    policy['kafka']['topics'].reverse()
    path.write_text(json.dumps(policy))
    values_path = tmp_path / 'fixture.yaml'
    values_path.write_text(yaml.safe_dump(fixture_values()))
    result = subprocess.run([HELM, 'template', 'held', str(chart), '-f', str(values_path)],
                            capture_output=True, text=True, check=True)
    obj = next(x for x in yaml.safe_load_all(result.stdout)
               if x and x['kind'] == 'StatefulSet' and x['metadata']['name'].endswith('kafka'))
    env = {e['name']: e.get('value') for e in obj['spec']['template']['spec']['containers'][0]['env']}
    assert int(env['KAFKA_LOG_RETENTION_BYTES']) == 2 * 1024**3


@pytest.mark.parametrize('violation', ['kafka', 'disk'])
def test_review_r2_rejects_overcommitted_storage(tmp_path, violation):
    import shutil
    chart = tmp_path / 'chart'
    shutil.copytree(CHART, chart)
    path = chart / 'storage-budget.json'
    policy = json.loads(path.read_text())
    if violation == 'kafka':
        policy['kafka']['topics'][0]['partitions'] = 100
    else:
        policy['writersGiB']['k3s'] = 150
    path.write_text(json.dumps(policy))
    values_path = tmp_path / 'fixture.yaml'
    values_path.write_text(yaml.safe_dump(fixture_values()))
    result = subprocess.run([HELM, 'template', 'held', str(chart), '-f', str(values_path)],
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert ('Kafka aggregate' if violation == 'kafka' else 'shared disk budget') in result.stderr


@pytest.mark.parametrize('violation', ['segment-overhead', 'metadata-overhead', 'reserve',
                                     'extra-partition', 'layout-drift', 'missing-policy'])
def test_review_r2_rejects_budget_and_enforcement_drift(tmp_path, violation):
    import shutil
    chart = tmp_path / 'chart'
    shutil.copytree(CHART, chart)
    path = chart / 'storage-budget.json'
    assert path.is_file(), 'Missing storage contract'
    policy = json.loads(path.read_text())
    if violation == 'segment-overhead':
        policy['kafka']['overshootSegmentsPerPartition'] = 100
    elif violation == 'metadata-overhead':
        policy['kafka']['metadataReserveBytes'] = 20 * 1024**3
    elif violation == 'reserve':
        policy['reserveBytes'] = 150_000_000_000
    elif violation == 'extra-partition':
        policy['kafka']['topics'][0]['partitions'] = 2
    elif violation == 'layout-drift':
        policy['writersGiB']['postgres'] = 39
    if violation == 'missing-policy':
        path.unlink()
    else:
        path.write_text(json.dumps(policy))
    values_path = tmp_path / 'fixture.yaml'
    values_path.write_text(yaml.safe_dump(fixture_values()))
    # The template admission must hold even when schema validation is unavailable.
    (chart / 'values.schema.json').unlink()
    result = subprocess.run([HELM, 'template', 'held', str(chart), '-f', str(values_path)],
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert 'BLOCKED:' in result.stderr
