"""Executable Helm ownership, resource and security policy (offline, real `helm`)."""

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from test_deployment_policy import CHART, HELM, fixture_values, objects, pod_specs, render  # noqa: E402

LOCAL = CHART.parent / 'local' / 'values-local-k3s.yaml'


def helm(*args):
    return subprocess.run([HELM, *args], capture_output=True, text=True, check=False)


def mem_mi(text):
    m = re.fullmatch(r'(\d+)(Mi|Gi)', text)
    return int(m[1]) * (1024 if m[2] == 'Gi' else 1)


def cpu_m(text):
    return int(text[:-1]) if text.endswith('m') else int(float(text) * 1000)


def containers(objs):
    return [c for s in pod_specs(objs) for c in s['containers'] + s.get('initContainers', [])]


def test_helm_lint_strict_default_and_local_values():
    default = helm('lint', '--strict', str(CHART))
    assert default.returncode == 0, default.stdout + default.stderr
    local = helm('lint', '--strict', str(CHART), '-f', str(LOCAL))
    assert local.returncode == 0, local.stdout + local.stderr


def test_default_values_keep_security_defaults():
    values = yaml.safe_load((CHART / 'values.yaml').read_text())
    assert values['enabled'] is False and values['replicas'] == 0
    assert values['local']['enabled'] is False
    assert all(v == '' for v in values['images'].values())
    assert values['existingSecret'] == ''


def test_local_overrides_live_only_in_local_values():
    local = yaml.safe_load(LOCAL.read_text())
    assert local['local']['enabled'] is True
    # local file may relax only arch/storageClass/admission inputs; never security knobs
    forbidden = {'securityContext', 'privileged', 'hostNetwork', 'hostPath', 'rbac', 'service'}
    assert not forbidden & set(json.dumps(local).replace('"', ' ').split())
    result = helm('template', 'sl', str(CHART), '-f', str(LOCAL))
    assert result.returncode == 0, result.stderr
    objs = [o for o in yaml.safe_load_all(result.stdout) if o]
    for c in containers(objs):
        sec = c['securityContext']
        assert sec['allowPrivilegeEscalation'] is False and not sec.get('privileged')
        assert sec['capabilities']['drop'] == ['ALL']


def test_helm_owns_namespace_workloads_services_pvc_config_jobs(tmp_path):
    kinds = {o['kind'] for o in objects(tmp_path)}
    assert {'Namespace', 'Deployment', 'StatefulSet', 'Service', 'PersistentVolumeClaim',
            'CronJob', 'NetworkPolicy', 'ResourceQuota', 'ServiceAccount'} <= kinds
    objs = objects(tmp_path)
    ns = next(o['metadata']['name'] for o in objs if o['kind'] == 'Namespace')
    cluster = {'Namespace', 'PersistentVolume'}
    for o in objs:
        if o['kind'] not in cluster:
            assert o['metadata']['namespace'] == ns, o['metadata']['name']


def test_namespace_enforces_restricted_pod_security(tmp_path):
    ns = next(o for o in objects(tmp_path) if o['kind'] == 'Namespace')
    assert ns['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'


def test_least_privilege_rbac_and_secret_separation(tmp_path):
    objs = objects(tmp_path)
    assert not any(o['kind'] in ('Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding') for o in objs)
    for o in objs:
        if o['kind'] == 'ServiceAccount':
            assert o['automountServiceAccountToken'] is False
        assert o['kind'] != 'Secret'
    text = json.dumps(objs)
    assert 'fixture-existing-secret' in text
    for c in containers(objs):
        for env in c.get('env', []):
            if re.search(r'PASSWORD|SECRET|TOKEN|_KEY$', env['name']):
                assert 'value' not in env, env['name']
                assert env['valueFrom']['secretKeyRef']['name'] == 'fixture-existing-secret'


def test_every_container_unprivileged_and_pod_hardened(tmp_path):
    for spec in pod_specs(objects(tmp_path)):
        assert spec['securityContext']['runAsNonRoot'] is True
        assert spec['securityContext']['runAsUser'] != 0
        assert spec['securityContext']['seccompProfile']['type'] == 'RuntimeDefault'
        assert not spec.get('hostIPC') and not spec.get('hostNetwork') and not spec.get('hostPID')
        assert not any('hostPath' in v for v in spec.get('volumes', []))
        for c in spec['containers']:
            sec = c['securityContext']
            assert not sec.get('privileged')
            assert sec['allowPrivilegeEscalation'] is False and sec['readOnlyRootFilesystem'] is True
            assert all('hostPort' not in p for p in c.get('ports', []))


def test_every_container_has_bounded_cpu_memory_ephemeral(tmp_path):
    for c in containers(objects(tmp_path)):
        res = c['resources']
        for kind in ('requests', 'limits'):
            assert {'cpu', 'memory', 'ephemeral-storage'} <= set(res[kind]), (c['name'], kind)
        assert mem_mi(res['requests']['memory']) <= mem_mi(res['limits']['memory'])
        assert cpu_m(res['requests']['cpu']) <= cpu_m(res['limits']['cpu'])


def test_resource_quota_covers_workload_totals_and_storage_budget(tmp_path):
    objs = objects(tmp_path)
    quota = next(o for o in objs if o['kind'] == 'ResourceQuota')['spec']['hard']
    cs = containers(objs)
    # steady-state pods only (suspended CronJob is admitted too, so sum all pods as worst case)
    assert sum(mem_mi(c['resources']['limits']['memory']) for c in cs) <= mem_mi(quota['limits.memory'])
    assert sum(mem_mi(c['resources']['requests']['memory']) for c in cs) <= mem_mi(quota['requests.memory'])
    assert sum(cpu_m(c['resources']['limits']['cpu']) for c in cs) <= cpu_m(quota['limits.cpu'])
    assert sum(cpu_m(c['resources']['requests']['cpu']) for c in cs) <= cpu_m(quota['requests.cpu'])
    pvcs = [o for o in objs if o['kind'] == 'PersistentVolumeClaim']
    assert int(quota['persistentvolumeclaims']) == len(pvcs)
    req = sum(mem_mi(p['spec']['resources']['requests']['storage']) for p in pvcs)
    assert req <= mem_mi(quota['requests.storage'])
    assert req <= 100 * 1024
    assert quota['services.loadbalancers'] == '0' and quota['services.nodeports'] == '0'
    budget = json.loads((CHART / 'storage-budget.json').read_text())
    assert req * 1024**2 <= budget['dataVolumeBytes'] - budget['reserveBytes']


def test_namespace_memory_quota_is_local_6gb_budget(tmp_path):
    quota = next(o for o in objects(tmp_path) if o['kind'] == 'ResourceQuota')['spec']['hard']
    assert quota['limits.memory'] == '6144Mi'
    assert mem_mi(quota['limits.memory']) == 6 * 1024
    assert '7680' not in (CHART / 'templates' / '_helpers.tpl').read_text()


def test_total_limits_fit_measured_allocatable_minus_reserve(tmp_path):
    objs = objects(tmp_path)
    values = fixture_values()['admission']
    room = values['allocatableMiB'] - values['osReserveMiB']
    steady = [c for s in pod_specs([o for o in objs if o['kind'] != 'CronJob']) for c in s['containers']]
    assert sum(mem_mi(c['resources']['requests']['memory']) for c in steady) <= room


def test_log_and_retention_bounds_are_fail_closed(tmp_path):
    objs = objects(tmp_path)
    kafka = next(o for o in objs if o['kind'] == 'StatefulSet' and o['metadata']['name'].endswith('kafka'))
    env = {e['name']: e.get('value') for e in kafka['spec']['template']['spec']['containers'][0]['env']}
    assert int(env['KAFKA_LOG_RETENTION_BYTES']) <= 8 * 1024**3
    assert env['KAFKA_LOG_RETENTION_HOURS'] == '24'
    over = fixture_values()
    over['admission']['allocatableMiB'] = 3000
    assert render(tmp_path, **over).returncode != 0


def test_no_public_exposure_or_unbounded_egress(tmp_path):
    objs = objects(tmp_path)
    assert not any(o['kind'] in ('Ingress', 'HTTPRoute', 'IngressRoute') for o in objs)
    for o in objs:
        if o['kind'] == 'Service':
            assert o['spec']['type'] == 'ClusterIP'
    for o in objs:
        if o['kind'] == 'NetworkPolicy':
            for rule in o['spec'].get('egress', []) + o['spec'].get('ingress', []):
                for peer in rule.get('to', []) + rule.get('from', []):
                    assert 'ipBlock' not in peer or peer['ipBlock']['cidr'] not in ('0.0.0.0/0', '::/0')


def test_every_image_is_digest_pinned_and_never_pulled(tmp_path):
    for c in containers(objects(tmp_path)):
        assert re.search(r'@sha256:[0-9a-f]{64}$', c['image'])
        assert c['imagePullPolicy'] == 'Never'


@pytest.mark.parametrize('field', ['app', 'kafka', 'postgres', 'spark'])
def test_unpinned_image_is_rejected(tmp_path, field):
    bad = fixture_values()
    bad['images'][field] = 'docker.io/library/x:latest'
    assert render(tmp_path, **bad).returncode != 0
