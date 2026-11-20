"""Real K3s PVC/restart/resource acceptance."""

import json
import os
import secrets
import shutil
import subprocess
import time

import pytest

NS = "sl-pvc-probe"
IMAGE = "postgres:18.6"


def kc(*args, check=True, input=None):
    kubeconfig = os.environ.get("TON_SENTINEL_K3S_KUBECONFIG")
    if not kubeconfig or not shutil.which("kubectl"):
        pytest.fail("BLOCKED: set TON_SENTINEL_K3S_KUBECONFIG to the local k3d kubeconfig and install kubectl")
    r = subprocess.run(["kubectl", "--kubeconfig", kubeconfig, *args], capture_output=True, text=True, input=input, timeout=240)
    if check and r.returncode:
        raise AssertionError(f"kubectl {args}: {r.stderr}")
    return r


def wait_ready(timeout=180):
    kc("-n", NS, "wait", "--for=condition=Ready", "pod/pg-0", f"--timeout={timeout}s")


def psql(sql):
    return kc("-n", NS, "exec", "pg-0", "--", "psql", "-U", "sentinel", "-d", "sentinel", "-Atc", sql).stdout.strip()


def manifest():
    return json.dumps({"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {"name": "q", "namespace": NS},
         "spec": {"hard": {"pods": "2", "limits.memory": "2Gi", "requests.storage": "2Gi", "persistentvolumeclaims": "1"}}},
        {"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": {"name": "pg", "namespace": NS},
         "spec": {"replicas": 1, "serviceName": "pg", "selector": {"matchLabels": {"app": "pg"}},
                  "template": {"metadata": {"labels": {"app": "pg"}}, "spec": {
                      "terminationGracePeriodSeconds": 30,
                      "securityContext": {"runAsNonRoot": True, "runAsUser": 999, "runAsGroup": 999, "fsGroup": 999,
                                          "seccompProfile": {"type": "RuntimeDefault"}},
                      "containers": [{"name": "pg", "image": IMAGE, "imagePullPolicy": "Never",
                                      "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                                          "capabilities": {"drop": ["ALL"]}},
                                      "env": [{"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
                                              {"name": "POSTGRES_DB", "value": "sentinel"},
                                              {"name": "POSTGRES_USER", "value": "sentinel"},
                                              {"name": "POSTGRES_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "pw", "key": "p"}}}],
                                      "args": ["-c", "shared_buffers=128MB", "-c", "max_connections=30"],
                                      "readinessProbe": {"exec": {"command": ["pg_isready", "-h", "127.0.0.1", "-U", "sentinel"]},
                                                         "periodSeconds": 2, "failureThreshold": 60},
                                      "resources": {"requests": {"cpu": "300m", "memory": "1024Mi", "ephemeral-storage": "256Mi"},
                                                    "limits": {"cpu": "1", "memory": "1536Mi", "ephemeral-storage": "512Mi"}},
                                      "volumeMounts": [{"name": "data", "mountPath": "/var/lib/postgresql/data"},
                                                       {"name": "tmp", "mountPath": "/tmp"},
                                                       {"name": "run", "mountPath": "/var/run/postgresql"}]}],
                      "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}},
                                  {"name": "run", "emptyDir": {"sizeLimit": "16Mi"}}]}},
                  "volumeClaimTemplates": [{"metadata": {"name": "data"}, "spec": {
                      "accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}}]}},
    ]})


@pytest.mark.k3s
def test_k3s_pvc_restarts():
    ctx = kc("config", "current-context").stdout.strip()
    assert ctx.startswith("k3d-ton-sentinel-local"), ctx
    assert "k3s" in kc("version", "-o", "json").stdout, "server is not K3s"
    kc("delete", "ns", NS, "--ignore-not-found", "--wait=true")
    kc("create", "ns", NS)
    try:
        kc("-n", NS, "create", "secret", "generic", "pw", f"--from-literal=p={secrets.token_hex(16)}")
        kc("apply", "-f", "-", input=manifest())
        time.sleep(3)
        wait_ready()
        psql("create table t(v text); insert into t values ('before-restart');")
        uid_before = kc("-n", NS, "get", "pod", "pg-0", "-o", "jsonpath={.metadata.uid}").stdout
        pvc = json.loads(kc("-n", NS, "get", "pvc", "data-pg-0", "-o", "json").stdout)
        assert pvc["status"]["phase"] == "Bound" and pvc["spec"]["resources"]["requests"]["storage"] == "1Gi"
        kc("-n", NS, "delete", "pod", "pg-0", "--wait=true")
        time.sleep(3)
        wait_ready()
        assert kc("-n", NS, "get", "pod", "pg-0", "-o", "jsonpath={.metadata.uid}").stdout != uid_before
        assert psql("select v from t") == "before-restart"
        kc("-n", NS, "scale", "sts/pg", "--replicas=0")
        kc("-n", NS, "wait", "--for=delete", "pod/pg-0", "--timeout=120s")
        assert kc("-n", NS, "get", "pvc", "data-pg-0", "-o", "jsonpath={.status.phase}").stdout == "Bound"
        kc("-n", NS, "scale", "sts/pg", "--replicas=1")
        time.sleep(3)
        wait_ready()
        assert psql("select v from t") == "before-restart"
        pod = json.loads(kc("-n", NS, "get", "pod", "pg-0", "-o", "json").stdout)
        assert pod["spec"]["containers"][0]["resources"]["limits"]["memory"] == "1536Mi"
        assert pod["status"]["qosClass"] == "Burstable"
        over = kc("-n", NS, "run", "over", "--image", IMAGE, "--image-pull-policy=Never", "--restart=Never",
                  "--overrides", json.dumps({"spec": {"containers": [{"name": "over", "image": IMAGE, "imagePullPolicy": "Never",
                                              "resources": {"limits": {"memory": "1Gi"}}}]}}), check=False)
        assert over.returncode != 0 and "exceeded quota" in over.stderr, over.stderr
        pvc2 = kc("-n", NS, "apply", "-f", "-", check=False, input=json.dumps({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "extra"},
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}}))
        assert pvc2.returncode != 0 and "exceeded quota" in pvc2.stderr, pvc2.stderr
    finally:
        if not os.environ.get("TON_SENTINEL_K3S_KEEP"):
            kc("delete", "ns", NS, "--ignore-not-found", "--wait=false", check=False)


# Uses deploy/helm/local/values-local-k3s.yaml (LOCAL ONLY: amd64 selector, local-path
# storage, replicas lift for postgres+kafka). Secrets are generated here and never committed.
import pathlib
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy/helm/ton-sentinel"
LOCAL_VALUES = CHART.parent / "local" / "values-local-k3s.yaml"
REL, CHART_NS, SECRET = "sl", "sl-ton-sentinel", "sl-secret"
NODE = "k3d-ton-sentinel-local-server-0"
# ref -> (containerd name, linux/amd64 manifest digest) as imported from the local docker store.
LOCAL_IMAGES = {
    "postgres:18.6": ("docker.io/library/postgres", "sha256:e923a22ac074261dd73970c57edf659475bf0497cb9456f17d98ed7cdd3ca913"),
    "apache/kafka:4.3.2": ("docker.io/apache/kafka", "sha256:8f325fc00143fed78eabe00b033a86747078bb06edc21345d06651f77ae44f30"),
}
APP_SPARK_IMAGES = {
    "ton-sentinel:local": ("docker.io/library/ton-sentinel", "sha256:8b1759daf094bd768b25108ae78e3e3bbc96a3706d542ea092c6620c5e68e7b2"),
    "spark:4.0.5-scala2.13-java21-python3-ubuntu": ("docker.io/library/spark", "sha256:60b99fa37465eac7749f2bad2ab90eb4b52dc04584a8b1b163f10926c82a7045"),
}


def run(cmd, check=True, input=None, timeout=600):
    r = subprocess.run(cmd, capture_output=True, text=True, input=input, timeout=timeout)
    if check and r.returncode:
        raise AssertionError(f"{cmd}: {r.stderr[-2000:]}")
    return r


def helm(*args, check=True):
    return run([shutil.which("helm") or os.path.expanduser("~/.local/bin/helm"),
                "--kubeconfig", os.environ["TON_SENTINEL_K3S_KUBECONFIG"], *args], check=check)


def load_images(images=None):
    """docker save (amd64) -> ctr import into k3d node containerd; only locally present pinned images."""
    have = run(["docker", "exec", NODE, "ctr", "-n", "k8s.io", "images", "ls", "-q"]).stdout
    for ref, (cname, digest) in (images or LOCAL_IMAGES).items():
        if f"{cname}@{digest}" in have:
            continue
        with tempfile.TemporaryDirectory() as d:
            tar = f"{d}/i.tar"
            run(["docker", "save", "--platform", "linux/amd64", ref, "-o", tar])
            run(["docker", "cp", tar, f"{NODE}:/tmp/i.tar"])
            run(["docker", "exec", NODE, "ctr", "-n", "k8s.io", "image", "import", "/tmp/i.tar"])
            # Pods reference name@digest with imagePullPolicy Never; give containerd that exact name.
            run(["docker", "exec", NODE, "ctr", "-n", "k8s.io", "images", "tag", "--force",
                 f"{cname}:{ref.rsplit(':', 1)[1]}", f"{cname}@{digest}"])
            run(["docker", "exec", NODE, "rm", "-f", "/tmp/i.tar"])


def make_secret(d):
    d = pathlib.Path(d)
    pw = secrets.token_hex(12)
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=sl-postgres",
         "-keyout", str(d / "pg.key"), "-out", str(d / "pg.crt")])
    for store in ("kafka.keystore.jks", "kafka.truststore.jks"):
        (d / store).unlink(missing_ok=True)
    run(["keytool", "-genkeypair", "-alias", "k", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
         "-dname", "CN=sl-ton-sentinel-kafka", "-ext", "SAN=DNS:sl-ton-sentinel-kafka,DNS:localhost",
         "-keystore", str(d / "kafka.keystore.jks"), "-storepass", pw, "-keypass", pw, "-storetype", "JKS"])
    run(["keytool", "-exportcert", "-alias", "k", "-keystore", str(d / "kafka.keystore.jks"), "-storepass", pw,
         "-file", str(d / "k.crt")])
    run(["keytool", "-importcert", "-noprompt", "-alias", "k", "-file", str(d / "k.crt"),
         "-keystore", str(d / "kafka.truststore.jks"), "-storepass", pw, "-storetype", "JKS"])
    # Broker requires client auth and trusts only its own self-signed cert: reuse it as the local client identity.
    run(["keytool", "-exportcert", "-rfc", "-alias", "k", "-keystore", str(d / "kafka.keystore.jks"),
         "-storepass", pw, "-file", str(d / "kafka-client.crt")])
    run(["keytool", "-importkeystore", "-noprompt", "-srckeystore", str(d / "kafka.keystore.jks"), "-srcstorepass", pw,
         "-destkeystore", str(d / "c.p12"), "-deststoretype", "PKCS12", "-deststorepass", pw, "-srcalias", "k"])
    run(["openssl", "pkcs12", "-in", str(d / "c.p12"), "-nodes", "-nocerts", "-passin", f"pass:{pw}",
         "-out", str(d / "kafka-client.key")])
    shutil.copy(d / "kafka-client.crt", d / "kafka-ca.crt")
    return ["create", "secret", "generic", SECRET, "-n", CHART_NS,
            f"--from-file=kafka-client.crt={d/'kafka-client.crt'}", f"--from-file=kafka-client.key={d/'kafka-client.key'}",
            f"--from-file=kafka-ca.crt={d/'kafka-ca.crt'}",
            f"--from-file=postgres.crt={d/'pg.crt'}", f"--from-file=postgres.key={d/'pg.key'}",
            f"--from-file=kafka.keystore.jks={d/'kafka.keystore.jks'}",
            f"--from-file=kafka.truststore.jks={d/'kafka.truststore.jks'}",
            f"--from-literal=kafka-keystore-password={pw}", f"--from-literal=kafka-truststore-password={pw}",
            f"--from-literal=postgres-password={pw}", f"--from-literal=kafka-credentials={pw}",
            f"--from-literal=database-dsn=postgresql://sentinel:{pw}@sl-ton-sentinel-postgres:5432/sentinel",
            "--from-literal=source-api-key=local-unused"]


def restricted_pod(name, image, limits_mem, mounts=()):
    c = {"name": name, "image": image, "imagePullPolicy": "Never", "command": ["sleep", "300"],
         "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
         "resources": {"requests": {"cpu": "10m", "memory": "16Mi", "ephemeral-storage": "8Mi"},
                       "limits": {"cpu": "100m", "memory": limits_mem, "ephemeral-storage": "16Mi"}},
         "volumeMounts": [{"name": m, "mountPath": f"/mnt/{m}"} for m in mounts]}
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": CHART_NS},
            "spec": {"restartPolicy": "Never", "nodeSelector": {"kubernetes.io/arch": "amd64"},
                     "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "fsGroup": 10001,
                                         "seccompProfile": {"type": "RuntimeDefault"}},
                     "containers": [c],
                     "volumes": [{"name": m, "persistentVolumeClaim": {"claimName": f"{REL}-ton-sentinel-{m}"}}
                                 for m in mounts]}}


def pg_exec(sql):
    return kc("-n", CHART_NS, "exec", f"{REL}-ton-sentinel-postgres-0", "--", "psql", "-U", "sentinel",
              "-d", "sentinel", "-Atc", sql).stdout.strip()


@pytest.mark.k3s
def test_chart_workloads_ready_with_local_values():
    assert LOCAL_VALUES.exists(), "BLOCKED: missing local-only values file"
    ctx = kc("config", "current-context").stdout.strip()
    assert ctx.startswith("k3d-ton-sentinel-local"), ctx
    load_images()
    helm("uninstall", REL, "-n", CHART_NS, check=False)
    kc("delete", "pv", "-l", f"app.kubernetes.io/instance={REL}", "--ignore-not-found", check=False)
    kc("delete", "ns", CHART_NS, "--ignore-not-found", "--wait=true")
    with tempfile.TemporaryDirectory() as d:
        helm("install", REL, str(CHART), "-f", str(LOCAL_VALUES), "-n", CHART_NS, "--create-namespace")
        kc(*make_secret(d))
    sts = [f"{REL}-ton-sentinel-postgres", f"{REL}-ton-sentinel-kafka"]
    for s in sts:
        kc("-n", CHART_NS, "rollout", "status", f"sts/{s}", "--timeout=300s")
        pod = json.loads(kc("-n", CHART_NS, "get", "pod", f"{s}-0", "-o", "json").stdout)
        assert any(c["type"] == "Ready" and c["status"] == "True" for c in pod["status"]["conditions"]), s
        pvc = json.loads(kc("-n", CHART_NS, "get", "pvc", s, "-o", "json").stdout)
        assert pvc["status"]["phase"] == "Bound" and pvc["spec"]["storageClassName"] == "local-path"
    # Remaining chart PVCs bind once a consumer is scheduled (WaitForFirstConsumer).
    pod = restricted_pod("pvc-binder", "rancher/mirrored-library-busybox:1.36.1", "32Mi",
                         ("checkpoints", "scratch", "metrics"))
    kc("apply", "-f", "-", input=json.dumps(pod))
    kc("-n", CHART_NS, "wait", "--for=condition=Ready", "pod/pvc-binder", "--timeout=120s")
    for n in ("checkpoints", "scratch", "metrics"):
        assert kc("-n", CHART_NS, "get", "pvc", f"{REL}-ton-sentinel-{n}", "-o", "jsonpath={.status.phase}").stdout == "Bound"
    kc("-n", CHART_NS, "delete", "pod", "pvc-binder", "--wait=true")
    pg_exec("create table t(v text); insert into t values ('before-restart');")
    before = kc("-n", CHART_NS, "get", "pod", f"{sts[0]}-0", "-o", "jsonpath={.metadata.uid}").stdout
    kc("-n", CHART_NS, "delete", "pod", f"{sts[0]}-0", "--wait=true")
    kc("-n", CHART_NS, "rollout", "status", f"sts/{sts[0]}", "--timeout=300s")
    assert kc("-n", CHART_NS, "get", "pod", f"{sts[0]}-0", "-o", "jsonpath={.metadata.uid}").stdout != before
    assert pg_exec("select v from t") == "before-restart"
    over = kc("apply", "-f", "-", check=False, input=json.dumps(
        restricted_pod("over", "rancher/mirrored-library-busybox:1.36.1", "6Gi")))
    assert over.returncode != 0 and "exceeded quota" in over.stderr, over.stderr
    pvc2 = kc("-n", CHART_NS, "apply", "-f", "-", check=False, input=json.dumps({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "extra"},
        "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}}))
    assert pvc2.returncode != 0 and "exceeded quota" in pvc2.stderr, pvc2.stderr
    # Release is the local-values release (default values.yaml is untouched, asserted by unit tests).
    assert json.loads(helm("get", "values", REL, "-n", CHART_NS, "-o", "json").stdout)["replicas"] == 1


def code_configmap_args():
    """Source bundle for the local-only probe entrypoints (the pinned app image does not contain src/)."""
    import io
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(ROOT / "src/ton_sentinel", arcname="src/ton_sentinel",
                filter=lambda ti: None if "__pycache__" in ti.name or ti.name.endswith(".pyc") else ti)
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "src.tgz").write_bytes(buf.getvalue())
    return ["create", "configmap", f"{REL}-ton-sentinel-code", "-n", CHART_NS, f"--from-file=src.tgz={d/'src.tgz'}"]


def probe_result(deploy, role):
    logs = kc("-n", CHART_NS, "logs", f"deploy/{REL}-ton-sentinel-{deploy}", "--tail=200").stdout
    lines = [l for l in logs.splitlines() if l.startswith(f"PROBE_OK role={role} ")]
    assert lines, f"no PROBE_OK for {role}; logs tail:\n{logs[-1500:]}"
    return json.loads(lines[-1].split(" ", 2)[2])


@pytest.mark.k3s
def test_app_and_spark_workloads_run_bounded_roundtrips():
    from ton_sentinel import k3s_probe
    assert LOCAL_VALUES.exists(), "BLOCKED: missing local-only values file"
    ctx = kc("config", "current-context").stdout.strip()
    assert ctx.startswith("k3d-ton-sentinel-local"), ctx
    load_images()
    load_images(APP_SPARK_IMAGES)
    helm("uninstall", REL, "-n", CHART_NS, check=False)
    kc("delete", "pv", "-l", f"app.kubernetes.io/instance={REL}", "--ignore-not-found", check=False)
    kc("delete", "ns", CHART_NS, "--ignore-not-found", "--wait=true")
    with tempfile.TemporaryDirectory() as d:
        helm("install", REL, str(CHART), "-f", str(LOCAL_VALUES), "-n", CHART_NS, "--create-namespace")
        kc(*make_secret(d))
        kc(*code_configmap_args())
    for s in (f"{REL}-ton-sentinel-postgres", f"{REL}-ton-sentinel-kafka"):
        kc("-n", CHART_NS, "rollout", "status", f"sts/{s}", "--timeout=300s")
    for name in ("ingestor", "api", "notifier", "spark"):
        kc("-n", CHART_NS, "rollout", "status", f"deploy/{REL}-ton-sentinel-{name}", "--timeout=420s")
        pods = json.loads(kc("-n", CHART_NS, "get", "pod", "-l", f"app.kubernetes.io/component={name}", "-o", "json").stdout)["items"]
        assert len(pods) == 1
        assert any(c["type"] == "Ready" and c["status"] == "True" for c in pods[0]["status"]["conditions"]), name
        cs = pods[0]["spec"]["containers"][0]
        assert cs["command"][0] != "/bin/false"
        assert cs["securityContext"]["readOnlyRootFilesystem"] is True
        assert cs["securityContext"]["capabilities"]["drop"] == ["ALL"]
    by_token = lambda rows: sorted(rows, key=lambda r: r["token"])
    assert by_token(probe_result("ingestor", "ingestor")["features"]) == by_token(k3s_probe.sample_features())
    assert sorted(f["token"] for f in probe_result("api", "api")["features"]) == ["TON", "jetton:A"]
    assert pg_exec("select token||':'||coalesce(input_raw::text,'-')||':'||coalesce(output_raw::text,'-') "
                   "from served_features order by token collate \"C\"") == "TON:1000:-\njetton:A:-:250"
    recs = probe_result("notifier", "notifier")["records"]
    assert [r[1]["n"] for r in recs] == [0, 1, 2] and len({r[1]["run"] for r in recs}) == 1
    got = probe_result("spark", "spark")["features"]
    assert got == sorted(f"{k}={v}" for k, v in k3s_probe.SPARK_EXPECTED.items())


APP_IMAGE_REF = "ton-sentinel@" + APP_SPARK_IMAGES["ton-sentinel:local"][1]


def stream_pod():
    """One-shot pod: app image (pyspark + Kafka source jars + psycopg), same restricted context as chart pods.

    Labelled like the chart's db clients so the chart NetworkPolicies admit it to postgres+kafka only.
    """
    n = f"{REL}-ton-sentinel"
    env = [{"name": "SENTINEL_DATABASE_DSN", "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "database-dsn"}}},
           {"name": "HOME", "value": "/tmp"}, {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
           {"name": "HADOOP_USER_NAME", "value": "sentinel"},
           {"name": "SENTINEL_KAFKA_BOOTSTRAP", "value": f"{n}-kafka:9092"},
           {"name": "SENTINEL_KAFKA_TLS_DIR", "value": "/kafka-tls"},
           {"name": "SPARK_DRIVER_MEMORY", "value": "768m"},
           {"name": "SPARK_LOCAL_DIRS", "value": "/var/sentinel/scratch"},
           {"name": "SENTINEL_STREAM_CHECKPOINT", "value": "/var/sentinel/checkpoints/stream-k3s"}]
    boot = ("import sys,tarfile; tarfile.open('/code/src.tgz').extractall('/tmp/code'); "
            "sys.path.insert(0,'/tmp/code/src'); from ton_sentinel.k3s_probe import main; sys.exit(main(sys.argv[1:]))")
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "stream-probe", "namespace": CHART_NS,
                         "labels": {"app.kubernetes.io/instance": REL, "app.kubernetes.io/component": "stream-probe",
                                    "sentinel-db-client": "true"}},
            "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False,
                     "nodeSelector": {"kubernetes.io/arch": "amd64"},
                     "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 10001,
                                         "seccompProfile": {"type": "RuntimeDefault"}},
                     "containers": [{
                         "name": "stream", "image": APP_IMAGE_REF, "imagePullPolicy": "Never",
                         "command": ["python3", "-c", boot, "stream", "--once"], "env": env,
                         "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                             "capabilities": {"drop": ["ALL"]}},
                         "resources": {"requests": {"cpu": "500m", "memory": "1024Mi", "ephemeral-storage": "128Mi"},
                                       "limits": {"cpu": "1", "memory": "2048Mi", "ephemeral-storage": "512Mi"}},
                         "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"},
                                          {"name": "code", "mountPath": "/code", "readOnly": True},
                                          {"name": "kafka-tls", "mountPath": "/kafka-tls", "readOnly": True},
                                          {"name": "checkpoints", "mountPath": "/var/sentinel/checkpoints"},
                                          {"name": "scratch", "mountPath": "/var/sentinel/scratch"}]}],
                     "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}},
                                 {"name": "code", "configMap": {"name": f"{n}-code"}},
                                 {"name": "kafka-tls", "secret": {"secretName": SECRET, "items": [
                                     {"key": "kafka-client.crt", "path": "kafka-client.crt"},
                                     {"key": "kafka-client.key", "path": "kafka-client.key"},
                                     {"key": "kafka-ca.crt", "path": "kafka-ca.crt"}]}},
                                 {"name": "checkpoints", "persistentVolumeClaim": {"claimName": f"{n}-checkpoints"}},
                                 {"name": "scratch", "persistentVolumeClaim": {"claimName": f"{n}-scratch"}}]}}


@pytest.mark.k3s
def test_kafka_spark_postgres_bounded_stream_lands_expected_rows():
    assert LOCAL_VALUES.exists(), "BLOCKED: missing local-only values file"
    ctx = kc("config", "current-context").stdout.strip()
    assert ctx.startswith("k3d-ton-sentinel-local"), ctx
    load_images()
    load_images(APP_SPARK_IMAGES)
    helm("uninstall", REL, "-n", CHART_NS, check=False)
    kc("delete", "pv", "-l", f"app.kubernetes.io/instance={REL}", "--ignore-not-found", check=False)
    kc("delete", "ns", CHART_NS, "--ignore-not-found", "--wait=true")
    with tempfile.TemporaryDirectory() as d:
        helm("install", REL, str(CHART), "-f", str(LOCAL_VALUES), "-n", CHART_NS, "--create-namespace")
        # The other probes would write the same serving window / topic; this test owns both.
        for name in ("ingestor", "api", "notifier", "spark"):
            kc("-n", CHART_NS, "scale", f"deploy/{REL}-ton-sentinel-{name}", "--replicas=0")
        kc(*make_secret(d))
        kc(*code_configmap_args())
    for s in (f"{REL}-ton-sentinel-postgres", f"{REL}-ton-sentinel-kafka"):
        kc("-n", CHART_NS, "rollout", "status", f"sts/{s}", "--timeout=300s")
    kc("apply", "-f", "-", input=json.dumps(stream_pod()))
    phase = ""
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        phase = kc("-n", CHART_NS, "get", "pod", "stream-probe", "-o", "jsonpath={.status.phase}").stdout
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(5)
    logs = kc("-n", CHART_NS, "logs", "stream-probe", "--tail=400", check=False).stdout
    assert phase == "Succeeded", f"phase={phase}; logs tail:\n{logs[-3000:]}"
    ok = [l for l in logs.splitlines() if l.startswith("PROBE_OK role=stream ")]
    assert ok, f"no PROBE_OK for stream; logs tail:\n{logs[-3000:]}"
    rows = pg_exec("select token||'|'||swaps||'|'||coalesce(input_raw::text,'-')||'|'||coalesce(output_raw::text,'-') "
                   "from served_features order by token collate \"C\"")
    assert rows == "JETTON:CANONICAL|2|-|96\nTON|2|320|-"
    assert pg_exec("select count(*) from stream_batches where topic='ton.swap_events.v1'") == "2"
    assert pg_exec("select count(*) from outbox where publication_state='acknowledged'") == "2"
    offsets = pg_exec("select string_agg(offset_metadata::text, ';' order by batch_id) from stream_batches")
    assert '"offset":0' in offsets.replace(" ", "") and '"offset":1' in offsets.replace(" ", "")
