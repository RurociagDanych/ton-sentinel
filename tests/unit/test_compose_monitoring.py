"""Static properties of deploy/compose.monitoring.yaml (Prometheus + Grafana, local only)."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
FILE = ROOT / "deploy" / "compose.monitoring.yaml"
SERVICES = ("prometheus", "grafana")


def _doc():
    return yaml.safe_load(FILE.read_text())


def test_images_pinned_by_digest():
    for name in SERVICES:
        assert re.search(r"@sha256:[0-9a-f]{64}$", _doc()["services"][name]["image"]), name


def test_ports_loopback_only_and_distinct():
    hosts = []
    for name in SERVICES:
        for p in _doc()["services"][name]["ports"]:
            assert p.startswith("127.0.0.1:"), p
            hosts.append(p.split(":")[1])
    assert len(set(hosts)) == len(hosts) == 2


def test_hardening_and_limits():
    for name in SERVICES:
        s = _doc()["services"][name]
        assert not s.get("privileged")
        assert s["cap_drop"] == ["ALL"] and "no-new-privileges:true" in s["security_opt"]
        assert s["read_only"] is True and s["mem_limit"] and s["cpus"]


def test_no_secret_in_tracked_files_and_password_from_file():
    text = FILE.read_text()
    g = _doc()["services"]["grafana"]
    assert g["environment"]["GF_SECURITY_ADMIN_PASSWORD__FILE"] == "/run/secrets/grafana_admin"
    assert "GF_SECURITY_ADMIN_PASSWORD:" not in text
    assert _doc()["secrets"]["grafana_admin"]["file"] == "../.cache/secrets/grafana_admin"
    assert g["environment"]["GF_AUTH_ANONYMOUS_ENABLED"] in ("false", False)
    for f in (ROOT / "deploy" / "monitoring").rglob("*"):
        if f.is_file():
            assert not re.search(r"(?i)password\s*[:=]\s*\S", f.read_text()), f


def test_grafana_provisioning_read_only_and_analytics_off():
    g = _doc()["services"]["grafana"]
    mounts = [v for v in g["volumes"] if "provisioning" in v or "dashboards" in v]
    assert mounts and all(v.endswith(":ro") for v in mounts)
    env = g["environment"]
    assert env["GF_ANALYTICS_REPORTING_ENABLED"] in ("false", False)
    assert env["GF_ANALYTICS_CHECK_FOR_UPDATES"] in ("false", False)
    assert env["GF_PLUGINS_PLUGIN_ADMIN_ENABLED"] in ("false", False)
    ds = yaml.safe_load((ROOT / "deploy/monitoring/grafana/provisioning/datasources/prometheus.yml").read_text())
    d = ds["datasources"][0]
    assert d["uid"] == "prom-local" and d["editable"] is False


def test_prometheus_scrape_and_retention():
    p = _doc()["services"]["prometheus"]["command"]
    assert "--storage.tsdb.retention.time=7d" in p and "--storage.tsdb.retention.size=512MB" in p
    assert not any("enable-lifecycle" in a or "enable-admin" in a for a in p)
    cfg = yaml.safe_load((ROOT / "deploy/monitoring/prometheus/prometheus.yml").read_text())
    assert cfg["global"]["scrape_interval"] == "15s"
    assert cfg["scrape_configs"][0]["static_configs"][0]["targets"] == ["api:8080"]
