"""Alertmanager + local webhook sink wiring (static checks)."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "compose.monitoring.yaml"
PROM = ROOT / "deploy" / "monitoring" / "prometheus" / "prometheus.yml"
AMCFG = ROOT / "deploy" / "monitoring" / "alertmanager" / "alertmanager.yml"


def _svc(name):
    return yaml.safe_load(COMPOSE.read_text())["services"][name]


def test_alertmanager_digest_pinned_and_loopback_port():
    s = _svc("alertmanager")
    assert re.search(r"^prom/alertmanager@sha256:[0-9a-f]{64}$", s["image"])
    assert s["ports"] == ["127.0.0.1:9093:9093"]


def test_alertmanager_hardening_and_readonly_config():
    for name in ("alertmanager", "alert-sink"):
        s = _svc(name)
        assert not s.get("privileged")
        assert s["cap_drop"] == ["ALL"] and "no-new-privileges:true" in s["security_opt"]
        assert s["read_only"] is True and s["mem_limit"]
        assert re.search(r"@sha256:[0-9a-f]{64}$", s["image"])
    vols = _svc("alertmanager")["volumes"]
    assert any("monitoring/alertmanager" in v and v.endswith(":ro") for v in vols)
    assert "alert-sink" not in str(_svc("alert-sink").get("ports", ""))


def test_prometheus_alerting_target():
    cfg = yaml.safe_load(PROM.read_text())
    targets = [t for am in cfg["alerting"]["alertmanagers"] for sc in am["static_configs"] for t in sc["targets"]]
    assert targets == ["alertmanager:9093"]


def test_receiver_local_only_no_credentials():
    text = AMCFG.read_text()
    cfg = yaml.safe_load(text)
    urls = [w["url"] for r in cfg["receivers"] for w in r.get("webhook_configs", [])]
    assert urls and all(re.match(r"http://alert-sink:\d+/", u) for u in urls)
    assert not re.search(r"password|token|secret|api_key", text, re.I)
