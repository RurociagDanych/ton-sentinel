"""Structure of the Prometheus alert rules (promtool itself runs as an integration step)."""
import re
from pathlib import Path

import yaml

from ton_sentinel import api

ROOT = Path(__file__).resolve().parents[2]
RULES = ROOT / "deploy/monitoring/prometheus/rules/alerts.yml"
EXPECTED = {
    "SentinelApiDown", "SentinelDatabaseUnreadable", "SentinelErrorRateHigh", "SentinelSlowRequests",
    "SentinelCollectorStale", "SentinelServingWriteStale", "SentinelOutboxBacklog",
    "SentinelRepairBacklog", "SentinelDatabaseGrowth", "SentinelMetricsSeriesBudget",
}
SYNTHETIC = {"up", "scrape_samples_scraped"}


def _rules():
    doc = yaml.safe_load(RULES.read_text())
    return [r for g in doc["groups"] for r in g["rules"]]


def test_exactly_the_ten_plan_alerts():
    assert {r["alert"] for r in _rules()} == EXPECTED and len(_rules()) == 10


def test_every_rule_is_labelled_local_default_with_note():
    for r in _rules():
        assert r["labels"]["threshold_class"] == "local_default", r["alert"]
        assert r["labels"]["severity"] in {"warning", "critical", "info"}
        text = r["annotations"]["note"].lower()
        assert "local default" in text and "not a production slo" in text, r["alert"]
        assert r["annotations"]["summary"] and r.get("for"), r["alert"]


def test_expressions_use_only_exported_metric_names():
    known = {"ton_sentinel_" + n for n in api._METRIC_FAMILIES}
    for r in _rules():
        for name in re.findall(r"\b(?:ton_sentinel_\w+|up|scrape_samples_scraped)\b", r["expr"]):
            base = re.sub(r"_(bucket|sum|count)$", "", name)
            assert name in SYNTHETIC or name in known or base in known, (r["alert"], name)


def test_prometheus_loads_the_rules_file_and_mounts_it():
    cfg = yaml.safe_load((ROOT / "deploy/monitoring/prometheus/prometheus.yml").read_text())
    assert cfg["rule_files"] == ["/etc/prometheus/rules/*.yml"]
    vols = yaml.safe_load((ROOT / "deploy/compose.monitoring.yaml").read_text())["services"]["prometheus"]["volumes"]
    assert "./monitoring/prometheus/rules:/etc/prometheus/rules:ro" in vols


def test_rules_glob_loads_only_rules_and_promtool_tests_live_apart():
    rules_dir = RULES.parent
    assert sorted(p.name for p in rules_dir.glob("*.yml")) == ["alerts.yml"]
    unit = ROOT / "deploy/monitoring/prometheus/tests/alerts.test.yml"
    doc = yaml.safe_load(unit.read_text())
    covered = {a["alertname"] for t in doc["tests"] for a in t["alert_rule_test"]}
    assert covered == EXPECTED
    for name in EXPECTED:
        results = [bool(a["exp_alerts"]) for t in doc["tests"] for a in t["alert_rule_test"] if a["alertname"] == name]
        assert True in results and False in results, name
