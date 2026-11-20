"""Provisioned Grafana dashboards reference only exported metrics."""

import json
import re
from pathlib import Path

import pytest

from ton_sentinel.api import _METRIC_FAMILIES

DIR = Path(__file__).resolve().parents[2] / "deploy" / "monitoring" / "grafana" / "dashboards"
FILES = sorted(DIR.glob("*.json"))
NAMES = {f"ton_sentinel_{n}" for n in _METRIC_FAMILIES}
SUFFIXES = ("_bucket", "_sum", "_count")


def _panels(dash):
    for p in dash["panels"]:
        yield p
        yield from p.get("panels", [])


def _base(name):
    for s in SUFFIXES:
        if name.endswith(s) and name[: -len(s)] in NAMES:
            return name[: -len(s)]
    return name


def test_dashboards_exist():
    assert len(FILES) >= 3


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_dashboard_shape(path):
    d = json.loads(path.read_text())
    assert d["title"] and d["uid"]
    panels = [p for p in _panels(d) if p["type"] != "row"]
    assert panels
    for p in panels:
        assert p["title"]
        assert "Data source:" in p["description"], p["title"]
        assert p["datasource"] == {"type": "prometheus", "uid": "prom-local"}
        assert p["targets"]
        for t in p["targets"]:
            assert t["datasource"] == {"type": "prometheus", "uid": "prom-local"}
            assert t["expr"]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_metrics_exist_in_catalogue(path):
    d = json.loads(path.read_text())
    used = set()
    for p in _panels(d):
        for t in p.get("targets", []):
            used |= set(re.findall(r"\bton_sentinel_\w+", t["expr"]))
    assert used
    unknown = {_base(u) for u in used} - NAMES
    assert not unknown, unknown


def test_unique_uids_and_required_coverage():
    ds = [json.loads(f.read_text()) for f in FILES]
    assert len({d["uid"] for d in ds}) == len(ds)
    used = set()
    for d in ds:
        for p in _panels(d):
            for t in p.get("targets", []):
                used |= {_base(u) for u in re.findall(r"\bton_sentinel_\w+", t["expr"])}
    need = {"outbox_pending", "outbox_oldest_pending_age_seconds", "repair_pending", "pipeline_backlog",
            "quality_issues", "http_requests_total", "http_request_duration_seconds", "source_up",
            "database_size_bytes", "table_size_bytes", "last_activity_timestamp_seconds"}
    assert {f"ton_sentinel_{n}" for n in need} <= used
