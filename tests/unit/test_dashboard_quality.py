"""Dashboard API unit tests: /v1/quality validation, response shape, unavailable-not-zero, safety filter."""
import json
import re
from unittest.mock import patch

import pytest

from ton_sentinel import api, dashboard_payload
from ton_sentinel.db import serving
from ton_sentinel.db.serving import read_quality

PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
T0 = 1_710_000_000 - (1_710_000_000 % 300)
COUNTERS = {"served_rows_by_coverage_status", "served_windows", "dirty_windows_pending_by_role",
            "revision_quarantine_open", "delivery_quarantine", "scan_quarantine",
            "coverage_ranges_pending", "coverage_ranges_bounded_observed",
            "qualifications_by_class"}


class Conn:
    """Fake connection: statements naming a table in ``fail`` raise; others return ``rows``."""
    def __init__(self, fail=(), rows=None):
        self.fail, self.rows, self.sql = set(fail), rows or {}, []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        self.sql.append(statement)
        for table in self.fail:
            if re.search(rf"\b{table}\b", statement):
                raise RuntimeError("permission denied")
        self.current = next((v for k, v in self.rows.items() if re.search(rf"\b{k}\b", statement)),
                            [] if "GROUP BY" in statement else [(0,)])
        return self

    def fetchall(self):
        return self.current


def call(query, path="/v1/quality", method="GET"):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(api.create_app(dsn="x")({"REQUEST_METHOD": method, "PATH_INFO": path,
                                             "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


def q(**over):
    base = {"network": "testnet", "from": T0, "to": T0 + 3600}
    base.update(over)
    return "&".join(f"{k}={v}" for k, v in base.items() if v is not None)


def quality_value(**over):
    value = {name: {"status": "observed", "value": 0} for name in COUNTERS}
    value.update(over)
    return value


def run(query, quality):
    with patch("ton_sentinel.api.psycopg.connect", return_value=Conn()), \
         patch("ton_sentinel.api.read_quality", return_value=quality) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query)
    return status, body, mock


# ---- validation and bounds ---------------------------------------------------------------
@pytest.mark.parametrize("over", [
    {"to": T0}, {"to": T0 - 300}, {"to": T0 + 604800 + 300}, {"from": T0 + 1}, {"to": T0 + 3601},
    {"from": None}, {"to": None}, {"network": None}, {"network": "devnet"}, {"from": "-300"},
    {"from": "abc"}, {"token": "x"}, {"limit": 1}, {"cursor": "x"},
])
def test_quality_range_bounds_and_params_are_400(over):
    status, body, mock = run(q(**over), quality_value())
    assert status == "400 Bad Request" and body == {"error": "invalid_query"}
    mock.assert_not_called()


def test_quality_accepts_exactly_seven_days():
    status, _, mock = run(q(to=T0 + 604800), quality_value())
    assert status == "200 OK"
    assert mock.call_args.kwargs == {"network": "testnet", "window_from": T0,
                                     "window_to": T0 + 604800}


def test_quality_only_get_is_served():
    status, _ = call(q(), method="POST")
    assert status.startswith("405")


@pytest.mark.parametrize("over", [
    {"network": "devnet"}, {"window_from": None}, {"window_from": T0 + 1}, {"window_to": T0},
    {"window_to": T0 + 604800 + 300}, {"window_from": "x"},
])
def test_read_quality_validates_before_querying(over):
    args = dict(network="testnet", window_from=T0, window_to=T0 + 3600)
    args.update(over)
    conn = Conn()
    with pytest.raises(ValueError):
        read_quality(conn, **args)
    assert conn.sql == []


# ---- response shape ----------------------------------------------------------------------
def test_response_shape_carries_provenance_banner_and_label():
    status, body, _ = run(q(), quality_value())
    assert status == "200 OK"
    assert set(body) == {"network", "from", "to", "quality", "label", "honesty_banner",
                         "provenance"}
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["provenance"]["source_kind"] == "unverified_origin"


def test_label_states_metrics_do_not_assert_completeness():
    _, body, _ = run(q(), quality_value())
    label = body["label"].lower()
    assert "not" in label and "complete" in label
    assert "unavailable" in label


def test_read_quality_returns_every_counter_with_status():
    result = read_quality(Conn(), network="testnet", window_from=T0, window_to=T0 + 3600)
    assert set(result) == COUNTERS
    for counter in result.values():
        assert counter["status"] in ("observed", "unavailable")
        if counter["status"] == "unavailable":
            assert counter["value"] is None and counter["reason"]


def test_empty_observed_tables_are_observed_zero_not_unavailable():
    result = read_quality(Conn(), network="testnet", window_from=T0, window_to=T0 + 3600)
    assert result["delivery_quarantine"] == {"status": "observed", "value": 0}
    assert result["served_windows"] == {"status": "observed", "value": 0}


# ---- unavailable rule --------------------------------------------------------------------
def test_each_counter_unavailable_independently():
    rows = {"revision_quarantine": [(4,)], "delivery_quarantine": [(2,)]}
    conn = Conn(fail=["dirty_windows"], rows=rows)
    result = read_quality(conn, network="testnet", window_from=T0, window_to=T0 + 3600)
    assert result["dirty_windows_pending_by_role"]["status"] == "unavailable"
    assert result["dirty_windows_pending_by_role"]["value"] is None
    assert result["dirty_windows_pending_by_role"]["reason"]
    assert result["revision_quarantine_open"] == {"status": "observed", "value": 4}
    assert result["delivery_quarantine"] == {"status": "observed", "value": 2}
    assert result["served_windows"]["status"] == "observed"


@pytest.mark.parametrize("name", ["revision_quarantine", "delivery_quarantine", "dirty_windows",
                                  "served_features", "action_window_qualification"])
def test_failing_table_is_never_reported_as_zero(name):
    result = read_quality(Conn(fail=[name]), network="testnet", window_from=T0,
                          window_to=T0 + 3600)
    failed = [c for c in result.values() if c["status"] == "unavailable"
              and c.get("reason") != "not_network_scoped"]
    assert failed
    assert all(c["value"] is None for c in failed)


def test_tables_without_network_scope_are_unavailable_with_reason_not_zero():
    conn = Conn()
    result = read_quality(conn, network="testnet", window_from=T0, window_to=T0 + 3600)
    for name in ("scan_quarantine", "coverage_ranges_pending",
                 "coverage_ranges_bounded_observed"):
        assert result[name] == {"status": "unavailable", "value": None,
                                "reason": "not_network_scoped"}
    assert not any("scan_quarantine" in s or "coverage_ranges" in s for s in conn.sql)


def test_group_lists_bounded_and_flag_truncation():
    many = [(f"role{i}", i + 1) for i in range(serving.QUALITY_MAX_GROUPS + 1)]
    result = read_quality(Conn(rows={"dirty_windows": many}), network="testnet",
                          window_from=T0, window_to=T0 + 3600)
    counter = result["dirty_windows_pending_by_role"]
    assert counter["truncated"] is True
    assert len(counter["value"]) == serving.QUALITY_MAX_GROUPS
    few = read_quality(Conn(rows={"dirty_windows": [("target", 3)]}), network="testnet",
                       window_from=T0, window_to=T0 + 3600)
    assert few["dirty_windows_pending_by_role"]["truncated"] is False
    assert few["dirty_windows_pending_by_role"]["value"] == {"target": 3}


def test_qualification_classes_follow_provenance_rule():
    rows = {"action_window_qualification": [("bounded_observed", True, 3),
                                            ("bounded_observed", False, 2),
                                            ("certified", False, 1)]}
    result = read_quality(Conn(rows=rows), network="testnet", window_from=T0,
                          window_to=T0 + 3600)
    assert result["qualifications_by_class"] == {
        "status": "observed",
        "value": {"synthetic_bounded_observed": 3, "bounded_observed": 2,
                  "certified_claimed": 1}}


def test_unavailable_counter_survives_in_http_payload():
    value = quality_value(delivery_quarantine={"status": "unavailable", "value": None,
                                               "reason": "query_failed"})
    status, body, _ = run(q(), value)
    assert status == "200 OK"
    assert body["quality"]["delivery_quarantine"]["value"] is None
    assert body["quality"]["delivery_quarantine"]["status"] == "unavailable"


# ---- safety filter and reader tables -----------------------------------------------------
def test_forbidden_keys_are_stripped_and_forbidden_values_refused():
    dirty = quality_value(extra={"status": "observed", "value": 1, "lease_owner": "w1",
                                 "wallet": "EQ"})
    status, body, _ = run(q(), dirty)
    assert status == "200 OK"
    text = json.dumps(body)
    assert "lease_owner" not in text and "wallet" not in text
    leaky = quality_value(extra={"status": "unavailable", "value": None,
                                 "reason": "postgresql://u:p@h/db"})
    status, body, _ = run(q(), leaky)
    assert status == "503 Service Unavailable" and body == {"error": "unavailable"}


def test_read_quality_reads_only_reader_tables():
    source = open(serving.__file__, encoding="utf-8").read()
    start = source.index("def read_quality")
    body = source[start:source.index("\ndef ", start + 10)]
    tables = set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+)", body))
    assert tables and tables <= serving.READER_TABLES, tables
