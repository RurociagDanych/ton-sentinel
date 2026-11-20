"""READER_TABLES is the exact allowlist and covers every SQL the API readers run."""

import ast
import re
from pathlib import Path

from ton_sentinel.db import serving
from ton_sentinel.db.serving import READER_TABLES

DB = Path(serving.__file__).parent
SRC = DB.parent
EXPECTED = frozenset("""served_features action_window_qualification action_window_coverage
window_admission_refusals coverage_ranges detector_current detector_results alert_revisions
baseline_inputs detector_policies active_detector_policy outbox dirty_windows
revision_quarantine scan_quarantine delivery_quarantine stream_batches recovery_pauses
ingestion_pauses retention_runs""".split())
WRITERS = {"replace_features"}
TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)\b(?!\s*\()", re.I)


def _sql_strings(path, skip_functions=()):
    tree = ast.parse(path.read_text())
    skipped = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            if node.name in skip_functions:
                skipped.update(id(n) for n in ast.walk(node))
            elif ast.get_docstring(node) is not None:
                skipped.add(id(node.body[0].value))
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skipped]


def _referenced(path, skip_functions=()):
    found = set()
    for text in _sql_strings(path, skip_functions):
        if re.search(r"\bSELECT\b", text, re.I):
            found.update(m.group(1).lower() for m in TABLE_REF.finditer(text)
                         if m.group(1).lower() not in {"select"})
    return found


def test_reader_tables_listing_is_exact_frozenset():
    assert isinstance(READER_TABLES, frozenset)
    assert READER_TABLES == EXPECTED


def test_reader_tables_exclude_internal_state():
    assert not READER_TABLES & {"lease_state", "ingest_state", "canonical_actions"}


def test_reader_tables_cover_all_reader_sql():
    api = _referenced(SRC / "api.py")
    srv = _referenced(DB / "serving.py", WRITERS)
    assert api and srv, "parser found no table references; the test would be vacuous"
    assert {"served_features", "detector_current", "alert_revisions", "outbox"} <= api | srv
    assert (api | srv) <= READER_TABLES, sorted((api | srv) - READER_TABLES)


def test_migration_028_grants_exactly_reader_tables():
    sql = (DB / "migrations" / "028_reader_role.sql").read_text()
    granted = set(re.findall(r"'([a-z_]+)'", re.search(r"ARRAY\[(.*?)\]", sql, re.S).group(1)))
    assert granted == READER_TABLES
    code = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    assert not re.search(r"DEFAULT PRIVILEGES|ON ALL TABLES|INSERT|UPDATE\b.*TO|DELETE|TRUNCATE", code)
