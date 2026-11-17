"""Every test under tests/integration needs real services (PostgreSQL, Kafka, Spark, K3s).

Mark them all as ``integration`` so the static qualification, which deselects service-marked
tests, never tries to run them without the owned disposable services. Scoped to this directory:
pytest passes every collected item to this hook, not only the ones below this conftest.
"""
from pathlib import Path

import pytest

INTEGRATION_DIR = Path(__file__).resolve().parent


def pytest_collection_modifyitems(items):
    for item in items:
        if INTEGRATION_DIR in Path(str(item.fspath)).resolve().parents:
            item.add_marker(pytest.mark.integration)
