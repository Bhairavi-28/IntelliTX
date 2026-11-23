"""Integration tests need a MySQL server and the built engine.

Safety: the schema is DROPPED and recreated, so tests only run against a database whose name ends
with '_test' (set MYSQL_DATABASE=intellitx_test). Otherwise DB tests are skipped."""
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def pytest_configure(config):
    config.addinivalue_line("markers", "db: needs MySQL")
    config.addinivalue_line("markers", "engine: needs build/itx_cli")


@pytest.fixture(scope="session")
def db():
    from backend import database
    database.load_dotenv()
    name = os.environ.get("MYSQL_DATABASE", "")
    if not name.endswith("_test"):
        pytest.skip("set MYSQL_DATABASE to a database ending in _test (schema is recreated by tests)")
    ok, info = database.ping()
    if not ok:
        pytest.skip(f"MySQL not reachable: {info}")
    database.init_schema(with_seed=True)
    return database


@pytest.fixture(scope="session")
def engine_cli():
    from backend.services.engine import cli_path
    if not cli_path().exists():
        pytest.skip("build the engine first: cmake -S . -B build && cmake --build build")
    return cli_path()
