"""Shared fixtures for `ib_cgt.fx` unit tests.

Mirrors the DB fixture shape in `tests/unit/db/conftest.py`: every test
gets a fresh on-disk SQLite file with all migrations applied. We also
pin a stable test base URL for the Frankfurter client so `respx` routes
are easy to assert against.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ib_cgt.db import apply_migrations, open_connection

# Test-only Frankfurter base URL. Using a non-public host makes it
# obvious in logs that traffic should be mocked.
TEST_BASE_URL = "https://frankfurter.test"


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Yield a fresh, already-migrated SQLite connection."""
    db_path = tmp_path / "ibcgt.sqlite"
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        yield conn
    finally:
        conn.close()
