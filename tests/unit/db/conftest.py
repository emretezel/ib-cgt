"""Shared fixtures for `ib_cgt.db` unit tests.

Every test gets a fresh, already-migrated SQLite connection via the `db`
fixture. We use an on-disk tempfile rather than `:memory:` so that a
future multi-connection test (e.g. concurrent reads) would work without
reshuffling setup; for now the cost is a few ms per test.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from ib_cgt.db import apply_migrations, open_connection


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Open a fresh SQLite DB, apply migrations, yield the connection."""
    db_path = tmp_path / "ibcgt.sqlite"
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        yield conn
    finally:
        conn.close()
