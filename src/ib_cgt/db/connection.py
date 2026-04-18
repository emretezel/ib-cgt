"""Open a SQLite connection configured for the `ib-cgt` persistence layer.

Centralising connection setup keeps the PRAGMAs — which are per-connection,
not per-database — from drifting across callers. Every repo in this package
assumes the connection it receives was opened via `open_connection()` (or
`open_memory_connection()` in tests).

Design notes:

* `foreign_keys = ON` is mandatory — SQLite defaults it OFF, silently. Our
  schema relies on FK enforcement (trade → account, matched_disposal → run),
  so we set it on every new connection.
* `journal_mode = WAL` + `synchronous = NORMAL` is the durable, fast
  combination for a single-user desktop tool. WAL survives reader/writer
  concurrency within a process and is safe to lose at most the most recent
  transaction on hard power-off — acceptable for this use case.
* `isolation_level = None` puts sqlite3 in "autocommit" mode, but because
  we use explicit `with conn:` blocks in the repos, every write still runs
  inside a BEGIN…COMMIT. This avoids sqlite3's implicit "start a transaction
  on the first DML" behaviour, which has bitten us with unexpected nested
  transactions in other projects.
* `detect_types = 0` — we own type conversion via `codecs.py`; sqlite3's
  PARSE_DECLTYPES machinery would introduce a second, competing layer.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# PRAGMAs we always want on a fresh connection. Order matters: `foreign_keys`
# has to be set after opening but before any transaction runs, so we apply
# them immediately inside the helper.
_PRAGMAS: tuple[tuple[str, str], ...] = (
    ("foreign_keys", "ON"),
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("temp_store", "MEMORY"),
)


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply the standard PRAGMA set to an open connection.

    Kept as a private helper so both `open_connection` and
    `open_memory_connection` can share the exact same setup.
    """
    for name, value in _PRAGMAS:
        # Use `execute` rather than `executescript` so a bad PRAGMA surfaces
        # as an exception on this specific line instead of silently being
        # swallowed by sqlite3's script parser.
        conn.execute(f"PRAGMA {name} = {value}")


def open_connection(path: Path | str) -> sqlite3.Connection:
    """Open a SQLite database at `path` with the project's standard pragmas.

    Args:
        path: Filesystem location for the SQLite file. Created if absent;
            parent directories must already exist.

    Returns:
        A `sqlite3.Connection` configured with `sqlite3.Row` row factory
        and the pragmas listed at the top of this module. The caller owns
        the connection's lifecycle — close it when done.
    """
    # `str(path)` tolerates both `Path` and `str` inputs without forcing the
    # caller to convert. `detect_types=0` disables sqlite3's declarative
    # type-conversion machinery; we do all codec work explicitly.
    conn = sqlite3.connect(str(path), detect_types=0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def open_memory_connection() -> sqlite3.Connection:
    """Open an in-memory SQLite database with the project's standard pragmas.

    Useful for unit tests that need a fully-isolated DB per test. WAL is
    not available for `:memory:` databases (SQLite falls back to the
    default rollback journal automatically), so tests pay no WAL cost.
    """
    conn = sqlite3.connect(":memory:", detect_types=0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn
