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
* `isolation_level = None` puts sqlite3 in "autocommit" mode: every
  statement commits on its own unless an explicit ``BEGIN`` is open. This
  avoids sqlite3's implicit "start a transaction on the first DML"
  behaviour, which has bitten us with unexpected nested transactions in
  other projects. The flip side is that ``with conn:`` gives **no**
  atomicity in this mode (the context manager only calls ``commit()`` /
  ``rollback()``, which are no-ops with no transaction open). Multi-
  statement writes must therefore go through `transaction()` below.
* `detect_types = 0` — we own type conversion via `codecs.py`; sqlite3's
  PARSE_DECLTYPES machinery would introduce a second, competing layer.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
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


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """Run the enclosed block atomically: ``BEGIN`` … ``COMMIT``.

    The connection is in autocommit mode (see the module docstring), so
    this is the *only* way to make several statements succeed or fail
    together. On any exception the transaction is rolled back and the
    exception re-raised; on normal exit it is committed.

    Nesting is allowed and joins the enclosing transaction: if a
    transaction is already open on ``conn`` the block runs inside it
    and neither ``BEGIN`` nor ``COMMIT`` is issued. That lets a repo
    method that needs atomicity on its own (`InstrumentRepo.upsert`,
    `TaxRunRepo.replace_for`) be composed inside a larger unit of work
    (`ingest_statement`, `Calculator.persist`) without SQLite's
    "cannot start a transaction within a transaction" error. An inner
    failure still propagates to the outermost block, which performs
    the rollback — so the whole unit of work is undone, never a slice
    of it.

    Args:
        conn: A connection opened via `open_connection` /
            `open_memory_connection`.

    Yields:
        Nothing — the block simply runs inside the transaction.
    """
    if conn.in_transaction:
        # Join the enclosing transaction: the outermost `transaction()`
        # owns COMMIT / ROLLBACK, so an inner block must not touch either.
        yield
        return
    conn.execute("BEGIN")
    try:
        yield
    except BaseException:
        # `BaseException` so a KeyboardInterrupt mid-write also rolls
        # back rather than leaving a half-applied unit of work.
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
