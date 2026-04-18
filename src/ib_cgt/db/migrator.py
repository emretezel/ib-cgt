"""Hand-rolled migration runner.

`docs/architecture.md §Component map, item 2` specifies hand-rolled SQL
migrations under `db/migrations/NNN_*.sql` with no third-party tooling
(alembic / yoyo). This module implements the minimum useful version of
that: a `schema_migrations` bookkeeping table, deterministic ordering by
the numeric prefix on each file, and a single public entry point.

Algorithm:

1. Ensure `schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT)`.
2. List `NNN_*.sql` package resources, sorted by `NNN`.
3. For each file whose `NNN` is not already in `schema_migrations`:
     BEGIN;
       execute the script;
       INSERT a row into schema_migrations;
     COMMIT;
   If any statement fails, SQLite rolls the transaction back automatically
   and we re-raise so the caller sees the failure.

Callers should open the connection via `ib_cgt.db.connection.open_connection`
so the foreign-keys PRAGMA is already on — otherwise CREATE TABLE
... REFERENCES ... silently skips FK validation.

Author: Emre Tezel
"""

from __future__ import annotations

import contextlib
import re
import sqlite3
from datetime import UTC, datetime
from importlib.resources import files
from importlib.resources.abc import Traversable

# Expected filename shape: three-digit version prefix, underscore, slug,
# `.sql` suffix (e.g. `001_initial.sql`). The slug is free-form but must
# be non-empty so an accidental `042_.sql` is rejected.
_MIGRATION_FILENAME_PATTERN = re.compile(r"^(\d{3})_[A-Za-z0-9_]+\.sql$")

# Package path containing migration files. Using `importlib.resources` means
# the migrations are discoverable when ib-cgt is installed as a wheel — not
# just when running from the source tree.
_MIGRATIONS_PACKAGE = "ib_cgt.db.migrations"


def _ensure_bookkeeping_table(conn: sqlite3.Connection) -> None:
    """Create `schema_migrations` if missing.

    Kept outside any transaction so the first call on a fresh DB is a
    no-op-safe idempotent CREATE TABLE IF NOT EXISTS.
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version INTEGER PRIMARY KEY, "
        "applied_at TEXT NOT NULL"
        ") STRICT"
    )


def _discover_migrations() -> list[tuple[int, Traversable]]:
    """Return `(version, resource)` pairs sorted by numeric version.

    Filenames that don't match `_MIGRATION_FILENAME_PATTERN` are skipped
    silently — this lets us keep e.g. a `README.md` inside the migrations
    directory without it being treated as a migration.
    """
    root = files(_MIGRATIONS_PACKAGE)
    found: list[tuple[int, Traversable]] = []
    for entry in root.iterdir():
        if not entry.is_file():
            continue
        match = _MIGRATION_FILENAME_PATTERN.match(entry.name)
        if match is None:
            continue
        found.append((int(match.group(1)), entry))
    found.sort(key=lambda pair: pair[0])
    _check_no_duplicate_versions(found)
    return found


def _check_no_duplicate_versions(migrations: list[tuple[int, Traversable]]) -> None:
    """Two files with the same `NNN` prefix would be a silent ordering bug."""
    seen: set[int] = set()
    for version, entry in migrations:
        if version in seen:
            raise RuntimeError(
                f"duplicate migration version {version:03d} in {_MIGRATIONS_PACKAGE} "
                f"(offending file: {entry.name!r})"
            )
        seen.add(version)


def _applied_versions(conn: sqlite3.Connection) -> set[int]:
    """Return the set of versions already recorded in `schema_migrations`."""
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows}


def apply_migrations(conn: sqlite3.Connection) -> list[int]:
    """Apply every migration not yet recorded, in order, transactionally.

    Args:
        conn: An open SQLite connection, ideally from `open_connection`
            so PRAGMAs are set. The connection is left open on return.

    Returns:
        The list of versions applied by *this* call, in the order they
        were applied. Empty list if the DB is already up-to-date.
    """
    _ensure_bookkeeping_table(conn)
    already = _applied_versions(conn)
    pending = [(v, r) for v, r in _discover_migrations() if v not in already]

    applied_now: list[int] = []
    for version, resource in pending:
        _apply_one(conn, version, resource.read_text(encoding="utf-8"))
        applied_now.append(version)
    return applied_now


def _apply_one(conn: sqlite3.Connection, version: int, script: str) -> None:
    """Apply a single migration atomically, recording it in `schema_migrations`.

    `executescript` implicitly commits any pending transaction before
    running, so wrapping it with a Python-level BEGIN/COMMIT does not
    produce atomicity. We therefore embed `BEGIN;` and `COMMIT;` directly
    inside the script text along with the bookkeeping INSERT — SQLite
    then runs the whole thing as one transaction.

    The INSERT values (version int, ISO timestamp) are under our control
    and not user-supplied, so the string concatenation is safe here.
    """
    applied_at = datetime.now(UTC).isoformat()
    full_script = (
        "BEGIN;\n"
        + script.rstrip().rstrip(";")
        + ";\n"
        + "INSERT INTO schema_migrations (version, applied_at) "
        + f"VALUES ({int(version)}, '{applied_at}');\n"
        + "COMMIT;\n"
    )
    try:
        conn.executescript(full_script)
    except Exception:
        # If the failure happened before COMMIT ran, a transaction is still
        # active and we need to roll it back. If the failure happened after
        # COMMIT (impossible via executescript but defensive), there's
        # nothing to roll back — swallow the "no transaction" error.
        with contextlib.suppress(sqlite3.OperationalError):
            conn.execute("ROLLBACK")
        raise
