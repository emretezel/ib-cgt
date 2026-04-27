"""End-to-end ingestion orchestrator.

Composes the two ingestion primitives (hash → parse + map) with the
existing persistence repositories to turn a path-on-disk into rows in
the database. Everything inside `ingest_statement` runs in one SQLite
transaction, so a failure mid-way leaves the DB exactly as it was at
the start of the call — no half-imported statement rows, no orphaned
instrument upserts.

Idempotency is layered:

1. The statement hash short-circuits a re-import before we even parse:
   `StatementRepo.exists(hash)` → return `already_imported=True`.
2. Even if the file's hash is new but its trades overlap with rows
   already in the DB, the natural-key UNIQUE on `trades` (declared
   by migration 005) catches each duplicate at INSERT time. With
   `INSERT OR IGNORE` the duplicates silently no-op — the counts in
   the result reflect what actually landed.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ib_cgt.db.repos.accounts import AccountRepo
from ib_cgt.db.repos.statements import StatementRepo
from ib_cgt.db.repos.trades import TradeRepo
from ib_cgt.domain import Account
from ib_cgt.ingest.hashing import compute_statement_hash
from ib_cgt.ingest.mapper import map_rows
from ib_cgt.ingest.parser import parse_statement


@dataclass(frozen=True, slots=True, kw_only=True)
class IngestResult:
    """Summary returned by `ingest_statement`.

    Attributes:
        statement_hash: The SHA-256 of the source file (hex).
        account_id: Account the statement belonged to.
        trade_count: Number of trades the parser produced. Zero is a
            legal outcome for a statement with no activity.
        inserted_count: How many of those were new rows. On a repeat
            ingest of a modified statement this can be less than
            `trade_count` if some trades were already present from a
            prior, partially-overlapping import.
        already_imported: True iff the byte-identical statement had
            been imported before — parse/map were skipped. Mutually
            exclusive with `replaced`.
        replaced: True iff a prior import of this exact hash existed
            and was deleted before this fresh ingest landed (the
            `replace=True` path). Mutually exclusive with
            `already_imported`.
    """

    statement_hash: str
    account_id: str
    trade_count: int
    inserted_count: int
    already_imported: bool
    replaced: bool = False


def ingest_statement(
    path: Path,
    conn: sqlite3.Connection,
    *,
    replace: bool = False,
) -> IngestResult:
    """Parse the file at `path` and persist its trades via `conn`.

    Args:
        path: Absolute or CWD-relative path to an IB HTML `.htm` file.
        conn: An already-open, already-migrated SQLite connection
            (typically from `open_connection` + `apply_migrations`).
        replace: When True, a prior import of the same hash is
            *deleted* (along with its trades, via the migration-004
            cascade) before this fresh ingest runs. The default
            `False` preserves the historical short-circuit behaviour
            — a repeat ingest of an already-imported statement is a
            constant-time no-op.

    Returns:
        `IngestResult` — see docstring for field semantics.
    """
    # `read_bytes()` handles the file-close for us and avoids the
    # encoding guesswork that `read_text()` would introduce. Hashing
    # happens before the parser runs so a duplicate hash is a constant-
    # time short-circuit.
    source_bytes = path.read_bytes()
    statement_hash = compute_statement_hash(source_bytes)

    statements = StatementRepo(conn)
    prior_existed = statements.exists(statement_hash)

    if prior_existed and not replace:
        # We could re-derive the account id via a secondary SELECT on
        # `statements`, but it's cheaper to reparse only the header —
        # except that hash-based short-circuits exist precisely to avoid
        # re-parse costs. So we return the hash and let the caller
        # treat "already imported" as "nothing more to report".
        # For the account_id in the result we do a tiny SELECT — not
        # expensive, and keeps the result shape consistent.
        row = conn.execute(
            "SELECT account_id, trade_count FROM statements WHERE statement_hash = ?",
            (statement_hash,),
        ).fetchone()
        return IngestResult(
            statement_hash=statement_hash,
            account_id=str(row["account_id"]),
            trade_count=int(row["trade_count"]),
            inserted_count=0,
            already_imported=True,
        )

    parsed = parse_statement(source_bytes)
    trades = map_rows(parsed)

    accounts = AccountRepo(conn)
    trade_repo = TradeRepo(conn)

    # One transaction for everything the parser produced. `with conn:`
    # issues COMMIT on successful exit and ROLLBACK on exception, which
    # is exactly the atomicity the idempotency story relies on. When
    # `replace` is set, the prior-row delete also lives inside this
    # transaction so a failure between the delete and the re-insert
    # leaves the DB exactly as it was before the call.
    with conn:
        if prior_existed and replace:
            # Migration 004 made `trades.source_statement_hash`
            # ON DELETE CASCADE, so removing the `statements` row also
            # removes every trade that pointed at it.
            conn.execute(
                "DELETE FROM statements WHERE statement_hash = ?",
                (statement_hash,),
            )
        accounts.upsert(Account(account_id=parsed.account_id))
        statements.record(
            statement_hash=statement_hash,
            source_path=str(path),
            account_id=parsed.account_id,
            trade_count=len(trades),
        )
        inserted = trade_repo.insert_many(
            trades,
            source_statement_hash=statement_hash,
        )

    return IngestResult(
        statement_hash=statement_hash,
        account_id=parsed.account_id,
        trade_count=len(trades),
        inserted_count=inserted,
        already_imported=False,
        replaced=prior_existed and replace,
    )


__all__ = ["IngestResult", "ingest_statement"]
