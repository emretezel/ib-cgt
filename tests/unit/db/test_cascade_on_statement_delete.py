"""Tests for the migration-004 cascade rule on `trades.source_statement_hash`.

Verifies that deleting a `statements` row also removes every `trades`
row whose `source_statement_hash` pointed at it. This is the dev-time
contract `ib-cgt ingest --replace` and `ib-cgt db reset` rely on; if
the FK ever drifts back to RESTRICT, the failure will surface here
rather than as a confusing IntegrityError deep inside a CLI command.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ib_cgt.ingest.ingestor import ingest_statement

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


def test_delete_statement_cascades_to_trades(db: sqlite3.Connection) -> None:
    """`DELETE FROM statements` must also remove its trade rows."""
    # Ingest one statement so we have at least one row in each of
    # statements and trades that share a hash. The fixture is the
    # smallest one in the suite; the assertion is on the cascade
    # behaviour, not the trade count.
    result = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    statement_hash = result.statement_hash

    trades_before = db.execute(
        "SELECT COUNT(*) AS n FROM trades WHERE source_statement_hash = ?",
        (statement_hash,),
    ).fetchone()["n"]
    assert trades_before > 0, "fixture should produce at least one trade"

    # Issue the DELETE the same way `ingest --replace` and a manual
    # SQL session would — no separate trade cleanup.
    with db:
        db.execute(
            "DELETE FROM statements WHERE statement_hash = ?",
            (statement_hash,),
        )

    trades_after = db.execute(
        "SELECT COUNT(*) AS n FROM trades WHERE source_statement_hash = ?",
        (statement_hash,),
    ).fetchone()["n"]
    assert trades_after == 0, "cascade should have removed every dependent trade"

    # And the statement row itself, of course, is gone.
    statements_after = db.execute(
        "SELECT COUNT(*) AS n FROM statements WHERE statement_hash = ?",
        (statement_hash,),
    ).fetchone()["n"]
    assert statements_after == 0
