"""`ib-cgt db` — database administration: `db init` and `db reset`.

`db init` applies pending migrations. `db reset` empties the data
tables (never the schema and, by default, never the FX rate cache)
inside one transaction so a failed delete leaves every table intact.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.cli.app import db_app
from ib_cgt.cli.common import console
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection, transaction


@db_app.command("init")
def db_init() -> None:
    """Create the SQLite file (if absent) and apply every pending migration."""
    db_path = resolve_db_path()
    # `open_connection` already runs the standard PRAGMAs.
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
    finally:
        conn.close()
    console.print(f"[green]DB ready at[/] [bold]{db_path}[/]")


# Tables emptied by `db reset`, in dependency order. Children (or rows
# referenced by other tables we are also wiping) come first so the FK
# enforcement engine sees a consistent state at every step. The final
# two entries cascade through their child tables: deleting from
# `tax_runs` removes `matched_disposals`; deleting from `instruments`
# removes the four asset-class child rows.
_RESET_TABLES_DATA: tuple[str, ...] = (
    "tax_runs",  # cascades to matched_disposals
    "trades",
    "dividends",
    "bond_coupons",
    "cash_events",
    "statement_positions",
    "statements",
    "instruments",  # cascades to {stock,bond,future,fx}_instruments
    "accounts",
)


@db_app.command("reset")
def db_reset(
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip the confirmation prompt. Required for non-interactive use.",
        ),
    ] = False,
    include_fx: Annotated[
        bool,
        typer.Option(
            "--include-fx",
            help=(
                "Also clear the fx_rates cache. By default the FX cache "
                "is preserved because it is expensive to refetch from "
                "Frankfurter."
            ),
        ),
    ] = False,
) -> None:
    """Wipe ingested data, keep schema and (by default) the FX cache.

    Clears trades, statements, accounts, instruments, the four asset-
    class instrument children, tax_runs, and matched_disposals. The
    schema (tables, indexes, view) and the migration ledger are left
    intact, so a follow-up `ib-cgt ingest` works without re-running
    `db init`. The FX rate cache is preserved unless `--include-fx`
    is passed.
    """
    db_path = resolve_db_path()
    if not yes:
        target = "data tables and the FX cache" if include_fx else "data tables"
        if not typer.confirm(
            f"This will permanently delete every row in the {target} at {db_path}. Continue?",
            default=False,
        ):
            console.print("[yellow]Reset cancelled.[/]")
            raise typer.Exit(code=1)

    conn = open_connection(db_path)
    try:
        # Make sure the schema actually exists before we issue deletes —
        # a fresh file would otherwise produce a confusing "no such
        # table" error from the first DELETE.
        apply_migrations(conn)
        cleared = _execute_reset(conn, include_fx=include_fx)
    finally:
        conn.close()

    _render_reset_result(cleared, db_path)


def _execute_reset(
    conn: sqlite3.Connection,
    *,
    include_fx: bool,
) -> dict[str, int]:
    """Delete rows from each data table; return a per-table row-count."""
    targets = list(_RESET_TABLES_DATA)
    if include_fx:
        # `fx_rates` has no FK in or out, so it can be wiped at any
        # point in the sequence — appended last simply for readability
        # in the per-table summary.
        targets.append("fx_rates")

    cleared: dict[str, int] = {}
    # One transaction for the whole wipe: a failure part-way (e.g. a
    # table missing after a botched migration) leaves every table as
    # it was rather than half-emptied.
    with transaction(conn):
        for table in targets:
            # Capture the count before the delete so the summary is
            # accurate even on a fresh DB where every count is zero.
            before = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            cleared[table] = int(before["n"])
            conn.execute(f"DELETE FROM {table}")
    return cleared


def _render_reset_result(cleared: dict[str, int], db_path: Path) -> None:
    """Print a per-table row-count summary of the reset."""
    table = Table(
        title=f"DB reset → {db_path}",
        caption="[dim]rows removed per table[/]",
        header_style="bold",
    )
    table.add_column("Table")
    table.add_column("Rows removed", justify="right")
    for name, n_rows in cleared.items():
        table.add_row(name, str(n_rows))
    console.print(table)
