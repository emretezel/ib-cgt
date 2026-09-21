"""`ib-cgt fx sync` — fill the Frankfurter rate cache for every non-GBP currency.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.cli.app import fx_app
from ib_cgt.cli.common import console
from ib_cgt.config import resolve_db_path, resolve_fx_base_url
from ib_cgt.db import FXRateRepo, apply_migrations, open_connection
from ib_cgt.fx import FrankfurterClient, FXService


@fx_app.command("sync")
def fx_sync(
    currency: Annotated[
        list[str] | None,
        typer.Option(
            "--currency",
            "-c",
            help=(
                "Quote currency to fetch. Repeat the flag for multiple. "
                "When omitted, every non-GBP currency present in the "
                "instruments table is synced."
            ),
        ),
    ] = None,
) -> None:
    """Incrementally sync ECB rates for every observed currency.

    Per-pair behaviour:

    * First run (no cached rates) → pull full ECB history from
      1999-01-04.
    * Subsequent runs → pull only rates newer than the latest cached
      date.
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        # Explicit `--currency` overrides the DB-derived set — handy for
        # one-off fetches or tests without needing any ingested trades.
        if currency:
            currencies = sorted({c.upper() for c in currency if c.strip() != ""})
        else:
            currencies = _detect_all_currencies(conn)

        if not currencies:
            console.print(
                "[yellow]No non-GBP currencies observed in instruments[/] — nothing to sync."
            )
            return

        service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        summary = service.sync_currencies(currencies)
        # Re-read `max_rate_date` after the sync so the summary can show
        # the new watermark per pair.
        latest_by_ccy = {ccy: FXRateRepo(conn).max_rate_date("GBP", ccy) for ccy in summary}
    finally:
        conn.close()

    _render_fx_sync_result(summary, latest_by_ccy, db_path)


def _detect_all_currencies(conn: sqlite3.Connection) -> list[str]:
    """Return every non-GBP currency referenced by any instrument.

    Reads from the `v_instruments` view (parent + four asset-class
    children) so this code does not have to know about the table split.
    Each child's `ix_<class>_instruments_currency` index lets SQLite
    walk the indexes rather than the table heap. The result cardinality
    is small (a handful of currencies for a typical UK taxpayer's IB
    portfolio), so the ordering is a stable sort that makes the
    subsequent Rich table output deterministic.
    """
    rows = conn.execute(
        "SELECT DISTINCT currency FROM v_instruments WHERE currency != 'GBP' ORDER BY currency"
    ).fetchall()
    return [r["currency"] for r in rows]


def _render_fx_sync_result(
    summary: dict[str, int],
    latest_by_ccy: dict[str, date | None],
    db_path: Path,
) -> None:
    """Print a per-currency Rich table with the new rows and watermark."""
    table = Table(title=f"FX sync → {db_path}")
    table.add_column("Currency", style="bold")
    table.add_column("New rows", justify="right")
    table.add_column("Latest cached", justify="right")
    total = 0
    for ccy, rows_written in summary.items():
        total += rows_written
        latest = latest_by_ccy.get(ccy)
        table.add_row(
            ccy,
            str(rows_written),
            latest.isoformat() if latest is not None else "—",
        )
    console.print(table)
    console.print(f"[green]Synced[/] [bold]{total}[/] new rate(s) total.")
