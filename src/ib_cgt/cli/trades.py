"""`ib-cgt trades` — list ingested trades with optional filters.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.cli.app import app
from ib_cgt.cli.common import console
from ib_cgt.config import resolve_db_path
from ib_cgt.db import TradeRepo, open_connection
from ib_cgt.domain import Trade


@app.command("trades")
def trades(
    account: Annotated[
        str | None,
        typer.Option("--account", "-a", help="Filter to a single IB account id."),
    ] = None,
    symbol: Annotated[
        str | None,
        typer.Option("--symbol", "-s", help="Filter to a single instrument symbol."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Lower-bound on trade_date (YYYY-MM-DD). Inclusive.",
        ),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", min=1, help="Max rows to display."),
    ] = 50,
) -> None:
    """Print a table of stored trades, filtered by the given options."""
    since_date: date | None = None
    if since is not None:
        try:
            # `date.fromisoformat` accepts YYYY-MM-DD; let the user see a
            # friendly error rather than a raw traceback from deep in
            # SQL binding if they pass the wrong format.
            since_date = date.fromisoformat(since)
        except ValueError as exc:
            raise typer.BadParameter(f"invalid --since value {since!r}: {exc}") from exc

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        rows = TradeRepo(conn).list_filtered(
            account_id=account,
            symbol=symbol,
            since=since_date,
            limit=limit,
        )
    finally:
        conn.close()

    if not rows:
        console.print("[dim]No trades match the given filters.[/]")
        return

    _render_trades(rows, db_path)


def _render_trades(rows: list[Trade], db_path: Path) -> None:
    """Render `rows` as a rich.Table, newest first."""
    # Caption pins the source DB in the output — when running against
    # multiple environments (prod, tmp, test) this saves a lot of "wait,
    # which DB did I just read?" confusion.
    table = Table(
        title=f"Trades (newest first) — {len(rows)} rows",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Account")
    table.add_column("Asset")
    table.add_column("Symbol")
    table.add_column("Date (UK)")
    table.add_column("Time (UTC)")
    table.add_column("Action")
    table.add_column("Qty", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Fees", justify="right")

    for trade in rows:
        # UTC timestamp in the table keeps it comparable across rows that
        # might carry different local offsets; the UK-local trade_date is
        # the one that drives CGT matching so we show both.
        utc_time = trade.trade_datetime.astimezone(tz=datetime.now().astimezone().tzinfo)
        table.add_row(
            trade.account_id,
            trade.instrument.asset_class.value,
            trade.instrument.symbol,
            trade.trade_date.isoformat(),
            utc_time.strftime("%H:%M:%S %Z"),
            trade.action.value,
            f"{trade.quantity}",
            f"{trade.price.amount} {trade.price.currency}",
            f"{trade.fees.amount} {trade.fees.currency}",
        )

    console.print(table)
