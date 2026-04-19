"""Minimal Typer CLI — `db init`, `ingest`, `trades`, `fx sync`.

This ships ahead of the full CLI plan (architecture step 8) so the
ingestion and FX layers are runnable end-to-end. Commands:

* ``ib-cgt db init`` — open the configured DB and apply migrations.
* ``ib-cgt ingest PATH`` — parse and persist an IB HTML statement.
* ``ib-cgt trades [filters]`` — print a rich table of stored trades.
* ``ib-cgt fx sync --year YYYY`` — pull ECB rates for every currency
  observed in that tax year's trades (or a user-supplied list) and
  cache them locally.

The command surface and help text are deliberately terse; we'll flesh
them out when the calculator and reporting commands land in their own
plans.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ib_cgt.config import resolve_db_path, resolve_fx_base_url
from ib_cgt.db import (
    FXRateRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.codecs import date_to_text
from ib_cgt.domain import TaxYear, Trade
from ib_cgt.domain.tax_year import InvalidTaxYearError
from ib_cgt.fx import FrankfurterClient, FXService
from ib_cgt.ingest import IngestResult, ingest_statement

# Two sub-apps keeps related commands grouped in `--help` output: `db`
# hosts schema-management commands, the top level hosts the user-facing
# verbs.
app = typer.Typer(
    help="UK Capital Gains Tax calculator for Interactive Brokers statements.",
    no_args_is_help=True,
    # `rich_markup_mode="rich"` lets us embed Rich markup in help text if
    # we want it later without toggling the flag globally.
    rich_markup_mode="rich",
)
db_app = typer.Typer(
    help="Database administration (init migrations, etc.).",
    no_args_is_help=True,
)
app.add_typer(db_app, name="db")
fx_app = typer.Typer(
    help="FX rate cache management (Frankfurter / ECB).",
    no_args_is_help=True,
)
app.add_typer(fx_app, name="fx")

# One module-level Console so colour / width detection is shared across
# commands — cheaper than re-constructing it per call.
_console = Console()


# ---------------------------------------------------------------------------
# `db` subgroup
# ---------------------------------------------------------------------------


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
    _console.print(f"[green]DB ready at[/] [bold]{db_path}[/]")


# ---------------------------------------------------------------------------
# `ingest`
# ---------------------------------------------------------------------------


@app.command("ingest")
def ingest(
    path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="IB HTML activity statement (.htm).",
        ),
    ],
) -> None:
    """Parse an IB statement and persist its trades into the database."""
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Surface a friendly error if the DB hasn't been initialised yet —
        # SQLite will create empty files on `connect`, so the missing
        # schema manifests as a foreign-key failure deep in the ingestor.
        apply_migrations(conn)
        result = ingest_statement(path, conn)
    finally:
        conn.close()

    _render_ingest_result(result, path)


def _render_ingest_result(result: IngestResult, source: Path) -> None:
    """Print a short, structured summary of an ingestion run."""
    if result.already_imported:
        _console.print(
            f"[yellow]Already imported[/] — hash "
            f"[dim]{result.statement_hash[:12]}…[/] "
            f"(account {result.account_id}, {result.trade_count} trades on record)."
        )
        return

    _console.print(
        f"[green]Imported[/] [bold]{source.name}[/] "
        f"for account [bold]{result.account_id}[/]: "
        f"{result.inserted_count} new / {result.trade_count} parsed."
    )


# ---------------------------------------------------------------------------
# `trades`
# ---------------------------------------------------------------------------


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
        _console.print("[dim]No trades match the given filters.[/]")
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

    _console.print(table)


# ---------------------------------------------------------------------------
# `fx sync`
# ---------------------------------------------------------------------------


@fx_app.command("sync")
def fx_sync(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="UK tax year start calendar year (e.g. 2024 for the 2024/25 year).",
        ),
    ],
    currency: Annotated[
        list[str] | None,
        typer.Option(
            "--currency",
            "-c",
            help=(
                "Quote currency to fetch. Repeat the flag for multiple. "
                "Defaults to every non-GBP currency observed in that year's trades."
            ),
        ),
    ] = None,
) -> None:
    """Fetch ECB rates for the given tax year and cache them locally."""
    try:
        tax_year = TaxYear(year)
    except InvalidTaxYearError as exc:
        raise typer.BadParameter(f"invalid --year {year!r}: {exc}") from exc

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        # Auto-detect the currency set when the caller didn't supply one.
        # Single DISTINCT query rather than N per-instrument lookups so
        # this stays O(#traded instruments) at the DB layer.
        if currency:
            currencies = sorted({c.upper() for c in currency if c.strip() != ""})
        else:
            currencies = _detect_currencies_for_year(conn, tax_year)

        if not currencies:
            _console.print(
                f"[yellow]No non-GBP trades recorded for {tax_year.label}[/] — nothing to sync."
            )
            return

        service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        written = service.sync_for_tax_year(tax_year, currencies=currencies)
    finally:
        conn.close()

    _render_fx_sync_result(tax_year, currencies, written, db_path)


def _detect_currencies_for_year(conn: sqlite3.Connection, tax_year: TaxYear) -> list[str]:
    """Return every non-GBP instrument currency traded in `tax_year`.

    Uses the `ix_trades_trade_date` index so the scan is O(#trades in
    window). The join to `instruments` is by the narrow surrogate id,
    so the plan is an index range scan followed by a PK lookup per
    distinct instrument — fine for the result cardinality here (at
    most a handful of currencies in practice).
    """
    rows = conn.execute(
        "SELECT DISTINCT i.currency "
        "FROM trades t JOIN instruments i ON i.instrument_id = t.instrument_id "
        "WHERE t.trade_date BETWEEN ? AND ? AND i.currency != 'GBP' "
        "ORDER BY i.currency",
        (date_to_text(tax_year.start_date), date_to_text(tax_year.end_date)),
    ).fetchall()
    return [r["currency"] for r in rows]


def _render_fx_sync_result(
    tax_year: TaxYear,
    currencies: list[str],
    written: int,
    db_path: Path,
) -> None:
    """Print a short summary of the FX sync run."""
    _console.print(
        f"[green]Synced[/] [bold]{written}[/] new rate(s) for "
        f"[bold]{tax_year.label}[/] "
        f"across {', '.join(currencies)} "
        f"[dim]→ {db_path}[/]"
    )


if __name__ == "__main__":  # pragma: no cover — direct execution path
    app()
