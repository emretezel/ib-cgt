"""Minimal Typer CLI — `db init`, `ingest`, `trades`, `fx sync`, `match futures`.

This ships ahead of the full CLI plan (architecture step 8) so the
ingestion, FX, and futures-matching layers are runnable end-to-end.
Commands:

* ``ib-cgt db init`` — open the configured DB and apply migrations.
* ``ib-cgt ingest PATH`` — parse and persist an IB HTML statement.
* ``ib-cgt trades [filters]`` — print a rich table of stored trades.
* ``ib-cgt fx sync [--currency CODE]...`` — for every currency seen
  in imported statements, pull newer ECB rates from Frankfurter into
  the local cache. First run for a pair pulls the full ECB history;
  subsequent runs only fetch what is newer than what is cached.
* ``ib-cgt match futures [filters]`` — read-only debug command that
  runs the per-contract close-out engine against ingested futures
  trades and renders realisations / open positions to the terminal.
  Persists nothing.

The command surface and help text are deliberately terse; we'll flesh
them out when the calculator and reporting commands land in their own
plans.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ib_cgt.config import resolve_db_path, resolve_fx_base_url
from ib_cgt.db import (
    FXRateRepo,
    InstrumentRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.domain import (
    FutureInstrument,
    FutureRealisation,
    Money,
    OpenPosition,
    Trade,
)
from ib_cgt.fx import FrankfurterClient, FXService, RateNotFoundError
from ib_cgt.ingest import IngestResult, ingest_statement
from ib_cgt.rules import (
    FutureResult,
    FutureRuleEngine,
    InconsistentTradeError,
    WrongAssetClassError,
)

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
match_app = typer.Typer(
    help=(
        "Run rule engines against ingested data without persisting "
        "results — for debugging and audit only."
    ),
    no_args_is_help=True,
)
app.add_typer(match_app, name="match")

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


# Tables emptied by `db reset`, in dependency order. Children (or rows
# referenced by other tables we are also wiping) come first so the FK
# enforcement engine sees a consistent state at every step. The final
# two entries cascade through their child tables: deleting from
# `tax_runs` removes `matched_disposals`; deleting from `instruments`
# removes the four asset-class child rows.
_RESET_TABLES_DATA: tuple[str, ...] = (
    "tax_runs",  # cascades to matched_disposals
    "trades",
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
            _console.print("[yellow]Reset cancelled.[/]")
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
    with conn:
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
    for name, count in cleared.items():
        table.add_row(name, str(count))
    _console.print(table)


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
    replace: Annotated[
        bool,
        typer.Option(
            "--replace",
            "-r",
            help=(
                "If this statement was already imported, delete the prior "
                "import (cascading to its trades) and re-ingest fresh. "
                "Useful during development when the parser or mapper "
                "changes and you want to re-process a fixture."
            ),
        ),
    ] = False,
) -> None:
    """Parse an IB statement and persist its trades into the database."""
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Surface a friendly error if the DB hasn't been initialised yet —
        # SQLite will create empty files on `connect`, so the missing
        # schema manifests as a foreign-key failure deep in the ingestor.
        apply_migrations(conn)
        result = ingest_statement(path, conn, replace=replace)
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

    verb = "Replaced" if result.replaced else "Imported"
    _console.print(
        f"[green]{verb}[/] [bold]{source.name}[/] "
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
            _console.print(
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
    _console.print(table)
    _console.print(f"[green]Synced[/] [bold]{total}[/] new rate(s) total.")


# ---------------------------------------------------------------------------
# `match futures`
# ---------------------------------------------------------------------------


@match_app.command("futures")
def match_futures(
    account: Annotated[
        str | None,
        typer.Option("--account", "-a", help="Filter to a single IB account id."),
    ] = None,
    symbol: Annotated[
        str | None,
        typer.Option(
            "--symbol",
            "-s",
            help=(
                "Filter to one futures symbol (e.g. 'ES'). One symbol "
                "typically spans multiple expiries — this narrows by "
                "symbol but does not pin a single contract."
            ),
        ),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help=(
                "Inclusive lower bound on trade_date (YYYY-MM-DD). "
                "Caveat: matching is sensitive to date clipping; for "
                "production output omit this. Use only to construct "
                "edge-case scenarios for debugging."
            ),
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help=(
                "Inclusive upper bound on trade_date (YYYY-MM-DD). "
                "Same caveat as --since: clipping mid-slice produces "
                "misleading partial-close realisations."
            ),
        ),
    ] = None,
) -> None:
    """Dry-run the futures rule engine against ingested trades.

    Walks every futures instrument that matches the filters, runs
    `FutureRuleEngine.compute` against its trade history using the
    real `FXService`, and prints the resulting realisations and open
    positions. Nothing is written to the database — this command is a
    read-only audit tool.
    """
    since_date = _parse_iso_date(since, "--since")
    until_date = _parse_iso_date(until, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Defensive — same as `ingest`. A fresh DB file would otherwise
        # surface as a confusing "no such table" error.
        apply_migrations(conn)
        fx_service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        engine = FutureRuleEngine(fx_service)
        instruments = InstrumentRepo(conn).list_futures(symbol=symbol)
        results = _run_match_futures(
            conn=conn,
            engine=engine,
            instruments=instruments,
            account=account,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not instruments:
        _console.print("[yellow]No futures instruments match the given filters.[/]")
        return

    _render_match_futures(results, db_path)


def _parse_iso_date(value: str | None, flag_name: str) -> date | None:
    """Parse an optional ISO-format date, mapping errors to BadParameter."""
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        # Same friendly-error pattern as `trades --since`. Surfaces a
        # one-line `BadParameter` instead of a deep traceback.
        raise typer.BadParameter(f"invalid {flag_name} value {value!r}: {exc}") from exc


# A per-instrument outcome — either a successful `FutureResult` or the
# exception the engine raised. The CLI walks instruments rather than
# aborting on the first failure, so error isolation is the whole
# point of carrying both shapes through the same channel.
_MatchFuturesRow = tuple[FutureInstrument, FutureResult | None, Exception | None]


def _run_match_futures(
    *,
    conn: sqlite3.Connection,
    engine: FutureRuleEngine,
    instruments: list[tuple[int, FutureInstrument]],
    account: str | None,
    since: date | None,
    until: date | None,
) -> list[_MatchFuturesRow]:
    """Run the futures engine per-instrument with per-instrument error capture."""
    trade_repo = TradeRepo(conn)
    out: list[_MatchFuturesRow] = []
    for instrument_id, instrument in instruments:
        trades = trade_repo.for_instrument_with_ids(
            instrument_id,
            account_id=account,
            since=since,
            until=until,
        )
        # Catch only the engine and FX errors that this command is
        # specifically designed to surface — anything else (programmer
        # error, IO error) should still propagate so the operator
        # sees it loud and clear.
        try:
            result = engine.compute(instrument, trades)
        except (WrongAssetClassError, InconsistentTradeError, RateNotFoundError) as exc:
            out.append((instrument, None, exc))
            continue
        out.append((instrument, result, None))
    return out


# ---------------------------------------------------------------------------
# Rendering helpers (Rich)
# ---------------------------------------------------------------------------


def _render_match_futures(rows: list[_MatchFuturesRow], db_path: Path) -> None:
    """Render the realisations, open-positions, and summary tables."""
    _render_match_futures_realisations(rows)
    _render_match_futures_open_positions(rows)
    _render_match_futures_summary(rows, db_path)


def _render_match_futures_realisations(rows: list[_MatchFuturesRow]) -> None:
    """Print a per-instrument section: bold header then a Rich realisations table.

    Per-instrument sections (rather than one combined table) keep the
    output readable on narrow terminals — Rich's auto-sizing was
    splitting both the divider text and the empty-state placeholder
    across multiple lines per cell when everything was crammed into a
    single 9-column table.
    """
    _console.print("[bold]Futures realisations (dry-run)[/]")
    for instrument, result, error in rows:
        divider = _instrument_divider(instrument)
        _console.print(f"\n[bold cyan]{divider}[/]")
        if error is not None:
            # The ERROR line is the operator's whole reason for
            # running this command — print it plainly outside any
            # table so it can't be wrapped or hidden.
            _console.print(f"  [red]ERROR:[/] {error}")
            continue
        assert result is not None  # mypy — error/result are mutually exclusive
        if not result.realisations:
            _console.print("  [dim](no realisations)[/]")
            continue
        table = Table(header_style="bold", show_lines=False)
        table.add_column("Open ID", justify="right")
        table.add_column("Close ID", justify="right")
        table.add_column("Side")
        table.add_column("Open Date")
        table.add_column("Close Date")
        table.add_column("Qty", justify="right")
        table.add_column("Cost (GBP)", justify="right")
        table.add_column("Proceeds (GBP)", justify="right")
        table.add_column("Gain (GBP)", justify="right")
        for realisation in result.realisations:
            table.add_row(*_realisation_to_cells(realisation))
        _console.print(table)


def _render_match_futures_open_positions(rows: list[_MatchFuturesRow]) -> None:
    """One flat table of every still-open slice across all instruments."""
    open_positions = [
        (instrument, position)
        for instrument, result, error in rows
        if error is None and result is not None
        for position in result.open_positions
    ]
    if not open_positions:
        _console.print("[dim]No open positions remain after matching.[/]")
        return

    table = Table(
        title="Open positions at end of input",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Symbol")
    table.add_column("Expiry")
    table.add_column("Side")
    table.add_column("Open Date")
    table.add_column("Open Trade ID", justify="right")
    table.add_column("Qty", justify="right")
    table.add_column("Open Price")
    table.add_column("Fees Remaining")

    for _, position in open_positions:
        table.add_row(*_open_position_to_cells(position))

    _console.print(table)


def _render_match_futures_summary(rows: list[_MatchFuturesRow], db_path: Path) -> None:
    """Small summary: instrument counts, totals, total realised gain."""
    error_count = sum(1 for _, _, error in rows if error is not None)
    realisation_count = sum(
        len(result.realisations)
        for _, result, error in rows
        if error is None and result is not None
    )
    open_count = sum(
        len(result.open_positions)
        for _, result, error in rows
        if error is None and result is not None
    )
    # Aggregate gain via Money so currency invariants stay enforced —
    # every realisation is GBP per `FutureRealisation.__post_init__`,
    # so the running total stays GBP without explicit checks here.
    total_gain = Money.gbp(Decimal("0"))
    for _, result, error in rows:
        if error is not None or result is None:
            continue
        for realisation in result.realisations:
            total_gain = total_gain + realisation.gain_gbp

    table = Table(
        title="Summary",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
    )
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Instruments processed", str(len(rows)))
    table.add_row("…with errors", str(error_count))
    table.add_row("Realisations", str(realisation_count))
    table.add_row("Open positions", str(open_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row("Total realised gain (GBP)", f"[{gain_style}]{total_gain.amount}[/]")

    _console.print(table)


def _instrument_divider(instrument: FutureInstrument) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{instrument.symbol} {instrument.expiry_date.isoformat()} ({instrument.currency})"


def _realisation_to_cells(realisation: FutureRealisation) -> tuple[str, ...]:
    """Project a `FutureRealisation` into the columns of the realisations table."""
    gain = realisation.gain_gbp
    gain_style = "green" if gain.amount >= 0 else "red"
    return (
        str(realisation.open_trade_id),
        str(realisation.close_trade_id),
        realisation.side,
        realisation.open_date.isoformat(),
        realisation.close_date.isoformat(),
        f"{realisation.quantity}",
        f"{realisation.cost_gbp.amount}",
        f"{realisation.proceeds_gbp.amount}",
        f"[{gain_style}]{gain.amount}[/]",
    )


def _open_position_to_cells(position: OpenPosition) -> tuple[str, ...]:
    """Project an `OpenPosition` into the columns of the open-positions table."""
    return (
        position.instrument.symbol,
        position.instrument.expiry_date.isoformat(),
        position.side,
        position.open_date.isoformat(),
        str(position.open_trade_id),
        f"{position.quantity_remaining}",
        f"{position.open_price.amount} {position.open_price.currency}",
        f"{position.fees_remaining.amount} {position.fees_remaining.currency}",
    )


if __name__ == "__main__":  # pragma: no cover — direct execution path
    app()
