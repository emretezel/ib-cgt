"""Minimal Typer CLI — `db init`, `ingest`, `trades`, `fx sync`, `match` group.

This ships ahead of the full CLI plan (architecture step 8) so the
ingestion, FX, and rule-engine layers are runnable end-to-end.
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
* ``ib-cgt match stocks [filters]`` — read-only debug command that
  runs the four-rule UK matching engine against ingested stock
  trades. Cross-account by design (S.104 pools span every account
  belonging to the taxpayer). Persists nothing.
* ``ib-cgt match fx [filters]`` — read-only debug command that runs
  the four-rule UK matching engine per non-GBP currency pool. A
  cross-currency trade (e.g. ``EUR.USD``) feeds two pools at once.
  Cross-account by design. Persists nothing.

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
    AssetClass,
    DirectAcquisition,
    FutureInstrument,
    FutureRealisation,
    FXInstrument,
    MatchedDisposal,
    Money,
    OpenPosition,
    StockInstrument,
    TaxLot,
    TaxLotSnapshot,
    Trade,
    UnmatchedAcquisition,
)
from ib_cgt.fx import FrankfurterClient, FXService, RateNotFoundError
from ib_cgt.ingest import IngestResult, ingest_statement
from ib_cgt.rules import (
    FutureResult,
    FutureRuleEngine,
    FXRuleEngine,
    InconsistentTradeError,
    MatchingResult,
    StockRuleEngine,
    UnmatchedDisposalError,
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
        # The same FXService wiring used by `match` / `compute`. Needed
        # so cross-currency cash-merger Corporate Actions can be
        # synthesized into SELL trades at ingest time. If FX rates
        # haven't been synced for the merger's date+currencies, the
        # synthesis raises `RateNotFoundError`; the operator runs
        # `ib-cgt fx sync` to populate the cache and retries.
        fx_service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        result = ingest_statement(path, conn, replace=replace, fx_service=fx_service)
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
    summary = (
        f"[green]{verb}[/] [bold]{source.name}[/] "
        f"for account [bold]{result.account_id}[/]: "
        f"{result.inserted_count} new / {result.trade_count} parsed"
    )
    if result.merger_trade_count:
        plural = "" if result.merger_trade_count == 1 else "s"
        summary += f" (incl. {result.merger_trade_count} corporate-action disposal{plural})"
    _console.print(summary + ".")


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
    """Render the realisations, open-positions, summary, and errors blocks.

    Errors are deliberately printed last — collected together at the
    bottom of the output rather than scattered through the
    per-instrument realisations sections — so the operator can scan
    every failure in one place after a multi-instrument run.
    """
    _render_match_futures_realisations(rows)
    _render_match_futures_open_positions(rows)
    _render_match_futures_summary(rows, db_path)
    _render_match_futures_errors(rows)


def _render_match_futures_realisations(rows: list[_MatchFuturesRow]) -> None:
    """Print a per-instrument section: bold header then a Rich realisations table.

    Per-instrument sections (rather than one combined table) keep the
    output readable on narrow terminals and let the native-currency
    column headers carry the contract's own currency
    (`Gross P&L (USD)`, `Open Fee (EUR)`, etc.) without ambiguity.
    Each table is 14 columns: identifiers + dates + qty, then the
    gross P&L plus the open and close commissions in native currency,
    then the two FX rates, then the SA108 GBP triple — proceeds,
    cost, and the colour-coded gain.
    """
    _console.print("[bold]Futures realisations (dry-run)[/]")
    for instrument, result, error in rows:
        # Errors are surfaced together at the end of the output by
        # `_render_match_futures_errors`; skip them here so this
        # section only contains successful per-instrument tables.
        if error is not None:
            continue
        assert result is not None  # mypy — error/result are mutually exclusive
        divider = _instrument_divider(instrument)
        _console.print(f"\n[bold cyan]{divider}[/]")
        if not result.realisations:
            _console.print("  [dim](no realisations)[/]")
            continue
        ccy = instrument.currency
        table = Table(header_style="bold", show_lines=False)
        table.add_column("Open ID", justify="right")
        table.add_column("Close ID", justify="right")
        table.add_column("Side")
        table.add_column("Open Date")
        table.add_column("Close Date")
        table.add_column("Qty", justify="right")
        # Native-currency audit columns. Gross P&L is signed — the
        # CFD disposal consideration per TCGA92/S143(5). The open and
        # close commissions are shown separately so each one can be
        # reconciled against its source trade; the GBP cost column
        # combines them after translating each at its own date's
        # FX rate.
        table.add_column(f"Gross P&L ({ccy})", justify="right")
        table.add_column(f"Open Fee ({ccy})", justify="right")
        table.add_column(f"Close Fee ({ccy})", justify="right")
        # FX rates: stored "1 GBP = r native". 4dp gives 0.5 bp of
        # display precision — enough for any G10 cross.
        table.add_column("FX open", justify="right")
        table.add_column("FX close", justify="right")
        # GBP triple — the SA108 figures. Proceeds may be negative
        # on a losing trade (HMRC: "money paid is treated as an
        # incidental cost of disposal"); cost is always
        # non-negative; gain is signed.
        table.add_column("Proceeds (GBP)", justify="right")
        table.add_column("Cost (GBP)", justify="right")
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
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{_format_money_2dp(total_gain)}[/]",
    )

    _console.print(table)


def _render_match_futures_errors(rows: list[_MatchFuturesRow]) -> None:
    """Print every per-instrument error in one block at the end of the output.

    Each failing instrument gets a single bullet line — the same
    instrument label used in the realisations section followed by the
    captured exception's message. Returns silently when no rows
    failed so a clean run produces no trailing block at all.
    """
    error_rows = [(instrument, error) for instrument, _, error in rows if error is not None]
    if not error_rows:
        return
    _console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        _console.print(f"  [bold cyan]{_instrument_divider(instrument)}[/] [red]→ {error}[/]")


def _instrument_divider(instrument: FutureInstrument) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{instrument.symbol} {instrument.expiry_date.isoformat()} ({instrument.currency})"


def _realisation_to_cells(realisation: FutureRealisation) -> tuple[str, ...]:
    """Project a `FutureRealisation` into the 14 columns of the realisations table.

    Column order matches the `add_column` calls in
    `_render_match_futures_realisations`. Under the CFD model, the
    two FX columns map directly onto the realisation's two date
    fields — no LONG/SHORT special-casing.

    Money formatting:
    - Gross P&L: signed (`+,.2f`) — a losing trade reads at a glance
      without needing a colour code.
    - Proceeds (GBP) and Gain (GBP): plain 2dp with green/red
      colour-code on sign.
    - Cost (GBP), Open Fee, Close Fee: plain 2dp; all non-negative.
    """
    proceeds = realisation.proceeds_gbp
    proceeds_style = "green" if proceeds.amount >= 0 else "red"
    gain = realisation.gain_gbp
    gain_style = "green" if gain.amount >= 0 else "red"
    return (
        str(realisation.open_trade_id),
        str(realisation.close_trade_id),
        realisation.side,
        realisation.open_date.isoformat(),
        realisation.close_date.isoformat(),
        f"{realisation.quantity}",
        _format_money_signed_2dp(realisation.gross_pnl_native),
        _format_money_2dp(realisation.open_fee_native),
        _format_money_2dp(realisation.close_fee_native),
        _format_fx_rate(realisation.open_fx_rate),
        _format_fx_rate(realisation.close_fx_rate),
        f"[{proceeds_style}]{_format_money_2dp(proceeds)}[/]",
        _format_money_2dp(realisation.cost_gbp),
        f"[{gain_style}]{_format_money_2dp(gain)}[/]",
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
        f"{_format_money_2dp(position.open_price)} {position.open_price.currency}",
        f"{_format_money_2dp(position.fees_remaining)} {position.fees_remaining.currency}",
    )


def _format_money_2dp(value: Money) -> str:
    """Format a `Money.amount` to 2dp with thousands separators.

    Currency is *not* included — every consumer either has the
    currency in the column header (realisations table, summary) or
    appends it explicitly per cell (open-positions table).
    """
    return f"{value.amount:,.2f}"


def _format_money_signed_2dp(value: Money) -> str:
    """Format a `Money.amount` to 2dp with an explicit leading sign.

    Used for the Gross P&L column where the sign carries the whole
    "winning vs losing trade" signal — a leading `+` or `-` makes it
    visible without a colour code (which we reserve for the GBP
    proceeds/gain triple).
    """
    return f"{value.amount:+,.2f}"


def _format_fx_rate(rate: Decimal) -> str:
    """Format an FX rate to 4dp, no thousands separator.

    The 4dp window covers everything from JPY (~150) to KRW
    (~1,300) without losing 0.5 bp of precision; thousands
    separators are unhelpful for figures this small.
    """
    return f"{rate:.4f}"


# ---------------------------------------------------------------------------
# `match stocks`
# ---------------------------------------------------------------------------


@match_app.command("stocks")
def match_stocks(
    symbol: Annotated[
        str | None,
        typer.Option(
            "--symbol",
            "-s",
            help="Filter to one stock symbol (e.g. 'AAPL').",
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
                "edge-case scenarios for debugging — clipping mid-"
                "history breaks S.104 pool reconstruction."
            ),
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help=(
                "Inclusive upper bound on trade_date (YYYY-MM-DD). "
                "Same caveat as --since: clipping mid-history breaks "
                "S.104 pool reconstruction."
            ),
        ),
    ] = None,
) -> None:
    """Dry-run the stock rule engine against ingested trades.

    Walks every stock instrument that matches the filters, runs
    `StockRuleEngine.compute` against its trade history (across all
    accounts — UK CGT pools span every account belonging to the
    taxpayer) using the real `FXService`, and prints the resulting
    matched-disposal chunks, pool residuals, and final-pool
    aggregates. Nothing is written to the database — this command
    is a read-only audit tool.

    Note that there is intentionally no ``--account`` flag: filtering
    by account would silently break the matching invariants for
    instruments with cross-account histories (S.104 pools span
    accounts per `docs/architecture.md §Scope — Accounts`).
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
        engine = StockRuleEngine(fx_service)
        instruments = InstrumentRepo(conn).list_stocks(symbol=symbol)
        results = _run_match_stocks(
            conn=conn,
            engine=engine,
            instruments=instruments,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not instruments:
        _console.print("[yellow]No stock instruments match the given filters.[/]")
        return

    _render_match_stocks(results, db_path)


# A per-instrument outcome — either a successful `MatchingResult`,
# the trade-id → trade-date map needed by the renderer for direct-
# match basis dates, or the exception the engine raised. The CLI
# walks instruments rather than aborting on the first failure, so
# error isolation is the point of carrying all three through one
# channel.
_MatchStocksRow = tuple[
    StockInstrument,
    MatchingResult | None,
    dict[int, date],
    Exception | None,
]


def _run_match_stocks(
    *,
    conn: sqlite3.Connection,
    engine: StockRuleEngine,
    instruments: list[tuple[int, StockInstrument]],
    since: date | None,
    until: date | None,
) -> list[_MatchStocksRow]:
    """Run the stock engine per-instrument with per-instrument error capture.

    Builds a `{trade_id: trade_date}` map per instrument so the
    renderer can show the basis acquisition's date alongside its
    trade id (which makes short round-trips readable: the row pairs
    the sell-short Disp Date with the buy-to-cover Acq Date).
    """
    trade_repo = TradeRepo(conn)
    out: list[_MatchStocksRow] = []
    for instrument_id, instrument in instruments:
        # Cross-account: account_id=None pulls every trade for this
        # instrument across every account. Per the module docstring,
        # filtering by account here would break the S.104 pool.
        trades = trade_repo.for_instrument_with_ids(
            instrument_id,
            account_id=None,
            since=since,
            until=until,
        )
        # Trade-id → trade-date map for the renderer's "Acq Date"
        # column (cheap; the trades are already in memory).
        date_map = {trade_id: trade.trade_date for trade_id, trade in trades}
        try:
            result = engine.compute(instrument, trades)
        except (
            WrongAssetClassError,
            InconsistentTradeError,
            UnmatchedDisposalError,
            RateNotFoundError,
        ) as exc:
            out.append((instrument, None, date_map, exc))
            continue
        out.append((instrument, result, date_map, None))
    return out


# ---------------------------------------------------------------------------
# Rendering helpers — `match stocks`
# ---------------------------------------------------------------------------


def _render_match_stocks(rows: list[_MatchStocksRow], db_path: Path) -> None:
    """Render matched-disposals, residuals, final-pool, summary, errors."""
    _render_match_stocks_disposals(rows)
    _render_match_stocks_residuals(rows)
    _render_match_stocks_final_pools(rows)
    _render_match_stocks_summary(rows, db_path)
    _render_match_stocks_errors(rows)


def _render_match_stocks_disposals(rows: list[_MatchStocksRow]) -> None:
    """Per-instrument section: bold header then a Rich matched-disposals table.

    Columns, in display order:
      Disp ID, Disp Date, Rule, Qty, Acq ID / Basis, Acq Date,
      Proceeds (GBP), Disp Fees (GBP), Cost (GBP), Acq Fees (GBP),
      Gain (GBP).

    Each fee column sits next to its parent total — `Disp Fees`
    pairs with `Proceeds`, `Acq Fees` pairs with `Cost` — so the
    auditor can read principal vs. fees without scanning across the
    whole row. Subset semantics: `Disp Fees` is already deducted
    from `Proceeds`, and `Acq Fees` is already inside `Cost`.
    """
    _console.print("[bold]Stock matched disposals (dry-run)[/]")
    for instrument, result, date_map, error in rows:
        if error is not None:
            # Errors land in the trailing block — same pattern as
            # `match futures`.
            continue
        assert result is not None  # mypy — error/result are mutually exclusive
        divider = _stock_divider(instrument)
        _console.print(f"\n[bold cyan]{divider}[/]")
        if not result.matched_disposals:
            _console.print("  [dim](no matched disposals)[/]")
            continue
        table = Table(header_style="bold", show_lines=False)
        table.add_column("Disp ID", justify="right")
        table.add_column("Disp Date")
        table.add_column("Rule")
        table.add_column("Qty", justify="right")
        table.add_column("Acq ID / Basis")
        table.add_column("Acq Date")
        table.add_column("Proceeds (GBP)", justify="right")
        table.add_column("Disp Fees (GBP)", justify="right")
        table.add_column("Cost (GBP)", justify="right")
        table.add_column("Acq Fees (GBP)", justify="right")
        table.add_column("Gain (GBP)", justify="right")
        for md in result.matched_disposals:
            table.add_row(*_matched_disposal_to_cells(md, date_map))
        _console.print(table)


def _render_match_stocks_residuals(rows: list[_MatchStocksRow]) -> None:
    """One flat table of every UnmatchedAcquisition across all instruments."""
    residuals: list[tuple[StockInstrument, UnmatchedAcquisition]] = [
        (instrument, ua)
        for instrument, result, _, error in rows
        if error is None and result is not None
        for ua in result.unmatched_acquisitions
    ]
    if not residuals:
        _console.print("[dim]No pool residuals after matching.[/]")
        return

    table = Table(
        title="Pool residuals at end of input",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Symbol")
    table.add_column("Currency")
    table.add_column("Acq ID", justify="right")
    table.add_column("Acq Date")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Cost Remaining (GBP)", justify="right")
    for instrument, ua in residuals:
        table.add_row(
            instrument.symbol,
            instrument.currency,
            str(ua.trade_id),
            ua.acquisition_date.isoformat(),
            f"{ua.quantity_remaining}",
            _format_money_2dp(ua.cost_remaining_gbp),
        )
    _console.print(table)


def _render_match_stocks_final_pools(rows: list[_MatchStocksRow]) -> None:
    """Per-instrument final-pool aggregate — one flat table.

    Skips instruments whose pool is empty (the typical post-match
    state for an instrument that fully closed every position).
    """
    pools: list[tuple[StockInstrument, TaxLot]] = [
        (instrument, result.final_pool)
        for instrument, result, _, error in rows
        if error is None and result is not None and result.final_pool.quantity > 0
    ]
    if not pools:
        return

    table = Table(
        title="Final S.104 pools",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Symbol")
    table.add_column("Currency")
    table.add_column("Pool Qty", justify="right")
    table.add_column("Pool Cost (GBP)", justify="right")
    table.add_column("Avg Cost (GBP)", justify="right")
    for instrument, pool in pools:
        table.add_row(
            instrument.symbol,
            instrument.currency,
            f"{pool.quantity}",
            _format_money_2dp(pool.total_cost_gbp),
            _format_money_2dp(pool.average_cost_gbp),
        )
    _console.print(table)


def _render_match_stocks_summary(rows: list[_MatchStocksRow], db_path: Path) -> None:
    """Small summary: counts and total realised gain across all instruments."""
    error_count = sum(1 for _, _, _, error in rows if error is not None)
    md_count = sum(
        len(result.matched_disposals)
        for _, result, _, error in rows
        if error is None and result is not None
    )
    total_gain = Money.gbp(Decimal("0"))
    for _, result, _, error in rows:
        if error is not None or result is None:
            continue
        for md in result.matched_disposals:
            total_gain = total_gain + md.gain_gbp

    table = Table(
        title="Summary",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
    )
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Instruments processed", str(len(rows)))
    table.add_row("…with errors", str(error_count))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{_format_money_2dp(total_gain)}[/]",
    )
    _console.print(table)


def _render_match_stocks_errors(rows: list[_MatchStocksRow]) -> None:
    """Print every per-instrument error in one block at the end."""
    error_rows = [(instrument, error) for instrument, _, _, error in rows if error is not None]
    if not error_rows:
        return
    _console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        _console.print(f"  [bold cyan]{_stock_divider(instrument)}[/] [red]→ {error}[/]")


def _stock_divider(instrument: StockInstrument) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{instrument.symbol} ({instrument.currency})"


def _matched_disposal_to_cells(md: MatchedDisposal, date_map: dict[int, date]) -> tuple[str, ...]:
    """Project a `MatchedDisposal` into the 11 columns of the disposals table.

    `Acq ID / Basis` and `Acq Date` are basis-aware:
    - `DirectAcquisition` (SAME_DAY / BED_AND_BREAKFAST /
      LATER_ACQUISITION) → `acq #N` and the acquisition's
      `trade_date` looked up via `date_map`.
    - `TaxLotSnapshot` (SECTION_104) → `S.104 pool: qty=Q, avg=£X`
      and an em-dash for the date (the pool has no single
      acquisition date).

    The two fee columns (`Disp Fees`, `Acq Fees`) are rendered
    without colour styling — fees are always non-negative, so a
    sign-based colour cue would be noise. They sit next to their
    parent totals in the column ordering set by
    `_render_match_stocks_disposals`.
    """
    proceeds = md.matched_proceeds_gbp
    proceeds_style = "green" if proceeds.amount >= 0 else "red"
    gain = md.gain_gbp
    gain_style = "green" if gain.amount >= 0 else "red"
    basis_text, acq_date_text = _basis_cells(md.basis, date_map)
    return (
        str(md.disposal_trade_id),
        md.disposal_date.isoformat(),
        md.match_rule.value,
        f"{md.matched_quantity}",
        basis_text,
        acq_date_text,
        f"[{proceeds_style}]{_format_money_2dp(proceeds)}[/]",
        _format_money_2dp(md.matched_disposal_fees_gbp),
        _format_money_2dp(md.matched_cost_gbp),
        _format_money_2dp(md.matched_acquisition_fees_gbp),
        f"[{gain_style}]{_format_money_2dp(gain)}[/]",
    )


def _basis_cells(
    basis: DirectAcquisition | TaxLotSnapshot, date_map: dict[int, date]
) -> tuple[str, str]:
    """Return `(basis_text, acq_date_text)` for the two basis-aware columns."""
    if isinstance(basis, DirectAcquisition):
        acq_date = date_map.get(basis.acquisition_trade_id)
        # `acq_date` should always be present for any acquisition the
        # engine matched against — but we render an em-dash defensively
        # if a future calculator-orchestrator path ever feeds a basis
        # whose trade_id isn't in the per-instrument map.
        date_text = acq_date.isoformat() if acq_date is not None else "—"
        return f"acq #{basis.acquisition_trade_id}", date_text
    # SECTION_104 — TaxLotSnapshot. Show the pool's pre-draw size
    # and average cost so the auditor can reconstruct the basis.
    pool_text = (
        f"S.104 pool: qty={basis.quantity_before}, avg=£{_format_money_2dp(basis.average_cost_gbp)}"
    )
    return pool_text, "—"


# ---------------------------------------------------------------------------
# `match fx` — UK CGT matching per non-GBP currency pool
# ---------------------------------------------------------------------------


@match_app.command("fx")
def match_fx(
    currency: Annotated[
        str | None,
        typer.Option(
            "--currency",
            "-c",
            help="Filter to one non-GBP currency pool (e.g. 'USD').",
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
                "edge-case scenarios for debugging — clipping mid-"
                "history breaks S.104 pool reconstruction."
            ),
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help=(
                "Inclusive upper bound on trade_date (YYYY-MM-DD). "
                "Same caveat as --since: clipping mid-history breaks "
                "S.104 pool reconstruction."
            ),
        ),
    ] = None,
) -> None:
    """Dry-run the FX rule engine per non-GBP currency pool.

    Loads every ingested forex trade once, derives the set of
    non-GBP currencies that appear (as either base or quote), and
    runs `FXRuleEngine.compute(ccy, all_forex_trades)` per
    currency. A cross-currency trade like ``EUR.USD`` therefore
    contributes one acquisition / disposal leg to *each* of the
    two non-GBP pools it touches, with independent FX rates per
    leg. Nothing is written to the database — this command is a
    read-only audit tool.

    Note that there is intentionally no ``--account`` flag:
    filtering by account would silently break the matching
    invariants for currencies with cross-account histories
    (S.104 pools span accounts per
    ``docs/architecture.md §Scope — Accounts``).
    """
    since_date = _parse_iso_date(since, "--since")
    until_date = _parse_iso_date(until, "--until")
    if currency is not None:
        # Normalise here so both the engine and the renderer agree on
        # the casing — ISO-4217 codes are upper-case.
        currency = currency.upper()

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
        engine = FXRuleEngine(fx_service)
        forex_trades = TradeRepo(conn).for_asset_class(
            AssetClass.FX,
            since=since_date,
            until=until_date,
        )
        currencies = _resolve_fx_pool_currencies(forex_trades, requested=currency)
        results = _run_match_fx(
            engine=engine,
            forex_trades=forex_trades,
            currencies=currencies,
        )
    finally:
        conn.close()

    if not currencies:
        if currency is not None:
            _console.print(
                f"[yellow]Currency '{currency}' has no forex trades in the date range.[/]"
            )
        else:
            _console.print("[yellow]No forex trades found — nothing to match.[/]")
        return

    _render_match_fx(results, db_path)


# Per-currency outcome — either a successful `MatchingResult`, the
# trade-id → trade-date map needed by the renderer for direct-match
# basis dates, or the exception the engine raised. Mirrors the
# `_MatchStocksRow` shape so the rendering helpers can stay parallel.
_MatchFxRow = tuple[
    str,
    MatchingResult | None,
    dict[int, date],
    Exception | None,
]


def _resolve_fx_pool_currencies(
    forex_trades: list[tuple[int, Trade]],
    *,
    requested: str | None,
) -> list[str]:
    """Return the list of non-GBP currency pools to render, in stable order.

    When the user passes ``--currency``, we honour that single pool
    (validated against the trade set so the renderer doesn't run an
    engine call that's guaranteed to be empty). Otherwise we walk
    every trade and collect the union of `base` and `quote` ISO codes,
    minus GBP — sorted alphabetically so the output is stable
    run-to-run.
    """
    seen: set[str] = set()
    for _trade_id, trade in forex_trades:
        # Only forex trades reach this loop (TradeRepo filtered by
        # asset class) so the access on .currency_pair is safe — but
        # we guard with isinstance to keep mypy honest under strict.
        instrument = trade.instrument
        if not isinstance(instrument, FXInstrument):
            continue
        seen.add(instrument.currency_pair.base)
        seen.add(instrument.currency_pair.quote)
    seen.discard("GBP")
    if requested is None:
        return sorted(seen)
    if requested not in seen:
        return []
    return [requested]


def _run_match_fx(
    *,
    engine: FXRuleEngine,
    forex_trades: list[tuple[int, Trade]],
    currencies: list[str],
) -> list[_MatchFxRow]:
    """Run the FX engine per currency with per-currency error capture.

    Builds a `{trade_id: trade_date}` map shared across pools (the
    map is keyed by trade id, which is unique across the whole forex
    set). The renderer uses it to show a basis acquisition's date
    alongside its trade id.
    """
    date_map = {trade_id: trade.trade_date for trade_id, trade in forex_trades}
    out: list[_MatchFxRow] = []
    for ccy in currencies:
        try:
            result = engine.compute(ccy, forex_trades)
        except (
            WrongAssetClassError,
            InconsistentTradeError,
            UnmatchedDisposalError,
            RateNotFoundError,
            ValueError,
        ) as exc:
            out.append((ccy, None, date_map, exc))
            continue
        out.append((ccy, result, date_map, None))
    return out


# ---------------------------------------------------------------------------
# Rendering helpers — `match fx`
# ---------------------------------------------------------------------------


def _render_match_fx(rows: list[_MatchFxRow], db_path: Path) -> None:
    """Render matched-disposals, residuals, final-pool, summary, errors."""
    _render_match_fx_disposals(rows)
    _render_match_fx_residuals(rows)
    _render_match_fx_final_pools(rows)
    _render_match_fx_summary(rows, db_path)
    _render_match_fx_errors(rows)


def _render_match_fx_disposals(rows: list[_MatchFxRow]) -> None:
    """Per-currency section: bold header then a Rich matched-disposals table.

    Columns mirror `match stocks` exactly — same fee-pairing /
    subset-semantics conventions — so the auditor's eye doesn't have
    to retrain across asset classes.
    """
    _console.print("[bold]FX matched disposals (dry-run)[/]")
    for currency, result, date_map, error in rows:
        if error is not None:
            continue
        assert result is not None  # mypy — error/result are mutually exclusive
        divider = _fx_divider(currency)
        _console.print(f"\n[bold cyan]{divider}[/]")
        if not result.matched_disposals:
            _console.print("  [dim](no matched disposals)[/]")
            continue
        table = Table(header_style="bold", show_lines=False)
        table.add_column("Disp ID", justify="right")
        table.add_column("Disp Date")
        table.add_column("Rule")
        table.add_column("Qty", justify="right")
        table.add_column("Acq ID / Basis")
        table.add_column("Acq Date")
        table.add_column("Proceeds (GBP)", justify="right")
        table.add_column("Disp Fees (GBP)", justify="right")
        table.add_column("Cost (GBP)", justify="right")
        table.add_column("Acq Fees (GBP)", justify="right")
        table.add_column("Gain (GBP)", justify="right")
        for md in result.matched_disposals:
            table.add_row(*_matched_disposal_to_cells(md, date_map))
        _console.print(table)


def _render_match_fx_residuals(rows: list[_MatchFxRow]) -> None:
    """One flat table of every UnmatchedAcquisition across all currency pools."""
    residuals: list[tuple[str, UnmatchedAcquisition]] = [
        (currency, ua)
        for currency, result, _, error in rows
        if error is None and result is not None
        for ua in result.unmatched_acquisitions
    ]
    if not residuals:
        _console.print("[dim]No pool residuals after matching.[/]")
        return

    table = Table(
        title="Pool residuals at end of input",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Currency")
    table.add_column("Acq ID", justify="right")
    table.add_column("Acq Date")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Cost Remaining (GBP)", justify="right")
    for currency, ua in residuals:
        table.add_row(
            currency,
            str(ua.trade_id),
            ua.acquisition_date.isoformat(),
            f"{ua.quantity_remaining}",
            _format_money_2dp(ua.cost_remaining_gbp),
        )
    _console.print(table)


def _render_match_fx_final_pools(rows: list[_MatchFxRow]) -> None:
    """Per-currency final-pool aggregate. Skips empty pools."""
    pools: list[tuple[str, TaxLot]] = [
        (currency, result.final_pool)
        for currency, result, _, error in rows
        if error is None and result is not None and result.final_pool.quantity > 0
    ]
    if not pools:
        return

    table = Table(
        title="Final S.104 pools",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Currency")
    table.add_column("Pool Qty", justify="right")
    table.add_column("Pool Cost (GBP)", justify="right")
    table.add_column("Avg Cost (GBP)", justify="right")
    for currency, pool in pools:
        table.add_row(
            currency,
            f"{pool.quantity}",
            _format_money_2dp(pool.total_cost_gbp),
            _format_money_2dp(pool.average_cost_gbp),
        )
    _console.print(table)


def _render_match_fx_summary(rows: list[_MatchFxRow], db_path: Path) -> None:
    """Small summary: counts and total realised gain across all pools."""
    error_count = sum(1 for _, _, _, error in rows if error is not None)
    md_count = sum(
        len(result.matched_disposals)
        for _, result, _, error in rows
        if error is None and result is not None
    )
    total_gain = Money.gbp(Decimal("0"))
    for _, result, _, error in rows:
        if error is not None or result is None:
            continue
        for md in result.matched_disposals:
            total_gain = total_gain + md.gain_gbp

    table = Table(
        title="Summary",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
    )
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Currencies processed", str(len(rows)))
    table.add_row("…with errors", str(error_count))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{_format_money_2dp(total_gain)}[/]",
    )
    _console.print(table)


def _render_match_fx_errors(rows: list[_MatchFxRow]) -> None:
    """Print every per-currency error in one block at the end."""
    error_rows = [(currency, error) for currency, _, _, error in rows if error is not None]
    if not error_rows:
        return
    _console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for currency, error in error_rows:
        _console.print(f"  [bold cyan]{_fx_divider(currency)}[/] [red]→ {error}[/]")


def _fx_divider(currency: str) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{currency} vs GBP"


if __name__ == "__main__":  # pragma: no cover — direct execution path
    app()
