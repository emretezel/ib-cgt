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
* ``ib-cgt match bonds [filters]`` — read-only debug command that
  runs the bond rule engine against ingested bond trades. Exempt
  bonds (gilts / QCBs) surface in a "no CGT" summary table; non-
  exempt bonds run through the same four-rule matcher as stocks.
  Cross-account by design. Persists nothing.

The command surface and help text are deliberately terse; we'll flesh
them out when the calculator and reporting commands land in their own
plans.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import count
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ib_cgt.checks import CheckReport, CheckResult, Scope, Status, run_all
from ib_cgt.config import resolve_db_path, resolve_fx_base_url
from ib_cgt.db import (
    DividendRepo,
    FXRateRepo,
    InstrumentRepo,
    StatementRepo,
    StatementRow,
    StoredTrade,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.domain import (
    AssetClass,
    BondInstrument,
    DirectAcquisition,
    Dividend,
    DividendKind,
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
    TradeAction,
    UnmatchedAcquisition,
    UnmatchedDisposalChunk,
)
from ib_cgt.fx import FrankfurterClient, FXService, RateNotFoundError
from ib_cgt.ingest import IngestResult, ingest_statement
from ib_cgt.rules import (
    BondResult,
    BondRuleEngine,
    ExemptBondResult,
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
show_app = typer.Typer(
    help=(
        "Drill-down audit commands for matched output — print full "
        "trade / realisation / per-disposal context so figures from "
        "`match fx` (and friends) can be verified against the original "
        "IB statement by hand."
    ),
    no_args_is_help=True,
)
app.add_typer(show_app, name="show")
check_app = typer.Typer(
    help=(
        "Sanity checks against the live database. Tier A asserts data "
        "integrity (SQL only); Tiers B/C re-run the matching engines "
        "in memory and validate their results. Exit code is 0 clean, "
        "1 if any error fires, 2 if --strict and any warning fires."
    ),
    # `invoke_without_command=True` lets the callback fall through to
    # `check all` when `ib-cgt check` is invoked without a subcommand.
    # We deliberately do *not* set `no_args_is_help` — that would
    # short-circuit the callback and print help instead.
    invoke_without_command=True,
)
app.add_typer(check_app, name="check")
bonds_app = typer.Typer(
    help=(
        "Bond-instrument management — list ingested bonds and verify "
        "the CGT-exempt flag inferred at ingest time. Useful as a "
        "sanity check after re-ingesting statements with an updated "
        "gilt classifier or `IB_CGT_BONDS_EXEMPT` allowlist."
    ),
    no_args_is_help=True,
)
app.add_typer(bonds_app, name="bonds")

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
    "dividends",
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
    for name, n_rows in cleared.items():
        table.add_row(name, str(n_rows))
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
    if result.maturity_trade_count:
        plural = "" if result.maturity_trade_count == 1 else "s"
        summary += f" (incl. {result.maturity_trade_count} bond maturity disposal{plural})"
    if result.skipped_maturity_count:
        plural = "" if result.skipped_maturity_count == 1 else "s"
        summary += (
            f" (skipped {result.skipped_maturity_count} bond maturity row{plural} with "
            "no matching bond instrument)"
        )
    if result.dividend_count:
        plural = "" if result.dividend_count == 1 else "s"
        summary += (
            f"; {result.dividends_inserted} new / {result.dividend_count} dividend cashflow{plural}"
        )
    if result.bond_coupon_count:
        plural = "" if result.bond_coupon_count == 1 else "s"
        summary += (
            f"; {result.bond_coupons_inserted} new / {result.bond_coupon_count} bond coupon{plural}"
        )
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
        _format_qty_2dp(realisation.quantity),
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
        _format_qty_2dp(position.quantity_remaining),
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


def _format_qty_2dp(value: Decimal) -> str:
    """Format a quantity to 2dp with thousands separators.

    Used for every qty cell in the match command outputs. Two
    decimal places are wide enough for the fractional FX-pool
    amounts ("88.23 CHF") and stay tidy for whole-share / whole-
    contract figures ("100.00", "5.00").
    """
    return f"{value:,.2f}"


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
            _format_qty_2dp(ua.quantity_remaining),
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
            _format_qty_2dp(pool.quantity),
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
        _format_qty_2dp(md.matched_quantity),
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
        f"S.104 pool: qty={_format_qty_2dp(basis.quantity_before)}, "
        f"avg=£{_format_money_2dp(basis.average_cost_gbp)}"
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

    Loads every ingested forex trade, every non-GBP stock trade,
    every non-GBP futures trade, and runs `FutureRuleEngine` to
    derive realisations. The four cashflow streams are fed into
    `FXRuleEngine.compute(ccy, …)` per non-GBP currency so the
    pool reflects every foreign-currency cash movement IB reports
    (HMRC CG78315 — "foreign currency arising from any source").

    A cross-currency forex trade like ``EUR.USD`` contributes one
    leg to *each* of the two non-GBP pools it touches. Stock
    trades' settlement cash and futures realised P&L feed the
    relevant per-currency pool at trade_date / close_date with
    the corresponding GBP spot rate. Any residual disposal that
    can't be covered (typically an opening balance pre-dating the
    IB statements) surfaces as a yellow "unmatched disposals"
    warning rather than blanking the whole pool.

    Nothing is written to the database — this command is a
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
        audit = _load_fx_audit_data(conn, since=since_date, until=until_date)
        engine = FXRuleEngine(audit.fx_service)

        currencies = _resolve_fx_pool_currencies(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            dividends=audit.dividends,
            requested=currency,
        )
        source_descriptions = _build_fx_source_descriptions(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
        )
        date_map = _build_fx_event_date_map(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
        )
        id_label_map = _build_fx_id_label_map(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
            dividend_real_id=audit.dividend_real_id,
        )
        results = _run_match_fx(
            engine=engine,
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
            currencies=currencies,
            date_map=date_map,
            source_descriptions=source_descriptions,
            id_label_map=id_label_map,
        )
    finally:
        conn.close()

    if not currencies:
        if currency is not None:
            _console.print(f"[yellow]Currency '{currency}' has no events in the date range.[/]")
        else:
            _console.print("[yellow]No FX-relevant trades found — nothing to match.[/]")
        return

    _render_match_fx(results, db_path)


# Per-currency outcome carried through the renderer. `source_descriptions`
# augments the trade-id-keyed `date_map` with human-readable labels
# (e.g. "futures P&L close=…" for a synthetic id from a realisation).
# `id_label_map` is the *short* form used in narrow ID columns —
# `#5685` for real trade ids and `P&L #A→#B[i]` for futures
# realisations — so the table is citeable across runs.
_MatchFxRow = tuple[
    str,
    MatchingResult | None,
    dict[int, date],
    dict[int, str],
    dict[int, str],
    Exception | None,
]


def _resolve_fx_pool_currencies(
    *,
    forex_trades: list[tuple[int, Trade]],
    stock_trades: list[tuple[int, Trade]],
    future_trades: list[tuple[int, Trade]],
    dividends: list[tuple[int, Dividend]],
    requested: str | None,
) -> list[str]:
    """Return the list of non-GBP currency pools to render, in stable order.

    When the user passes ``--currency``, we honour that single pool
    (validated against the seen set so the renderer doesn't run an
    engine call that's guaranteed to be empty). Otherwise we walk
    every source and collect the union of currencies that appear,
    minus GBP — sorted alphabetically so the output is stable
    run-to-run. Dividends are surveyed so a pool with dividend-only
    activity (e.g. a still-held historical position dripping cash
    dividends with no trades in the window) still renders.
    """
    seen: set[str] = set()
    for _trade_id, trade in forex_trades:
        instrument = trade.instrument
        if not isinstance(instrument, FXInstrument):
            continue
        seen.add(instrument.currency_pair.base)
        seen.add(instrument.currency_pair.quote)
    for _trade_id, trade in stock_trades:
        seen.add(trade.instrument.currency)
    for _trade_id, trade in future_trades:
        seen.add(trade.instrument.currency)
    for _synth, dividend in dividends:
        seen.add(dividend.amount.currency)
    seen.discard("GBP")
    if requested is None:
        return sorted(seen)
    if requested not in seen:
        return []
    return [requested]


def _build_fx_source_descriptions(
    *,
    forex_trades: list[tuple[int, Trade]],
    stock_trades: list[tuple[int, Trade]],
    future_trades: list[tuple[int, Trade]],
    future_realisations: list[tuple[int, FutureRealisation, str]],
    dividends: list[tuple[int, Dividend]],
) -> dict[int, str]:
    """Build the trade-id → source-label map used by the FX renderer.

    Forex / stock / futures-fee events use the real `trades.trade_id`
    (globally unique). Realisation P&L events use the synthetic IDs
    the orchestrator generated (`itertools.count(10**12)`). Dividend
    events use a separate synthetic-ID range
    (`itertools.count(2 * 10**12)`) — same collision-avoidance
    motivation as realisations, just keyed on a different table.
    Multiple realisations can share a `close_trade_id`; the
    synthetic ID is what disambiguates them.
    """
    out: dict[int, str] = {}
    for tid, trade in forex_trades:
        out[tid] = f"forex {trade.instrument.symbol} {trade.action.value}"
    for tid, trade in stock_trades:
        out[tid] = f"stock {trade.instrument.symbol} {trade.action.value}"
    for tid, trade in future_trades:
        out[tid] = f"futures fee {trade.instrument.symbol} {trade.action.value}"
    for synth_id, realisation, _account in future_realisations:
        side = "P&L"
        out[synth_id] = (
            f"futures {side} {realisation.instrument.symbol} "
            f"open={realisation.open_trade_id} close={realisation.close_trade_id}"
        )
    for synth_id, dividend in dividends:
        out[synth_id] = f"dividend {dividend.instrument.symbol} {dividend.kind.value}"
    return out


def _build_fx_event_date_map(
    *,
    forex_trades: list[tuple[int, Trade]],
    stock_trades: list[tuple[int, Trade]],
    future_trades: list[tuple[int, Trade]],
    future_realisations: list[tuple[int, FutureRealisation, str]],
    dividends: list[tuple[int, Dividend]],
) -> dict[int, date]:
    """Map every event id (real or synthetic) to its event date.

    The renderer's "Acq Date" column reads this directly. Real
    trade events use `trade_date`; futures-realisation events use
    `close_date` (the date the P&L cashflow lands); dividend events
    use `pay_date` (the date the cash hits the foreign-currency
    balance).
    """
    out: dict[int, date] = {}
    for tid, trade in forex_trades:
        out[tid] = trade.trade_date
    for tid, trade in stock_trades:
        out[tid] = trade.trade_date
    for tid, trade in future_trades:
        out[tid] = trade.trade_date
    for synth_id, realisation, _account in future_realisations:
        out[synth_id] = realisation.close_date
    for synth_id, dividend in dividends:
        out[synth_id] = dividend.pay_date
    return out


@dataclass(frozen=True, slots=True)
class _FxAuditData:
    """Bundle of data needed to feed the FX engine, plus its FX service.

    Both `match fx` and `show match --disposal` need to load every
    forex / stock / futures trade and derive futures realisations
    via `FutureRuleEngine`. Bundling those into one DTO keeps both
    commands' bodies tidy and ensures they always feed the engine
    the *same* set of inputs.

    `dividends` carries `(synth_id, Dividend)` pairs ready for
    `FXRuleEngine.compute(..., dividends=...)`. The synth IDs are
    allocated from a high range (`itertools.count(2 * 10**12)`) so
    they don't collide with `trades.trade_id` values or with
    futures-realisation synth IDs (`10**12`-range). The map back
    from synth ID to real `dividend_id` lives on `dividend_real_id`
    so the renderer can show citeable labels like ``"Div #N"``.
    """

    forex_trades: list[tuple[int, Trade]]
    stock_trades: list[tuple[int, Trade]]
    future_trades: list[tuple[int, Trade]]
    future_realisations: list[tuple[int, FutureRealisation, str]]
    dividends: list[tuple[int, Dividend]]
    dividend_real_id: dict[int, int]
    fx_service: FXService


def _load_fx_audit_data(
    conn: sqlite3.Connection,
    *,
    since: date | None = None,
    until: date | None = None,
) -> _FxAuditData:
    """Load the four cashflow sources + run `FutureRuleEngine` per instrument.

    Same semantics `match fx` already uses: forex trades carried
    whole, non-GBP stock + futures trades filtered in Python, and
    the futures realisation list materialised by computing per
    futures instrument. Per-futures-instrument errors are
    swallowed — this loader's job is to assemble inputs for the FX
    engine, not to diagnose futures-engine failures (that's what
    `match futures` is for).
    """
    fx_service = FXService(
        FXRateRepo(conn),
        FrankfurterClient(base_url=resolve_fx_base_url()),
    )
    future_engine = FutureRuleEngine(fx_service)

    trade_repo = TradeRepo(conn)
    forex_trades = trade_repo.for_asset_class(AssetClass.FX, since=since, until=until)
    stock_trades = [
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.STOCK, since=since, until=until)
        if t.instrument.currency != "GBP"
    ]
    future_trades = [
        (tid, t)
        for tid, t in trade_repo.for_asset_class(AssetClass.FUTURE, since=since, until=until)
        if t.instrument.currency != "GBP"
    ]

    future_realisations: list[tuple[int, FutureRealisation, str]] = []
    synth_id_gen = count(10**12)
    future_trade_account: dict[int, str] = {tid: t.account_id for tid, t in future_trades}
    for instrument_id, instrument in InstrumentRepo(conn).list_futures():
        if instrument.currency == "GBP":
            continue
        inst_trades = trade_repo.for_instrument_with_ids(instrument_id, since=since, until=until)
        try:
            future_result = future_engine.compute(instrument, inst_trades)
        except (
            WrongAssetClassError,
            InconsistentTradeError,
            RateNotFoundError,
        ):
            continue
        for r in future_result.realisations:
            future_realisations.append(
                (
                    next(synth_id_gen),
                    r,
                    future_trade_account.get(r.close_trade_id, "U?"),
                )
            )

    # Dividends — load every non-GBP cashflow row across the whole
    # corpus and assign synth IDs from a separate high range so the
    # `id_label_map` machinery can resolve them without colliding
    # with trade IDs or futures-realisation synth IDs.
    dividends: list[tuple[int, Dividend]] = []
    dividend_real_id: dict[int, int] = {}
    dividend_synth_gen = count(2 * 10**12)
    div_repo = DividendRepo(conn)
    seen_currencies: set[str] = set()
    for _tid, t in stock_trades:
        seen_currencies.add(t.instrument.currency)
    for _tid, t in future_trades:
        seen_currencies.add(t.instrument.currency)
    for _tid, t in forex_trades:
        if isinstance(t.instrument, FXInstrument):
            seen_currencies.add(t.instrument.currency_pair.base)
            seen_currencies.add(t.instrument.currency_pair.quote)
    seen_currencies.discard("GBP")
    # The dividends-section can also reference instruments the user
    # never traded in the date window (e.g. a dividend on a still-held
    # historical position), so widen the currency set by a separate
    # query rather than relying purely on the trade-side intersection.
    dividend_currencies = conn.execute(
        "SELECT DISTINCT currency FROM dividends WHERE currency != 'GBP'"
    ).fetchall()
    seen_currencies.update(str(r["currency"]) for r in dividend_currencies)
    for ccy in sorted(seen_currencies):
        for did, dividend in div_repo.for_currency(ccy, since=since, until=until):
            synth = next(dividend_synth_gen)
            dividends.append((synth, dividend))
            dividend_real_id[synth] = did

    return _FxAuditData(
        forex_trades=forex_trades,
        stock_trades=stock_trades,
        future_trades=future_trades,
        future_realisations=future_realisations,
        dividends=dividends,
        dividend_real_id=dividend_real_id,
        fx_service=fx_service,
    )


def _build_fx_id_label_map(
    *,
    forex_trades: list[tuple[int, Trade]],
    stock_trades: list[tuple[int, Trade]],
    future_trades: list[tuple[int, Trade]],
    future_realisations: list[tuple[int, FutureRealisation, str]],
    dividends: list[tuple[int, Dividend]],
    dividend_real_id: dict[int, int],
) -> dict[int, str]:
    """Build the short ID label map used in narrow `Disp ID` / `Acq ID` cells.

    Real trade IDs (forex / stock / futures-fee events) format as
    `#N` so the column stays compact. Futures-realisation events
    use a stable `P&L #A→#B` notation derived from the open and
    close trade IDs of the realisation — synthetic engine IDs
    (`10**12 + N`) shift between runs and aren't citeable, so the
    renderer never exposes them.

    Dividend events follow the same principle: the synth ID
    (`2 * 10**12 + N`) is internal plumbing; the user-facing label
    carries the real `dividend_id` (`Div #N` for cash dividends and
    payment-in-lieu, `WHT #N` for withholding-tax) which they can
    cite in a future ``ib-cgt show dividend <id>`` audit command.

    Multi-slice closeouts: when a single close trade drains
    several open slices, multiple realisations share the same
    `(open, close)` close-trade key. Disambiguate with `[i]`
    suffixed in the order the futures engine emitted them
    (orchestrator already preserves that order in
    `future_realisations`). Single-realisation closes get no
    suffix to keep the common case compact.
    """
    out: dict[int, str] = {}
    for tid, _trade in forex_trades:
        out[tid] = f"#{tid}"
    for tid, _trade in stock_trades:
        out[tid] = f"#{tid}"
    for tid, _trade in future_trades:
        out[tid] = f"#{tid}"

    # Group realisations by close_trade_id so we can emit `[i]`
    # only when ambiguous. A first pass counts; a second pass
    # assigns indices.
    close_count: dict[int, int] = {}
    for _synth_id, realisation, _account in future_realisations:
        close_count[realisation.close_trade_id] = close_count.get(realisation.close_trade_id, 0) + 1
    next_index: dict[int, int] = {}
    for synth_id, realisation, _account in future_realisations:
        close_id = realisation.close_trade_id
        open_id = realisation.open_trade_id
        if close_count[close_id] > 1:
            i = next_index.get(close_id, 0)
            next_index[close_id] = i + 1
            out[synth_id] = f"P&L #{open_id}→#{close_id}[{i}]"
        else:
            out[synth_id] = f"P&L #{open_id}→#{close_id}"

    # Dividends — `Div #N` for inflows, `WHT #N` for withholding.
    # The real `dividend_id` is carried in the side map so the
    # cell stays citeable across runs.
    for synth_id, dividend in dividends:
        real_id = dividend_real_id[synth_id]
        prefix = "WHT" if dividend.kind is DividendKind.WITHHOLDING_TAX else "Div"
        out[synth_id] = f"{prefix} #{real_id}"
    return out


def _run_match_fx(
    *,
    engine: FXRuleEngine,
    forex_trades: list[tuple[int, Trade]],
    stock_trades: list[tuple[int, Trade]],
    future_trades: list[tuple[int, Trade]],
    future_realisations: list[tuple[int, FutureRealisation, str]],
    dividends: list[tuple[int, Dividend]],
    currencies: list[str],
    date_map: dict[int, date],
    source_descriptions: dict[int, str],
    id_label_map: dict[int, str],
) -> list[_MatchFxRow]:
    """Run the FX engine per currency with per-currency error capture."""
    out: list[_MatchFxRow] = []
    for ccy in currencies:
        try:
            result = engine.compute(
                ccy,
                forex_trades=forex_trades,
                stock_trades=stock_trades,
                future_trades=future_trades,
                future_realisations=future_realisations,
                dividends=dividends,
            )
        except (
            WrongAssetClassError,
            InconsistentTradeError,
            UnmatchedDisposalError,
            RateNotFoundError,
            ValueError,
        ) as exc:
            out.append((ccy, None, date_map, source_descriptions, id_label_map, exc))
            continue
        out.append((ccy, result, date_map, source_descriptions, id_label_map, None))
    return out


# ---------------------------------------------------------------------------
# Rendering helpers — `match fx`
# ---------------------------------------------------------------------------


def _render_match_fx(rows: list[_MatchFxRow], db_path: Path) -> None:
    """Render matched-disposals, unmatched-warnings, residuals, final-pool, summary, errors."""
    _render_match_fx_disposals(rows)
    _render_match_fx_unmatched_disposals(rows)
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
    for currency, result, date_map, source_descriptions, id_label_map, error in rows:
        if error is not None:
            continue
        assert result is not None  # mypy — error/result are mutually exclusive
        divider = _fx_divider(currency)
        _console.print(f"\n[bold cyan]{divider}[/]")
        if not result.matched_disposals:
            _console.print("  [dim](no matched disposals)[/]")
            continue
        table = Table(header_style="bold", show_lines=False)
        table.add_column("Disp ID")
        table.add_column("Disp Source")
        table.add_column("Disp Date")
        table.add_column("Rule")
        table.add_column("Qty", justify="right")
        table.add_column("Acq ID / Basis")
        table.add_column("Acq Source")
        table.add_column("Acq Date")
        table.add_column("Proceeds (GBP)", justify="right")
        table.add_column("Disp Fees (GBP)", justify="right")
        table.add_column("Cost (GBP)", justify="right")
        table.add_column("Acq Fees (GBP)", justify="right")
        table.add_column("Gain (GBP)", justify="right")
        for md in result.matched_disposals:
            table.add_row(
                *_fx_matched_disposal_to_cells(md, date_map, source_descriptions, id_label_map)
            )
        _console.print(table)


def _render_match_fx_unmatched_disposals(rows: list[_MatchFxRow]) -> None:
    """Yellow warning block for soft-residual unmatched disposals.

    Surfaces the FX engine's `unmatched_disposals` in a per-pool
    section so an opening-balance shortfall (or any other cause)
    is impossible to miss. Each row shows the un-covered quantity
    and the proportional GBP value of the disposal that couldn't
    be matched.
    """
    chunks: list[tuple[str, dict[int, str], dict[int, str], UnmatchedDisposalChunk]] = []
    for currency, result, _date_map, source_descriptions, id_label_map, error in rows:
        if error is not None or result is None:
            continue
        for chunk in result.unmatched_disposals:
            chunks.append((currency, source_descriptions, id_label_map, chunk))
    if not chunks:
        return

    _console.print(
        f"\n[bold yellow]Unmatched disposals ({len(chunks)}) — opening-balance "
        "shortfall or incomplete history[/]"
    )
    table = Table(header_style="bold yellow", show_lines=False)
    table.add_column("Currency")
    table.add_column("Disp ID")
    table.add_column("Disp Source")
    table.add_column("Disp Date")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Proceeds Remaining (GBP)", justify="right")
    for currency, source_descriptions, id_label_map, chunk in chunks:
        tid = chunk.disposal_trade_id
        table.add_row(
            currency,
            id_label_map.get(tid, f"#{tid}"),
            source_descriptions.get(tid, "—"),
            chunk.disposal_date.isoformat(),
            _format_qty_2dp(chunk.quantity_remaining),
            _format_money_2dp(chunk.proceeds_remaining_gbp),
        )
    _console.print(table)


def _render_match_fx_residuals(rows: list[_MatchFxRow]) -> None:
    """One flat table of every UnmatchedAcquisition across all currency pools."""
    residuals: list[tuple[str, dict[int, str], dict[int, str], UnmatchedAcquisition]] = [
        (currency, source_descriptions, id_label_map, ua)
        for currency, result, _date_map, source_descriptions, id_label_map, error in rows
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
    table.add_column("Acq ID")
    table.add_column("Acq Source")
    table.add_column("Acq Date")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Cost Remaining (GBP)", justify="right")
    for currency, source_descriptions, id_label_map, ua in residuals:
        table.add_row(
            currency,
            id_label_map.get(ua.trade_id, f"#{ua.trade_id}"),
            source_descriptions.get(ua.trade_id, "—"),
            ua.acquisition_date.isoformat(),
            _format_qty_2dp(ua.quantity_remaining),
            _format_money_2dp(ua.cost_remaining_gbp),
        )
    _console.print(table)


def _render_match_fx_final_pools(rows: list[_MatchFxRow]) -> None:
    """Per-currency final-pool aggregate. Skips empty pools."""
    pools: list[tuple[str, TaxLot]] = [
        (currency, result.final_pool)
        for currency, result, _date_map, _src, _id_map, error in rows
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
            _format_qty_2dp(pool.quantity),
            _format_money_2dp(pool.total_cost_gbp),
            _format_money_2dp(pool.average_cost_gbp),
        )
    _console.print(table)


def _render_match_fx_summary(rows: list[_MatchFxRow], db_path: Path) -> None:
    """Small summary: counts and total realised gain across all pools."""
    error_count = sum(1 for _, _, _, _, _, error in rows if error is not None)
    md_count = sum(
        len(result.matched_disposals)
        for _, result, _, _, _, error in rows
        if error is None and result is not None
    )
    pools_with_residual = sum(
        1
        for _, result, _, _, _, error in rows
        if error is None and result is not None and result.unmatched_disposals
    )
    total_gain = Money.gbp(Decimal("0"))
    for _, result, _, _, _, error in rows:
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
    table.add_row("…with residual disposals", str(pools_with_residual))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{_format_money_2dp(total_gain)}[/]",
    )
    _console.print(table)


def _render_match_fx_errors(rows: list[_MatchFxRow]) -> None:
    """Print every per-currency error in one block at the end."""
    error_rows = [(currency, error) for currency, _, _, _, _, error in rows if error is not None]
    if not error_rows:
        return
    _console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for currency, error in error_rows:
        _console.print(f"  [bold cyan]{_fx_divider(currency)}[/] [red]→ {error}[/]")


def _fx_matched_disposal_to_cells(
    md: MatchedDisposal,
    date_map: dict[int, date],
    source_descriptions: dict[int, str],
    id_label_map: dict[int, str],
) -> tuple[str, ...]:
    """FX-specific projection of a `MatchedDisposal` row.

    Adds two columns the stocks renderer doesn't have — Disp Source
    and Acq Source — so the auditor can immediately see whether each
    leg came from a forex trade, a stock trade, a futures fee, or a
    futures realisation P&L. ID columns consume `id_label_map` so a
    futures-realisation event renders as `P&L #A→#B[i]` instead of
    the run-unstable synthetic int.
    """
    proceeds = md.matched_proceeds_gbp
    proceeds_style = "green" if proceeds.amount >= 0 else "red"
    gain = md.gain_gbp
    gain_style = "green" if gain.amount >= 0 else "red"
    basis_text, acq_source_text, acq_date_text = _fx_basis_cells(
        md.basis, date_map, source_descriptions, id_label_map
    )
    disp_id = md.disposal_trade_id
    disp_source = source_descriptions.get(disp_id, "—")
    disp_label = id_label_map.get(disp_id, f"#{disp_id}")
    return (
        disp_label,
        disp_source,
        md.disposal_date.isoformat(),
        md.match_rule.value,
        _format_qty_2dp(md.matched_quantity),
        basis_text,
        acq_source_text,
        acq_date_text,
        f"[{proceeds_style}]{_format_money_2dp(proceeds)}[/]",
        _format_money_2dp(md.matched_disposal_fees_gbp),
        _format_money_2dp(md.matched_cost_gbp),
        _format_money_2dp(md.matched_acquisition_fees_gbp),
        f"[{gain_style}]{_format_money_2dp(gain)}[/]",
    )


def _fx_basis_cells(
    basis: DirectAcquisition | TaxLotSnapshot,
    date_map: dict[int, date],
    source_descriptions: dict[int, str],
    id_label_map: dict[int, str],
) -> tuple[str, str, str]:
    """Return `(basis_text, source_text, acq_date_text)` for the FX basis columns.

    `basis_text` is the `id_label_map` lookup prefixed with `acq` —
    `acq #5685` for ordinary forex / stock / futures-fee events,
    `acq P&L #5421→#8732[0]` for a futures-realisation acquisition.
    """
    if isinstance(basis, DirectAcquisition):
        acq_id = basis.acquisition_trade_id
        acq_date = date_map.get(acq_id)
        date_text = acq_date.isoformat() if acq_date is not None else "—"
        source_text = source_descriptions.get(acq_id, "—")
        label = id_label_map.get(acq_id, f"#{acq_id}")
        return f"acq {label}", source_text, date_text
    pool_text = (
        f"S.104 pool: qty={_format_qty_2dp(basis.quantity_before)}, "
        f"avg=£{_format_money_2dp(basis.average_cost_gbp)}"
    )
    return pool_text, "—", "—"


def _fx_divider(currency: str) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{currency} vs GBP"


# ---------------------------------------------------------------------------
# `match bonds` — UK CGT bond engine (exempt summary + four-rule matching)
# ---------------------------------------------------------------------------


@match_app.command("bonds")
def match_bonds(
    symbol: Annotated[
        str | None,
        typer.Option(
            "--symbol",
            "-s",
            help="Filter to one bond symbol (e.g. 'UKT 0 1/8 01/30/26').",
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
    """Dry-run the bond rule engine against ingested trades.

    Walks every bond instrument that matches the filters, runs
    `BondRuleEngine.compute` against its trade history (across all
    accounts — UK CGT pools span every account belonging to the
    taxpayer), and renders the output. The engine returns a sealed
    union, so the rendering branches on the result shape:

    * **Exempt bonds** (gilts / QCBs) surface in a yellow "no CGT"
      table with their native-currency buy / sell aggregates.
      No FX conversion, no S.104 pool, no `MatchedDisposal` rows.
    * **Non-exempt bonds** produce the same five sections
      `match stocks` does — matched-disposal table, pool residuals,
      final S.104 pools, summary, errors.

    Nothing is written to the database — this command is a read-only
    audit tool. As with `match stocks`, there is intentionally no
    `--account` flag: filtering by account would silently break the
    matching invariants for non-exempt bonds with cross-account
    histories.
    """
    since_date = _parse_iso_date(since, "--since")
    until_date = _parse_iso_date(until, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Same defensive `apply_migrations` call as `match stocks` —
        # a fresh DB would otherwise surface a confusing missing-
        # table error deep in the rule engine.
        apply_migrations(conn)
        # The exempt branch never touches the FX cache, but the
        # engine constructor accepts the converter unconditionally
        # (uniform calculator-injection contract). Reusing the real
        # `FXService` keeps the non-exempt path live for any
        # corporate / foreign-issuer bond the user may later trade.
        fx_service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        engine = BondRuleEngine(fx_service)
        instruments = InstrumentRepo(conn).list_bonds(symbol=symbol)
        results = _run_match_bonds(
            conn=conn,
            engine=engine,
            instruments=instruments,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not instruments:
        _console.print("[yellow]No bond instruments match the given filters.[/]")
        return

    _render_match_bonds(results, db_path)


# Per-instrument outcome — either a successful `BondResult`
# (`MatchingResult` for non-exempt, `ExemptBondResult` for gilts /
# QCBs), the trade-id → trade-date map needed by the renderer for
# direct-match basis dates, or the exception the engine raised.
# Mirrors `_MatchStocksRow`'s shape so error-isolation logic is
# uniform across `match` commands.
_MatchBondsRow = tuple[
    BondInstrument,
    BondResult | None,
    dict[int, date],
    Exception | None,
]


def _run_match_bonds(
    *,
    conn: sqlite3.Connection,
    engine: BondRuleEngine,
    instruments: list[tuple[int, BondInstrument]],
    since: date | None,
    until: date | None,
) -> list[_MatchBondsRow]:
    """Run the bond engine per-instrument with per-instrument error capture.

    Builds a `{trade_id: trade_date}` map per instrument so the
    renderer can show the basis acquisition's date alongside its
    trade id (only relevant for the non-exempt branch — the exempt
    branch never produces `MatchedDisposal` rows).
    """
    trade_repo = TradeRepo(conn)
    out: list[_MatchBondsRow] = []
    for instrument_id, instrument in instruments:
        # Cross-account: account_id=None pulls every trade for this
        # bond across every account. Per the docstring, filtering
        # by account would break the S.104 pool for non-exempt bonds.
        trades = trade_repo.for_instrument_with_ids(
            instrument_id,
            account_id=None,
            since=since,
            until=until,
        )
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
# Rendering helpers — `match bonds`
# ---------------------------------------------------------------------------


def _render_match_bonds(rows: list[_MatchBondsRow], db_path: Path) -> None:
    """Render the six sections of a `match bonds` run."""
    _render_match_bonds_exempt(rows)
    _render_match_bonds_disposals(rows)
    _render_match_bonds_residuals(rows)
    _render_match_bonds_final_pools(rows)
    _render_match_bonds_summary(rows, db_path)
    _render_match_bonds_errors(rows)


def _render_match_bonds_exempt(rows: list[_MatchBondsRow]) -> None:
    """Yellow summary table for exempt bonds — no CGT, audit only.

    UK gilts and QCBs produce no `MatchedDisposal` rows; this table
    is the only window the user has into what the engine saw. The
    native-currency totals make it cheap to spot-check that every
    expected buy / sell registered.
    """
    exempt_rows: list[tuple[BondInstrument, ExemptBondResult]] = [
        (instrument, result)
        for instrument, result, _, error in rows
        if error is None and isinstance(result, ExemptBondResult)
    ]
    if not exempt_rows:
        return

    table = Table(
        title="Exempt bonds (gilts / QCBs — no CGT)",
        header_style="bold yellow",
        show_lines=False,
    )
    table.add_column("Symbol")
    table.add_column("Currency")
    table.add_column("ISIN")
    table.add_column("Buys", justify="right")
    table.add_column("Sells", justify="right")
    table.add_column("Total Buy (native)", justify="right")
    table.add_column("Total Sell (native)", justify="right")
    for instrument, exempt in exempt_rows:
        table.add_row(
            instrument.symbol,
            instrument.currency,
            instrument.isin or "[dim]—[/]",
            str(exempt.exempt_buy_count),
            str(exempt.exempt_sell_count),
            _format_money_2dp(exempt.total_buy_native),
            _format_money_signed_2dp(exempt.total_sell_native),
        )
    _console.print(table)


def _render_match_bonds_disposals(rows: list[_MatchBondsRow]) -> None:
    """Per-non-exempt-bond bold header + matched-disposal table.

    Reuses `_matched_disposal_to_cells` — the projection is asset-
    class-agnostic, so the same 11-column shape that drives the
    stock disposal table works here unchanged.
    """
    matching_rows: list[tuple[BondInstrument, MatchingResult, dict[int, date]]] = [
        (instrument, result, date_map)
        for instrument, result, date_map, error in rows
        if error is None and isinstance(result, MatchingResult)
    ]
    if not matching_rows:
        return

    _console.print("\n[bold]Bond matched disposals (dry-run)[/]")
    for instrument, result, date_map in matching_rows:
        divider = _bond_divider(instrument)
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


def _render_match_bonds_residuals(rows: list[_MatchBondsRow]) -> None:
    """One flat table of every UnmatchedAcquisition across non-exempt bonds."""
    residuals: list[tuple[BondInstrument, UnmatchedAcquisition]] = [
        (instrument, ua)
        for instrument, result, _, error in rows
        if error is None and isinstance(result, MatchingResult)
        for ua in result.unmatched_acquisitions
    ]
    if not residuals:
        return

    table = Table(
        title="Pool residuals at end of input (non-exempt bonds)",
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
            _format_qty_2dp(ua.quantity_remaining),
            _format_money_2dp(ua.cost_remaining_gbp),
        )
    _console.print(table)


def _render_match_bonds_final_pools(rows: list[_MatchBondsRow]) -> None:
    """Per-non-exempt-bond final-pool aggregate — one flat table."""
    pools: list[tuple[BondInstrument, TaxLot]] = [
        (instrument, result.final_pool)
        for instrument, result, _, error in rows
        if error is None and isinstance(result, MatchingResult) and result.final_pool.quantity > 0
    ]
    if not pools:
        return

    table = Table(
        title="Final S.104 pools (non-exempt bonds)",
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
            _format_qty_2dp(pool.quantity),
            _format_money_2dp(pool.total_cost_gbp),
            _format_money_2dp(pool.average_cost_gbp),
        )
    _console.print(table)


def _render_match_bonds_summary(rows: list[_MatchBondsRow], db_path: Path) -> None:
    """Counts table — exempt vs non-exempt vs error split, plus realised gain."""
    exempt_count = sum(
        1 for _, result, _, error in rows if error is None and isinstance(result, ExemptBondResult)
    )
    matched_count = sum(
        1 for _, result, _, error in rows if error is None and isinstance(result, MatchingResult)
    )
    error_count = sum(1 for _, _, _, error in rows if error is not None)
    md_count = sum(
        len(result.matched_disposals)
        for _, result, _, error in rows
        if error is None and isinstance(result, MatchingResult)
    )
    total_gain = Money.gbp(Decimal("0"))
    for _, result, _, error in rows:
        if error is not None or not isinstance(result, MatchingResult):
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
    table.add_row("…exempt (skipped)", str(exempt_count))
    table.add_row("…non-exempt matched", str(matched_count))
    table.add_row("…with errors", str(error_count))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{_format_money_2dp(total_gain)}[/]",
    )
    _console.print(table)


def _render_match_bonds_errors(rows: list[_MatchBondsRow]) -> None:
    """Print every per-instrument error in one block at the end."""
    error_rows = [(instrument, error) for instrument, _, _, error in rows if error is not None]
    if not error_rows:
        return
    _console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        _console.print(f"  [bold cyan]{_bond_divider(instrument)}[/] [red]→ {error}[/]")


def _bond_divider(instrument: BondInstrument) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{instrument.symbol} ({instrument.currency})"


# ---------------------------------------------------------------------------
# `show trade` — single-trade audit dossier
# ---------------------------------------------------------------------------


@show_app.command("trade")
def show_trade(
    trade_id: Annotated[
        int,
        typer.Argument(
            help="The trade_id printed in `match fx` / `match stocks` / `match futures` output.",
        ),
    ],
) -> None:
    """Print a full audit dossier for a single trade.

    Drills down from a `trade_id` (as displayed in any matching
    command's table) to the source IB statement, native amounts,
    and the FX rate the relevant rule engine would apply at
    `trade_date`. Lets you verify GBP figures by hand against the
    original statement.
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        stored = TradeRepo(conn).get(trade_id)
        if stored is None:
            _console.print(f"[red]No trade found with trade_id={trade_id}.[/]")
            raise typer.Exit(code=1)
        statement = StatementRepo(conn).get(stored.statement_hash)
        # FX-conversion preview at trade_date — for non-GBP trades only.
        # Build a real `FXService` so the rate the dossier prints is
        # exactly what the rule engines would consume.
        fx_service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        rate_preview = _fx_rate_preview(stored.trade, fx_service)
    finally:
        conn.close()

    _render_show_trade(stored, statement, rate_preview, db_path)


def _fx_rate_preview(trade: Trade, fx_service: FXService) -> Decimal | None:
    """Return the trade-date GBP rate for non-GBP trades, else `None`.

    Uses `convert_with_rate` so the dossier shows the same
    "1 GBP = r native" figure the rule engines see. GBP-denominated
    trades short-circuit (no FX needed). A rate-not-found is
    swallowed and rendered as "—" by the caller; we don't want a
    missing rate to stop the dossier from printing the rest of the
    trade's data.
    """
    if trade.price.currency == "GBP":
        return None
    try:
        _, rate = fx_service.convert_with_rate(
            Money.of(Decimal(1), trade.price.currency),
            target="GBP",
            on=trade.trade_date,
        )
    except RateNotFoundError:
        return None
    return rate


def _render_show_trade(
    stored: StoredTrade,
    statement: StatementRow | None,
    rate_preview: Decimal | None,
    db_path: Path,
) -> None:
    """Render a Rich panel with all audit-relevant facts about one trade."""
    trade = stored.trade
    instrument = trade.instrument
    asset_class = instrument.asset_class.value
    title = (
        f"Trade #{stored.trade_id} — {asset_class} {trade.action.value} "
        f"{instrument.symbol} ({instrument.currency})"
    )

    table = Table(
        title=title,
        title_style="bold",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
        show_lines=False,
        show_header=False,
    )
    table.add_column("Field", style="bold")
    table.add_column("Value")

    table.add_row("Account", trade.account_id)
    if statement is not None:
        table.add_row(
            "Statement",
            f"{statement.source_path}  (row {stored.statement_row_index})",
        )
        table.add_row("Imported at", statement.imported_at)
    else:
        table.add_row("Statement", f"[red]hash {stored.statement_hash} not found[/]")
    table.add_row("Trade datetime", trade.trade_datetime.isoformat())
    table.add_row("Trade date (UK)", trade.trade_date.isoformat())
    table.add_row("Settlement date", trade.settlement_date.isoformat())
    table.add_row(
        "Native qty/price/fees",
        f"qty={trade.quantity}  price={trade.price.amount} {trade.price.currency}  "
        f"fees={trade.fees.amount} {trade.fees.currency}",
    )
    if trade.accrued_interest is not None:
        table.add_row(
            "Accrued interest",
            f"{trade.accrued_interest.amount} {trade.accrued_interest.currency}",
        )

    # Asset-class-specific extras + GBP preview.
    _render_show_trade_extras(table, stored, rate_preview)

    _console.print(table)


def _render_show_trade_extras(
    table: Table, stored: StoredTrade, rate_preview: Decimal | None
) -> None:
    """Add asset-class-specific rows + GBP preview to the dossier table."""
    trade = stored.trade
    instrument = trade.instrument

    # FX-rate preview for non-GBP trades. Format matches the
    # production "1 GBP = r native" convention the cache stores.
    if trade.price.currency != "GBP":
        if rate_preview is not None:
            table.add_row(
                f"FX rate ({trade.trade_date.isoformat()})",
                f"1 GBP = {rate_preview} {trade.price.currency}",
            )
        else:
            table.add_row(
                f"FX rate ({trade.trade_date.isoformat()})",
                "[red]not cached — run `ib-cgt fx sync`[/]",
            )

    # GBP equivalents per asset class. We compute these here using
    # the same arithmetic the relevant rule engine uses, so the
    # dossier reads the exact figures `match *` would feed forward.
    if isinstance(instrument, StockInstrument):
        if trade.action is TradeAction.BUY:
            native_total = trade.price.amount * trade.quantity + trade.fees.amount
            label = "Native cost (price*qty + fees)"
        else:
            native_total = trade.price.amount * trade.quantity - trade.fees.amount
            label = "Native proceeds (price*qty - fees)"
        table.add_row(label, f"{native_total} {trade.price.currency}")
        if rate_preview is not None:
            gbp = native_total / rate_preview
            table.add_row("GBP equivalent", f"£{gbp:.2f}")
        elif trade.price.currency == "GBP":
            table.add_row("GBP equivalent", f"£{native_total}")

    elif isinstance(instrument, FXInstrument):
        # For an FX trade, the *price* is quote-per-base; qty*price
        # is the quote-currency total. Show both legs explicitly so
        # the auditor doesn't have to reason about the price-tagging
        # caveat documented in `ingest/mapper.py`.
        base = instrument.currency_pair.base
        quote = instrument.currency_pair.quote
        quote_amount = trade.quantity * trade.price.amount
        if trade.action is TradeAction.BUY:
            table.add_row("Bought / paid", f"{trade.quantity} {base}  /  {quote_amount} {quote}")
        else:
            table.add_row("Sold / received", f"{trade.quantity} {base}  /  {quote_amount} {quote}")

    elif isinstance(instrument, FutureInstrument):
        table.add_row(
            "Contract",
            f"multiplier={instrument.contract_multiplier}  "
            f"expiry={instrument.expiry_date.isoformat()}",
        )
        table.add_row(
            "Realisation linkage",
            "see `ib-cgt show realisation --close <id>` for any closeout this trade drove",
        )

    # GBP equivalents for forex trades — both legs, since each is
    # potentially its own pool event.
    if isinstance(instrument, FXInstrument) and rate_preview is not None:
        # The price's currency is base (per mapper invariant). We
        # already have the GBP rate for base. The quote leg's GBP
        # value would need a second cache call — defer to
        # `show match --disposal` for the full per-pool audit.
        gbp_for_base = trade.quantity / rate_preview
        table.add_row(
            f"GBP equivalent of {instrument.currency_pair.base} leg",
            f"£{gbp_for_base:.4f}",
        )


# ---------------------------------------------------------------------------
# `show realisation` — futures realisation audit dossier
# ---------------------------------------------------------------------------


@show_app.command("realisation")
def show_realisation(
    close: Annotated[
        int,
        typer.Option(
            "--close",
            help=(
                "Close trade id (the CLOSE_LONG/CLOSE_SHORT row's trade_id) "
                "whose realisations to print."
            ),
        ),
    ],
    open_id: Annotated[
        int | None,
        typer.Option(
            "--open",
            help=(
                "Optional: narrow to realisations from a specific open trade id "
                "(disambiguates multi-slice closeouts)."
            ),
        ),
    ] = None,
) -> None:
    """Print every futures realisation produced by one close trade.

    Re-runs `FutureRuleEngine` for the futures instrument owning
    the close trade, filters to realisations whose
    `close_trade_id` matches, and prints one panel per realisation
    with full P&L computation, FX rates, and GBP figures so the
    auditor can verify against the IB statement by hand.

    The same `P&L #A→#B[i]` notation used by `match fx` appears
    in each panel's title, so output rows can be cited back and
    forth.
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        stored = TradeRepo(conn).get(close)
        if stored is None:
            _console.print(f"[red]No trade found with trade_id={close}.[/]")
            raise typer.Exit(code=1)
        if not isinstance(stored.trade.instrument, FutureInstrument):
            _console.print(
                f"[red]Trade #{close} is not a futures trade — "
                f"it's a {stored.trade.instrument.asset_class.value} trade.[/]"
            )
            raise typer.Exit(code=1)
        if stored.trade.action not in (TradeAction.CLOSE_LONG, TradeAction.CLOSE_SHORT):
            _console.print(
                f"[red]Trade #{close} is a {stored.trade.action.value} — only "
                "CLOSE_LONG / CLOSE_SHORT trades produce realisations. Did you "
                "mean to pass an open trade id to `--open`?[/]"
            )
            raise typer.Exit(code=1)

        instrument = stored.trade.instrument
        instrument_trades = TradeRepo(conn).for_instrument_with_ids(stored.instrument_id)
        fx_service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        engine = FutureRuleEngine(fx_service)
        try:
            result = engine.compute(instrument, instrument_trades)
        except (
            WrongAssetClassError,
            InconsistentTradeError,
            RateNotFoundError,
        ) as exc:
            _console.print(f"[red]Could not compute realisations: {exc}[/]")
            raise typer.Exit(code=1) from exc

        matching = [r for r in result.realisations if r.close_trade_id == close]
        if open_id is not None:
            matching = [r for r in matching if r.open_trade_id == open_id]
        # Pre-fetch statement provenance for every trade id we are
        # about to render, so the auditor can see the source HTML
        # path inline rather than chasing it via `show trade`.
        statement_lookup = _build_statement_lookup(conn, matching)
    finally:
        conn.close()

    if not matching:
        msg = f"No realisations with close_trade_id={close}"
        if open_id is not None:
            msg += f" and open_trade_id={open_id}"
        _console.print(f"[yellow]{msg}.[/]")
        return

    _render_show_realisation(matching, result.realisations, db_path, statement_lookup)


def _build_statement_lookup(
    conn: sqlite3.Connection,
    realisations: list[FutureRealisation],
) -> dict[int, tuple[StatementRow | None, int]]:
    """Resolve every open/close trade id to its source-statement metadata.

    Returns a dict keyed by `trade_id` with `(statement_row, row_index)`.
    `statement_row` is `None` only if the underlying `statements` row
    is missing — the same defensive branch `show trade` carries
    (`_render_show_trade`'s "hash not found" path).
    """
    trade_ids: set[int] = set()
    for r in realisations:
        trade_ids.add(r.open_trade_id)
        trade_ids.add(r.close_trade_id)

    trade_repo = TradeRepo(conn)
    statement_repo = StatementRepo(conn)
    statement_cache: dict[str, StatementRow | None] = {}
    out: dict[int, tuple[StatementRow | None, int]] = {}
    for trade_id in trade_ids:
        stored_t = trade_repo.get(trade_id)
        if stored_t is None:
            # Realisations always reference real trade ids — if this
            # ever fires, something has corrupted the run; skip the
            # row rather than raising mid-render.
            continue
        statement_hash = stored_t.statement_hash
        if statement_hash not in statement_cache:
            statement_cache[statement_hash] = statement_repo.get(statement_hash)
        out[trade_id] = (statement_cache[statement_hash], stored_t.statement_row_index)
    return out


def _render_show_realisation(
    matching: list[FutureRealisation],
    all_realisations_for_close: tuple[FutureRealisation, ...],
    db_path: Path,
    statement_lookup: dict[int, tuple[StatementRow | None, int]],
) -> None:
    """Render one panel per realisation with the slice index notation."""
    # Build slice-index map matching the FX renderer's convention:
    # only emit `[i]` when more than one realisation shares a close.
    close_count: dict[int, int] = {}
    for r in all_realisations_for_close:
        close_count[r.close_trade_id] = close_count.get(r.close_trade_id, 0) + 1
    slice_index: dict[tuple[int, int], int] = {}
    next_index: dict[int, int] = {}
    for r in all_realisations_for_close:
        if close_count[r.close_trade_id] > 1:
            i = next_index.get(r.close_trade_id, 0)
            slice_index[(r.open_trade_id, r.close_trade_id)] = i
            next_index[r.close_trade_id] = i + 1

    for r in matching:
        suffix = ""
        if (r.open_trade_id, r.close_trade_id) in slice_index:
            suffix = f"[{slice_index[(r.open_trade_id, r.close_trade_id)]}]"
        title = f"P&L #{r.open_trade_id}→#{r.close_trade_id}{suffix}"
        _render_one_realisation(title, r, db_path, statement_lookup)


def _render_one_realisation(
    title: str,
    r: FutureRealisation,
    db_path: Path,
    statement_lookup: dict[int, tuple[StatementRow | None, int]],
) -> None:
    """Render a single realisation as a Rich panel-style table."""
    table = Table(
        title=title,
        title_style="bold",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
        show_lines=False,
        show_header=False,
    )
    table.add_column("Field", style="bold")
    table.add_column("Value")

    table.add_row(
        "Instrument",
        f"{r.instrument.symbol} ({r.instrument.currency})  "
        f"multiplier={r.instrument.contract_multiplier}  "
        f"expiry={r.instrument.expiry_date.isoformat()}",
    )
    table.add_row("Side", r.side)
    table.add_row(
        "Open trade",
        f"#{r.open_trade_id}  on {r.open_date.isoformat()}  "
        f"fee={r.open_fee_native.amount} {r.open_fee_native.currency}",
    )
    _add_statement_row(table, "Open statement", statement_lookup.get(r.open_trade_id))
    table.add_row(
        "Close trade",
        f"#{r.close_trade_id}  on {r.close_date.isoformat()}  "
        f"fee={r.close_fee_native.amount} {r.close_fee_native.currency}",
    )
    _add_statement_row(table, "Close statement", statement_lookup.get(r.close_trade_id))
    table.add_row("Quantity", f"{r.quantity}")
    table.add_row(
        "gross_pnl_native",
        f"{r.gross_pnl_native.amount} {r.gross_pnl_native.currency}",
    )
    table.add_row(
        f"FX rate (open  {r.open_date.isoformat()})",
        f"1 GBP = {r.open_fx_rate} {r.instrument.currency}",
    )
    table.add_row(
        f"FX rate (close {r.close_date.isoformat()})",
        f"1 GBP = {r.close_fx_rate} {r.instrument.currency}",
    )
    table.add_row("proceeds_gbp", f"£{r.proceeds_gbp.amount}")
    table.add_row("cost_gbp", f"£{r.cost_gbp.amount}")
    gain_style = "green" if r.gain_gbp.amount >= 0 else "red"
    table.add_row("gain_gbp", f"[{gain_style}]£{r.gain_gbp.amount}[/]")
    _console.print(table)


def _add_statement_row(
    table: Table,
    label: str,
    entry: tuple[StatementRow | None, int] | None,
) -> None:
    """Render the source-statement provenance for one realisation leg."""
    if entry is None:
        # `_build_statement_lookup` skips trade ids it cannot resolve;
        # mirror `show trade`'s defensive branch so the dossier is
        # still readable when the underlying statement row is missing.
        table.add_row(label, "[red]trade row missing[/]")
        return
    statement_row, row_idx = entry
    if statement_row is None:
        table.add_row(label, "[red]hash not found[/]")
    else:
        table.add_row(label, f"{statement_row.source_path}  (row {row_idx})")


# ---------------------------------------------------------------------------
# `show match --disposal` — per-disposal chunk audit
# ---------------------------------------------------------------------------


@show_app.command("match")
def show_match(
    disposal: Annotated[
        int,
        typer.Option(
            "--disposal",
            help=(
                "Disposal trade_id whose matched chunks you want to audit. "
                "Use the trade_id printed in `match fx` (real DB id, not a "
                "synthetic P&L id)."
            ),
        ),
    ],
) -> None:
    """Print every matched chunk attached to one disposal, with running residual.

    Re-runs the FX engine for the currency pool(s) the disposal
    touches, filters matched chunks to the supplied disposal id,
    and prints each chunk in match order with the residual that
    remained after consuming it. Lets the auditor confirm whether
    a small chunk is the entire disposal or the tail of a larger
    one matched in pieces (same-day → 30-day → S.104 →
    s.105(2)).
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        stored = TradeRepo(conn).get(disposal)
        if stored is None:
            _console.print(f"[red]No trade found with trade_id={disposal}.[/]")
            raise typer.Exit(code=1)
        pools = _pools_disposal_could_touch(stored.trade)
        if not pools:
            _console.print(
                f"[yellow]Trade #{disposal} doesn't touch any non-GBP pool — "
                "it can't appear in `match fx` output.[/]"
            )
            raise typer.Exit(code=0)

        audit = _load_fx_audit_data(conn)
        engine = FXRuleEngine(audit.fx_service)
        source_descriptions = _build_fx_source_descriptions(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
        )
        date_map = _build_fx_event_date_map(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
        )
        id_label_map = _build_fx_id_label_map(
            forex_trades=audit.forex_trades,
            stock_trades=audit.stock_trades,
            future_trades=audit.future_trades,
            future_realisations=audit.future_realisations,
            dividends=audit.dividends,
            dividend_real_id=audit.dividend_real_id,
        )
        per_pool_results: list[tuple[str, MatchingResult]] = []
        for ccy in pools:
            try:
                result = engine.compute(
                    ccy,
                    forex_trades=audit.forex_trades,
                    stock_trades=audit.stock_trades,
                    future_trades=audit.future_trades,
                    future_realisations=audit.future_realisations,
                    dividends=audit.dividends,
                )
            except (
                WrongAssetClassError,
                InconsistentTradeError,
                RateNotFoundError,
                ValueError,
            ) as exc:
                _console.print(f"[red]Pool {ccy}: {exc}[/]")
                continue
            per_pool_results.append((ccy, result))
    finally:
        conn.close()

    _render_show_match(
        disposal_trade_id=disposal,
        stored=stored,
        per_pool_results=per_pool_results,
        date_map=date_map,
        source_descriptions=source_descriptions,
        id_label_map=id_label_map,
        db_path=db_path,
    )


def _pools_disposal_could_touch(trade: Trade) -> list[str]:
    """Return non-GBP currencies a disposal of `trade` could feed.

    Mirrors the projection logic in `fx_cashflow.py`:
    - StockInstrument: the listing currency (if non-GBP).
    - FutureInstrument: the contract currency (futures-fee path).
    - FXInstrument: both legs of the pair, minus GBP.
    """
    instrument = trade.instrument
    if isinstance(instrument, StockInstrument):
        return [instrument.currency] if instrument.currency != "GBP" else []
    if isinstance(instrument, FutureInstrument):
        return [instrument.currency] if instrument.currency != "GBP" else []
    if isinstance(instrument, FXInstrument):
        legs = {instrument.currency_pair.base, instrument.currency_pair.quote}
        legs.discard("GBP")
        return sorted(legs)
    return []


def _render_show_match(
    *,
    disposal_trade_id: int,
    stored: StoredTrade,
    per_pool_results: list[tuple[str, MatchingResult]],
    date_map: dict[int, date],
    source_descriptions: dict[int, str],
    id_label_map: dict[int, str],
    db_path: Path,
) -> None:
    """Render per-pool tables of chunks belonging to one disposal."""
    source_label = source_descriptions.get(disposal_trade_id, "—")
    _console.print(
        f"\n[bold]Disposal #{disposal_trade_id} — {source_label} on "
        f"{stored.trade.trade_date.isoformat()}[/]"
    )

    found_anything = False
    for ccy, result in per_pool_results:
        chunks = [m for m in result.matched_disposals if m.disposal_trade_id == disposal_trade_id]
        residual = next(
            (u for u in result.unmatched_disposals if u.disposal_trade_id == disposal_trade_id),
            None,
        )
        if not chunks and residual is None:
            continue
        found_anything = True
        _console.print(f"\n[bold cyan]{_fx_divider(ccy)}[/]")

        # Compute running residual for context: total matched qty up to
        # and including each chunk, plus what's left after this chunk
        # vs. the disposal's full quantity (which we infer by summing
        # all chunks + final residual).
        total_matched = sum((m.matched_quantity for m in chunks), Decimal(0))
        total_disposal_qty = total_matched + (
            residual.quantity_remaining if residual is not None else Decimal(0)
        )

        table = Table(header_style="bold", show_lines=False)
        table.add_column("#", justify="right")
        table.add_column("Rule")
        table.add_column("Qty", justify="right")
        table.add_column("Cost (GBP)", justify="right")
        table.add_column("Proceeds (GBP)", justify="right")
        table.add_column("Basis")
        table.add_column("Residual after", justify="right")

        consumed = Decimal(0)
        for i, md in enumerate(chunks, start=1):
            consumed += md.matched_quantity
            after = total_disposal_qty - consumed
            basis_text, _src, _date = _fx_basis_cells(
                md.basis, date_map, source_descriptions, id_label_map
            )
            table.add_row(
                str(i),
                md.match_rule.value,
                _format_qty_2dp(md.matched_quantity),
                _format_money_2dp(md.matched_cost_gbp),
                _format_money_2dp(md.matched_proceeds_gbp),
                basis_text,
                _format_qty_2dp(after),
            )

        if residual is not None:
            table.add_row(
                "—",
                "[yellow]UNMATCHED[/]",
                _format_qty_2dp(residual.quantity_remaining),
                "—",
                _format_money_2dp(residual.proceeds_remaining_gbp),
                "[yellow]opening-balance shortfall[/]",
                _format_qty_2dp(Decimal(0)),
            )
        _console.print(table)
        _console.print(
            f"  [dim]total disposal qty (this pool): {_format_qty_2dp(total_disposal_qty)} "
            f"({len(chunks)} matched chunk(s))[/]"
        )

    if not found_anything:
        _console.print(
            "\n[yellow]No matched chunks found for this disposal in any pool. "
            "Either the trade isn't a disposal, or it didn't reach the FX engine "
            "(e.g. GBP-denominated stock trade).[/]"
        )
    _console.print(f"\n[dim]{db_path}[/]")


# ---------------------------------------------------------------------------
# `check` subgroup — sanity-check facility (see src/ib_cgt/checks/)
# ---------------------------------------------------------------------------


# A common option spec for every `check ...` subcommand. Each is wrapped
# in `Annotated` at the call site so Typer renders consistent --help text.
_CHECK_STRICT_HELP = "Escalate warnings to failures: exit code 2 if any warning fires."
_CHECK_JSON_HELP = (
    "Emit results as a single JSON document instead of a Rich table. "
    "Useful for scripting (e.g. `ib-cgt check all --json | jq`)."
)
_CHECK_SYMBOL_HELP = (
    "Restrict Tier B/C engine replays to one stock or futures symbol. "
    "Has no effect on Tier A SQL invariants."
)
_CHECK_SINCE_HELP = (
    "Inclusive lower bound on trade_date (YYYY-MM-DD). Caveat: clipping "
    "the trade history invalidates S.104 pool reconstruction; omit for "
    "production audits."
)
_CHECK_UNTIL_HELP = "Inclusive upper bound on trade_date (YYYY-MM-DD). Same caveat as --since."


def _execute_check(
    *,
    scope: Scope,
    strict: bool,
    as_json: bool,
    symbol: str | None,
    since_str: str | None,
    until_str: str | None,
) -> None:
    """Shared body of every `check ...` subcommand.

    Resolves filters, opens the DB, builds the FX service, calls
    `run_all`, renders the report, and exits with the report's
    Unix-style code.
    """
    since_date = _parse_iso_date(since_str, "--since")
    until_date = _parse_iso_date(until_str, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Defensive — same as every other read command. A fresh DB
        # would otherwise surface as "no such table".
        apply_migrations(conn)
        fx_service = FXService(
            FXRateRepo(conn),
            FrankfurterClient(base_url=resolve_fx_base_url()),
        )
        report = run_all(
            conn,
            fx=fx_service,
            strict=strict,
            symbol=symbol,
            since=since_date,
            until=until_date,
            scope=scope,
        )
    finally:
        conn.close()

    if as_json:
        _render_check_report_json(report)
    else:
        _render_check_report(report, db_path)
    if report.exit_code != 0:
        raise typer.Exit(code=report.exit_code)


@check_app.callback()
def check_root(
    ctx: typer.Context,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run every sanity check (alias of `check all`) when no subcommand is given.

    With no subcommand, defaults to `check all` so the operator can
    just run `ib-cgt check` and get the full sweep.
    """
    if ctx.invoked_subcommand is not None:
        return
    _execute_check(
        scope=Scope.ALL,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("all")
def check_all(
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run every registered check across all four tiers."""
    _execute_check(
        scope=Scope.ALL,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("data")
def check_data(
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
) -> None:
    """Run only Tier A — pre-match data integrity (SQL only).

    Cheap, no engine replay. Run after every ingest to catch
    referential / shape problems before they corrupt downstream
    matching.
    """
    _execute_check(
        scope=Scope.DATA,
        strict=strict,
        as_json=as_json,
        symbol=None,
        since_str=None,
        until_str=None,
    )


@check_app.command("stocks")
def check_stocks(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run stock-specific Tier B + C invariants only."""
    _execute_check(
        scope=Scope.STOCKS,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("fx")
def check_fx(
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run FX-specific Tier B + C invariants only."""
    _execute_check(
        scope=Scope.FX,
        strict=strict,
        as_json=as_json,
        symbol=None,
        since_str=since,
        until_str=until,
    )


@check_app.command("futures")
def check_futures(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run futures-specific Tier C invariants only."""
    _execute_check(
        scope=Scope.FUTURES,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


@check_app.command("pool")
def check_pool(
    symbol: Annotated[str | None, typer.Option("--symbol", "-s", help=_CHECK_SYMBOL_HELP)] = None,
    strict: Annotated[bool, typer.Option("--strict", help=_CHECK_STRICT_HELP)] = False,
    as_json: Annotated[bool, typer.Option("--json", help=_CHECK_JSON_HELP)] = False,
    since: Annotated[str | None, typer.Option("--since", help=_CHECK_SINCE_HELP)] = None,
    until: Annotated[str | None, typer.Option("--until", help=_CHECK_UNTIL_HELP)] = None,
) -> None:
    """Run only the S.104 pool invariants (the user's explicit ask).

    Restricts to per-acquisition / per-disposal conservation and
    pool-reconstructibility checks (B1, B3, B5, B6, B7, plus the
    consistency invariants that bear on pool integrity). Useful for
    quickly verifying the pool is well-formed without running the
    full Tier A SQL sweep.
    """
    _execute_check(
        scope=Scope.POOL,
        strict=strict,
        as_json=as_json,
        symbol=symbol,
        since_str=since,
        until_str=until,
    )


# ---------------------------------------------------------------------------
# `check` rendering helpers
# ---------------------------------------------------------------------------


_STATUS_STYLE: dict[Status, str] = {
    Status.OK: "[green]OK[/]",
    Status.WARN: "[yellow]WARN[/]",
    Status.FAIL: "[red]FAIL[/]",
    Status.SKIPPED: "[dim]SKIP[/]",
}


def _render_check_report(report: CheckReport, db_path: Path) -> None:
    """Render a `CheckReport` as a Rich table grouped by tier.

    Evidence rows are listed below each non-OK check in compact
    form; the renderer caps the per-row evidence at 5 lines so
    one bad invariant doesn't drown out the rest of the report.
    """
    # Group by tier so the operator sees the failure cluster.
    table = Table(
        title=f"ib-cgt check ({len(report.results)} checks)",
        show_lines=False,
    )
    table.add_column("ID", style="bold", no_wrap=True)
    table.add_column("Tier", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Description", overflow="fold")
    table.add_column("Detail", overflow="fold")

    for r in report.results:
        table.add_row(
            r.name,
            r.tier.value,
            _STATUS_STYLE[r.status],
            r.description,
            r.detail,
        )

    _console.print(table)

    # Print evidence for failed / warning checks so the operator can
    # diagnose without re-running with --json. Capped per-check.
    for r in report.results:
        if r.status in (Status.FAIL, Status.WARN) and r.evidence:
            _console.print(f"\n[bold]{r.name}[/] evidence:")
            for row in r.evidence[:5]:
                _console.print(f"  • {row}")
            if len(r.evidence) > 5:
                _console.print(f"  [dim]… and {len(r.evidence) - 5} more[/]")

    summary = (
        f"{report.passed} OK, {report.failures} FAIL, {report.warnings} WARN, {report.skipped} SKIP"
    )
    _console.print(f"\n[bold]{summary}[/]")
    _console.print(f"[dim]{db_path}[/]")
    if report.exit_code != 0:
        _console.print(f"[red]exit code: {report.exit_code}[/]")


def _render_check_report_json(report: CheckReport) -> None:
    """Emit a single JSON document covering every check + summary.

    Evidence rows are forwarded verbatim so downstream tools (jq,
    a structured-log pipeline, etc.) can drill in without parsing
    Rich output.
    """
    import json

    payload = {
        "checks": [
            {
                "name": r.name,
                "description": r.description,
                "tier": r.tier.value,
                "severity": r.severity.value,
                "status": r.status.value,
                "detail": r.detail,
                "evidence": [{k: _json_safe(v) for k, v in row.items()} for row in r.evidence],
            }
            for r in report.results
        ],
        "summary": {
            "passed": report.passed,
            "failures": report.failures,
            "warnings": report.warnings,
            "skipped": report.skipped,
            "exit_code": report.exit_code,
            "strict": report.strict,
        },
    }
    typer.echo(json.dumps(payload, indent=2))


def _json_safe(value: object) -> object:
    """Coerce a CheckResult evidence value to a JSON-serialisable shape."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


# A handle to silence ruff's "imported but unused" warnings on
# CheckResult — the type appears in `_render_check_report`'s
# signature only via the `report.results` access, so the explicit
# import is needed for the annotation in this module.
_CHECK_RESULT_REF: type[CheckResult] = CheckResult


# ---------------------------------------------------------------------------
# `bonds` subgroup
# ---------------------------------------------------------------------------


@bonds_app.command("list")
def bonds_list(
    symbol: Annotated[
        str | None,
        typer.Option("--symbol", "-s", help="Filter to bonds with that exact symbol."),
    ] = None,
) -> None:
    """List every ingested bond with its inferred CGT-exempt flag.

    The flag is set at ingest time by the gilt classifier
    (`ingest/mapper.py:_classify_bond_exempt`). Use this command to
    verify the classification after re-ingesting statements or
    extending the `IB_CGT_BONDS_EXEMPT` allowlist.
    """
    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        rows = InstrumentRepo(conn).list_bonds(symbol=symbol)
    finally:
        conn.close()

    if not rows:
        _console.print("[dim]No bond instruments found.[/]")
        return

    _render_bonds(rows, db_path)


def _render_bonds(rows: list[tuple[int, BondInstrument]], db_path: Path) -> None:
    """Render a `(id, BondInstrument)` list as a rich.Table."""
    # Yellow on exempt, dim grey on non-exempt — at a glance you see
    # which bonds the engine will pool vs skip.
    table = Table(
        title=f"Bond instruments — {len(rows)} rows",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("ID", justify="right")
    table.add_column("Symbol")
    table.add_column("Currency")
    table.add_column("ISIN")
    table.add_column("CGT-exempt")

    for instrument_id, bond in rows:
        flag_text = "[bold yellow]YES[/]" if bond.is_cgt_exempt else "[dim]no[/]"
        table.add_row(
            str(instrument_id),
            bond.symbol,
            bond.currency,
            bond.isin or "[dim]—[/]",
            flag_text,
        )

    _console.print(table)


if __name__ == "__main__":  # pragma: no cover — direct execution path
    app()
