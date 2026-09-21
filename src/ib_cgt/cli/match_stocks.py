"""`ib-cgt match stocks` — dry-run the four-rule share-matching engine for stocks.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import StockEngineRun, run_stock_engine
from ib_cgt.cli.app import match_app
from ib_cgt.cli.common import (
    build_fx_service,
    console,
    format_money_2dp,
    format_qty_2dp,
    parse_iso_date,
)
from ib_cgt.cli.matching_render import (
    matched_disposal_to_cells,
    render_instrument_unmatched_disposals,
    trade_dates,
)
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection
from ib_cgt.domain import Money, StockInstrument, TaxLot, UnmatchedAcquisition


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
    matched-disposal chunks, unmatched residuals, pool residuals, and
    final-pool aggregates. Nothing is written to the database — this
    command is a read-only audit tool.

    Note that there is intentionally no ``--account`` flag: filtering
    by account would silently break the matching invariants for
    instruments with cross-account histories (S.104 pools span
    accounts per `docs/architecture.md §Scope — Accounts`).
    """
    since_date = parse_iso_date(since, "--since")
    until_date = parse_iso_date(until, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Defensive — same as `ingest`. A fresh DB file would otherwise
        # surface as a confusing "no such table" error.
        apply_migrations(conn)
        runs = run_stock_engine(
            conn,
            build_fx_service(conn),
            symbol=symbol,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not runs:
        console.print("[yellow]No stock instruments match the given filters.[/]")
        return

    _render_match_stocks(runs, db_path)


def _render_match_stocks(runs: Sequence[StockEngineRun], db_path: Path) -> None:
    """Render matched-disposals, unmatched, residuals, final-pool, summary, errors."""
    _render_match_stocks_disposals(runs)
    _render_match_stocks_unmatched_disposals(runs)
    _render_match_stocks_residuals(runs)
    _render_match_stocks_final_pools(runs)
    _render_match_stocks_summary(runs, db_path)
    _render_match_stocks_errors(runs)


def _render_match_stocks_disposals(runs: Sequence[StockEngineRun]) -> None:
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
    console.print("[bold]Stock matched disposals (dry-run)[/]")
    for run in runs:
        if run.error is not None:
            # Errors land in the trailing block — same pattern as
            # `match futures`.
            continue
        result = run.result
        assert result is not None  # mypy — error/result are mutually exclusive
        divider = _stock_divider(run.instrument)
        console.print(f"\n[bold cyan]{divider}[/]")
        if not result.matched_disposals:
            console.print("  [dim](no matched disposals)[/]")
            continue
        date_map = trade_dates(run.trades)
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
            table.add_row(*matched_disposal_to_cells(md, date_map))
        console.print(table)


def _render_match_stocks_unmatched_disposals(runs: Sequence[StockEngineRun]) -> None:
    """Yellow block of every soft-residual unmatched disposal across stocks."""
    render_instrument_unmatched_disposals(
        [
            (run.instrument, chunk)
            for run in runs
            if run.error is None and run.result is not None
            for chunk in run.result.unmatched_disposals
        ]
    )


def _render_match_stocks_residuals(runs: Sequence[StockEngineRun]) -> None:
    """One flat table of every UnmatchedAcquisition across all instruments."""
    residuals: list[tuple[StockInstrument, UnmatchedAcquisition]] = [
        (run.instrument, ua)
        for run in runs
        if run.error is None and run.result is not None
        for ua in run.result.unmatched_acquisitions
    ]
    if not residuals:
        console.print("[dim]No pool residuals after matching.[/]")
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
            format_qty_2dp(ua.quantity_remaining),
            format_money_2dp(ua.cost_remaining_gbp),
        )
    console.print(table)


def _render_match_stocks_final_pools(runs: Sequence[StockEngineRun]) -> None:
    """Per-instrument final-pool aggregate — one flat table.

    Skips instruments whose pool is empty (the typical post-match
    state for an instrument that fully closed every position).
    """
    pools: list[tuple[StockInstrument, TaxLot]] = [
        (run.instrument, run.result.final_pool)
        for run in runs
        if run.error is None and run.result is not None and run.result.final_pool.quantity > 0
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
            format_qty_2dp(pool.quantity),
            format_money_2dp(pool.total_cost_gbp),
            format_money_2dp(pool.average_cost_gbp),
        )
    console.print(table)


def _render_match_stocks_summary(runs: Sequence[StockEngineRun], db_path: Path) -> None:
    """Small summary: counts and total realised gain across all instruments."""
    error_count = sum(1 for run in runs if run.error is not None)
    md_count = sum(
        len(run.result.matched_disposals)
        for run in runs
        if run.error is None and run.result is not None
    )
    total_gain = Money.gbp(Decimal("0"))
    for run in runs:
        if run.error is not None or run.result is None:
            continue
        for md in run.result.matched_disposals:
            total_gain = total_gain + md.gain_gbp

    table = Table(
        title="Summary",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
    )
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Instruments processed", str(len(runs)))
    table.add_row("…with errors", str(error_count))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{format_money_2dp(total_gain)}[/]",
    )
    console.print(table)


def _render_match_stocks_errors(runs: Sequence[StockEngineRun]) -> None:
    """Print every per-instrument error in one block at the end."""
    error_rows = [(run.instrument, run.error) for run in runs if run.error is not None]
    if not error_rows:
        return
    console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        console.print(f"  [bold cyan]{_stock_divider(instrument)}[/] [red]→ {error}[/]")


def _stock_divider(instrument: StockInstrument) -> str:
    """Stable label used in section dividers and error rows.

    The conid is appended because the symbol is display text IB may
    rename between statements (`JNKEz` → `JNKE`); the contract id is
    what stays constant.
    """
    return f"{instrument.symbol} ({instrument.currency}) conid={instrument.conid}"
