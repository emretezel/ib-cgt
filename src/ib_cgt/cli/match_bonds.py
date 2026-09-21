"""`ib-cgt match bonds` — dry-run the bond engine (exempt and non-exempt branches).

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import BondEngineRun, run_bond_engine
from ib_cgt.cli.app import match_app
from ib_cgt.cli.common import (
    build_fx_service,
    console,
    format_money_2dp,
    format_money_signed_2dp,
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
from ib_cgt.domain import BondInstrument, Money, TaxLot, Trade, UnmatchedAcquisition
from ib_cgt.rules import ExemptBondResult, MatchingResult


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
    * **Non-exempt bonds** produce the same sections `match stocks`
      does — matched-disposal table, unmatched residuals, pool
      residuals, final S.104 pools, summary, errors.

    Nothing is written to the database — this command is a read-only
    audit tool. As with `match stocks`, there is intentionally no
    `--account` flag: filtering by account would silently break the
    matching invariants for non-exempt bonds with cross-account
    histories.
    """
    since_date = parse_iso_date(since, "--since")
    until_date = parse_iso_date(until, "--until")

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
        runs = run_bond_engine(
            conn,
            build_fx_service(conn),
            symbol=symbol,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not runs:
        console.print("[yellow]No bond instruments match the given filters.[/]")
        return

    _render_match_bonds(runs, db_path)


def _render_match_bonds(runs: Sequence[BondEngineRun], db_path: Path) -> None:
    """Render the seven sections of a `match bonds` run."""
    _render_match_bonds_exempt(runs)
    _render_match_bonds_disposals(runs)
    _render_match_bonds_unmatched_disposals(runs)
    _render_match_bonds_residuals(runs)
    _render_match_bonds_final_pools(runs)
    _render_match_bonds_summary(runs, db_path)
    _render_match_bonds_errors(runs)


def _render_match_bonds_exempt(runs: Sequence[BondEngineRun]) -> None:
    """Yellow summary table for exempt bonds — no CGT, audit only.

    UK gilts and QCBs produce no `MatchedDisposal` rows; this table
    is the only window the user has into what the engine saw. The
    native-currency totals make it cheap to spot-check that every
    expected buy / sell registered.
    """
    exempt_rows: list[tuple[BondInstrument, ExemptBondResult]] = [
        (run.instrument, run.result)
        for run in runs
        if run.error is None and isinstance(run.result, ExemptBondResult)
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
            instrument.isin,
            str(exempt.exempt_buy_count),
            str(exempt.exempt_sell_count),
            format_money_2dp(exempt.total_buy_native),
            format_money_signed_2dp(exempt.total_sell_native),
        )
    console.print(table)


def _matching_bond_runs(
    runs: Sequence[BondEngineRun],
) -> list[tuple[BondInstrument, MatchingResult, tuple[tuple[int, Trade], ...]]]:
    """Narrow a bond pass to the non-exempt runs that produced a `MatchingResult`."""
    return [
        (run.instrument, run.result, run.trades)
        for run in runs
        if run.error is None and isinstance(run.result, MatchingResult)
    ]


def _render_match_bonds_disposals(runs: Sequence[BondEngineRun]) -> None:
    """Per-non-exempt-bond bold header + matched-disposal table.

    Reuses `matched_disposal_to_cells` — the projection is asset-
    class-agnostic, so the same 11-column shape that drives the
    stock disposal table works here unchanged.
    """
    matching_rows = _matching_bond_runs(runs)
    if not matching_rows:
        return

    console.print("\n[bold]Bond matched disposals (dry-run)[/]")
    for instrument, result, trades in matching_rows:
        divider = _bond_divider(instrument)
        console.print(f"\n[bold cyan]{divider}[/]")
        if not result.matched_disposals:
            console.print("  [dim](no matched disposals)[/]")
            continue
        date_map = trade_dates(trades)
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


def _render_match_bonds_unmatched_disposals(runs: Sequence[BondEngineRun]) -> None:
    """Yellow block of every soft-residual unmatched disposal across non-exempt bonds."""
    render_instrument_unmatched_disposals(
        [
            (instrument, chunk)
            for instrument, result, _trades in _matching_bond_runs(runs)
            for chunk in result.unmatched_disposals
        ]
    )


def _render_match_bonds_residuals(runs: Sequence[BondEngineRun]) -> None:
    """One flat table of every UnmatchedAcquisition across non-exempt bonds."""
    residuals: list[tuple[BondInstrument, UnmatchedAcquisition]] = [
        (instrument, ua)
        for instrument, result, _trades in _matching_bond_runs(runs)
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
            format_qty_2dp(ua.quantity_remaining),
            format_money_2dp(ua.cost_remaining_gbp),
        )
    console.print(table)


def _render_match_bonds_final_pools(runs: Sequence[BondEngineRun]) -> None:
    """Per-non-exempt-bond final-pool aggregate — one flat table."""
    pools: list[tuple[BondInstrument, TaxLot]] = [
        (instrument, result.final_pool)
        for instrument, result, _trades in _matching_bond_runs(runs)
        if result.final_pool.quantity > 0
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
            format_qty_2dp(pool.quantity),
            format_money_2dp(pool.total_cost_gbp),
            format_money_2dp(pool.average_cost_gbp),
        )
    console.print(table)


def _render_match_bonds_summary(runs: Sequence[BondEngineRun], db_path: Path) -> None:
    """Counts table — exempt vs non-exempt vs error split, plus realised gain."""
    exempt_count = sum(
        1 for run in runs if run.error is None and isinstance(run.result, ExemptBondResult)
    )
    matching_rows = _matching_bond_runs(runs)
    matched_count = len(matching_rows)
    error_count = sum(1 for run in runs if run.error is not None)
    md_count = sum(len(result.matched_disposals) for _instrument, result, _trades in matching_rows)
    total_gain = Money.gbp(Decimal("0"))
    for _instrument, result, _trades in matching_rows:
        for md in result.matched_disposals:
            total_gain = total_gain + md.gain_gbp

    table = Table(
        title="Summary",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
    )
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Instruments processed", str(len(runs)))
    table.add_row("…exempt (skipped)", str(exempt_count))
    table.add_row("…non-exempt matched", str(matched_count))
    table.add_row("…with errors", str(error_count))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{format_money_2dp(total_gain)}[/]",
    )
    console.print(table)


def _render_match_bonds_errors(runs: Sequence[BondEngineRun]) -> None:
    """Print every per-instrument error in one block at the end."""
    error_rows = [(run.instrument, run.error) for run in runs if run.error is not None]
    if not error_rows:
        return
    console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        console.print(f"  [bold cyan]{_bond_divider(instrument)}[/] [red]→ {error}[/]")


def _bond_divider(instrument: BondInstrument) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{instrument.symbol} ({instrument.currency})"
