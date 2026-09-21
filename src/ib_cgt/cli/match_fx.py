"""`ib-cgt match fx` — dry-run the per-currency FX pool engine and render it.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import FXEngineRun, run_fx_engine
from ib_cgt.cli.app import match_app
from ib_cgt.cli.common import (
    build_fx_service,
    console,
    format_money_2dp,
    format_qty_2dp,
    parse_iso_date,
)
from ib_cgt.cli.fx_labels import FxLabels, fx_basis_cells, fx_divider
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection
from ib_cgt.domain import (
    MatchedDisposal,
    Money,
    TaxLot,
    UnmatchedAcquisition,
    UnmatchedDisposalChunk,
)


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
    every non-GBP futures trade, every dividend and coupon, and runs
    `FutureRuleEngine` to derive realisations. The cashflow streams
    are fed into `FXRuleEngine.compute(ccy, …)` per non-GBP currency
    (via the calculator's shared runner) so the pool reflects every
    foreign-currency cash movement IB reports (HMRC CG78315 —
    "foreign currency arising from any source").

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
    since_date = parse_iso_date(since, "--since")
    until_date = parse_iso_date(until, "--until")
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
        runs = run_fx_engine(
            conn,
            build_fx_service(conn),
            currency=currency,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not runs:
        if currency is not None:
            console.print(f"[yellow]Currency '{currency}' has no events in the date range.[/]")
        else:
            console.print("[yellow]No FX-relevant trades found — nothing to match.[/]")
        return

    _render_match_fx(runs, db_path)


def _render_match_fx(runs: Sequence[FXEngineRun], db_path: Path) -> None:
    """Render matched-disposals, unmatched-warnings, residuals, final-pool, summary, errors."""
    labels = FxLabels.from_inputs(runs[0].inputs)
    _render_match_fx_disposals(runs, labels)
    _render_match_fx_unmatched_disposals(runs, labels)
    _render_match_fx_residuals(runs, labels)
    _render_match_fx_final_pools(runs)
    _render_match_fx_summary(runs, db_path)
    _render_match_fx_errors(runs)


def _render_match_fx_disposals(runs: Sequence[FXEngineRun], labels: FxLabels) -> None:
    """Per-currency section: bold header then a Rich matched-disposals table.

    Columns mirror `match stocks` exactly — same fee-pairing /
    subset-semantics conventions — so the auditor's eye doesn't have
    to retrain across asset classes.
    """
    console.print("[bold]FX matched disposals (dry-run)[/]")
    for run in runs:
        if run.error is not None:
            continue
        result = run.result
        assert result is not None  # mypy — error/result are mutually exclusive
        divider = fx_divider(run.currency)
        console.print(f"\n[bold cyan]{divider}[/]")
        if not result.matched_disposals:
            console.print("  [dim](no matched disposals)[/]")
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
            table.add_row(*_fx_matched_disposal_to_cells(md, labels))
        console.print(table)


def _render_match_fx_unmatched_disposals(runs: Sequence[FXEngineRun], labels: FxLabels) -> None:
    """Yellow warning block for soft-residual unmatched disposals.

    Surfaces the FX engine's `unmatched_disposals` in a per-pool
    section so an opening-balance shortfall (or any other cause)
    is impossible to miss. Each row shows the un-covered quantity
    and the proportional GBP value of the disposal that couldn't
    be matched.
    """
    chunks: list[tuple[str, UnmatchedDisposalChunk]] = [
        (run.currency, chunk)
        for run in runs
        if run.error is None and run.result is not None
        for chunk in run.result.unmatched_disposals
    ]
    if not chunks:
        return

    console.print(
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
    for currency, chunk in chunks:
        tid = chunk.disposal_trade_id
        table.add_row(
            currency,
            labels.id_label_map.get(tid, f"#{tid}"),
            labels.source_descriptions.get(tid, "—"),
            chunk.disposal_date.isoformat(),
            format_qty_2dp(chunk.quantity_remaining),
            format_money_2dp(chunk.proceeds_remaining_gbp),
        )
    console.print(table)


def _render_match_fx_residuals(runs: Sequence[FXEngineRun], labels: FxLabels) -> None:
    """One flat table of every UnmatchedAcquisition across all currency pools."""
    residuals: list[tuple[str, UnmatchedAcquisition]] = [
        (run.currency, ua)
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
    table.add_column("Currency")
    table.add_column("Acq ID")
    table.add_column("Acq Source")
    table.add_column("Acq Date")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Cost Remaining (GBP)", justify="right")
    for currency, ua in residuals:
        table.add_row(
            currency,
            labels.id_label_map.get(ua.trade_id, f"#{ua.trade_id}"),
            labels.source_descriptions.get(ua.trade_id, "—"),
            ua.acquisition_date.isoformat(),
            format_qty_2dp(ua.quantity_remaining),
            format_money_2dp(ua.cost_remaining_gbp),
        )
    console.print(table)


def _render_match_fx_final_pools(runs: Sequence[FXEngineRun]) -> None:
    """Per-currency final-pool aggregate. Skips empty pools."""
    pools: list[tuple[str, TaxLot]] = [
        (run.currency, run.result.final_pool)
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
    table.add_column("Currency")
    table.add_column("Pool Qty", justify="right")
    table.add_column("Pool Cost (GBP)", justify="right")
    table.add_column("Avg Cost (GBP)", justify="right")
    for currency, pool in pools:
        table.add_row(
            currency,
            format_qty_2dp(pool.quantity),
            format_money_2dp(pool.total_cost_gbp),
            format_money_2dp(pool.average_cost_gbp),
        )
    console.print(table)


def _render_match_fx_summary(runs: Sequence[FXEngineRun], db_path: Path) -> None:
    """Small summary: counts and total realised gain across all pools."""
    error_count = sum(1 for run in runs if run.error is not None)
    md_count = sum(
        len(run.result.matched_disposals)
        for run in runs
        if run.error is None and run.result is not None
    )
    pools_with_residual = sum(
        1
        for run in runs
        if run.error is None and run.result is not None and run.result.unmatched_disposals
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
    table.add_row("Currencies processed", str(len(runs)))
    table.add_row("…with errors", str(error_count))
    table.add_row("…with residual disposals", str(pools_with_residual))
    table.add_row("Matched disposal chunks", str(md_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{format_money_2dp(total_gain)}[/]",
    )
    console.print(table)


def _render_match_fx_errors(runs: Sequence[FXEngineRun]) -> None:
    """Print every per-currency error in one block at the end."""
    error_rows = [(run.currency, run.error) for run in runs if run.error is not None]
    if not error_rows:
        return
    console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for currency, error in error_rows:
        console.print(f"  [bold cyan]{fx_divider(currency)}[/] [red]→ {error}[/]")


def _fx_matched_disposal_to_cells(md: MatchedDisposal, labels: FxLabels) -> tuple[str, ...]:
    """FX-specific projection of a `MatchedDisposal` row.

    Adds two columns the stocks renderer doesn't have — Disp Source
    and Acq Source — so the auditor can immediately see whether each
    leg came from a forex trade, a stock trade, a futures fee, a
    futures realisation P&L, a dividend, or a coupon. ID columns
    consume `labels.id_label_map` so a futures-realisation event
    renders as `P&L #A→#B[i]` instead of the run-unstable synthetic
    int.
    """
    proceeds = md.matched_proceeds_gbp
    proceeds_style = "green" if proceeds.amount >= 0 else "red"
    gain = md.gain_gbp
    gain_style = "green" if gain.amount >= 0 else "red"
    basis_text, acq_source_text, acq_date_text = fx_basis_cells(md.basis, labels)
    disp_id = md.disposal_trade_id
    disp_source = labels.source_descriptions.get(disp_id, "—")
    disp_label = labels.id_label_map.get(disp_id, f"#{disp_id}")
    return (
        disp_label,
        disp_source,
        md.disposal_date.isoformat(),
        md.match_rule.value,
        format_qty_2dp(md.matched_quantity),
        basis_text,
        acq_source_text,
        acq_date_text,
        f"[{proceeds_style}]{format_money_2dp(proceeds)}[/]",
        format_money_2dp(md.matched_disposal_fees_gbp),
        format_money_2dp(md.matched_cost_gbp),
        format_money_2dp(md.matched_acquisition_fees_gbp),
        f"[{gain_style}]{format_money_2dp(gain)}[/]",
    )
