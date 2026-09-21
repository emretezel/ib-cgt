"""`ib-cgt match futures` — dry-run the futures close-out engine and render it.

Runs `run_future_engine` over the ingested history without persisting
anything, then prints per-instrument realisation tables, open slices,
a summary and any per-instrument errors.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import FutureEngineRun, run_future_engine
from ib_cgt.cli.app import match_app
from ib_cgt.cli.common import (
    build_fx_service,
    console,
    format_fx_rate,
    format_money_2dp,
    format_money_signed_2dp,
    format_qty_2dp,
    parse_iso_date,
)
from ib_cgt.config import resolve_db_path
from ib_cgt.db import apply_migrations, open_connection
from ib_cgt.domain import FutureInstrument, FutureRealisation, Money, OpenPosition


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
    real `FXService` (via the calculator's shared runner, so the
    figures are exactly what `compute` would persist), and prints the
    resulting realisations and open positions. Nothing is written to
    the database — this command is a read-only audit tool.
    """
    since_date = parse_iso_date(since, "--since")
    until_date = parse_iso_date(until, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        # Defensive — same as `ingest`. A fresh DB file would otherwise
        # surface as a confusing "no such table" error.
        apply_migrations(conn)
        runs = run_future_engine(
            conn,
            build_fx_service(conn),
            symbol=symbol,
            account_id=account,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not runs:
        console.print("[yellow]No futures instruments match the given filters.[/]")
        return

    _render_match_futures(runs, db_path)


def _render_match_futures(runs: Sequence[FutureEngineRun], db_path: Path) -> None:
    """Render the realisations, open-positions, summary, and errors blocks.

    Errors are deliberately printed last — collected together at the
    bottom of the output rather than scattered through the
    per-instrument realisations sections — so the operator can scan
    every failure in one place after a multi-instrument run.
    """
    _render_match_futures_realisations(runs)
    _render_match_futures_open_positions(runs)
    _render_match_futures_summary(runs, db_path)
    _render_match_futures_errors(runs)


def _render_match_futures_realisations(runs: Sequence[FutureEngineRun]) -> None:
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
    console.print("[bold]Futures realisations (dry-run)[/]")
    for run in runs:
        # Errors are surfaced together at the end of the output by
        # `_render_match_futures_errors`; skip them here so this
        # section only contains successful per-instrument tables.
        if run.error is not None:
            continue
        result = run.result
        assert result is not None  # mypy — error/result are mutually exclusive
        instrument = run.instrument
        divider = _instrument_divider(instrument)
        console.print(f"\n[bold cyan]{divider}[/]")
        if not result.realisations:
            console.print("  [dim](no realisations)[/]")
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
        console.print(table)


def _render_match_futures_open_positions(runs: Sequence[FutureEngineRun]) -> None:
    """One flat table of every still-open slice across all instruments."""
    open_positions = [
        (run.instrument, position)
        for run in runs
        if run.error is None and run.result is not None
        for position in run.result.open_positions
    ]
    if not open_positions:
        console.print("[dim]No open positions remain after matching.[/]")
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

    console.print(table)


def _render_match_futures_summary(runs: Sequence[FutureEngineRun], db_path: Path) -> None:
    """Small summary: instrument counts, totals, total realised gain."""
    error_count = sum(1 for run in runs if run.error is not None)
    realisation_count = sum(
        len(run.result.realisations) for run in runs if run.error is None and run.result is not None
    )
    open_count = sum(
        len(run.result.open_positions)
        for run in runs
        if run.error is None and run.result is not None
    )
    # Aggregate gain via Money so currency invariants stay enforced —
    # every realisation is GBP per `FutureRealisation.__post_init__`,
    # so the running total stays GBP without explicit checks here.
    total_gain = Money.gbp(Decimal("0"))
    for run in runs:
        if run.error is not None or run.result is None:
            continue
        for realisation in run.result.realisations:
            total_gain = total_gain + realisation.gain_gbp

    table = Table(
        title="Summary",
        caption=f"[dim]{db_path}[/]",
        header_style="bold",
    )
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Instruments processed", str(len(runs)))
    table.add_row("…with errors", str(error_count))
    table.add_row("Realisations", str(realisation_count))
    table.add_row("Open positions", str(open_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row(
        "Total realised gain (GBP)",
        f"[{gain_style}]{format_money_2dp(total_gain)}[/]",
    )

    console.print(table)


def _render_match_futures_errors(runs: Sequence[FutureEngineRun]) -> None:
    """Print every per-instrument error in one block at the end of the output.

    Each failing instrument gets a single bullet line — the same
    instrument label used in the realisations section followed by the
    captured exception's message. Returns silently when no rows
    failed so a clean run produces no trailing block at all.
    """
    error_rows = [(run.instrument, run.error) for run in runs if run.error is not None]
    if not error_rows:
        return
    console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        console.print(f"  [bold cyan]{_instrument_divider(instrument)}[/] [red]→ {error}[/]")


def _instrument_divider(instrument: FutureInstrument) -> str:
    """Stable label used in section dividers and error rows.

    The conid is appended because the symbol is display text IB may
    rename between statements; the contract id is what stays constant.
    """
    return (
        f"{instrument.symbol} {instrument.expiry_date.isoformat()} ({instrument.currency}) "
        f"conid={instrument.conid}"
    )


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
        format_qty_2dp(realisation.quantity),
        format_money_signed_2dp(realisation.gross_pnl_native),
        format_money_2dp(realisation.open_fee_native),
        format_money_2dp(realisation.close_fee_native),
        format_fx_rate(realisation.open_fx_rate),
        format_fx_rate(realisation.close_fx_rate),
        f"[{proceeds_style}]{format_money_2dp(proceeds)}[/]",
        format_money_2dp(realisation.cost_gbp),
        f"[{gain_style}]{format_money_2dp(gain)}[/]",
    )


def _open_position_to_cells(position: OpenPosition) -> tuple[str, ...]:
    """Project an `OpenPosition` into the columns of the open-positions table."""
    return (
        position.instrument.symbol,
        position.instrument.expiry_date.isoformat(),
        position.side,
        position.open_date.isoformat(),
        str(position.open_trade_id),
        format_qty_2dp(position.quantity_remaining),
        f"{format_money_2dp(position.open_price)} {position.open_price.currency}",
        f"{format_money_2dp(position.fees_remaining)} {position.fees_remaining.currency}",
    )
