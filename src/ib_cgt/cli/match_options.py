"""`ib-cgt match options` — dry-run the options rule engine and render both sides.

Runs `run_option_engine` over the ingested history without persisting
anything and prints, per series, the holder's matched disposals (the
same share-matching columns `match stocks` uses), then across every
series the written options' grants with their later events, the
grants still open, the exercise transfers handed to the stock engine,
a summary and any per-series errors.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import OptionEngineRun, run_option_engine
from ib_cgt.cli.app import match_app
from ib_cgt.cli.common import (
    build_fx_service,
    console,
    format_fx_rate,
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
from ib_cgt.domain import Money, OpenGrant, OptionExerciseTransfer, OptionGrant, OptionInstrument


@match_app.command("options")
def match_options(
    symbol: Annotated[
        str | None,
        typer.Option(
            "--symbol",
            "-s",
            help="Filter to one option series by its display symbol, e.g. 'XSPAM 20DEC14 140.0 P'.",
        ),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help=(
                "Inclusive lower bound on trade_date (YYYY-MM-DD). Caveat: clipping "
                "mid-history breaks pool reconstruction and the grant ledger; debugging only."
            ),
        ),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option(
            "--until",
            help="Inclusive upper bound on trade_date (YYYY-MM-DD). Same caveat as --since.",
        ),
    ] = None,
) -> None:
    """Dry-run the options rule engine against ingested trades.

    Walks every option series that matches the filters, runs
    `OptionRuleEngine.compute` against its trade history (across all
    accounts — bought options are pooled by series per taxpayer) using
    the real `FXService` through the calculator's shared runner, and
    prints both sides: the holder's matched disposals and the writer's
    grants (TCGA 1992 s.144(1)) with every later close, plus the
    exercise transfers the stock engine will fold into share trades.
    Nothing is written to the database.
    """
    since_date = parse_iso_date(since, "--since")
    until_date = parse_iso_date(until, "--until")

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        runs = run_option_engine(
            conn,
            build_fx_service(conn),
            symbol=symbol,
            since=since_date,
            until=until_date,
        )
    finally:
        conn.close()

    if not runs:
        console.print("[yellow]No option series match the given filters.[/]")
        return

    _render_match_options(runs, db_path)


def _render_match_options(runs: Sequence[OptionEngineRun], db_path: Path) -> None:
    """Render matched disposals, unmatched, grants, open grants, transfers, summary, errors."""
    _render_matched_disposals(runs)
    render_instrument_unmatched_disposals(
        [
            (run.instrument, chunk)
            for run in runs
            if run.error is None and run.result is not None
            for chunk in run.result.matched.unmatched_disposals
        ]
    )
    _render_grants(runs)
    _render_open_grants(runs)
    _render_transfers(runs)
    _render_summary(runs, db_path)
    _render_errors(runs)


def _render_matched_disposals(runs: Sequence[OptionEngineRun]) -> None:
    """Per-series section for the holder's side, in the share-matching columns."""
    console.print("[bold]Option matched disposals — bought options (dry-run)[/]")
    for run in runs:
        if run.error is not None:
            continue
        result = run.result
        assert result is not None  # mypy — error/result are mutually exclusive
        console.print(f"\n[bold cyan]{_divider(run.instrument)}[/]")
        if not result.matched.matched_disposals:
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
        for md in result.matched.matched_disposals:
            table.add_row(*matched_disposal_to_cells(md, date_map))
        console.print(table)


def _render_grants(runs: Sequence[OptionEngineRun]) -> None:
    """One flat table of every written option's grant across all series."""
    grants: list[tuple[OptionInstrument, OptionGrant]] = [
        (run.instrument, grant)
        for run in runs
        if run.error is None and run.result is not None
        for grant in run.result.grants
    ]
    if not grants:
        console.print("[dim]No written options.[/]")
        return
    table = Table(
        title="Written options — the grant is the disposal (s.144(1))",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Symbol")
    table.add_column("Grant ID", justify="right")
    table.add_column("Grant Date")
    table.add_column("Written", justify="right")
    table.add_column("Charged", justify="right")
    table.add_column("Premium", justify="right")
    table.add_column("Fee", justify="right")
    table.add_column("FX grant", justify="right")
    table.add_column("Later events")
    table.add_column("Proceeds (GBP)", justify="right")
    table.add_column("Costs (GBP)", justify="right")
    table.add_column("Gain (GBP)", justify="right")
    for instrument, grant in grants:
        gain = grant.gain_gbp
        gain_style = "green" if gain.amount >= 0 else "red"
        table.add_row(
            instrument.symbol,
            str(grant.grant_trade_id),
            grant.grant_date.isoformat(),
            format_qty_2dp(grant.quantity),
            format_qty_2dp(grant.chargeable_quantity),
            f"{format_money_2dp(grant.premium_native)} {grant.premium_native.currency}",
            f"{format_money_2dp(grant.grant_fee_native)} {grant.grant_fee_native.currency}",
            format_fx_rate(grant.grant_fx_rate),
            _closes_text(grant),
            format_money_2dp(grant.chargeable_proceeds_gbp),
            format_money_2dp(grant.incidental_costs_gbp),
            f"[{gain_style}]{format_money_2dp(gain)}[/]",
        )
    console.print(table)


def _closes_text(grant: OptionGrant) -> str:
    """`purchase #7 2012-11-01 1.00 for 142.45 USD; lapse #9 …` or `open`."""
    if not grant.closes:
        return "open"
    parts = []
    for close in grant.closes:
        paid = close.premium_native + close.fee_native
        parts.append(
            f"{close.kind.value} #{close.close_trade_id} {close.close_date.isoformat()} "
            f"{format_qty_2dp(close.quantity)} for {format_money_2dp(paid)} {paid.currency}"
        )
    return "; ".join(parts)


def _render_open_grants(runs: Sequence[OptionEngineRun]) -> None:
    """Written contracts still open at end of input, across all series."""
    open_grants: list[tuple[OptionInstrument, OpenGrant]] = [
        (run.instrument, grant)
        for run in runs
        if run.error is None and run.result is not None
        for grant in run.result.open_grants
    ]
    if not open_grants:
        console.print("[dim]No written options remain open.[/]")
        return
    table = Table(title="Written options still open", header_style="bold", show_lines=False)
    table.add_column("Symbol")
    table.add_column("Grant ID", justify="right")
    table.add_column("Grant Date")
    table.add_column("Qty Open", justify="right")
    table.add_column("Premium / unit")
    table.add_column("Fees Remaining")
    for instrument, grant in open_grants:
        table.add_row(
            instrument.symbol,
            str(grant.grant_trade_id),
            grant.grant_date.isoformat(),
            format_qty_2dp(grant.quantity_remaining),
            f"{grant.premium_price.amount} {grant.premium_price.currency}",
            f"{format_money_2dp(grant.fees_remaining)} {grant.fees_remaining.currency}",
        )
    console.print(table)


def _render_transfers(runs: Sequence[OptionEngineRun]) -> None:
    """What each exercise or assignment carries into its share trade (s.144(2)-(3))."""
    transfers: list[tuple[OptionInstrument, OptionExerciseTransfer]] = [
        (run.instrument, transfer)
        for run in runs
        if run.error is None and run.result is not None
        for transfer in run.result.transfers
    ]
    if not transfers:
        return
    table = Table(
        title="Exercises and assignments — carried into the share trade (s.144(2)-(3))",
        header_style="bold",
        show_lines=False,
    )
    table.add_column("Symbol")
    table.add_column("Option ID", justify="right")
    table.add_column("Share ID", justify="right")
    table.add_column("Side")
    table.add_column("Right")
    table.add_column("Date")
    table.add_column("Qty", justify="right")
    table.add_column("Amount (GBP)", justify="right")
    table.add_column("Fees (GBP)", justify="right")
    table.add_column("Effect on share trade")
    for instrument, transfer in transfers:
        table.add_row(
            instrument.symbol,
            str(transfer.option_trade_id),
            str(transfer.share_trade_id),
            transfer.side,
            instrument.right.value,
            transfer.on.isoformat(),
            format_qty_2dp(transfer.quantity),
            format_money_2dp(transfer.amount_gbp),
            format_money_2dp(transfer.fees_gbp),
            _effect(transfer),
        )
    console.print(table)


def _effect(transfer: OptionExerciseTransfer) -> str:
    """The four s.144 cases in words."""
    is_call = transfer.instrument.right.value == "call"
    if transfer.side == "LONG":
        return "added to share cost" if is_call else "cost of the share disposal"
    return "added to share proceeds" if is_call else "deducted from share cost"


def _render_summary(runs: Sequence[OptionEngineRun], db_path: Path) -> None:
    """Counts and the total realised gain over both sides."""
    error_count = sum(1 for run in runs if run.error is not None)
    chunk_count = 0
    grant_count = 0
    total_gain = Money.gbp(Decimal("0"))
    for run in runs:
        if run.error is not None or run.result is None:
            continue
        chunk_count += len(run.result.matched.matched_disposals)
        grant_count += len(run.result.grants)
        for md in run.result.matched.matched_disposals:
            total_gain = total_gain + md.gain_gbp
        for grant in run.result.grants:
            total_gain = total_gain + grant.gain_gbp
    table = Table(title="Summary", caption=f"[dim]{db_path}[/]", header_style="bold")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Series processed", str(len(runs)))
    table.add_row("…with errors", str(error_count))
    table.add_row("Matched disposal chunks", str(chunk_count))
    table.add_row("Grants", str(grant_count))
    gain_style = "green" if total_gain.amount >= 0 else "red"
    table.add_row("Total realised gain (GBP)", f"[{gain_style}]{format_money_2dp(total_gain)}[/]")
    console.print(table)


def _render_errors(runs: Sequence[OptionEngineRun]) -> None:
    """Every per-series error in one block at the end."""
    error_rows = [(run.instrument, run.error) for run in runs if run.error is not None]
    if not error_rows:
        return
    console.print(f"\n[bold red]Errors ({len(error_rows)})[/]")
    for instrument, error in error_rows:
        console.print(f"  [bold cyan]{_divider(instrument)}[/] [red]→ {error}[/]")


def _divider(instrument: OptionInstrument) -> str:
    """Stable label used in section dividers and error rows."""
    return (
        f"{instrument.symbol} ({instrument.currency}) {instrument.right.value} "
        f"{instrument.strike} on {instrument.underlying} expiry "
        f"{instrument.expiry_date.isoformat()} conid={instrument.conid}"
    )
