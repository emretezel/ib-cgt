"""`ib-cgt show match --disposal ID` — every chunk matched against one FX disposal.

The disposal is normally a real trade. A corporate action that paid
cash (`CA #N` in the match tables) is cited by its synthetic event id
instead; `--corporate-action N` names it by the row id the tables
print, and `--disposal` accepts the event id itself.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import (
    corporate_action_event_id,
    corporate_action_id_of,
    load_fx_inputs,
    run_future_engine,
    run_fx_pools,
)
from ib_cgt.cli.app import show_app
from ib_cgt.cli.common import build_fx_service, console, format_money_2dp, format_qty_2dp
from ib_cgt.cli.fx_labels import FxLabels, fx_basis_cells, fx_divider
from ib_cgt.config import resolve_db_path
from ib_cgt.db import CorporateActionRepo, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import CorporateAction, FutureInstrument, FXInstrument, StockInstrument, Trade
from ib_cgt.report.labels import corporate_action_label
from ib_cgt.rules import MatchingResult


@dataclass(frozen=True, slots=True, kw_only=True)
class _DisposalHeader:
    """What the report line names: the disposal's citeable label and its date."""

    label: str
    on: date


@show_app.command("match")
def show_match(
    disposal: Annotated[
        int | None,
        typer.Option(
            "--disposal",
            help=(
                "Disposal id whose matched chunks you want to audit: a trade_id as printed "
                "in `match fx`, or the synthetic event id of a corporate action."
            ),
        ),
    ] = None,
    corporate_action: Annotated[
        int | None,
        typer.Option(
            "--corporate-action",
            help=(
                "The N of a `CA #N` label — the corporate action whose cash leg you want "
                "to audit. Equivalent to --disposal with its event id."
            ),
        ),
    ] = None,
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
    if disposal is not None and corporate_action is None:
        event_id = disposal
    elif corporate_action is not None and disposal is None:
        event_id = corporate_action_event_id(corporate_action)
    else:
        console.print("[red]Give exactly one of --disposal or --corporate-action.[/]")
        raise typer.Exit(code=2)

    db_path = resolve_db_path()
    conn = open_connection(db_path)
    try:
        apply_migrations(conn)
        header, pools = _locate(conn, event_id)
        if not pools:
            console.print(
                f"[yellow]{header.label} doesn't touch any non-GBP pool — "
                "it can't appear in `match fx` output.[/]"
            )
            raise typer.Exit(code=0)

        # Same loader `match fx` uses (via the calculator's runner) so
        # the chunks printed here are exactly the ones that command
        # rendered. Only the pools this disposal can touch are re-run.
        fx_service = build_fx_service(conn)
        inputs = load_fx_inputs(conn, future_runs=run_future_engine(conn, fx_service))
        labels = FxLabels.from_inputs(inputs)
        per_pool_results: list[tuple[str, MatchingResult]] = []
        for run in run_fx_pools(fx_service, inputs, pools):
            if run.error is not None:
                console.print(f"[red]Pool {run.currency}: {run.error}[/]")
                continue
            assert run.result is not None  # mypy — error/result are mutually exclusive
            per_pool_results.append((run.currency, run.result))
    finally:
        conn.close()

    _render_show_match(
        disposal_trade_id=event_id,
        header=header,
        per_pool_results=per_pool_results,
        labels=labels,
        db_path=db_path,
    )


def _locate(conn: sqlite3.Connection, event_id: int) -> tuple[_DisposalHeader, list[str]]:
    """Resolve an event id to its header line and the pools it could touch.

    A real trade resolves through `trades`; a corporate-action event
    id through `corporate_actions` (the pool is its cash currency).
    Anything else is an error the user sees.
    """
    stored = TradeRepo(conn).get(event_id)
    if stored is not None:
        header = _DisposalHeader(label=f"#{event_id}", on=stored.trade.trade_date)
        return header, _pools_disposal_could_touch(stored.trade)
    action_id = corporate_action_id_of(event_id)
    stored_action = CorporateActionRepo(conn).get(action_id) if action_id is not None else None
    if action_id is None or stored_action is None:
        console.print(f"[red]No trade or corporate action found for id={event_id}.[/]")
        raise typer.Exit(code=1)
    header = _DisposalHeader(
        label=f"{corporate_action_label(action_id)} (event id {event_id})",
        on=stored_action.action.effective_date,
    )
    return header, _pools_corporate_action_could_touch(stored_action.action)


def _pools_corporate_action_could_touch(action: CorporateAction) -> list[str]:
    """The pool a corporate action's cash leg feeds: its cash currency, unless GBP."""
    if action.cash is None or action.cash.currency == "GBP":
        return []
    return [action.cash.currency]


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
    header: _DisposalHeader,
    per_pool_results: list[tuple[str, MatchingResult]],
    labels: FxLabels,
    db_path: Path,
) -> None:
    """Render per-pool tables of chunks belonging to one disposal."""
    source_label = labels.source_descriptions.get(disposal_trade_id, "—")
    console.print(f"\n[bold]Disposal {header.label} — {source_label} on {header.on.isoformat()}[/]")

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
        console.print(f"\n[bold cyan]{fx_divider(ccy)}[/]")

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
            basis_text, _src, _date = fx_basis_cells(md.basis, labels)
            table.add_row(
                str(i),
                md.match_rule.value,
                format_qty_2dp(md.matched_quantity),
                format_money_2dp(md.matched_cost_gbp),
                format_money_2dp(md.matched_proceeds_gbp),
                basis_text,
                format_qty_2dp(after),
            )

        if residual is not None:
            table.add_row(
                "—",
                "[yellow]UNMATCHED[/]",
                format_qty_2dp(residual.quantity_remaining),
                "—",
                format_money_2dp(residual.proceeds_remaining_gbp),
                "[yellow]opening-balance shortfall[/]",
                format_qty_2dp(Decimal(0)),
            )
        console.print(table)
        console.print(
            f"  [dim]total disposal qty (this pool): {format_qty_2dp(total_disposal_qty)} "
            f"({len(chunks)} matched chunk(s))[/]"
        )

    if not found_anything:
        console.print(
            "\n[yellow]No matched chunks found for this disposal in any pool. "
            "Either the trade isn't a disposal, or it didn't reach the FX engine "
            "(e.g. GBP-denominated stock trade).[/]"
        )
    console.print(f"\n[dim]{db_path}[/]")
