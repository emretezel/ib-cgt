"""`ib-cgt show match --disposal ID` — every chunk matched against one FX disposal.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.calculator import load_fx_inputs, run_future_engine, run_fx_pools
from ib_cgt.cli.app import show_app
from ib_cgt.cli.common import build_fx_service, console, format_money_2dp, format_qty_2dp
from ib_cgt.cli.fx_labels import FxLabels, fx_basis_cells, fx_divider
from ib_cgt.config import resolve_db_path
from ib_cgt.db import StoredTrade, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import FutureInstrument, FXInstrument, StockInstrument, Trade
from ib_cgt.rules import MatchingResult


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
            console.print(f"[red]No trade found with trade_id={disposal}.[/]")
            raise typer.Exit(code=1)
        pools = _pools_disposal_could_touch(stored.trade)
        if not pools:
            console.print(
                f"[yellow]Trade #{disposal} doesn't touch any non-GBP pool — "
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
        disposal_trade_id=disposal,
        stored=stored,
        per_pool_results=per_pool_results,
        labels=labels,
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
    labels: FxLabels,
    db_path: Path,
) -> None:
    """Render per-pool tables of chunks belonging to one disposal."""
    source_label = labels.source_descriptions.get(disposal_trade_id, "—")
    console.print(
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
