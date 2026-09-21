"""`ib-cgt show realisation` — audit one futures close-out against its trades.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.cli.app import show_app
from ib_cgt.cli.common import build_fx_service, console
from ib_cgt.config import resolve_db_path
from ib_cgt.db import StatementRepo, StatementRow, TradeRepo, apply_migrations, open_connection
from ib_cgt.domain import FutureInstrument, FutureRealisation, TradeAction
from ib_cgt.fx import RateNotFoundError
from ib_cgt.rules import FutureRuleEngine, InconsistentTradeError, WrongAssetClassError


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
            console.print(f"[red]No trade found with trade_id={close}.[/]")
            raise typer.Exit(code=1)
        if not isinstance(stored.trade.instrument, FutureInstrument):
            console.print(
                f"[red]Trade #{close} is not a futures trade — "
                f"it's a {stored.trade.instrument.asset_class.value} trade.[/]"
            )
            raise typer.Exit(code=1)
        if stored.trade.action not in (TradeAction.CLOSE_LONG, TradeAction.CLOSE_SHORT):
            console.print(
                f"[red]Trade #{close} is a {stored.trade.action.value} — only "
                "CLOSE_LONG / CLOSE_SHORT trades produce realisations. Did you "
                "mean to pass an open trade id to `--open`?[/]"
            )
            raise typer.Exit(code=1)

        instrument = stored.trade.instrument
        instrument_trades = TradeRepo(conn).for_instrument_with_ids(stored.instrument_id)
        fx_service = build_fx_service(conn)
        engine = FutureRuleEngine(fx_service)
        try:
            result = engine.compute(instrument, instrument_trades)
        except (
            WrongAssetClassError,
            InconsistentTradeError,
            RateNotFoundError,
        ) as exc:
            console.print(f"[red]Could not compute realisations: {exc}[/]")
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
        console.print(f"[yellow]{msg}.[/]")
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
    console.print(table)


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
