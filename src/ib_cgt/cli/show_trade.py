"""`ib-cgt show trade ID` — drill from a trade id back to its statement row.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table

from ib_cgt.cli.app import show_app
from ib_cgt.cli.common import build_fx_service, console
from ib_cgt.config import resolve_db_path
from ib_cgt.db import (
    OptionExerciseLinkRepo,
    StatementRepo,
    StatementRow,
    StoredTrade,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.domain import (
    FutureInstrument,
    FXInstrument,
    Money,
    OptionInstrument,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FXService, RateNotFoundError


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
            console.print(f"[red]No trade found with trade_id={trade_id}.[/]")
            raise typer.Exit(code=1)
        statement = StatementRepo(conn).get(stored.statement_hash)
        # FX-conversion preview at trade_date — for non-GBP trades only.
        # Build a real `FXService` so the rate the dossier prints is
        # exactly what the rule engines would consume.
        fx_service = build_fx_service(conn)
        rate_preview = _fx_rate_preview(stored.trade, fx_service)
        # The share trade an exercised / assigned option produced, if
        # the ingest linked one (`option_exercise_links`).
        linked_share = OptionExerciseLinkRepo(conn).for_option_trades([trade_id]).get(trade_id)
    finally:
        conn.close()

    _render_show_trade(stored, statement, rate_preview, db_path, linked_share)


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
    linked_share: int | None = None,
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
    if statement is not None:
        # The clock as the statement prints it — the value to look for
        # in the source file when verifying the row by hand.
        printed = trade.trade_datetime.astimezone(statement.time_zone)
        table.add_row(
            "As printed",
            f"{printed.strftime('%Y-%m-%d, %H:%M:%S')}  ({statement.time_zone.key})",
        )
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
    _render_show_trade_extras(table, stored, rate_preview, linked_share)

    console.print(table)


def _render_show_trade_extras(
    table: Table,
    stored: StoredTrade,
    rate_preview: Decimal | None,
    linked_share: int | None = None,
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
        table.add_row("Conid", str(instrument.conid))
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
            f"conid={instrument.conid}  "
            f"multiplier={instrument.contract_multiplier}  "
            f"expiry={instrument.expiry_date.isoformat()}",
        )
        table.add_row(
            "Realisation linkage",
            "see `ib-cgt show realisation --close <id>` for any closeout this trade drove",
        )

    elif isinstance(instrument, OptionInstrument):
        table.add_row(
            "Series",
            f"conid={instrument.conid}  {instrument.right.value} on {instrument.underlying}  "
            f"strike={instrument.strike}  expiry={instrument.expiry_date.isoformat()}  "
            f"multiplier={instrument.contract_multiplier}",
        )
        # The premium a row moves is price x multiplier x contracts.
        premium = trade.price.amount * instrument.contract_multiplier * trade.quantity
        table.add_row(
            "Premium (price*mult*qty)",
            f"{premium} {trade.price.currency}  fees={trade.fees.amount} {trade.fees.currency}",
        )
        if rate_preview is not None:
            table.add_row("GBP equivalent of premium", f"£{premium / rate_preview:.2f}")
        if trade.action in (TradeAction.EXERCISE_LONG, TradeAction.ASSIGN_SHORT):
            table.add_row(
                "Exercise linkage",
                f"share trade #{linked_share} (one transaction, TCGA 1992 s.144)"
                if linked_share is not None
                else "no share trade linked — treated as cash-settled (s.144A)",
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
