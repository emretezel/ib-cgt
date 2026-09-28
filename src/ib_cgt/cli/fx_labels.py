"""FX provenance labels shared by `match fx` and `show match`.

The FX engine works on synthetic integer event ids. These helpers map
them back to the citeable labels documented in `docs/audit.md`
(`Div #N`, `Cpn #N`, `Cash #N`, `P&L #a→#b`, …), to event dates, and
to the short id labels that fit a narrow table cell. `FxLabels`
bundles the three side maps so both commands build them the same way.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ib_cgt.calculator import FXInputs
from ib_cgt.cli.common import format_money_2dp, format_qty_2dp
from ib_cgt.domain import (
    BondCouponRef,
    CashEventRef,
    DirectAcquisition,
    DividendKind,
    DividendRef,
    TaxLotSnapshot,
)


@dataclass(frozen=True, slots=True)
class FxLabels:
    """The three id-keyed side maps the FX renderers read.

    Every FX run in a pass shares one `FXInputs` bundle, so the
    labels are built once per pass from that bundle rather than
    threaded through each row. `date_map` feeds the "Acq Date"
    column, `source_descriptions` the "Disp Source" / "Acq Source"
    columns, and `id_label_map` the narrow citeable id cells.
    """

    date_map: dict[int, date]
    source_descriptions: dict[int, str]
    id_label_map: dict[int, str]

    @classmethod
    def from_inputs(cls, inputs: FXInputs) -> FxLabels:
        """Derive every side map from one shared input bundle."""
        return cls(
            date_map=build_fx_event_date_map(inputs),
            source_descriptions=build_fx_source_descriptions(inputs),
            id_label_map=build_fx_id_label_map(inputs),
        )


def build_fx_source_descriptions(inputs: FXInputs) -> dict[int, str]:
    """Build the event-id → source-label map used by the FX renderer.

    Forex / stock / bond / futures-fee / option events use the real
    `trades.trade_id` (globally unique). Realisation P&L, dividend and
    coupon events use the synthetic ids the runner allocated from
    disjoint high ranges; the label carries enough of the source to be
    readable without the id.
    """
    out: dict[int, str] = {}
    for tid, trade in inputs.forex_trades:
        out[tid] = f"forex {trade.instrument.symbol} {trade.action.value}"
    for tid, trade in inputs.stock_trades:
        out[tid] = f"stock {trade.instrument.symbol} {trade.action.value}"
    for tid, trade in inputs.bond_trades:
        out[tid] = f"bond {trade.instrument.symbol} {trade.action.value}"
    for tid, trade in inputs.future_trades:
        out[tid] = f"futures fee {trade.instrument.symbol} {trade.action.value}"
    for tid, trade in inputs.option_trades:
        out[tid] = f"option {trade.instrument.symbol} {trade.action.value}"
    for synth_id, realisation, _account in inputs.future_realisations:
        side = "P&L"
        out[synth_id] = (
            f"futures {side} {realisation.instrument.symbol} "
            f"open={realisation.open_trade_id} close={realisation.close_trade_id}"
        )
    for synth_id, dividend in inputs.dividends:
        out[synth_id] = f"dividend {dividend.symbol} {dividend.kind.value}"
    for synth_id, coupon in inputs.bond_coupons:
        out[synth_id] = f"bond coupon {coupon.instrument.symbol}"
    for synth_id, event in inputs.cash_events:
        out[synth_id] = f"{event.kind.value}: {event.description}"
    return out


def build_fx_event_date_map(inputs: FXInputs) -> dict[int, date]:
    """Map every event id (real or synthetic) to its event date.

    The renderer's "Acq Date" column reads this directly. Real
    trade events use `trade_date`; futures-realisation events use
    `close_date` (the date the P&L cashflow lands); dividend and
    coupon events use `pay_date` (the date the cash hits the
    foreign-currency balance).
    """
    out: dict[int, date] = {}
    for tid, trade in inputs.forex_trades:
        out[tid] = trade.trade_date
    for tid, trade in inputs.stock_trades:
        out[tid] = trade.trade_date
    for tid, trade in inputs.bond_trades:
        out[tid] = trade.trade_date
    for tid, trade in inputs.future_trades:
        out[tid] = trade.trade_date
    for tid, trade in inputs.option_trades:
        out[tid] = trade.trade_date
    for synth_id, realisation, _account in inputs.future_realisations:
        out[synth_id] = realisation.close_date
    for synth_id, dividend in inputs.dividends:
        out[synth_id] = dividend.pay_date
    for synth_id, coupon in inputs.bond_coupons:
        out[synth_id] = coupon.pay_date
    for synth_id, event in inputs.cash_events:
        out[synth_id] = event.value_date
    return out


def build_fx_id_label_map(inputs: FXInputs) -> dict[int, str]:
    """Build the short id label map used in narrow `Disp ID` / `Acq ID` cells.

    Real trade ids (forex / stock / futures-fee events) format as
    `#N` so the column stays compact. Futures-realisation events
    use a stable `P&L #A→#B` notation derived from the open and
    close trade ids of the realisation — synthetic engine ids shift
    between runs and aren't citeable, so the renderer never exposes
    them.

    Dividend, coupon and cash events follow the same principle: the
    synthetic id is internal plumbing; the user-facing label carries
    the real row id recovered from `inputs.sources` (`Div #N` for
    cash dividends and payment-in-lieu, `WHT #N` for withholding
    tax, `Cpn #N` for bond coupons, `Cash #N` for interest / transfer
    / fee rows).

    Multi-slice closeouts: when a single close trade drains
    several open slices, multiple realisations share the same
    `(open, close)` close-trade key. Disambiguate with `[i]`
    suffixed in the order the futures engine emitted them (the
    runner preserves that order in `future_realisations`).
    Single-realisation closes get no suffix to keep the common
    case compact.
    """
    out: dict[int, str] = {}
    for tid, _trade in (*inputs.forex_trades, *inputs.stock_trades, *inputs.bond_trades):
        out[tid] = f"#{tid}"
    for tid, _trade in (*inputs.future_trades, *inputs.option_trades):
        out[tid] = f"#{tid}"

    # Group realisations by close_trade_id so we can emit `[i]`
    # only when ambiguous. A first pass counts; a second pass
    # assigns indices.
    close_count: dict[int, int] = {}
    for _synth_id, realisation, _account in inputs.future_realisations:
        close_count[realisation.close_trade_id] = close_count.get(realisation.close_trade_id, 0) + 1
    next_index: dict[int, int] = {}
    for synth_id, realisation, _account in inputs.future_realisations:
        close_id = realisation.close_trade_id
        open_id = realisation.open_trade_id
        if close_count[close_id] > 1:
            i = next_index.get(close_id, 0)
            next_index[close_id] = i + 1
            out[synth_id] = f"P&L #{open_id}→#{close_id}[{i}]"
        else:
            out[synth_id] = f"P&L #{open_id}→#{close_id}"

    # Dividends — `Div #N` for inflows, `WHT #N` for withholding —
    # and coupons — `Cpn #N`. The real row id comes from the runner's
    # provenance map so the cell stays citeable across runs.
    for synth_id, dividend in inputs.dividends:
        source = inputs.sources[synth_id]
        real_id = source.dividend_id if isinstance(source, DividendRef) else synth_id
        prefix = "WHT" if dividend.kind is DividendKind.WITHHOLDING_TAX else "Div"
        out[synth_id] = f"{prefix} #{real_id}"
    for synth_id, _coupon in inputs.bond_coupons:
        source = inputs.sources[synth_id]
        real_id = source.bond_coupon_id if isinstance(source, BondCouponRef) else synth_id
        out[synth_id] = f"Cpn #{real_id}"
    for synth_id, _event in inputs.cash_events:
        source = inputs.sources[synth_id]
        real_id = source.cash_event_id if isinstance(source, CashEventRef) else synth_id
        out[synth_id] = f"Cash #{real_id}"
    return out


def fx_basis_cells(
    basis: DirectAcquisition | TaxLotSnapshot,
    labels: FxLabels,
) -> tuple[str, str, str]:
    """Return `(basis_text, source_text, acq_date_text)` for the FX basis columns.

    `basis_text` is the `id_label_map` lookup prefixed with `acq` —
    `acq #5685` for ordinary forex / stock / futures-fee events,
    `acq P&L #5421→#8732[0]` for a futures-realisation acquisition.
    """
    if isinstance(basis, DirectAcquisition):
        acq_id = basis.acquisition_trade_id
        acq_date = labels.date_map.get(acq_id)
        date_text = acq_date.isoformat() if acq_date is not None else "—"
        source_text = labels.source_descriptions.get(acq_id, "—")
        label = labels.id_label_map.get(acq_id, f"#{acq_id}")
        return f"acq {label}", source_text, date_text
    pool_text = (
        f"S.104 pool: qty={format_qty_2dp(basis.quantity_before)}, "
        f"avg=£{format_money_2dp(basis.average_cost_gbp)}"
    )
    return pool_text, "—", "—"


def fx_divider(currency: str) -> str:
    """Stable label used in section dividers and error rows."""
    return f"{currency} vs GBP"
