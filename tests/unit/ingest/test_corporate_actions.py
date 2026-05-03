"""Tests for `ib_cgt.ingest.corporate_actions`.

The cash-merger synthesizer pairs IB's two-row Corporate Actions events
(GBP-listed instrument + USD cash row, e.g. the IEMI merger) into a
single SELL Trade. These tests pin down:

1. Same-currency mergers (no FX call needed).
2. Cross-currency mergers (proceeds FX-converted to listing currency).
3. Non-merger Corporate Actions rows (bond maturities, dividends-as-CA,
   share-for-share mergers) are silently skipped.
4. Ordering across multiple events in one statement.
5. Malformed groups raise `MappingError` rather than producing a
   silently-wrong disposal.

The FX service is a tiny in-memory stub — `corporate_actions.py` only
ever calls `convert`, so a record-and-return shim is enough.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import Money, StockInstrument, TradeAction
from ib_cgt.ingest.corporate_actions import map_corporate_actions
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.parser import (
    ParsedStatement,
    RawCorporateActionRow,
    RawInstrumentInfo,
    RawTradeRow,
)

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


@dataclass
class _FXStub:
    """Minimal stand-in for `FXService`.

    Records every `convert` call and returns a deterministic Money.
    The mapper only needs `convert(amount, target=, on=)`, so we don't
    bother with `convert_with_rate` / `sync_currencies`.
    """

    rate: Decimal  # 1 source-ccy = `rate` target-ccy
    calls: list[tuple[Money, str, date]] = field(default_factory=list)

    def convert(self, amount: Money, *, target: str, on: date) -> Money:
        self.calls.append((amount, target, on))
        if amount.currency == target:
            return amount
        return Money.of(amount.amount * self.rate, target)


def _ca_row(
    *,
    asset_class: str = "Stocks",
    currency: str,
    description: str,
    quantity_text: str,
    proceeds_text: str,
    datetime_text: str = "2025-08-15, 20:25:00",
) -> RawCorporateActionRow:
    return RawCorporateActionRow(
        asset_class=asset_class,
        currency=currency,
        report_date_text="2025-08-22",
        datetime_text=datetime_text,
        description=description,
        quantity_text=quantity_text,
        proceeds_text=proceeds_text,
    )


def _make(rows: list[RawCorporateActionRow]) -> ParsedStatement:
    return ParsedStatement(
        account_id="U9999998",
        trades=(),
        instruments=(),
        corporate_actions=tuple(rows),
        dividends=(),
    )


# ---------------------------------------------------------------------------
# Same-currency cash mergers
# ---------------------------------------------------------------------------


def test_synthesizes_sell_trade_from_cash_merger_same_currency() -> None:
    """USD-listed stock merged for USD cash collapses to a single row.

    The mapper must read both quantity and proceeds from that one row
    and never invoke FX (proceeds are already in listing currency).
    """
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=(
                    "ABC(US0000000123) Merged(Acquisition) for USD 12.500000 per Share "
                    "(ABC, ACME CORP, US0000000123)"
                ),
                quantity_text="-100",
                proceeds_text="1,250.00",
            ),
        ]
    )

    fx = _FXStub(rate=Decimal("1.5"))
    [trade] = map_corporate_actions(parsed, fx_service=fx)

    assert isinstance(trade.instrument, StockInstrument)
    assert trade.instrument.symbol == "ABC"
    assert trade.instrument.currency == "USD"
    assert trade.action is TradeAction.SELL
    assert trade.quantity == Decimal("100")
    assert trade.price.currency == "USD"
    assert trade.price.amount == Decimal("12.5")
    assert trade.fees == Money.of(Decimal(0), "USD")
    assert fx.calls == [], "no FX call expected for same-currency merger"


# ---------------------------------------------------------------------------
# Cross-currency cash mergers (the IEMI shape)
# ---------------------------------------------------------------------------


def test_synthesizes_sell_trade_from_cash_merger_cross_currency() -> None:
    """The IEMI shape: GBP listing, USD cash. Proceeds FX-converted at trade-date."""
    description = (
        "IEMI(IE00B2NPL135) Merged(Acquisition) for USD 17.506705 per Share "
        "(IEMI, ISHARES EM INFRASTRUCTURE, IE00B2NPL135)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="GBP",
                description=description,
                quantity_text="-824",
                proceeds_text="0.00",
            ),
            _ca_row(
                currency="USD",
                description=description,
                quantity_text="0",
                proceeds_text="14,425.52",
            ),
        ]
    )

    # Stub: 1 USD = 0.74 GBP. So $14,425.52 → £10,674.88 → /824 ≈ £12.95.
    fx = _FXStub(rate=Decimal("0.74"))
    [trade] = map_corporate_actions(parsed, fx_service=fx)

    assert isinstance(trade.instrument, StockInstrument)
    assert trade.instrument.symbol == "IEMI"
    assert trade.instrument.currency == "GBP"
    assert trade.quantity == Decimal("824")
    assert trade.price.currency == "GBP"
    expected = (Decimal("14425.52") * Decimal("0.74")) / Decimal("824")
    assert trade.price.amount == expected

    # Exactly one FX call, with the proceeds row's currency / amount.
    [(amount, target, on)] = fx.calls
    assert amount == Money.of(Decimal("14425.52"), "USD")
    assert target == "GBP"
    assert on == date(2025, 8, 15)


# ---------------------------------------------------------------------------
# Filtering: non-merger Corporate Actions rows
# ---------------------------------------------------------------------------


def test_skips_bond_maturity_rows() -> None:
    """Bond maturities are out of scope (bonds → BondRuleEngine separately)."""
    parsed = _make(
        [
            _ca_row(
                asset_class="Bonds",
                currency="GBP",
                description=(
                    "(GB00B7Z53659)  Bond Maturity FOR GBP 1.00 PER BOND "
                    "(UKT 2 1/4 09/07/23, UKT 2 1/4 09/07/23, GB00B7Z53659)"
                ),
                quantity_text="-170,000",
                proceeds_text="170,000.00",
                datetime_text="2023-09-06, 20:25:00",
            ),
        ]
    )

    assert map_corporate_actions(parsed, fx_service=_FXStub(rate=Decimal(1))) == []


def test_skips_share_for_share_mergers() -> None:
    """Share-for-share mergers (no `<CCY> <PRICE> per Share`) are not synthesized.

    UK CGT s.135 reorganisations need a fresh acquisition row on the new
    instrument and are explicitly out of scope for v1.
    """
    parsed = _make(
        [
            _ca_row(
                currency="GBP",
                description=(
                    "ABC(US0000000123) Merged(Acquisition) for 0.5 shares of XYZ "
                    "(ABC, ACME, US0000000123)"
                ),
                quantity_text="-100",
                proceeds_text="0",
            ),
        ]
    )

    assert map_corporate_actions(parsed, fx_service=_FXStub(rate=Decimal(1))) == []


def test_skips_corporate_actions_section_when_no_mergers_present() -> None:
    """No matching rows → empty list, no FX calls, no exceptions."""
    parsed = _make([])
    fx = _FXStub(rate=Decimal(1))
    assert map_corporate_actions(parsed, fx_service=fx) == []
    assert fx.calls == []


# ---------------------------------------------------------------------------
# Ordering across multiple events
# ---------------------------------------------------------------------------


def test_two_disposals_in_same_section_ordered_by_datetime() -> None:
    earlier_desc = (
        "AAA(US0000001111) Merged(Acquisition) for USD 1.000000 per Share (AAA, AAA, US0000001111)"
    )
    later_desc = (
        "BBB(US0000002222) Merged(Acquisition) for USD 2.000000 per Share (BBB, BBB, US0000002222)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=later_desc,
                quantity_text="-50",
                proceeds_text="100.00",
                datetime_text="2025-09-01, 12:00:00",
            ),
            _ca_row(
                currency="USD",
                description=earlier_desc,
                quantity_text="-10",
                proceeds_text="10.00",
                datetime_text="2025-08-15, 12:00:00",
            ),
        ]
    )

    trades = map_corporate_actions(parsed, fx_service=_FXStub(rate=Decimal(1)))
    assert [t.instrument.symbol for t in trades] == ["AAA", "BBB"]


# ---------------------------------------------------------------------------
# Malformed groups
# ---------------------------------------------------------------------------


def test_raises_when_group_has_no_disposal_row() -> None:
    """A merger group with zero quantity in every row is malformed."""
    description = (
        "ABC(US0000000123) Merged(Acquisition) for USD 1.000000 per Share (ABC, ABC, US0000000123)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=description,
                quantity_text="0",
                proceeds_text="0",
            ),
        ]
    )

    with pytest.raises(MappingError, match="no row with non-zero quantity"):
        map_corporate_actions(parsed, fx_service=_FXStub(rate=Decimal(1)))


def test_raises_when_group_has_no_proceeds_row() -> None:
    """A merger group with zero proceeds in every row is malformed."""
    description = (
        "ABC(US0000000123) Merged(Acquisition) for USD 1.000000 per Share (ABC, ABC, US0000000123)"
    )
    parsed = _make(
        [
            _ca_row(
                currency="USD",
                description=description,
                quantity_text="-100",
                proceeds_text="0",
            ),
        ]
    )

    with pytest.raises(MappingError, match="no row with non-zero proceeds"):
        map_corporate_actions(parsed, fx_service=_FXStub(rate=Decimal(1)))


# ---------------------------------------------------------------------------
# Containment — corporate actions don't pollute regular trade mapping
# ---------------------------------------------------------------------------


def test_synthesizer_ignores_regular_trades_and_instruments() -> None:
    """`map_corporate_actions` only consults `parsed.corporate_actions`.

    Belt-and-braces: a stray `RawTradeRow` in `parsed.trades` must not
    influence the synthesizer, and vice versa.
    """
    parsed = ParsedStatement(
        account_id="U9999998",
        trades=(
            RawTradeRow(
                asset_class="Stocks",
                currency="USD",
                symbol="DECOY",
                datetime_text="2025-08-15, 09:30:00",
                quantity_text="100",
                price_text="50.0",
                fees_text="-1.0",
                code="O",
            ),
        ),
        instruments=(
            RawInstrumentInfo(
                asset_class="Stocks",
                symbol="DECOY",
                description="DECOY CORP",
                multiplier_text=None,
                expiry_text=None,
                listing_exch="NASDAQ",
            ),
        ),
        corporate_actions=(
            _ca_row(
                currency="USD",
                description=(
                    "ABC(US0000000123) Merged(Acquisition) for USD 12.500000 per Share "
                    "(ABC, ACME CORP, US0000000123)"
                ),
                quantity_text="-100",
                proceeds_text="1,250.00",
            ),
        ),
        dividends=(),
    )

    [trade] = map_corporate_actions(parsed, fx_service=_FXStub(rate=Decimal(1)))
    assert trade.instrument.symbol == "ABC"
