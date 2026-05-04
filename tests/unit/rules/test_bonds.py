"""Tests for `ib_cgt.rules.bonds.BondRuleEngine`.

Covers:
- exempt-bond short-circuit (gilts / QCBs) — no FX, no matching,
  `ExemptBondResult` with correct aggregates,
- non-exempt projection (BUY → Acquisition, SELL → Disposal) with
  accrued interest folded into cost / proceeds,
- four-rule matching delegation via the shared `MatchingEngine`,
- defensive validation (wrong asset class, mixed-instrument trades,
  invalid action),
- the empty-trades degenerate case.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    BondInstrument,
    MatchRule,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.rules.bonds import BondRuleEngine, ExemptBondResult
from ib_cgt.rules.errors import InconsistentTradeError, WrongAssetClassError
from ib_cgt.rules.matching import MatchingResult

from .conftest import StubFXService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gilt() -> BondInstrument:
    """A typical UK gilt — exempt by ingest classifier."""
    return BondInstrument(
        symbol="UKT 0 1/8 01/30/26",
        currency="GBP",
        is_cgt_exempt=True,
    )


def _corp_bond_gbp() -> BondInstrument:
    """Hypothetical non-exempt GBP corporate bond."""
    return BondInstrument(
        symbol="ACME 5 2030",
        currency="GBP",
        is_cgt_exempt=False,
    )


def _corp_bond_usd() -> BondInstrument:
    """Hypothetical non-exempt USD corporate bond — exercises FX path."""
    return BondInstrument(
        symbol="ACME 5 2030 USD",
        currency="USD",
        is_cgt_exempt=False,
    )


def _bond_trade(
    *,
    action: TradeAction,
    on: date,
    qty: Decimal | int | str,
    price: Decimal | int | str,
    fees: Decimal | int | str = Decimal("0"),
    accrued: Decimal | int | str | None = None,
    instrument: BondInstrument,
    account_id: str = "U1",
) -> Trade:
    """Build a bond `Trade` with optional accrued interest."""
    dt = datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC)
    accrued_money = Money.of(Decimal(accrued), instrument.currency) if accrued is not None else None
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=dt,
        trade_date=on,
        settlement_date=on,
        quantity=Decimal(qty),
        price=Money.of(price, instrument.currency),
        fees=Money.of(fees, instrument.currency),
        accrued_interest=accrued_money,
    )


# ---------------------------------------------------------------------------
# Exempt branch — UK gilts skip matching
# ---------------------------------------------------------------------------


def test_exempt_bond_returns_summary_no_matching() -> None:
    """A gilt with buys + sells produces an `ExemptBondResult`, never a match."""
    fx = StubFXService({})  # exempt branch never touches FX
    engine = BondRuleEngine(fx)
    gilt = _gilt()

    trades = [
        (
            1,
            _bond_trade(
                action=TradeAction.BUY,
                on=date(2024, 5, 1),
                qty=100,
                price=98,
                fees=2,
                instrument=gilt,
            ),
        ),
        (
            2,
            _bond_trade(
                action=TradeAction.SELL,
                on=date(2025, 1, 1),
                qty=100,
                price=99,
                fees=1,
                instrument=gilt,
            ),
        ),
    ]
    result = engine.compute(gilt, trades)

    assert isinstance(result, ExemptBondResult)
    assert result.instrument == gilt
    assert result.exempt_buy_count == 1
    assert result.exempt_sell_count == 1
    # 100 * 98 + 0 (no accrued) + 2 fees = 9802 cash outlay
    assert result.total_buy_native == Money.of(Decimal("9802"), "GBP")
    # 100 * 99 + 0 - 1 = 9899 cash receipt
    assert result.total_sell_native == Money.of(Decimal("9899"), "GBP")


def test_exempt_bond_with_accrued_includes_in_totals() -> None:
    """Accrued interest folds into both buy outlay and sell receipt."""
    engine = BondRuleEngine(StubFXService({}))
    gilt = _gilt()

    trades = [
        (
            1,
            _bond_trade(
                action=TradeAction.BUY,
                on=date(2024, 5, 1),
                qty=100,
                price=98,
                fees=2,
                accrued=12,
                instrument=gilt,
            ),
        ),
        (
            2,
            _bond_trade(
                action=TradeAction.SELL,
                on=date(2025, 1, 1),
                qty=100,
                price=99,
                fees=1,
                accrued=8,
                instrument=gilt,
            ),
        ),
    ]
    result = engine.compute(gilt, trades)
    assert isinstance(result, ExemptBondResult)
    # Buy: 100*98 + 12 + 2 = 9814; Sell: 100*99 + 8 - 1 = 9907.
    assert result.total_buy_native == Money.of(Decimal("9814"), "GBP")
    assert result.total_sell_native == Money.of(Decimal("9907"), "GBP")


def test_exempt_bond_empty_trades() -> None:
    """No trades on an exempt bond is a legal degenerate input."""
    engine = BondRuleEngine(StubFXService({}))
    gilt = _gilt()
    result = engine.compute(gilt, [])
    assert isinstance(result, ExemptBondResult)
    assert result.exempt_buy_count == 0
    assert result.exempt_sell_count == 0
    assert result.total_buy_native == Money.of(Decimal("0"), "GBP")
    assert result.total_sell_native == Money.of(Decimal("0"), "GBP")


# ---------------------------------------------------------------------------
# Non-exempt branch — four-rule matching
# ---------------------------------------------------------------------------


def test_non_exempt_gbp_same_day_match() -> None:
    """A GBP corporate bond same-day round trip matches under SAME_DAY."""
    fx = StubFXService({})  # GBP identity path needs no rates
    engine = BondRuleEngine(fx)
    bond = _corp_bond_gbp()
    d = date(2024, 5, 1)

    trades = [
        (1, _bond_trade(action=TradeAction.BUY, on=d, qty=10, price=100, fees=2, instrument=bond)),
        (2, _bond_trade(action=TradeAction.SELL, on=d, qty=10, price=101, fees=1, instrument=bond)),
    ]
    result = engine.compute(bond, trades)
    assert isinstance(result, MatchingResult)
    assert len(result.matched_disposals) == 1
    md = result.matched_disposals[0]
    assert md.match_rule is MatchRule.SAME_DAY
    # Cost basis: 10*100 + 0 accrued + 2 fees = 1002 GBP
    # Proceeds: 10*101 + 0 - 1 = 1009 GBP
    # Gain: 7 GBP
    assert md.matched_cost_gbp == Money.gbp(Decimal("1002"))
    assert md.matched_proceeds_gbp == Money.gbp(Decimal("1009"))


def test_non_exempt_accrued_interest_folds_into_cost_and_proceeds() -> None:
    """Accrued interest adjusts both legs."""
    engine = BondRuleEngine(StubFXService({}))
    bond = _corp_bond_gbp()
    d = date(2024, 5, 1)

    trades = [
        # Buyer paid 10*100 + 5 accrued + 2 fees = 1007
        (
            1,
            _bond_trade(
                action=TradeAction.BUY, on=d, qty=10, price=100, fees=2, accrued=5, instrument=bond
            ),
        ),
        # Seller received 10*101 + 3 accrued - 1 fees = 1012
        (
            2,
            _bond_trade(
                action=TradeAction.SELL, on=d, qty=10, price=101, fees=1, accrued=3, instrument=bond
            ),
        ),
    ]
    result = engine.compute(bond, trades)
    assert isinstance(result, MatchingResult)
    md = result.matched_disposals[0]
    assert md.matched_cost_gbp == Money.gbp(Decimal("1007"))
    assert md.matched_proceeds_gbp == Money.gbp(Decimal("1012"))


def test_non_exempt_usd_bond_uses_fx_at_trade_date() -> None:
    """A USD corporate bond gets converted to GBP at trade-date spot."""
    # 1 USD = 0.80 GBP on the trade date.
    d = date(2024, 5, 1)
    fx = StubFXService({d: Decimal("0.80")})
    engine = BondRuleEngine(fx)
    bond = _corp_bond_usd()

    trades = [
        (1, _bond_trade(action=TradeAction.BUY, on=d, qty=10, price=100, fees=2, instrument=bond)),
        (2, _bond_trade(action=TradeAction.SELL, on=d, qty=10, price=101, fees=1, instrument=bond)),
    ]
    result = engine.compute(bond, trades)
    assert isinstance(result, MatchingResult)
    md = result.matched_disposals[0]
    # Cost native: 10*100 + 2 = 1002 USD → 801.60 GBP
    # Proceeds native: 10*101 - 1 = 1009 USD → 807.20 GBP
    assert md.matched_cost_gbp == Money.gbp(Decimal("801.60"))
    assert md.matched_proceeds_gbp == Money.gbp(Decimal("807.20"))


# ---------------------------------------------------------------------------
# Defensive validation
# ---------------------------------------------------------------------------


def test_wrong_asset_class_raises() -> None:
    """Passing a non-bond instrument raises immediately."""
    engine = BondRuleEngine(StubFXService({}))
    stock = StockInstrument(symbol="AAPL", currency="USD")
    with pytest.raises(WrongAssetClassError):
        engine.compute(stock, [])


def test_mixed_instrument_raises_value_error() -> None:
    """A trade for a different bond than the engine was asked about fails."""
    engine = BondRuleEngine(StubFXService({}))
    bond_a = _corp_bond_gbp()
    bond_b = BondInstrument(symbol="OTHER", currency="GBP", is_cgt_exempt=False)
    trade = _bond_trade(
        action=TradeAction.BUY, on=date(2024, 5, 1), qty=1, price=100, instrument=bond_b
    )
    with pytest.raises(ValueError, match="different bond"):
        engine.compute(bond_a, [(1, trade)])


def test_invalid_action_on_non_exempt_bond_raises() -> None:
    """Only BUY/SELL are valid actions for a bond.

    `Trade.__post_init__` rejects OPEN_*/CLOSE_* on a bond at
    construction time, so we must bypass the dataclass init via
    `object.__new__` to simulate the in-memory mock / corrupted-DB
    scenario the engine guards against.
    """
    from zoneinfo import ZoneInfo

    engine = BondRuleEngine(StubFXService({}))
    bond = _corp_bond_gbp()
    bad_trade = object.__new__(Trade)
    object.__setattr__(bad_trade, "account_id", "U1")
    object.__setattr__(bad_trade, "instrument", bond)
    object.__setattr__(bad_trade, "action", TradeAction.OPEN_LONG)
    object.__setattr__(
        bad_trade,
        "trade_datetime",
        datetime(2024, 5, 1, 12, 0, tzinfo=ZoneInfo("Europe/London")),
    )
    object.__setattr__(bad_trade, "trade_date", date(2024, 5, 1))
    object.__setattr__(bad_trade, "settlement_date", date(2024, 5, 1))
    object.__setattr__(bad_trade, "quantity", Decimal("10"))
    object.__setattr__(bad_trade, "price", Money.of(100, "GBP"))
    object.__setattr__(bad_trade, "fees", Money.of(0, "GBP"))
    object.__setattr__(bad_trade, "accrued_interest", None)
    with pytest.raises(InconsistentTradeError) as exc_info:
        engine.compute(bond, [(99, bad_trade)])
    assert exc_info.value.trade_id == 99
    assert "open_long" in exc_info.value.detail


def test_mixed_instrument_on_exempt_bond_also_raises() -> None:
    """Exempt branch validates instrument identity too — mixing
    instruments would silently pollute the audit aggregate."""
    engine = BondRuleEngine(StubFXService({}))
    gilt_a = _gilt()
    gilt_b = BondInstrument(symbol="UKT 1 2030", currency="GBP", is_cgt_exempt=True)
    trade = _bond_trade(
        action=TradeAction.BUY, on=date(2024, 5, 1), qty=1, price=100, instrument=gilt_b
    )
    with pytest.raises(ValueError, match="different bond"):
        engine.compute(gilt_a, [(1, trade)])
