"""Shared builders for the option tests — the real series from the statements.

The four series the taxpayer traded, as IB's instrument tables print
them, plus a call on a stock for the exercise cases the history does
not contain, a flat-rate FX stub, and terse trade builders. Kept in
one place so the mapper, engine, calculator, report and CLI tests all
speak about the same instruments.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from ib_cgt.domain import Money, OptionInstrument, OptionRight, StockInstrument, Trade, TradeAction
from tests.conid import fake_conid

# The 2012 XAUUSD pair (OGFX) — both written.
XAU_CALL = OptionInstrument(
    conid=92738240,
    symbol="XAUUSD 21DEC12 1920.0 C",
    currency="USD",
    underlying="XAUUSD",
    contract_multiplier=Decimal("100"),
    expiry_date=date(2012, 12, 21),
    strike=Decimal("1920"),
    right=OptionRight.CALL,
)
XAU_PUT = OptionInstrument(
    conid=86616752,
    symbol="XAUUSD 21DEC12 1600.0 P",
    currency="USD",
    underlying="XAUUSD",
    contract_multiplier=Decimal("100"),
    expiry_date=date(2012, 12, 21),
    strike=Decimal("1600"),
    right=OptionRight.PUT,
)
# The 2013-14 XSP put — bought; IB renamed the root to XSPAM under one conid.
XSP_PUT = OptionInstrument(
    conid=99465795,
    symbol="XSPAM 20DEC14 140.0 P",
    currency="USD",
    underlying="XSPAM",
    contract_multiplier=Decimal("100"),
    expiry_date=date(2014, 12, 20),
    strike=Decimal("140"),
    right=OptionRight.PUT,
)
# The 2019 TUR puts — bought and exercised into a share sale at the strike.
TUR_PUT = OptionInstrument(
    conid=334765297,
    symbol="TUR 17MAY19 22.0 P",
    currency="USD",
    underlying="TUR",
    contract_multiplier=Decimal("100"),
    expiry_date=date(2019, 5, 17),
    strike=Decimal("22"),
    right=OptionRight.PUT,
)
TUR = StockInstrument(conid=49954174, symbol="TUR", currency="USD")
# A call on a stock, for the assignment / exercise cases the history lacks.
AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
AAPL_CALL = OptionInstrument(
    conid=fake_conid("AAPL", "USD", "2025-12-19", "200", "C"),
    symbol="AAPL 19DEC25 200.0 C",
    currency="USD",
    underlying="AAPL",
    contract_multiplier=Decimal("100"),
    expiry_date=date(2025, 12, 19),
    strike=Decimal("200"),
    right=OptionRight.CALL,
)
AAPL_PUT = OptionInstrument(
    conid=fake_conid("AAPL", "USD", "2025-12-19", "150", "P"),
    symbol="AAPL 19DEC25 150.0 P",
    currency="USD",
    underlying="AAPL",
    contract_multiplier=Decimal("100"),
    expiry_date=date(2025, 12, 19),
    strike=Decimal("150"),
    right=OptionRight.PUT,
)


class FlatFX:
    """An `FXConverter` with one "1 GBP = r native" rate for every date and currency.

    `rate` 1.25 turns 770 USD into 616 GBP — round figures the tests can
    write down. GBP passes through at rate 1, as `FXService` does.
    """

    def __init__(self, rate: Decimal | str = "1.25") -> None:
        """Fix the rate."""
        self.rate = Decimal(rate)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        """Divide by the rate; identity for GBP."""
        assert target == "GBP"
        if amount.currency == "GBP":
            return amount, Decimal(1)
        return Money.gbp(amount.amount / self.rate), self.rate


def at(on: date, hour: int = 12, minute: int = 0) -> datetime:
    """A UTC instant on `on` that is the same date in London."""
    return datetime(on.year, on.month, on.day, hour, minute, tzinfo=UTC)


def option_trade(
    instrument: OptionInstrument,
    action: TradeAction,
    on: date,
    qty: str,
    price: str,
    *,
    fees: str = "0",
    account_id: str = "U1",
    seq: int = 0,
    when: datetime | None = None,
) -> Trade:
    """An option trade at noon UTC on `on` (+`seq` minutes), or at `when`."""
    instant = when if when is not None else at(on) + timedelta(minutes=seq)
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=instant,
        trade_date=Trade.uk_date_of(instant),
        settlement_date=Trade.uk_date_of(instant),
        quantity=Decimal(qty),
        price=Money.of(price, instrument.currency),
        fees=Money.of(fees, instrument.currency),
    )


def share_trade(
    instrument: StockInstrument,
    action: TradeAction,
    on: date,
    qty: str,
    price: str,
    *,
    fees: str = "0",
    account_id: str = "U1",
    seq: int = 0,
    when: datetime | None = None,
) -> Trade:
    """A stock trade with the same instant conventions as `option_trade`."""
    instant = when if when is not None else at(on) + timedelta(minutes=seq)
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=instant,
        trade_date=Trade.uk_date_of(instant),
        settlement_date=Trade.uk_date_of(instant),
        quantity=Decimal(qty),
        price=Money.of(price, instrument.currency),
        fees=Money.of(fees, instrument.currency),
    )


__all__ = [
    "AAPL",
    "AAPL_CALL",
    "AAPL_PUT",
    "TUR",
    "TUR_PUT",
    "XAU_CALL",
    "XAU_PUT",
    "XSP_PUT",
    "FlatFX",
    "at",
    "option_trade",
    "share_trade",
]
