"""Shared fixtures and builders for rules-engine tests.

The helpers are deliberately local rather than imported from a central
factory: keeping the test setup visible inside each test module makes
the intent of each case obvious without jumping to a builder.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from ib_cgt.domain import (
    Acquisition,
    Disposal,
    FutureInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)

# ---------------------------------------------------------------------------
# Default instruments
# ---------------------------------------------------------------------------


def aapl() -> StockInstrument:
    """Plain stock for matching-engine tests."""
    return StockInstrument(symbol="AAPL", currency="USD")


def es_future(
    *,
    expiry: date = date(2025, 3, 21),
    multiplier: Decimal = Decimal("50"),
    currency: str = "USD",
) -> FutureInstrument:
    """E-mini S&P futures contract — the workhorse for futures tests."""
    return FutureInstrument(
        symbol="ES",
        currency=currency,
        contract_multiplier=multiplier,
        expiry_date=expiry,
    )


# ---------------------------------------------------------------------------
# Acquisition / Disposal builders (matching-engine inputs)
# ---------------------------------------------------------------------------


def acq(
    *,
    trade_id: int,
    on: date,
    qty: Decimal | int | str,
    cost_gbp: Decimal | int | str,
    instrument: StockInstrument | None = None,
    account_id: str = "U1",
) -> Acquisition:
    """Build an `Acquisition` with terse keyword args."""
    return Acquisition(
        trade_id=trade_id,
        account_id=account_id,
        instrument=instrument or aapl(),
        acquisition_date=on,
        quantity=Decimal(qty),
        cost_gbp=Money.gbp(cost_gbp),
    )


def disp(
    *,
    trade_id: int,
    on: date,
    qty: Decimal | int | str,
    proceeds_gbp: Decimal | int | str,
    instrument: StockInstrument | None = None,
    account_id: str = "U1",
) -> Disposal:
    """Build a `Disposal` with terse keyword args."""
    return Disposal(
        trade_id=trade_id,
        account_id=account_id,
        instrument=instrument or aapl(),
        disposal_date=on,
        quantity=Decimal(qty),
        proceeds_gbp=Money.gbp(proceeds_gbp),
    )


# ---------------------------------------------------------------------------
# Trade builders (futures-engine inputs)
# ---------------------------------------------------------------------------


def future_trade(
    *,
    action: TradeAction,
    on: date,
    qty: Decimal | int | str,
    price: Decimal | int | str,
    fees: Decimal | int | str = Decimal("0"),
    instrument: FutureInstrument | None = None,
    account_id: str = "U1",
    seq: int = 0,  # offset minutes within the day for deterministic ordering
) -> Trade:
    """Build a futures `Trade`. `seq` lets a test put two trades in order."""
    inst = instrument or es_future()
    # Construct a tz-aware datetime that projects to the requested date
    # in UK-local time. Trade construction validates this — we use
    # 12:00 UTC + `seq` minutes so the date stays the same in London
    # regardless of BST offset and intra-day FIFO order is determined
    # by `seq`.
    dt = datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC) + timedelta(minutes=seq)
    return Trade(
        account_id=account_id,
        instrument=inst,
        action=action,
        trade_datetime=dt,
        trade_date=on,
        settlement_date=on,
        quantity=Decimal(qty),
        price=Money.of(price, inst.currency),
        fees=Money.of(fees, inst.currency),
    )


# ---------------------------------------------------------------------------
# Stub FX service for futures tests
# ---------------------------------------------------------------------------


class StubFXService:
    """Deterministic in-memory FX stand-in for unit tests.

    Mirrors the interface of `ib_cgt.fx.service.FXService.convert` —
    enough that `FutureRuleEngine` cannot tell it apart. Asserts on
    unknown dates rather than going to the network, mirroring the
    `_NetworkBannedClient` pattern in `tests/unit/fx/conftest.py`.

    Rates are stored as `1 native = rate * GBP`, consistent with the
    way the test cases verbalise FX (e.g. `fx_close=Decimal("0.82")`
    meaning "1 USD = 0.82 GBP").
    """

    def __init__(self, rates_native_to_gbp: Mapping[date, Decimal]) -> None:
        """Initialise with a date→rate map (native→GBP)."""
        self._rates = dict(rates_native_to_gbp)

    def convert(self, amount: Money, target: str, on: date) -> Money:
        """Convert `amount` to `target` currency at the rate for `on`.

        Tests in this module only need native→GBP, so the stub only
        supports that direction. Any other path is asserted to fail
        loudly so a misuse in a test produces a useful error.
        """
        assert target == "GBP", f"StubFXService only supports native→GBP, asked for {target}"
        if on not in self._rates:
            raise AssertionError(f"StubFXService has no rate configured for {on}")
        rate = self._rates[on]
        # Native amount * (GBP per native) = GBP amount.
        return Money.gbp(amount.amount * rate)
