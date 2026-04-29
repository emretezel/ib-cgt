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
    fees_gbp: Decimal | int | str = 0,
    instrument: StockInstrument | None = None,
    account_id: str = "U1",
) -> Acquisition:
    """Build an `Acquisition` with terse keyword args.

    `fees_gbp` is **subset of** `cost_gbp` — the helper does not add
    it on top. Defaults to zero, matching the engine's input shape
    for fee-free test cases.
    """
    return Acquisition(
        trade_id=trade_id,
        account_id=account_id,
        instrument=instrument or aapl(),
        acquisition_date=on,
        quantity=Decimal(qty),
        cost_gbp=Money.gbp(cost_gbp),
        fees_gbp=Money.gbp(fees_gbp),
    )


def disp(
    *,
    trade_id: int,
    on: date,
    qty: Decimal | int | str,
    proceeds_gbp: Decimal | int | str,
    fees_gbp: Decimal | int | str = 0,
    instrument: StockInstrument | None = None,
    account_id: str = "U1",
) -> Disposal:
    """Build a `Disposal` with terse keyword args.

    `proceeds_gbp` is the **net** figure (already net of fees).
    `fees_gbp` is what was deducted; defaults to zero.
    """
    return Disposal(
        trade_id=trade_id,
        account_id=account_id,
        instrument=instrument or aapl(),
        disposal_date=on,
        quantity=Decimal(qty),
        proceeds_gbp=Money.gbp(proceeds_gbp),
        fees_gbp=Money.gbp(fees_gbp),
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


def stock_trade(
    *,
    action: TradeAction,
    on: date,
    qty: Decimal | int | str,
    price: Decimal | int | str,
    fees: Decimal | int | str = Decimal("0"),
    instrument: StockInstrument | None = None,
    account_id: str = "U1",
    seq: int = 0,
) -> Trade:
    """Build a stock `Trade`. Mirrors `future_trade` for stocks.

    Default instrument is `aapl()` (USD-denominated AAPL); pass an
    explicit `instrument=` for GBP / EUR / etc. cases. `seq` is the
    intra-day minute offset on top of 12:00 UTC, used when a test
    needs deterministic ordering of multiple same-day trades.
    """
    inst = instrument or aapl()
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

    Implements the `FXConverter` Protocol consumed by
    `FutureRuleEngine` — i.e. `convert_with_rate(amount, *, target,
    on) -> (Money, Decimal)`. Asserts on unknown dates rather than
    going to the network, mirroring the `_NetworkBannedClient`
    pattern in `tests/unit/fx/conftest.py`.

    Convention split worth knowing: the stub's **stored** rate map is
    keyed by date and holds "1 native = r GBP" values (e.g.
    `Decimal("0.80")` means "1 USD = 0.80 GBP"). This matches how
    test cases verbalise FX. The **rate exposed** to the engine via
    `convert_with_rate` is the *inverse* — "1 GBP = r native" —
    because that is the production convention (matches what
    `FXService` returns from its real Frankfurter-backed cache) and
    therefore what `FutureRealisation.proceeds_fx_rate` /
    `cost_fx_rate` must hold for the renderer to display the right
    figure. Tests asserting on a rate field should pick stored
    values whose inverse is exact (e.g. `0.80` → `1.25`,
    `0.50` → `2`, `1` → `1`).
    """

    def __init__(self, rates_native_to_gbp: Mapping[date, Decimal]) -> None:
        """Initialise with a date→rate map (native→GBP)."""
        self._rates = dict(rates_native_to_gbp)

    def convert_with_rate(self, amount: Money, *, target: str, on: date) -> tuple[Money, Decimal]:
        """Convert `amount` to GBP and return the production-convention rate.

        Tests in this module only need native→GBP, so the stub only
        supports that direction. Any other path is asserted to fail
        loudly so a misuse in a test produces a useful error.
        """
        assert target == "GBP", f"StubFXService only supports native→GBP, asked for {target}"
        if amount.currency == "GBP":
            # Identity path — engine should never hit this for a
            # non-GBP-denominated future, but it's the documented
            # contract of `convert_with_rate` and we honour it so
            # GBP-future test cases work without a special branch.
            return amount, Decimal(1)
        if on not in self._rates:
            raise AssertionError(f"StubFXService has no rate configured for {on}")
        stored = self._rates[on]
        # Native amount * (GBP per native) = GBP amount.
        gbp = Money.gbp(amount.amount * stored)
        # Production convention is "1 GBP = r native" — invert the
        # stored "1 native = r GBP" so the engine sees what
        # `FXService.convert_with_rate` would return.
        return gbp, Decimal(1) / stored
