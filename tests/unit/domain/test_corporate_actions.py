"""Tests for the `CorporateAction` value object: legs, kinds and their invariants.

Covers:
1. a `cash_disposal` needs an instrument, a negative quantity and
   positive cash — the shape the engines model;
2. an `unsupported` row may lack an instrument, carry any quantity
   sign and any or no cash;
3. the date invariant (effective date is the London date of the
   instant) and the FX-pair rejection;
4. the derived properties the engines and check A16 read.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ib_cgt.domain import (
    CorporateAction,
    CorporateActionKind,
    CurrencyPair,
    FXInstrument,
    InvalidCorporateActionError,
    Money,
    StockInstrument,
)

IEMI = StockInstrument(conid=59262240, symbol="IEMI", currency="GBP")
# 2025-08-15 20:25 Eastern = 2025-08-16 00:25 UTC = 01:25 London on the 16th.
INSTANT = datetime(2025, 8, 16, 0, 25, tzinfo=UTC)
# The IEMI consideration: the cash leg every disposal-shaped test starts from.
USD_CASH = Money.of("14425.52", "USD")


def _action(
    *,
    kind: CorporateActionKind = CorporateActionKind.CASH_DISPOSAL,
    instrument: StockInstrument | FXInstrument | None = IEMI,
    quantity: str = "-824",
    cash: Money | None = USD_CASH,
    effective_date: date = date(2025, 8, 16),
    account_id: str = "U10049818",
    description: str = "IEMI(IE00B2NPL135) Merged(Acquisition) for USD 17.506705 per Share",
) -> CorporateAction:
    return CorporateAction(
        account_id=account_id,
        kind=kind,
        instrument=instrument,
        effective_datetime=INSTANT,
        effective_date=effective_date,
        report_date=date(2025, 8, 22),
        quantity=Decimal(quantity),
        cash=cash,
        description=description,
    )


def test_kind_values_match_the_schema_check() -> None:
    assert {k.value for k in CorporateActionKind} == {"cash_disposal", "unsupported"}


def test_cash_disposal_exposes_its_legs() -> None:
    action = _action()
    assert action.is_cash_disposal is True
    assert action.disposed_quantity == Decimal("824")
    assert action.has_effect is True
    assert action.cash == Money.of("14425.52", "USD")
    assert action.effective_date == date(2025, 8, 16)


def test_cash_disposal_needs_an_instrument() -> None:
    with pytest.raises(InvalidCorporateActionError, match="instrument"):
        _action(instrument=None)


def test_cash_disposal_needs_a_negative_quantity() -> None:
    with pytest.raises(InvalidCorporateActionError, match="negative quantity"):
        _action(quantity="824")
    with pytest.raises(InvalidCorporateActionError, match="negative quantity"):
        _action(quantity="0")


def test_cash_disposal_needs_positive_cash() -> None:
    with pytest.raises(InvalidCorporateActionError, match="positive cash"):
        _action(cash=None)
    with pytest.raises(InvalidCorporateActionError, match="positive cash"):
        _action(cash=Money.of("-1", "USD"))


def test_unsupported_row_may_have_any_shape() -> None:
    split = _action(kind=CorporateActionKind.UNSUPPORTED, quantity="100", cash=None)
    assert split.is_cash_disposal is False
    assert split.has_effect is True
    return_of_capital = _action(
        kind=CorporateActionKind.UNSUPPORTED, quantity="0", cash=Money.of("12.34", "USD")
    )
    assert return_of_capital.has_effect is True
    unresolved = _action(kind=CorporateActionKind.UNSUPPORTED, instrument=None, quantity="-5")
    assert unresolved.instrument is None
    informational = _action(kind=CorporateActionKind.UNSUPPORTED, quantity="0", cash=None)
    assert informational.has_effect is False


def test_zero_cash_is_no_cash_leg() -> None:
    with pytest.raises(InvalidCorporateActionError, match="non-zero or None"):
        _action(kind=CorporateActionKind.UNSUPPORTED, quantity="-1", cash=Money.of("0", "USD"))


def test_effective_date_must_be_the_london_date_of_the_instant() -> None:
    with pytest.raises(InvalidCorporateActionError, match="Europe/London"):
        _action(effective_date=date(2025, 8, 15))


def test_naive_instant_is_rejected() -> None:
    with pytest.raises(InvalidCorporateActionError, match="timezone-aware"):
        CorporateAction(
            account_id="U1",
            kind=CorporateActionKind.UNSUPPORTED,
            instrument=None,
            effective_datetime=datetime(2025, 8, 16, 0, 25),
            effective_date=date(2025, 8, 16),
            report_date=date(2025, 8, 22),
            quantity=Decimal(0),
            cash=None,
            description="x",
        )


def test_fx_pair_is_rejected() -> None:
    pair = FXInstrument(
        symbol="USD.GBP", currency="USD", currency_pair=CurrencyPair(base="USD", quote="GBP")
    )
    with pytest.raises(InvalidCorporateActionError, match="FX pair"):
        _action(kind=CorporateActionKind.UNSUPPORTED, instrument=pair, quantity="1", cash=None)


def test_empty_account_or_description_is_rejected() -> None:
    with pytest.raises(InvalidCorporateActionError, match="account_id"):
        _action(account_id=" ")
    with pytest.raises(InvalidCorporateActionError, match="description"):
        _action(description="")
