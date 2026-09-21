"""Tests for the Instrument hierarchy in `ib_cgt.domain.trading`."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain.enums import AssetClass
from ib_cgt.domain.money import CurrencyPair
from ib_cgt.domain.trading import (
    BondInstrument,
    FutureInstrument,
    FXInstrument,
    InvalidInstrumentError,
    StockInstrument,
)

# ---------------------------------------------------------------------------
# StockInstrument
# ---------------------------------------------------------------------------


def test_stock_instrument_carries_correct_asset_class() -> None:
    s = StockInstrument(
        conid=66468935,
        symbol="AAPL",
        currency="USD",
    )
    assert s.asset_class is AssetClass.STOCK
    # ClassVar is shared across every instance.
    assert StockInstrument.asset_class is AssetClass.STOCK


def test_stock_instrument_rejects_empty_symbol() -> None:
    with pytest.raises(InvalidInstrumentError):
        StockInstrument(conid=230178124, symbol="   ", currency="USD")


def test_stock_instrument_rejects_bad_currency() -> None:
    with pytest.raises(ValueError):
        StockInstrument(conid=43644709, symbol="AAPL", currency="usd")


# ---------------------------------------------------------------------------
# BondInstrument
# ---------------------------------------------------------------------------


def test_bond_instrument_defaults_non_exempt() -> None:
    b = BondInstrument(isin="XS5160487137", symbol="GILT25", currency="GBP")
    assert b.asset_class is AssetClass.BOND
    assert b.is_cgt_exempt is False


def test_bond_instrument_marked_exempt() -> None:
    b = BondInstrument(isin="XS5160487137", symbol="GILT25", currency="GBP", is_cgt_exempt=True)
    assert b.is_cgt_exempt is True


# ---------------------------------------------------------------------------
# FutureInstrument
# ---------------------------------------------------------------------------


def test_future_instrument_ok() -> None:
    f = FutureInstrument(
        conid=209080823,
        symbol="ESM5",
        currency="USD",
        contract_multiplier=Decimal("50"),
        expiry_date=date(2025, 6, 20),
    )
    assert f.asset_class is AssetClass.FUTURE
    assert f.contract_multiplier == Decimal("50")


def test_future_instrument_rejects_non_positive_multiplier() -> None:
    with pytest.raises(InvalidInstrumentError):
        FutureInstrument(
            conid=209080823,
            symbol="ESM5",
            currency="USD",
            contract_multiplier=Decimal("0"),
            expiry_date=date(2025, 6, 20),
        )


def test_future_instrument_requires_multiplier_and_expiry() -> None:
    # kw_only=True ensures these two must be passed explicitly.
    with pytest.raises(TypeError):
        FutureInstrument(symbol="ESM5", currency="USD")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# FXInstrument
# ---------------------------------------------------------------------------


def test_fx_instrument_ok() -> None:
    fx = FXInstrument(
        symbol="EUR.GBP",
        currency="EUR",
        currency_pair=CurrencyPair(base="EUR", quote="GBP"),
    )
    assert fx.asset_class is AssetClass.FX
    assert fx.currency_pair.base == "EUR"


def test_fx_instrument_requires_base_matches_currency() -> None:
    # Instrument currency is the *base* of the pair — USD base but currency=EUR
    # is a contradiction and the constructor should reject it.
    with pytest.raises(InvalidInstrumentError):
        FXInstrument(
            symbol="USD.GBP",
            currency="EUR",
            currency_pair=CurrencyPair(base="USD", quote="GBP"),
        )


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_instrument_is_frozen() -> None:
    s = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
    with pytest.raises(FrozenInstanceError):
        s.symbol = "MSFT"  # type: ignore[misc]


def test_each_subclass_has_distinct_asset_class() -> None:
    # Guards against accidental copy-paste of the ClassVar value across
    # subclasses — a class of bug static analysis won't catch.
    assert StockInstrument.asset_class is AssetClass.STOCK
    assert BondInstrument.asset_class is AssetClass.BOND
    assert FutureInstrument.asset_class is AssetClass.FUTURE
    assert FXInstrument.asset_class is AssetClass.FX


# ---------------------------------------------------------------------------
# Identity keys — conid on stocks / futures, ISIN on bonds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -7, True])
def test_stock_conid_must_be_a_positive_int(bad: int) -> None:
    """Zero, negatives and `bool` (an `int` subclass) are all rejected."""
    with pytest.raises(InvalidInstrumentError, match="conid"):
        StockInstrument(conid=bad, symbol="AAPL", currency="USD")


def test_future_conid_must_be_a_positive_int() -> None:
    with pytest.raises(InvalidInstrumentError, match="conid"):
        FutureInstrument(
            conid=0,
            symbol="ESM5",
            currency="USD",
            contract_multiplier=Decimal("50"),
            expiry_date=date(2025, 6, 20),
        )


def test_bond_isin_must_be_non_blank() -> None:
    with pytest.raises(InvalidInstrumentError, match="isin"):
        BondInstrument(isin="   ", symbol="GILT25", currency="GBP")


def test_same_conid_different_symbol_compares_unequal_as_a_value_object() -> None:
    """Dataclass equality is field-wise; identity is the DB's job, not the object's.

    This pins down the contract the persistence layer and the engines
    rely on: two renderings of one listing are *different values* here,
    and only `InstrumentRepo.upsert` (by conid) and the surrogate
    `instrument_id` say they are the same instrument.
    """
    a = StockInstrument(conid=102048570, symbol="JNKEz", currency="CHF")
    b = StockInstrument(conid=102048570, symbol="JNKE", currency="CHF")
    assert a != b
    assert a.conid == b.conid
