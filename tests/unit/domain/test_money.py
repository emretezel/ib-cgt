"""Tests for `ib_cgt.domain.money`."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from ib_cgt.domain.money import (
    CurrencyMismatchError,
    CurrencyPair,
    Money,
    validate_currency_code,
)

# ---------------------------------------------------------------------------
# validate_currency_code
# ---------------------------------------------------------------------------


def test_validate_currency_code_accepts_valid_codes() -> None:
    assert validate_currency_code("GBP") == "GBP"
    assert validate_currency_code("USD") == "USD"


@pytest.mark.parametrize("bad", ["gbp", "GB", "GBPX", "", "123", "G1P"])
def test_validate_currency_code_rejects_bad_shape(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_currency_code(bad)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_money_direct_construction_requires_decimal() -> None:
    assert Money(Decimal("10"), "GBP").amount == Decimal("10")
    with pytest.raises(TypeError):
        Money(10, "GBP")  # type: ignore[arg-type]


def test_money_direct_construction_validates_currency() -> None:
    with pytest.raises(ValueError):
        Money(Decimal("10"), "gbp")


def test_money_of_coerces_int_and_str() -> None:
    assert Money.of(10, "USD").amount == Decimal(10)
    assert Money.of("10.25", "USD").amount == Decimal("10.25")


def test_money_of_rejects_float() -> None:
    with pytest.raises(TypeError):
        Money.of(10.5, "USD")  # type: ignore[arg-type]


def test_money_of_rejects_bool() -> None:
    # `bool` is a subclass of `int` so we have to special-case this.
    with pytest.raises(TypeError):
        Money.of(True, "USD")


def test_money_gbp_shortcut() -> None:
    m = Money.gbp("100.50")
    assert m.currency == "GBP"
    assert m.amount == Decimal("100.50")


def test_money_zero() -> None:
    z = Money.zero("USD")
    assert z.amount == Decimal(0)
    assert z.currency == "USD"


# ---------------------------------------------------------------------------
# Arithmetic
# ---------------------------------------------------------------------------


def test_money_add_same_currency() -> None:
    assert Money.gbp(3) + Money.gbp("2.50") == Money.gbp("5.50")


def test_money_sub_same_currency() -> None:
    assert Money.gbp(10) - Money.gbp(3) == Money.gbp(7)


def test_money_neg() -> None:
    assert -Money.gbp(5) == Money.gbp(-5)


def test_money_mul_scalar() -> None:
    # Decimal scalar
    assert Money.gbp(4) * Decimal("2.5") == Money.gbp("10.0")
    # int scalar
    assert Money.gbp(4) * 3 == Money.gbp(12)
    # right multiplication
    assert 3 * Money.gbp(4) == Money.gbp(12)


def test_money_mul_rejects_float_and_bool() -> None:
    with pytest.raises(TypeError):
        Money.gbp(4) * 2.5  # type: ignore[operator]
    with pytest.raises(TypeError):
        Money.gbp(4) * True


def test_money_mixed_currency_raises() -> None:
    # Addition and subtraction must reject mismatched currencies.
    with pytest.raises(CurrencyMismatchError):
        Money.gbp(1) + Money.of(1, "USD")
    with pytest.raises(CurrencyMismatchError):
        Money.gbp(1) - Money.of(1, "USD")


# ---------------------------------------------------------------------------
# Predicates and presentation
# ---------------------------------------------------------------------------


def test_is_gbp() -> None:
    assert Money.gbp(1).is_gbp()
    assert not Money.of(1, "USD").is_gbp()


def test_repr_includes_currency_and_amount() -> None:
    assert repr(Money.gbp("1234.56")) == "GBP 1,234.56"


def test_money_is_immutable() -> None:
    m = Money.gbp(1)
    with pytest.raises(FrozenInstanceError):
        m.amount = Decimal(2)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# CurrencyPair
# ---------------------------------------------------------------------------


def test_currency_pair_repr() -> None:
    assert repr(CurrencyPair(base="EUR", quote="GBP")) == "EURGBP"


def test_currency_pair_validates_codes() -> None:
    with pytest.raises(ValueError):
        CurrencyPair(base="eur", quote="GBP")
    with pytest.raises(ValueError):
        CurrencyPair(base="EUR", quote="gbp")


def test_currency_pair_rejects_degenerate() -> None:
    with pytest.raises(ValueError):
        CurrencyPair(base="GBP", quote="GBP")
