"""Tests for `ib_cgt.domain.tax_year`.

The tax-year boundary is one of the most important correctness invariants
in the whole library — a disposal on the wrong side of 6 April pushes an
entire matching chain into the wrong year. These tests pin down the
boundary and the label parser.
"""

from __future__ import annotations

from datetime import date

import pytest

from ib_cgt.domain.tax_year import InvalidTaxYearError, TaxYear

# ---------------------------------------------------------------------------
# Construction and validation
# ---------------------------------------------------------------------------


def test_tax_year_basic() -> None:
    ty = TaxYear(2024)
    assert ty.start_year == 2024
    assert ty.start_date == date(2024, 4, 6)
    assert ty.end_date == date(2025, 4, 5)
    assert ty.label == "2024/25"


def test_tax_year_label_rollover() -> None:
    # Century rollover: 2099/00 is valid; the suffix zero-pads.
    assert TaxYear(2099).label == "2099/00"


def test_tax_year_rejects_too_old() -> None:
    with pytest.raises(InvalidTaxYearError):
        TaxYear(1999)


def test_tax_year_rejects_non_int() -> None:
    with pytest.raises(InvalidTaxYearError):
        TaxYear("2024")  # type: ignore[arg-type]
    with pytest.raises(InvalidTaxYearError):
        TaxYear(True)  # bool is an int subclass; reject explicitly


# ---------------------------------------------------------------------------
# from_label
# ---------------------------------------------------------------------------


def test_from_label_valid() -> None:
    assert TaxYear.from_label("2024/25") == TaxYear(2024)


def test_from_label_rejects_bad_shape() -> None:
    for bad in ["2024-25", "2024/2025", "24/25", "2024/25 ", ""]:
        with pytest.raises(InvalidTaxYearError):
            TaxYear.from_label(bad)


def test_from_label_rejects_inconsistent_suffix() -> None:
    # 2024 starts 2024/25, not 2024/26 — suffix must be start_year+1 mod 100.
    with pytest.raises(InvalidTaxYearError):
        TaxYear.from_label("2024/26")


def test_from_label_requires_string() -> None:
    with pytest.raises(InvalidTaxYearError):
        TaxYear.from_label(2024)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# containing — the critical 6-April boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("d", "expected_start"),
    [
        (date(2024, 4, 5), 2023),  # last day of 2023/24
        (date(2024, 4, 6), 2024),  # first day of 2024/25
        (date(2025, 4, 5), 2024),  # last day of 2024/25
        (date(2025, 4, 6), 2025),  # first day of 2025/26
        (date(2024, 12, 31), 2024),  # mid-year
        (date(2025, 1, 1), 2024),  # new calendar year, still 2024/25
    ],
)
def test_containing_boundary(d: date, expected_start: int) -> None:
    assert TaxYear.containing(d).start_year == expected_start


def test_containing_requires_date() -> None:
    with pytest.raises(TypeError):
        TaxYear.containing("2024-04-06")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# contains
# ---------------------------------------------------------------------------


def test_contains_inclusive_boundaries() -> None:
    ty = TaxYear(2024)
    assert ty.contains(date(2024, 4, 6))  # first day
    assert ty.contains(date(2025, 4, 5))  # last day
    assert not ty.contains(date(2024, 4, 5))  # day before
    assert not ty.contains(date(2025, 4, 6))  # day after


# ---------------------------------------------------------------------------
# Ordering & repr
# ---------------------------------------------------------------------------


def test_tax_year_ordering() -> None:
    assert TaxYear(2023) < TaxYear(2024)
    assert sorted([TaxYear(2025), TaxYear(2023), TaxYear(2024)]) == [
        TaxYear(2023),
        TaxYear(2024),
        TaxYear(2025),
    ]


def test_tax_year_repr() -> None:
    assert repr(TaxYear(2024)) == "TaxYear(2024/25)"
