"""Tests for `ib_cgt.report.document` — the format-neutral page and cell formatting.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ib_cgt.domain import Money
from ib_cgt.report.document import Column, ColumnKind, Heading, Table, format_cell


def test_format_cell_by_kind() -> None:
    assert format_cell(None, ColumnKind.MONEY) == "—"
    assert format_cell("text", ColumnKind.TEXT) == "text"
    assert format_cell(date(2025, 6, 1), ColumnKind.DATE) == "2025-06-01"
    assert format_cell(Money.gbp("1234.5"), ColumnKind.MONEY) == "1,234.50"
    assert format_cell(Money.of("1234.5", "USD"), ColumnKind.NATIVE) == "1,234.50 USD"
    assert format_cell(Decimal("1234.5"), ColumnKind.QUANTITY) == "1,234.50"
    assert format_cell(Decimal("1.25"), ColumnKind.RATE) == "1.2500"
    assert format_cell(Decimal("7"), ColumnKind.INTEGER) == "7"
    assert format_cell(1000, ColumnKind.INTEGER) == "1,000"


def test_format_cell_without_thousands_separators() -> None:
    assert format_cell(Money.gbp("1234.5"), ColumnKind.MONEY, thousands=False) == "1234.50"
    assert format_cell(1000, ColumnKind.INTEGER, thousands=False) == "1000"


def test_numeric_kinds_are_everything_but_text_and_date() -> None:
    assert not ColumnKind.TEXT.numeric
    assert not ColumnKind.DATE.numeric
    assert all(k.numeric for k in (ColumnKind.MONEY, ColumnKind.QUANTITY, ColumnKind.RATE))


def test_table_rejects_ragged_rows_and_footers() -> None:
    columns = (Column(header="a"), Column(header="b", kind=ColumnKind.MONEY))
    Table(columns=columns, rows=(("x", Decimal(1)),), footer=("t", Decimal(1)))
    with pytest.raises(ValueError, match="row 0"):
        Table(columns=columns, rows=(("x",),))
    with pytest.raises(ValueError, match="footer"):
        Table(columns=columns, rows=(), footer=("t",))
    with pytest.raises(ValueError, match="at least one column"):
        Table(columns=(), rows=())


def test_heading_levels_are_bounded() -> None:
    Heading(text="ok", level=4)
    with pytest.raises(ValueError, match=r"1\.\.4"):
        Heading(text="too deep", level=5)
