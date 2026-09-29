"""Unit tests for `ingest/cash_balances.py:map_cash_balances`.

Covers the per-currency mapping of the Cash Report's two balance
lines, the blocks that are skipped (base-currency summary, segment
sub-blocks), the loud failures, and the number parsing.

Author: Emre Tezel
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.ingest.cash_balances import map_cash_balances
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.raw import ParsedStatement, RawCashReportRow


def _row(currency: str, label: str, total: str) -> RawCashReportRow:
    return RawCashReportRow(currency=currency, label=label, total_text=total)


def _parsed(*rows: RawCashReportRow) -> ParsedStatement:
    return ParsedStatement(
        time_zone=ZoneInfo("America/New_York"),
        account_id="U1",
        period_start=date(2025, 4, 7),
        period_end=date(2026, 4, 3),
        trades=(),
        instruments=(),
        corporate_actions=(),
        dividends=(),
        cash_report=rows,
    )


def test_one_balance_per_currency_in_first_seen_order() -> None:
    parsed = _parsed(
        _row("Base Currency Summary", "Starting Cash", "10,293.40"),
        _row("Base Currency Summary", "Ending Cash", "81,332.17"),
        _row("USD", "Starting Cash", "212.10"),
        _row("USD", "Dividends", "2,889.86"),
        _row("USD", "Ending Cash", "1,127.55"),
        _row("EUR", "Starting Cash", "-108.55"),
        _row("EUR", "Ending Cash", "0.00"),
    )
    balances = map_cash_balances(parsed)
    assert [(b.currency, b.starting_cash, b.ending_cash) for b in balances] == [
        ("USD", Decimal("212.10"), Decimal("1127.55")),
        ("EUR", Decimal("-108.55"), Decimal("0.00")),
    ]


def test_pdf_base_currency_block_has_no_currency_and_is_skipped() -> None:
    parsed = _parsed(
        _row("", "Starting Cash", "0.00"),
        _row("", "Ending Cash", "12,933.53"),
        _row("JPY", "Starting Cash", "0"),
        _row("JPY", "Ending Cash", "0"),
    )
    [jpy] = map_cash_balances(parsed)
    assert (jpy.currency, jpy.starting_cash, jpy.ending_cash) == ("JPY", Decimal(0), Decimal(0))


def test_missing_ending_cash_raises() -> None:
    parsed = _parsed(_row("USD", "Starting Cash", "1.00"))
    with pytest.raises(MappingError, match="lacks 'Ending Cash'"):
        map_cash_balances(parsed)


def test_conflicting_duplicate_line_raises_and_identical_duplicate_is_ignored() -> None:
    parsed = _parsed(
        _row("USD", "Starting Cash", "1.00"),
        _row("USD", "Starting Cash", "1.00"),
        _row("USD", "Ending Cash", "2.00"),
    )
    [usd] = map_cash_balances(parsed)
    assert usd.starting_cash == Decimal("1.00")
    parsed = _parsed(
        _row("USD", "Starting Cash", "1.00"),
        _row("USD", "Starting Cash", "3.00"),
        _row("USD", "Ending Cash", "2.00"),
    )
    with pytest.raises(MappingError, match="twice"):
        map_cash_balances(parsed)


def test_unparseable_amount_raises() -> None:
    parsed = _parsed(_row("USD", "Starting Cash", "n/a"), _row("USD", "Ending Cash", "1"))
    with pytest.raises(MappingError, match="Unparseable"):
        map_cash_balances(parsed)


def test_statement_without_a_cash_report_maps_to_nothing() -> None:
    assert map_cash_balances(_parsed()) == []
