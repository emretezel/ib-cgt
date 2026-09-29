"""Map the Cash Report section to `StatementCashBalance` objects.

The parser emits one `RawCashReportRow` per line of the Cash Report
(`Starting Cash`, `Commissions`, `Dividends`, …, `Ending Cash`), each
stamped with the currency sub-header it sat under. This module keeps
the two balance lines per real currency and turns them into one
`StatementCashBalance` per currency, the broker's own statement of
the foreign-currency holdings the FX pools model.

What is skipped: the `Base Currency Summary` block (every currency
translated into GBP — a derived figure, not a balance) and the
segment sub-blocks IB prints on newer statements (`Cash Detail`,
`Collateral Value Detail`, `Net Cash Detail`), whose rows carry other
labels. Only `Starting Cash` and `Ending Cash` are read; the movement
categories between them are IB's own breakdown and the engines
derive their own.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Final

from ib_cgt.domain import StatementCashBalance
from ib_cgt.ingest.mapper import MappingError
from ib_cgt.ingest.raw import ParsedStatement, RawCashReportRow

# The two lines a balance is built from, as IB prints them.
_STARTING: Final = "Starting Cash"
_ENDING: Final = "Ending Cash"

# A real currency block is headed by a bare ISO-4217 code; the HTML
# adapter stamps the base-currency block `"Base Currency Summary"` and
# the PDF adapter stamps it `""`, and neither is a balance.
_CURRENCY_CODE: Final = re.compile(r"[A-Z]{3}")


def map_cash_balances(parsed: ParsedStatement) -> list[StatementCashBalance]:
    """Translate the Cash Report into one balance per currency, in first-seen order.

    Args:
        parsed: Output of `parser.parse_statement`.

    Returns:
        One `StatementCashBalance` per currency block (GBP included),
        or an empty list for a statement without a Cash Report.

    Raises:
        MappingError: A currency block lacks `Starting Cash` or
            `Ending Cash`, prints one of them twice with different
            values, or carries an unparseable amount.
    """
    balances: dict[str, dict[str, Decimal]] = {}
    for raw in parsed.cash_report:
        if not _CURRENCY_CODE.fullmatch(raw.currency):
            continue
        if raw.label not in (_STARTING, _ENDING):
            continue
        amount = _parse_decimal(raw)
        lines = balances.setdefault(raw.currency, {})
        # A statement whose Cash Report repeats a balance line inside
        # one currency block (a page-break artefact would be the only
        # imaginable cause) must agree with itself; a silent "last one
        # wins" could hide a mis-parse.
        previous = lines.get(raw.label)
        if previous is not None and previous != amount:
            raise MappingError(
                f"Cash Report prints {raw.label!r} twice for {raw.currency} with different "
                f"values ({previous} and {amount})"
            )
        lines[raw.label] = amount

    out: list[StatementCashBalance] = []
    for currency, lines in balances.items():
        missing = [label for label in (_STARTING, _ENDING) if label not in lines]
        if missing:
            raise MappingError(
                f"Cash Report block for {currency} lacks {', '.join(repr(m) for m in missing)}"
            )
        out.append(
            StatementCashBalance(
                currency=currency,
                starting_cash=lines[_STARTING],
                ending_cash=lines[_ENDING],
            )
        )
    return out


def _parse_decimal(raw: RawCashReportRow) -> Decimal:
    """Parse a comma-formatted, signed IB amount cell into `Decimal`."""
    cleaned = raw.total_text.replace(",", "").strip()
    if not cleaned:
        raise MappingError(f"Empty Cash Report amount on {raw.currency} {raw.label!r}")
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable Cash Report amount {raw.total_text!r} on {raw.currency} {raw.label!r}"
        ) from exc


__all__ = ["map_cash_balances"]
