"""Map an IB statement's Open Positions rows to `StatementPosition` objects.

The parser emits one `RawOpenPositionRow` per symbol still held on
the last day of the statement period. This module resolves each row
to the same instrument identity the trade mapper produces — a stock
by `(symbol, currency)`, a bond by ISIN through the Financial
Instrument Information section, a futures contract by its multiplier
and expiry from that same section — so the trade-derived position and
the statement position can be compared instrument for instrument.

Resolution is FII-based and pure. A held-over futures contract on a
legacy statement (traded in an earlier year, never touched in this
one) can lack an instrument-information row; such rows are returned
to the caller as leftovers rather than raised, because the ingestor
can still resolve them against contracts already in the database.
Nothing here decides whether a position *should* be there — that is
the calculator's reconciliation.

Author: Emre Tezel
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from ib_cgt.domain import AnyInstrument, StatementPosition, StockInstrument
from ib_cgt.ingest.mapper import (
    MappingError,
    build_bond_instrument,
    build_future_instrument,
)
from ib_cgt.ingest.parser import ParsedStatement, RawInstrumentInfo, RawOpenPositionRow

# Section labels, post-normalisation, that the mapper handles. Mirrors
# the trade mapper's label sets; anything else (options, which the
# parser already drops; unknown future sections) is a loud failure.
_STOCK_LABELS = frozenset({"Stocks"})
_BOND_LABELS = frozenset({"Bonds", "Corporate and Municipal Bonds"})
_FUTURE_LABELS = frozenset({"Futures"})
_SUPPORTED_LABELS = _STOCK_LABELS | _BOND_LABELS | _FUTURE_LABELS


def map_open_positions(
    parsed: ParsedStatement,
) -> tuple[list[StatementPosition], list[RawOpenPositionRow]]:
    """Translate every Open Positions row into a `StatementPosition`.

    Args:
        parsed: Output of `parser.parse_statement`.

    Returns:
        `(positions, leftovers)` — the resolved positions in statement
        order, and the rows whose instrument could not be built from
        this statement's instrument-information section (bonds with
        no Security ID, futures with no multiplier / expiry row). The
        ingestor resolves leftovers against instruments already in the
        database and reports whatever is still unknown.

    Raises:
        MappingError: On a row that is malformed rather than merely
            unresolvable — an unparseable quantity, or an asset class
            this mapper does not model.
    """
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo] = {
        (info.asset_class, info.symbol): info for info in parsed.instruments
    }
    positions: list[StatementPosition] = []
    leftovers: list[RawOpenPositionRow] = []
    for raw in parsed.open_positions:
        if raw.asset_class not in _SUPPORTED_LABELS:
            # Checked before the resolver so an unmodelled class is a
            # loud failure rather than a silent leftover: the parser
            # already drops options, so anything else here is new.
            raise MappingError(
                f"Unsupported asset class in Open Positions section: {raw.asset_class!r} ({raw=})"
            )
        quantity = _parse_quantity(raw)
        if quantity == 0:
            # A zero-quantity line is not a position (IB prints one for
            # a contract closed on the period's last day); skipping it
            # matches "a flat instrument has no row".
            continue
        try:
            instrument = _resolve_instrument(raw, info_by_symbol)
        except MappingError:
            leftovers.append(raw)
            continue
        positions.append(
            StatementPosition(
                account_id=parsed.account_id,
                instrument=instrument,
                quantity=quantity,
            )
        )
    return positions, leftovers


def _resolve_instrument(
    raw: RawOpenPositionRow,
    info_by_symbol: dict[tuple[str, str], RawInstrumentInfo],
) -> AnyInstrument:
    """Build the instrument a position row refers to, from this statement alone."""
    if raw.asset_class in _STOCK_LABELS:
        return StockInstrument(symbol=raw.symbol, currency=raw.currency)
    if raw.asset_class in _BOND_LABELS:
        return build_bond_instrument(raw.symbol, raw.currency, info_by_symbol)
    # `map_open_positions` has already rejected every other label.
    return build_future_instrument(raw.symbol, raw.currency, info_by_symbol)


def _parse_quantity(raw: RawOpenPositionRow) -> Decimal:
    """Parse the signed, comma-formatted quantity cell."""
    cleaned = raw.quantity_text.replace(",", "").strip()
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise MappingError(
            f"Unparseable open-position quantity {raw.quantity_text!r} for {raw.symbol!r}"
        ) from exc


__all__ = ["map_open_positions"]
