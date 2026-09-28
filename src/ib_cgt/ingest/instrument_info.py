"""Lookup index over a statement's Financial Instrument Information rows.

Every IB statement carries one "Financial Instrument Information"
block with one table per asset class (Stocks, Futures, Bonds). Those
rows are the only place the statement prints IB's `conid` and the
bond ISIN, so four different mappers — trades, open positions, bond
coupons and corporate actions — all need to resolve a symbol printed
elsewhere in the statement to its instrument-information row. Before
this module each of them built its own ad-hoc dict; this class builds
the index once per statement and offers the two lookups they share.

The index is a pure, statement-local structure: it never touches the
database and never raises. Deciding what a missing row means (loud
`MappingError`, or a leftover to resolve later) is the caller's job.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from ib_cgt.ingest.raw import ParsedStatement, RawInstrumentInfo


class InstrumentInfoIndex:
    """Symbol- and security-id-keyed views of one statement's instrument rows.

    Keys are `(asset_class, symbol)` and `(asset_class, security_id)`
    with the asset class exactly as the parser labels the table
    (`"Stocks"`, `"Futures"`, `"Bonds"`). Within one statement IB
    prints a symbol consistently across sections, so an exact symbol
    match is the primary lookup; the security-id lookup covers rows
    that name the instrument by ISIN instead (dividend and merger
    descriptions, bond maturities).

    Bonds need a fuzzier walk (yield-suffixed symbols, description
    matches) which `mapper.resolve_bond_info` layers on top of
    `rows_for`.
    """

    __slots__ = ("_by_security_id", "_by_symbol", "_rows")

    def __init__(self, rows: Iterable[RawInstrumentInfo]) -> None:
        """Index `rows`; later duplicates of a key win, matching dict semantics."""
        self._rows: tuple[RawInstrumentInfo, ...] = tuple(rows)
        self._by_symbol: dict[tuple[str, str], RawInstrumentInfo] = {
            (info.asset_class, info.symbol): info for info in self._rows
        }
        self._by_security_id: dict[tuple[str, str], RawInstrumentInfo] = {
            (info.asset_class, info.security_id): info for info in self._rows if info.security_id
        }

    @classmethod
    def from_parsed(cls, parsed: ParsedStatement) -> InstrumentInfoIndex:
        """Build the index for one parsed statement."""
        return cls(parsed.instruments)

    def by_symbol(self, asset_class: str, symbol: str) -> RawInstrumentInfo | None:
        """Return the row whose `Symbol` cell is exactly `symbol`, or `None`."""
        return self._by_symbol.get((asset_class, symbol))

    def by_security_id(self, asset_class: str, security_id: str) -> RawInstrumentInfo | None:
        """Return the row whose `Security ID` cell is `security_id`, or `None`."""
        return self._by_security_id.get((asset_class, security_id))

    def rows_for(self, asset_class: str) -> Iterator[RawInstrumentInfo]:
        """Iterate the rows of one asset class in statement order."""
        return (info for info in self._rows if info.asset_class == asset_class)


__all__ = ["InstrumentInfoIndex"]
