"""Domain enumerations.

Three small, stable string-valued enums that downstream code — persistence,
rule engines, reporting — depends on for routing and audit. Using `StrEnum`
(Python 3.11+) gives each member a deterministic string value, which means
SQLite persistence, JSON serialisation, and CSV exports can all round-trip
an enum without any custom codec.

Author: Emre Tezel
"""

from __future__ import annotations

from enum import StrEnum


class AssetClass(StrEnum):
    """UK CGT asset classes supported by v1 of the calculator.

    The scope decision in `docs/architecture.md §Scope` fixes this set:
    stocks, bonds, futures, and FX (the last one treated as a CGT asset
    class per currency pair vs GBP, not merely a conversion mechanism).
    """

    STOCK = "stock"
    BOND = "bond"
    FUTURE = "future"
    FX = "fx"


class MatchRule(StrEnum):
    """The four UK share-matching rules (TCGA 1992 s.104 / s.105 / s.106A).

    Order matters to the matching engine but not to this enum — the engine
    applies them in this strict precedence:

    1. `SAME_DAY` — disposals matched against same-date acquisitions
       (TCGA92/S105(1)(b)).
    2. `BED_AND_BREAKFAST` — disposals matched against acquisitions in
       the 30 days *following* the disposal (TCGA92/S106A(5) and (5A)).
       The `BED_AND_BREAKFAST` member name keeps the UK-accountant
       nickname for the rule.
    3. `SECTION_104` — disposals matched against the pooled holding
       (TCGA92/S104), weighted-average cost basis.
    4. `LATER_ACQUISITION` — residual disposals matched against
       acquisitions made *after* the 30-day window (TCGA92/S105(2)),
       earliest acquisition first. This rule is what covers a
       sell-short followed by a buy-to-cover more than 30 days later,
       and any disposal that runs past an under-sized S.104 pool.

    The string values are chosen for readability in persisted audit
    rows; persisted enum strings are stable so changing them is a
    breaking change for the `matched_disposals` table.
    """

    SAME_DAY = "same_day"
    BED_AND_BREAKFAST = "bed_and_breakfast"
    SECTION_104 = "section_104"
    LATER_ACQUISITION = "later_acquisition"


class TradeAction(StrEnum):
    """Superset of trade directions covering all four asset classes.

    Stocks, bonds, and FX use the simple `BUY` / `SELL` pair. Futures need
    more detail: individual-investor CGT treatment is per-contract close-
    out, so the engine has to know whether a trade is *opening* or
    *closing* a position, and on which side (long vs short). Keeping the
    action at this granularity avoids re-deriving position state from
    trade history every time a future trade is processed.
    """

    BUY = "buy"
    SELL = "sell"
    OPEN_LONG = "open_long"
    CLOSE_LONG = "close_long"
    OPEN_SHORT = "open_short"
    CLOSE_SHORT = "close_short"
