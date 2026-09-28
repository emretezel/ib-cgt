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
    """UK CGT asset classes the calculator models.

    The scope decision in `docs/architecture.md §Scope` fixes this set:
    stocks, bonds, futures, FX (treated as a CGT asset class per currency
    vs GBP, not merely a conversion mechanism) and, since 2026-09-28,
    exchange-traded options (TCGA 1992 s.144 / s.148 — see
    `docs/options.md`). `OPTION` is declared last so the enum order the
    reports print in stays stable for the four original classes.
    """

    STOCK = "stock"
    BOND = "bond"
    FUTURE = "future"
    FX = "fx"
    OPTION = "option"


class OptionRight(StrEnum):
    """Whether an option is a call (right to buy) or a put (right to sell).

    Decides which way an exercise or assignment moves the premium into
    the share trade under TCGA 1992 s.144(2)-(3): a call's premium joins
    the share *purchase* cost (holder) or the share *sale* proceeds
    (writer); a put's premium is a cost of the share *sale* (holder) or a
    deduction from the share *purchase* cost (writer).
    """

    CALL = "call"
    PUT = "put"


class OptionCloseKind(StrEnum):
    """How a written option's grant was (partly) closed — one per drain of a grant.

    `PURCHASE`: the writer bought the option back (a closing purchase,
        TCGA 1992 s.148) — its cost is an incidental cost of the grant.
    `LAPSE`: the option expired unexercised — no effect on the grantor
        (HMRC CG55536); recorded so the grant is seen to be closed.
    `ASSIGNMENT`: the holder exercised and shares changed hands — the
        grant and the share trade are one transaction (s.144(2)); the
        assigned contracts' premium leaves the grant for the share trade.
    `CASH_SETTLEMENT`: the holder exercised a cash-settled option — the
        cash the writer paid is a cost of the grant (s.144A).
    """

    PURCHASE = "purchase"
    LAPSE = "lapse"
    ASSIGNMENT = "assignment"
    CASH_SETTLEMENT = "cash_settlement"


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
    """Superset of trade directions covering every asset class.

    Stocks, bonds, and FX use the simple `BUY` / `SELL` pair. Futures need
    more detail: individual-investor CGT treatment is per-contract close-
    out, so the engine has to know whether a trade is *opening* or
    *closing* a position, and on which side (long vs short). Keeping the
    action at this granularity avoids re-deriving position state from
    trade history every time a future trade is processed.

    Options use the same four open / close actions when the position is
    opened or closed *by trade*, plus four qualified closes for the
    other ways an option can end — each is its own tax event under TCGA
    1992 s.144, so the action carries the distinction rather than a
    nullable qualifier column:

    * `LAPSE_LONG` / `LAPSE_SHORT` — the option expired unexercised (IB
      code `Ep`). A lapsed long is a disposal for nil (s.144(4)); a lapsed
      short leaves the grant charge untouched.
    * `EXERCISE_LONG` — the holder exercised (IB code `Ex`): not a
      disposal (s.144(3)); the option's cost moves into the share trade.
    * `ASSIGN_SHORT` — the writer was assigned (IB code `A`): the grant
      and the share trade are one transaction (s.144(2)).
    """

    BUY = "buy"
    SELL = "sell"
    OPEN_LONG = "open_long"
    CLOSE_LONG = "close_long"
    OPEN_SHORT = "open_short"
    CLOSE_SHORT = "close_short"
    LAPSE_LONG = "lapse_long"
    EXERCISE_LONG = "exercise_long"
    LAPSE_SHORT = "lapse_short"
    ASSIGN_SHORT = "assign_short"
