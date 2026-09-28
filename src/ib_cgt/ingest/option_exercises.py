"""Link an exercised or assigned option row to the share trade IB books for it.

When an option is exercised, IB prints two rows at the same instant:
the option row, closed at price 0 with code `Ex` (holder) or `A`
(writer), and a Stocks row for the underlying at the strike price for
`contracts x multiplier` shares (the 2019 `TUR 17MAY19 22.0 P` exercise
in `statements/futures/19_20.htm`: option `-15 @ 0.0000, C;Ex`, stock
`TUR -1,500 @ 22.0000, Ex;O`). Under TCGA 1992 s.144(2)-(3) the two are
one transaction, so the calculator needs to know which share trade each
option row produced. This module finds that pairing inside one
statement, on the mapped `Trade` objects, before anything is persisted:

* same execution instant (`trade_datetime`),
* a `StockInstrument` whose symbol is the option's underlying, in the
  option's currency,
* `quantity == contracts x multiplier` and `price == strike`,
* the direction the right implies: a holder's call or a writer's put
  *buys* shares, a holder's put or a writer's call *sells* them.

Exactly one candidate is a link. None means IB booked no share trade —
a cash-settled option (s.144A), or a shape this module does not know —
and the row is reported so the calculator can treat it as cash-settled
and warn. Several candidates, or one share trade claimed by two option
rows, is a `MappingError`: guessing would mis-tax both.

The links are positions in the mapped trade list, because the trades
have no ids until they are inserted; the ingestor turns them into
`option_exercise_links` rows once it knows the ids.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from ib_cgt.domain import (
    OptionInstrument,
    StockInstrument,
    Trade,
    TradeAction,
    option_share_action,
)
from ib_cgt.ingest.mapper import MappingError

# The option actions that produce a share trade.
_EXERCISE_ACTIONS: Final[frozenset[TradeAction]] = frozenset(
    {TradeAction.EXERCISE_LONG, TradeAction.ASSIGN_SHORT}
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ExerciseLink:
    """An option row and the share trade it produced, as positions in the mapped trade list.

    Attributes:
        option_index: Index of the `EXERCISE_LONG` / `ASSIGN_SHORT` trade.
        share_index: Index of the stock trade booked at the strike.
    """

    option_index: int
    share_index: int


def share_action_for(instrument: OptionInstrument, action: TradeAction) -> TradeAction:
    """The stock action an exercise or assignment of `instrument` books.

    A holder's exercise is the `LONG` side, a writer's assignment the
    `SHORT` side; `option_share_action` in the domain decides the
    direction from the right.

    Raises:
        ValueError: `action` is not an exercise or an assignment.
    """
    if action is TradeAction.EXERCISE_LONG:
        return option_share_action(instrument.right, "LONG")
    if action is TradeAction.ASSIGN_SHORT:
        return option_share_action(instrument.right, "SHORT")
    raise ValueError(f"{action.value!r} is not an exercise or an assignment")


def map_option_exercise_links(
    trades: Sequence[Trade],
) -> tuple[list[ExerciseLink], list[int]]:
    """Pair every exercise / assignment in `trades` with its share trade.

    Args:
        trades: The mapped trades of one statement, in statement order —
            the same list whose positions the ingestor persists as
            `statement_row_index`.

    Returns:
        `(links, unlinked)`: one `ExerciseLink` per option row that has
        exactly one candidate share trade, and the positions of the
        option rows that have none (to be treated as cash-settled).

    Raises:
        MappingError: An option row has several candidate share trades,
            or two option rows claim the same share trade.
    """
    links: list[ExerciseLink] = []
    unlinked: list[int] = []
    claimed: dict[int, int] = {}
    for option_index, trade in enumerate(trades):
        instrument = trade.instrument
        if not isinstance(instrument, OptionInstrument) or trade.action not in _EXERCISE_ACTIONS:
            continue
        candidates = [
            share_index
            for share_index, candidate in enumerate(trades)
            if _is_share_leg_of(candidate, trade, instrument)
        ]
        if not candidates:
            unlinked.append(option_index)
            continue
        if len(candidates) > 1:
            raise MappingError(
                f"Option {instrument.symbol} {trade.action.value} at {trade.trade_datetime} "
                f"matches {len(candidates)} share trades; cannot tell which one it produced"
            )
        (share_index,) = candidates
        if share_index in claimed:
            raise MappingError(
                f"Share trade at position {share_index} is claimed by two option rows "
                f"(positions {claimed[share_index]} and {option_index})"
            )
        claimed[share_index] = option_index
        links.append(ExerciseLink(option_index=option_index, share_index=share_index))
    return links, unlinked


def _is_share_leg_of(candidate: Trade, option_trade: Trade, instrument: OptionInstrument) -> bool:
    """True iff `candidate` is the stock trade IB booked for `option_trade`."""
    stock = candidate.instrument
    if not isinstance(stock, StockInstrument):
        return False
    return (
        stock.symbol == instrument.underlying
        and stock.currency == instrument.currency
        and candidate.trade_datetime == option_trade.trade_datetime
        and candidate.action is share_action_for(instrument, option_trade.action)
        and candidate.quantity == option_trade.quantity * instrument.contract_multiplier
        and candidate.price.amount == instrument.strike
    )


__all__ = ["ExerciseLink", "map_option_exercise_links", "share_action_for"]
