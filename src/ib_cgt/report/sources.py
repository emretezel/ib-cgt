"""Resolving the ids on a persisted run back to the events behind them.

A `matched_disposals` row cites its disposal and (for a direct basis)
its acquisition by integer id. For stocks, bonds and forex trades that
is a real `trades.trade_id`. For the non-trade events the engines
consume — dividends, withholding tax, coupons, cash movements, futures
P&L, corporate actions — it is a synthetic id the runner allocated,
and the run's `event_sources` table says which row each one stands
for.

`DbEventResolver` turns either kind of id into an `EventRef` (label,
date, account, description) with one repository lookup per distinct
id. `StaticEventResolver` does the same from a fixed mapping so the
builder can be tested without a database.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from typing import Protocol, assert_never

from ib_cgt.db import BondCouponRepo, CashEventRepo, CorporateActionRepo, DividendRepo, TradeRepo
from ib_cgt.domain import (
    BondCouponRef,
    CashEventRef,
    CorporateActionRef,
    DividendRef,
    EventSource,
    FutureInstrument,
    FutureRealisationRef,
    OptionExerciseTransfer,
)
from ib_cgt.report.labels import (
    cash_description,
    cash_label,
    corporate_action_description,
    corporate_action_label,
    coupon_description,
    coupon_label,
    dividend_description,
    dividend_label,
    realisation_description,
    realisation_label,
    trade_description,
    trade_label,
    transfer_note,
)
from ib_cgt.report.model import EventRef


class EventResolver(Protocol):
    """Anything that can turn an engine event id into an `EventRef`."""

    def resolve(self, event_id: int) -> EventRef:
        """The reference for `event_id`; never raises for an unknown id."""
        ...


def unresolved(event_id: int) -> EventRef:
    """The reference used when no row can be found for an id.

    A statement withdrawn after the run was computed leaves its trade
    ids dangling (the run tables deliberately do not cascade from
    `trades`). The line is still reported — the figures are the run's
    — but the reader is told the row is gone rather than shown a
    made-up description.
    """
    return EventRef(
        event_id=event_id,
        label=trade_label(event_id),
        on=None,
        account_id=None,
        description="unresolved — the source row is no longer in the database",
    )


class StaticEventResolver:
    """A resolver over a fixed id → reference mapping (tests, pure callers)."""

    def __init__(self, refs: Mapping[int, EventRef]) -> None:
        """Keep the mapping; ids outside it resolve as unresolved."""
        self._refs = dict(refs)

    def resolve(self, event_id: int) -> EventRef:
        """Look the id up, falling back to the unresolved reference."""
        return self._refs.get(event_id) or unresolved(event_id)


class DbEventResolver:
    """Resolve ids through the repositories, guided by the run's provenance map."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        event_sources: Mapping[int, EventSource],
        exercise_transfers: Iterable[OptionExerciseTransfer] = (),
    ) -> None:
        """Bind to an open connection, the run's synthetic-id map and its exercise transfers.

        `exercise_transfers` are the run's `option_exercise_transfers`;
        a share trade one of them modified gets the s.144 note appended
        to its description so the stock line explains its cost.
        """
        self._trades = TradeRepo(conn)
        self._dividends = DividendRepo(conn)
        self._coupons = BondCouponRepo(conn)
        self._cash_events = CashEventRepo(conn)
        self._corporate_actions = CorporateActionRepo(conn)
        self._sources = event_sources
        self._transfers: dict[int, list[OptionExerciseTransfer]] = {}
        for transfer in exercise_transfers:
            self._transfers.setdefault(transfer.share_trade_id, []).append(transfer)
        # A disposal cited by several lines, or an acquisition matched
        # by several disposals, is looked up once.
        self._cache: dict[int, EventRef] = {}

    def resolve(self, event_id: int) -> EventRef:
        """The reference for `event_id`, memoised per id."""
        ref = self._cache.get(event_id)
        if ref is None:
            ref = self._lookup(event_id)
            self._cache[event_id] = ref
        return ref

    def _lookup(self, event_id: int) -> EventRef:
        """Synthetic ids go through the provenance map; anything else is a trade id."""
        source = self._sources.get(event_id)
        if source is None:
            return self._trade(event_id)
        if isinstance(source, DividendRef):
            return self._dividend(event_id, source)
        if isinstance(source, BondCouponRef):
            return self._coupon(event_id, source)
        if isinstance(source, CashEventRef):
            return self._cash_event(event_id, source)
        if isinstance(source, CorporateActionRef):
            return self._corporate_action(event_id, source)
        if isinstance(source, FutureRealisationRef):
            return self._realisation(event_id, source)
        # The union is sealed; a new member needs a branch here.
        assert_never(source)

    def _trade(self, event_id: int) -> EventRef:
        """A real trade: label `#N`, dated and described from the row.

        A share trade an option exercise modified carries the s.144
        note for each transfer, so the reader sees why its cost or
        proceeds differ from price times quantity.
        """
        stored = self._trades.get(event_id)
        if stored is None:
            return unresolved(event_id)
        trade = stored.trade
        description = trade_description(trade)
        for transfer in self._transfers.get(event_id, ()):
            description = f"{description}; {transfer_note(transfer)}"
        return EventRef(
            event_id=event_id,
            label=trade_label(event_id),
            on=trade.trade_date,
            account_id=trade.account_id,
            description=description,
        )

    def _dividend(self, event_id: int, source: DividendRef) -> EventRef:
        """A dividend or withholding-tax row, dated on its pay date."""
        stored = self._dividends.get(source.dividend_id)
        if stored is None:
            return unresolved(event_id)
        dividend = stored.dividend
        return EventRef(
            event_id=event_id,
            label=dividend_label(dividend.kind, source.dividend_id),
            on=dividend.pay_date,
            account_id=dividend.account_id,
            description=dividend_description(dividend),
        )

    def _coupon(self, event_id: int, source: BondCouponRef) -> EventRef:
        """A bond coupon, dated on its pay date."""
        stored = self._coupons.get(source.bond_coupon_id)
        if stored is None:
            return unresolved(event_id)
        coupon = stored.coupon
        return EventRef(
            event_id=event_id,
            label=coupon_label(source.bond_coupon_id),
            on=coupon.pay_date,
            account_id=coupon.account_id,
            description=coupon_description(coupon),
        )

    def _cash_event(self, event_id: int, source: CashEventRef) -> EventRef:
        """An instrument-less cash movement, dated on its value date."""
        stored = self._cash_events.get(source.cash_event_id)
        if stored is None:
            return unresolved(event_id)
        event = stored.event
        return EventRef(
            event_id=event_id,
            label=cash_label(source.cash_event_id),
            on=event.value_date,
            account_id=event.account_id,
            description=cash_description(event),
        )

    def _corporate_action(self, event_id: int, source: CorporateActionRef) -> EventRef:
        """A corporate action, dated on its effective date — the disposal and the pool date."""
        stored = self._corporate_actions.get(source.corporate_action_id)
        if stored is None:
            return unresolved(event_id)
        action = stored.action
        return EventRef(
            event_id=event_id,
            label=corporate_action_label(source.corporate_action_id),
            on=action.effective_date,
            account_id=action.account_id,
            description=corporate_action_description(action),
        )

    def _realisation(self, event_id: int, source: FutureRealisationRef) -> EventRef:
        """A futures P&L cashflow: dated and accounted like its close trade."""
        stored = self._trades.get(source.close_trade_id)
        if stored is None:
            return unresolved(event_id)
        trade = stored.trade
        instrument = trade.instrument
        if not isinstance(instrument, FutureInstrument):
            return unresolved(event_id)
        return EventRef(
            event_id=event_id,
            label=realisation_label(source.open_trade_id, source.close_trade_id),
            on=trade.trade_date,
            account_id=trade.account_id,
            description=realisation_description(
                instrument, source.open_trade_id, source.close_trade_id
            ),
        )


__all__ = ["DbEventResolver", "EventResolver", "StaticEventResolver", "unresolved"]
