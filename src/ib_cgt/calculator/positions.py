"""Reconcile trade-derived positions against the latest statements' open positions.

The ingested trades imply a signed holding for every instrument:
buys and long opens add, sells and long closes subtract, short opens
subtract, short closes add. Corporate actions move holdings too — a
cash merger or a maturity takes the whole position out, a split or a
spin-off (unmodelled, stored as `unsupported`) changes it — so their
signed quantities are added on the same side. The broker states its
own view at the end of every statement — the Open Positions section.
The two must agree as of each account's *latest* statement, or the
history is incomplete:

* trades net to a holding no statement lists — a sale was never
  ingested, or an over-sold stock;
* a statement lists a holding the trades never built — bought
  before the earliest statement (cost basis unknown);
* both sides exist but disagree — a partial fill or a missing
  statement in between.

The comparison is made **per taxpayer, not per account**. UK CGT
pools span every account the taxpayer holds (`docs/rules.md`), and
so does the residual the matching engines report; a holding moved
between the taxpayer's own IB accounts is not a trade and is never
ingested, so a per-account comparison would flag every transferred
position twice — as a phantom long in the account that bought it
and as an unexplained holding in the account that now lists it.
Summing both sides across accounts is what the pools actually see.
Each account's contribution is taken as of its own latest
statement's `period_end`; accounts with no statement contribute
nothing (there is nothing to reconcile against).

A position both sides agree on is what makes an uncovered short (a
soft residual from the matching engines) *expected*: the statement
confirms the short is still open, so its gain is simply deferred.
The tax-year calculator turns every non-`MATCH` outcome into a run
issue and check C7 reports the same reconciliation standalone.

FX is outside this module: currency balances are reconciled against
the statements' Cash Report by `calculator/cash_balances.py`.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from ib_cgt.db import (
    CorporateActionRepo,
    InstrumentRepo,
    StatementPositionRepo,
    StatementRepo,
    StatementRow,
    TradeRepo,
)
from ib_cgt.domain import AnyInstrument, AssetClass


class PositionStatus(StrEnum):
    """Outcome of comparing one instrument's trade-derived and statement positions.

    `match`: both sides carry the same signed quantity.
    `mismatch`: both sides carry a quantity and they differ.
    `not_on_statement`: the trades net to a non-zero holding but no
        latest statement has a row for the instrument.
    `no_trades`: the statements list a holding the trades never
        built — nothing in the ingested history acquired it (the
        trade-derived total is zero while the stated total is not).
    """

    MATCH = "match"
    MISMATCH = "mismatch"
    NOT_ON_STATEMENT = "not_on_statement"
    NO_TRADES = "no_trades"


@dataclass(frozen=True, slots=True, kw_only=True)
class AccountPosition:
    """One account's side of a position: what its trades say versus its statement.

    Attributes:
        account_id: The account.
        statement: The account's latest statement — the comparison
            is as of its `period_end`.
        trade_quantity: Signed holding implied by the account's
            trades and corporate actions up to that `period_end`
            (zero when there are none).
        statement_quantity: The statement's signed quantity, or
            `None` when the statement lists no row.
    """

    account_id: str
    statement: StatementRow
    trade_quantity: Decimal
    statement_quantity: Decimal | None

    def describe(self) -> str:
        """One-line `account: trades X, statement Y` summary for evidence and messages."""
        stated = "none" if self.statement_quantity is None else str(self.statement_quantity)
        return f"{self.account_id}: trades {self.trade_quantity}, statement {stated}"


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionReconciliation:
    """One instrument across the taxpayer's accounts: trade-derived versus stated totals.

    Attributes:
        instrument_id: The instrument's row id.
        instrument: The instrument itself.
        accounts: Every account (with a latest statement) that holds
            the instrument on either side, in account-id order.
    """

    instrument_id: int
    instrument: AnyInstrument
    accounts: tuple[AccountPosition, ...]

    @property
    def trade_quantity(self) -> Decimal:
        """Signed holding the trades imply across all accounts."""
        return sum((a.trade_quantity for a in self.accounts), Decimal(0))

    @property
    def statement_quantity(self) -> Decimal | None:
        """Signed holding the latest statements list across all accounts, or `None` if none does."""
        stated = [a.statement_quantity for a in self.accounts if a.statement_quantity is not None]
        if not stated:
            return None
        return sum(stated, Decimal(0))

    @property
    def status(self) -> PositionStatus:
        """Classify the comparison of the two totals — see `PositionStatus`."""
        stated = self.statement_quantity
        if stated is None:
            # Trades that net to zero across accounts (bought in one,
            # sold from the other) with no statement row anywhere are
            # flat on both sides — agreement, not a missing row.
            if self.trade_quantity == 0:
                return PositionStatus.MATCH
            return PositionStatus.NOT_ON_STATEMENT
        if self.trade_quantity == stated:
            # Equal totals agree — including two accounts whose long
            # and short legs net to zero on both sides.
            return PositionStatus.MATCH
        if self.trade_quantity == 0:
            return PositionStatus.NO_TRADES
        return PositionStatus.MISMATCH

    def describe_accounts(self) -> str:
        """Per-account breakdown, `; `-joined, for evidence rows and issue messages."""
        return "; ".join(a.describe() for a in self.accounts)


def reconcile_positions(conn: sqlite3.Connection) -> tuple[PositionReconciliation, ...]:
    """Compare the taxpayer's trade-derived holdings with the latest statements.

    Accounts with no statement are skipped — there is nothing to
    reconcile against. For each account with one, the trades and the
    corporate actions are netted up to and including the statement's
    `period_end` and joined with the statement's Open Positions rows. The per-account
    sides are then grouped by instrument, so every instrument present
    on either side in any account yields exactly one row whose totals
    decide the status. FX pairs are excluded (they cannot appear on a
    statement's positions and are never reconciled).

    Returns:
        One row per instrument, ordered by instrument id — stable
        run-to-run for a given database.
    """
    statement_repo = StatementRepo(conn)
    position_repo = StatementPositionRepo(conn)
    trade_repo = TradeRepo(conn)
    action_repo = CorporateActionRepo(conn)
    instrument_repo = InstrumentRepo(conn)

    sides: dict[int, list[AccountPosition]] = {}
    # `latest_per_account` is ordered by account id, so each
    # instrument's account tuple comes out in account order for free.
    for statement in statement_repo.latest_per_account():
        traded = _net_quantities(
            trade_repo.signed_quantity_by_instrument(
                statement.account_id, up_to=statement.period_end
            ),
            action_repo.signed_quantity_by_instrument(
                statement.account_id, up_to=statement.period_end
            ),
        )
        stated = {
            instrument_id: position.quantity
            for instrument_id, position in position_repo.for_statement(statement.statement_hash)
        }
        for instrument_id in set(traded) | set(stated):
            sides.setdefault(instrument_id, []).append(
                AccountPosition(
                    account_id=statement.account_id,
                    statement=statement,
                    trade_quantity=traded.get(instrument_id, Decimal(0)),
                    statement_quantity=stated.get(instrument_id),
                )
            )

    out: list[PositionReconciliation] = []
    for instrument_id in sorted(sides):
        instrument = instrument_repo.get(instrument_id)
        if instrument.asset_class is AssetClass.FX:
            continue
        out.append(
            PositionReconciliation(
                instrument_id=instrument_id,
                instrument=instrument,
                accounts=tuple(sides[instrument_id]),
            )
        )
    return tuple(out)


def _net_quantities(*sides: dict[int, Decimal]) -> dict[int, Decimal]:
    """Sum signed quantities per instrument across sources, dropping those that net to zero."""
    totals: dict[int, Decimal] = {}
    for side in sides:
        for instrument_id, quantity in side.items():
            totals[instrument_id] = totals.get(instrument_id, Decimal(0)) + quantity
    return {iid: qty for iid, qty in totals.items() if qty != 0}


def instrument_reconciles(
    reconciliations: Iterable[PositionReconciliation], instrument_id: int
) -> bool:
    """True iff the instrument's row (if any) is a `MATCH`.

    An instrument with no row reconciles vacuously — no account's
    trades or statements carry it, so it is flat everywhere.
    """
    return all(
        rec.status is PositionStatus.MATCH
        for rec in reconciliations
        if rec.instrument_id == instrument_id
    )


__all__ = [
    "AccountPosition",
    "PositionReconciliation",
    "PositionStatus",
    "instrument_reconciles",
    "reconcile_positions",
]
