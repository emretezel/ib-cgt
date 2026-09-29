"""Reconcile the engines' foreign-currency balances against the statements' Cash Report.

Every projected FX-pool event is a signed movement of one currency in
one account: a forex leg, a stock or bond settlement, a futures fee or
P&L, a dividend, a coupon, a cash event, a corporate action's cash.
Summed per account and currency they say how much of that currency
the engines believe the account holds. The broker states the same
figure independently at the start and end of every statement — the
Cash Report's `Starting Cash` / `Ending Cash` per currency, stored in
`statement_cash_balances`. The two must agree, or a pool is missing a
source (the IEMI merger cash that never reached the USD pool, the
payments in lieu booked the wrong way round) and every later
disposal of that currency is matched on a wrong cost.

The comparison is made **per account** (IB keeps cash per account,
and so does every projected event) over the **whole history**: the
engine's balance at the account's *latest* statement's `period_end`
against IB's ending cash on that statement less IB's starting cash on
the account's *earliest* statement — so a history that begins with a
balance already held (pre-history cash the pools never saw) still
reconciles on the movement.

Open futures are the one systematic difference between the two views
and are adjusted for exactly. IB settles variation margin daily, so
its cash already holds the unrealised P&L of every open contract; the
engine posts a contract's P&L only on close. For each of the engine's
own open lots (`FutureResult.open_positions`, FIFO-identified) at the
latest statement, `(close_price - open_price) x multiplier x signed
quantity` valued at the statement's Close Price is what IB has
settled and the engine has not. Valuing the *engine's* lots at the
statement's price — rather than reading IB's own unrealised P/L,
which it computes from an average cost — is what makes the two
agree to the cent. A lot whose contract the statement no longer lists
cannot be priced; it is counted, skipped, and is the position
reconciliation's finding (C7), not this one's.

The tolerance is one unit of the currency: IB rounds each ledger
line to the cent independently, and over fifteen years those pennies
add up to a few cents — never to a pound. Anything larger is a source
the pools do not see.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Final

from ib_cgt.calculator.runner import project_pool, run_future_engine
from ib_cgt.calculator.runs import FutureEngineRun, FXInputs
from ib_cgt.db import StatementCashBalanceRepo, StatementPositionRepo, StatementRepo, StatementRow
from ib_cgt.rules.futures import FXConverter

# One unit of the currency. IB posts every ledger line to the cent
# independently of the trade it settles, so the engine's balance and
# IB's can differ by the sum of those roundings — a few cents over a
# long history — and never by a whole unit unless a source is missing.
CASH_TOLERANCE: Final = Decimal("1.00")


class CashBalanceStatus(StrEnum):
    """Outcome of comparing one account's currency balance with the Cash Report.

    `match`: the movement IB reports equals the engine's balance plus
        the open-futures adjustment, within `CASH_TOLERANCE`.
    `mismatch`: it does not — a pool is missing a source or carrying a
        phantom one.
    """

    MATCH = "match"
    MISMATCH = "mismatch"


@dataclass(frozen=True, slots=True, kw_only=True)
class CashBalanceReconciliation:
    """One account's holding of one currency: the Cash Report versus the engines.

    Attributes:
        account_id: The account.
        currency: The non-GBP currency reconciled.
        earliest: The account's earliest statement — the origin of the
            comparison, whose `Starting Cash` is the balance the
            pools never saw.
        latest: The account's latest statement — the comparison is as
            of its `period_end`.
        ib_starting: IB's `Starting Cash` on the earliest statement
            (zero when that statement carries no row for the currency).
        ib_ending: IB's `Ending Cash` on the latest statement (zero when
            it carries no row).
        engine_balance: sum of acquisitions less disposals of the currency
            projected for this account, dated on or before the latest
            `period_end`.
        futures_adjustment: The unrealised P&L of the engine's open
            futures lots in this currency at the latest statement's
            close prices — what IB has settled and the engine has not.
        unpriced_open_lots: Open lots whose contract the latest
            statement does not list, left out of the adjustment.
    """

    account_id: str
    currency: str
    earliest: StatementRow
    latest: StatementRow
    ib_starting: Decimal
    ib_ending: Decimal
    engine_balance: Decimal
    futures_adjustment: Decimal
    unpriced_open_lots: int

    @property
    def ib_delta(self) -> Decimal:
        """The movement IB reports between the two statements."""
        return self.ib_ending - self.ib_starting

    @property
    def engine_total(self) -> Decimal:
        """The engine's balance once open futures are marked at the statement's close."""
        return self.engine_balance + self.futures_adjustment

    @property
    def difference(self) -> Decimal:
        """IB's movement less the engine's total — positive means IB holds more."""
        return self.ib_delta - self.engine_total

    @property
    def status(self) -> CashBalanceStatus:
        """`MATCH` within the tolerance, else `MISMATCH`."""
        if abs(self.difference) <= CASH_TOLERANCE:
            return CashBalanceStatus.MATCH
        return CashBalanceStatus.MISMATCH

    def describe(self) -> str:
        """One line with both sides and the difference, for evidence rows and issue messages."""
        text = (
            f"{self.account_id} {self.currency} at {self.latest.period_end.isoformat()}: "
            f"IB {self.ib_starting:,.2f} -> {self.ib_ending:,.2f} (moved {self.ib_delta:,.2f}); "
            f"engine {self.engine_balance:,.2f} + open futures {self.futures_adjustment:,.2f} "
            f"= {self.engine_total:,.2f}; difference {self.difference:,.2f}"
        )
        if self.unpriced_open_lots:
            plural = "" if self.unpriced_open_lots == 1 else "s"
            text += f" ({self.unpriced_open_lots} open futures lot{plural} not on the statement)"
        return text


def reconcile_cash_balances(
    conn: sqlite3.Connection,
    fx: FXConverter,
    *,
    fx_inputs: FXInputs,
) -> tuple[CashBalanceReconciliation, ...]:
    """Compare every account's currency balances with its statements' Cash Report.

    Accounts with no statement, or whose earliest and latest
    statements both lack a Cash Report (a vintage without one, or
    rows ingested before the report was read), are skipped — there is
    nothing to reconcile against. For each of the rest and each
    non-GBP currency either side mentions, the account's projected
    events up to the latest `period_end` are summed, the engine's open
    futures lots at that date are marked at the statement's close
    prices, and both are set against IB's movement between the two
    statements. Currencies that are zero on every side are left out.

    Args:
        conn: Open, migrated connection.
        fx: FX converter shared by the engines.
        fx_inputs: The whole-history FX input bundle — the same one
            the FX pools are matched from, so the two views of a
            balance can never drift apart.

    Returns:
        One row per account and currency, ordered by account then
        currency — stable run-to-run for a given database.
    """
    statements = StatementRepo(conn)
    balances = StatementCashBalanceRepo(conn)
    positions = StatementPositionRepo(conn)

    out: list[CashBalanceReconciliation] = []
    for latest in statements.latest_per_account():
        account_id = latest.account_id
        earliest = statements.earliest_for_account(account_id) or latest
        ending = {b.currency: b.ending_cash for b in balances.for_statement(latest.statement_hash)}
        starting = {
            b.currency: b.starting_cash for b in balances.for_statement(earliest.statement_hash)
        }
        if not ending and not starting:
            continue
        close_prices = {
            instrument_id: position.close_price
            for instrument_id, position in positions.for_statement(latest.statement_hash)
        }
        # The engine's open lots in this account as of the statement's
        # last day — a date-clipped futures pass, per account because
        # IB settles margin per account.
        future_runs = run_future_engine(conn, fx, account_id=account_id, until=latest.period_end)
        currencies = sorted((set(ending) | set(starting) | set(fx_inputs.currencies)) - {"GBP"})
        for currency in currencies:
            engine_balance = _engine_balance(fx, fx_inputs, currency, account_id, latest)
            adjustment, unpriced = _open_futures_adjustment(future_runs, currency, close_prices)
            ib_starting = starting.get(currency, Decimal(0))
            ib_ending = ending.get(currency, Decimal(0))
            if (
                ib_starting == 0
                and ib_ending == 0
                and engine_balance == 0
                and adjustment == 0
                and unpriced == 0
            ):
                continue
            out.append(
                CashBalanceReconciliation(
                    account_id=account_id,
                    currency=currency,
                    earliest=earliest,
                    latest=latest,
                    ib_starting=ib_starting,
                    ib_ending=ib_ending,
                    engine_balance=engine_balance,
                    futures_adjustment=adjustment,
                    unpriced_open_lots=unpriced,
                )
            )
    return tuple(out)


def _engine_balance(
    fx: FXConverter,
    fx_inputs: FXInputs,
    currency: str,
    account_id: str,
    latest: StatementRow,
) -> Decimal:
    """Acquisitions less disposals of `currency` in `account_id` up to the statement's end."""
    acquisitions, disposals = project_pool(fx, fx_inputs, currency)
    acquired = sum(
        (
            a.quantity
            for a in acquisitions
            if a.account_id == account_id and a.acquisition_date <= latest.period_end
        ),
        Decimal(0),
    )
    disposed = sum(
        (
            d.quantity
            for d in disposals
            if d.account_id == account_id and d.disposal_date <= latest.period_end
        ),
        Decimal(0),
    )
    return acquired - disposed


def _open_futures_adjustment(
    future_runs: Sequence[FutureEngineRun],
    currency: str,
    close_prices: Mapping[int, Decimal],
) -> tuple[Decimal, int]:
    """Mark the engine's open lots in `currency` at the statement's close prices.

    Returns the total `(close - open) x multiplier x signed quantity`
    and the number of lots left out because the statement lists no
    close price for their contract.
    """
    adjustment = Decimal(0)
    unpriced = 0
    for run in future_runs:
        if run.result is None or run.instrument.currency != currency:
            continue
        close = close_prices.get(run.instrument_id)
        for lot in run.result.open_positions:
            if close is None:
                unpriced += 1
                continue
            # LONG lots gain when the price rises, SHORT lots when it falls:
            # the sign of the quantity carries that.
            signed = lot.quantity_remaining if lot.side == "LONG" else -lot.quantity_remaining
            adjustment += (
                (close - lot.open_price.amount) * run.instrument.contract_multiplier * signed
            )
    return adjustment, unpriced


__all__ = [
    "CASH_TOLERANCE",
    "CashBalanceReconciliation",
    "CashBalanceStatus",
    "reconcile_cash_balances",
]
