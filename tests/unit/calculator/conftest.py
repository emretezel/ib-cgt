"""Shared fixtures and seed helpers for `ib_cgt.calculator` tests.

One seeded database exercises every engine and every FX cashflow
source at once, so the runner tests can assert on cross-engine
behaviour (futures before FX, coupons reaching the pool, GBP futures
excluded from FX inputs) without each test building its own world.

Scenario (all trades in April 2025 unless stated):

* Stocks — `ISF` (GBP) same-day round-trip; `AAPL` (USD) two
  cross-account buys drained by one sell; `TSLA` (USD) a sell-short
  with no cover, i.e. a soft-residual unmatched disposal.
* Bonds — an exempt gilt bought and sold; a non-exempt USD corporate
  bond bought and held.
* Futures — `ES` (USD) long round-trip closed 8 April; `ZG` (GBP)
  long round-trip; `CL` (USD) five opened, three closed.
* Forex — one `USD.GBP` buy in March, seeding the USD pool.
* Dividends — an `AAPL` USD cash dividend and its withholding tax; an
  `ASML` EUR dividend on a stock that is never traded (pool discovery:
  dividends are instrument-less, so the EUR pool exists purely because
  a dividend row is in EUR).
* Coupons — one USD coupon on the corporate bond.
* FX cache — GBP/USD 1.25 and GBP/EUR 1.16 from March to June 2025.

The helpers are plain functions (not fixtures) so a test can seed a
variant of the scenario into its own connection.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import (
    AccountRepo,
    BondCouponRepo,
    DividendRepo,
    FXRateRepo,
    StatementRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.db.repos.fx_rates import FXRate
from ib_cgt.domain import (
    Account,
    AnyInstrument,
    BondCoupon,
    BondInstrument,
    CurrencyPair,
    Dividend,
    DividendKind,
    FutureInstrument,
    FXInstrument,
    Money,
    StockInstrument,
    Trade,
    TradeAction,
)
from ib_cgt.fx import FrankfurterClient, FXService

STATEMENT_HASH = "hash-calc"

# ---------------------------------------------------------------------------
# Instruments
# ---------------------------------------------------------------------------

ISF = StockInstrument(conid=68499944, symbol="ISF", currency="GBP")
AAPL = StockInstrument(conid=66468935, symbol="AAPL", currency="USD")
TSLA = StockInstrument(conid=171756085, symbol="TSLA", currency="USD")
ASML = StockInstrument(conid=149536668, symbol="ASML", currency="EUR")
GILT = BondInstrument(
    symbol="UKT 0 1/8 01/30/26", currency="GBP", isin="GB00BL68HJ26", is_cgt_exempt=True
)
CORP_USD = BondInstrument(
    symbol="ACME 5 2030", currency="USD", isin="US000000AA11", is_cgt_exempt=False
)
ES = FutureInstrument(
    conid=14826456,
    symbol="ES",
    currency="USD",
    contract_multiplier=Decimal("50"),
    expiry_date=date(2025, 12, 19),
)
ZG = FutureInstrument(
    conid=73948901,
    symbol="ZG",
    currency="GBP",
    contract_multiplier=Decimal("10"),
    expiry_date=date(2025, 12, 19),
)
CL = FutureInstrument(
    conid=100697936,
    symbol="CL",
    currency="USD",
    contract_multiplier=Decimal("1000"),
    expiry_date=date(2025, 6, 20),
)
USD_GBP = FXInstrument(
    symbol="USD.GBP", currency="USD", currency_pair=CurrencyPair(base="USD", quote="GBP")
)


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def trade(
    instrument: AnyInstrument,
    action: TradeAction,
    on: date,
    qty: str,
    price: str,
    *,
    fees: str = "0",
    account_id: str = "U1",
    seq: int = 0,
) -> Trade:
    """A trade at 12:00 UTC on `on` (+`seq` minutes for intra-day order).

    Forex fees are GBP by IB convention (the domain enforces it);
    every other class pays fees in the instrument's currency.
    """
    fees_currency = "GBP" if isinstance(instrument, FXInstrument) else instrument.currency
    return Trade(
        account_id=account_id,
        instrument=instrument,
        action=action,
        trade_datetime=datetime(on.year, on.month, on.day, 12, 0, tzinfo=UTC)
        + timedelta(minutes=seq),
        trade_date=on,
        settlement_date=on,
        quantity=Decimal(qty),
        price=Money.of(price, instrument.currency),
        fees=Money.of(fees, fees_currency),
    )


def dividend(
    instrument: StockInstrument,
    kind: DividendKind,
    on: date,
    amount: str,
    *,
    account_id: str = "U1",
) -> Dividend:
    """A dividend-shaped cashflow, paid in the stock's trade currency.

    Dividends are instrument-less (migration 021); the stock is only
    used here to pick the symbol label and the payment currency.
    """
    return Dividend(
        account_id=account_id,
        symbol=instrument.symbol,
        kind=kind,
        pay_date=on,
        amount=Money.of(amount, instrument.currency),
        description=f"{instrument.symbol} {kind.value}",
    )


def coupon(
    instrument: BondInstrument, on: date, amount: str, *, account_id: str = "U1"
) -> BondCoupon:
    """A coupon payment in the bond's currency."""
    return BondCoupon(
        account_id=account_id,
        instrument=instrument,
        pay_date=on,
        amount=Money.of(amount, instrument.currency),
        description=f"Bond Coupon Payment ({instrument.symbol} - {instrument.symbol})",
    )


def seed_fx_rates(conn: sqlite3.Connection) -> None:
    """GBP/USD 1.25 and GBP/EUR 1.16 for every day from March to June 2025."""
    rates: list[FXRate] = []
    cur = date(2025, 3, 1)
    while cur <= date(2025, 6, 30):
        rates.append(FXRate(base="GBP", quote="USD", rate_date=cur, rate=Decimal("1.25")))
        rates.append(FXRate(base="GBP", quote="EUR", rate_date=cur, rate=Decimal("1.16")))
        cur += timedelta(days=1)
    FXRateRepo(conn).upsert_many(rates)


def seed_accounts_and_statement(conn: sqlite3.Connection) -> None:
    """Two accounts (pools span both) and the one statement every row cites."""
    AccountRepo(conn).upsert(Account(account_id="U1"))
    AccountRepo(conn).upsert(Account(account_id="U2"))
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=STATEMENT_HASH,
        source_path="/tmp/calc.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )


def baseline_trades() -> list[Trade]:
    """The trade mix described in the module docstring."""

    def apr(d: int) -> date:
        """Terse date helper: the d-th of April 2025."""
        return date(2025, 4, d)

    return [
        # ISF: same-day round-trip.
        trade(ISF, TradeAction.BUY, apr(5), "10", "100", fees="2", account_id="U2"),
        trade(ISF, TradeAction.SELL, apr(5), "10", "120", fees="3", account_id="U2", seq=60),
        # AAPL: cross-account pool drained by one sell.
        trade(AAPL, TradeAction.BUY, apr(1), "10", "100"),
        trade(AAPL, TradeAction.BUY, apr(2), "10", "200", account_id="U2"),
        trade(AAPL, TradeAction.SELL, apr(20), "20", "250", account_id="U2"),
        # TSLA: sell-short with no cover → soft residual.
        trade(TSLA, TradeAction.SELL, apr(10), "5", "300"),
        # Gilt: exempt round-trip.
        trade(GILT, TradeAction.BUY, apr(3), "10000", "0.98"),
        trade(GILT, TradeAction.SELL, apr(10), "10000", "0.99"),
        # USD corporate bond: bought and held.
        trade(CORP_USD, TradeAction.BUY, apr(4), "100", "0.95"),
        # ES: long round-trip, +50 points x 50 x 2 = +5,000 USD.
        trade(ES, TradeAction.OPEN_LONG, apr(1), "2", "5000", fees="2.50"),
        trade(ES, TradeAction.CLOSE_LONG, apr(8), "2", "5050", fees="2.50"),
        # ZG: GBP future round-trip (never touches an FX pool).
        trade(ZG, TradeAction.OPEN_LONG, apr(3), "1", "100", fees="1"),
        trade(ZG, TradeAction.CLOSE_LONG, apr(5), "1", "110", fees="1"),
        # CL: five opened, three closed, two still open.
        trade(CL, TradeAction.OPEN_LONG, apr(2), "5", "80", fees="2.50"),
        trade(CL, TradeAction.CLOSE_LONG, apr(16), "3", "82", fees="2.50"),
        # Forex: buy 1,000 USD with GBP in March.
        trade(USD_GBP, TradeAction.BUY, date(2025, 3, 20), "1000", "0.80", fees="1"),
    ]


def seed_baseline(conn: sqlite3.Connection) -> None:
    """Seed the full scenario: accounts, trades, dividends, coupons, FX cache."""
    seed_accounts_and_statement(conn)
    TradeRepo(conn).insert_many(baseline_trades(), source_statement_hash=STATEMENT_HASH)
    DividendRepo(conn).insert_many(
        [
            dividend(AAPL, DividendKind.CASH_DIVIDEND, date(2025, 4, 15), "50"),
            dividend(AAPL, DividendKind.WITHHOLDING_TAX, date(2025, 4, 15), "7.50"),
            dividend(ASML, DividendKind.CASH_DIVIDEND, date(2025, 4, 22), "12"),
        ],
        source_statement_hash=STATEMENT_HASH,
    )
    BondCouponRepo(conn).insert_many(
        [coupon(CORP_USD, date(2025, 4, 18), "30")],
        source_statement_hash=STATEMENT_HASH,
    )
    seed_fx_rates(conn)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Migrated on-disk DB seeded with the baseline scenario."""
    conn = open_connection(tmp_path / "ibcgt.sqlite")
    try:
        apply_migrations(conn)
        seed_baseline(conn)
        yield conn
    finally:
        conn.close()


@pytest.fixture
def fx_service(db: sqlite3.Connection) -> FXService:
    """A real `FXService` over the warmed cache; the client can never fire."""
    return FXService(FXRateRepo(db), FrankfurterClient(base_url="https://example.invalid"))
