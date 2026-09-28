"""End-to-end ingest of a statement with an exercised option.

`with_equity_options.htm` mirrors the real 2019 statement: the TUR put
exercised with `C;Ex` and the 1,500-share sale IB booked for it at the
strike (`Ex;O`), bracketed by a plain stock row and a forex row.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ib_cgt.db import (
    InstrumentRepo,
    OptionExerciseLinkRepo,
    TradeRepo,
    apply_migrations,
    open_connection,
)
from ib_cgt.domain import AssetClass, OptionInstrument, OptionRight, TradeAction
from ib_cgt.ingest.ingestor import ingest_statement

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    conn = open_connection(tmp_path / "ibcgt.sqlite")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_option_trade_instrument_and_exercise_link_land(db: sqlite3.Connection) -> None:
    result = ingest_statement(_FIXTURES / "with_equity_options.htm", db)

    assert result.trade_count == 3
    assert result.inserted_count == 3
    assert result.option_link_count == 1
    assert result.unlinked_exercise_count == 0

    instruments = InstrumentRepo(db)
    ((option_id, option),) = instruments.list_options()
    assert option == OptionInstrument(
        conid=654321,
        symbol="TUR 17MAY19 PUT 22.0",
        currency="USD",
        underlying="TUR",
        contract_multiplier=Decimal("100"),
        expiry_date=date(2019, 5, 17),
        strike=Decimal("22.0"),
        right=OptionRight.PUT,
    )
    ((stock_id, _stock),) = instruments.find_by_symbol(AssetClass.STOCK, "TUR", "USD")

    trades = TradeRepo(db)
    option_trades = trades.for_instrument_with_ids(option_id)
    share_trades = trades.for_instrument_with_ids(stock_id)
    assert [t.action for _id, t in option_trades] == [TradeAction.EXERCISE_LONG]
    assert [t.action for _id, t in share_trades] == [TradeAction.SELL]
    (exercise_id, _), (share_id, _) = option_trades[0], share_trades[0]
    assert OptionExerciseLinkRepo(db).all_links() == {exercise_id: share_id}


def test_links_die_with_their_statement(db: sqlite3.Connection) -> None:
    result = ingest_statement(_FIXTURES / "with_equity_options.htm", db)
    assert OptionExerciseLinkRepo(db).count() == 1
    db.execute("DELETE FROM statements WHERE statement_hash = ?", (result.statement_hash,))
    assert OptionExerciseLinkRepo(db).count() == 0


def test_reingest_with_replace_recreates_the_link(db: sqlite3.Connection) -> None:
    ingest_statement(_FIXTURES / "with_equity_options.htm", db)
    second = ingest_statement(_FIXTURES / "with_equity_options.htm", db, replace=True)
    assert second.option_link_count == 1
    assert OptionExerciseLinkRepo(db).count() == 1
