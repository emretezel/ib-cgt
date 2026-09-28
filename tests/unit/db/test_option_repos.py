"""Unit tests for the option tables of migration 023 and their repositories.

`option_instruments` through `InstrumentRepo`; `option_exercise_links`
through `OptionExerciseLinkRepo`; `option_grants` / `option_grant_closes`
through `OptionGrantRepo`; `option_exercise_transfers` through
`OptionExerciseTransferRepo`; and the migration's wipe.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from importlib.resources import files
from typing import Literal
from zoneinfo import ZoneInfo

import pytest

from ib_cgt.db import (
    AccountRepo,
    InstrumentRepo,
    OptionExerciseLinkRepo,
    OptionExerciseTransferRepo,
    OptionGrantRepo,
    StatementRepo,
    TaxRunRepo,
    TradeRepo,
    open_memory_connection,
)
from ib_cgt.db.migrator import _applied_versions, _apply_one, _ensure_bookkeeping_table
from ib_cgt.domain import (
    Account,
    AssetClass,
    Money,
    OptionCloseKind,
    OptionExerciseTransfer,
    OptionGrant,
    OptionGrantClose,
    OptionInstrument,
    OptionRight,
    TaxYear,
    TradeAction,
)
from tests.options_fixtures import TUR, TUR_PUT, XAU_CALL, XSP_PUT, at, option_trade, share_trade

# ---------------------------------------------------------------------------
# option_instruments
# ---------------------------------------------------------------------------


def test_option_round_trips_and_is_keyed_by_conid(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    iid = repo.upsert(XSP_PUT)
    assert repo.get(iid) == XSP_PUT
    assert repo.upsert(XSP_PUT) == iid
    assert db.execute("SELECT asset_class FROM instruments").fetchone()["asset_class"] == "option"
    assert db.execute("SELECT COUNT(*) FROM option_instruments").fetchone()[0] == 1


def test_renamed_root_collapses_to_one_row_and_latest_symbol_wins(db: sqlite3.Connection) -> None:
    """XSP became XSPAM under conid 99465795: one series, the newer display symbol kept."""
    repo = InstrumentRepo(db)
    old = OptionInstrument(
        conid=XSP_PUT.conid,
        symbol="XSP 20DEC14 140.0 P",
        currency="USD",
        underlying="XSP",
        contract_multiplier=Decimal("100"),
        expiry_date=date(2014, 12, 20),
        strike=Decimal("140"),
        right=OptionRight.PUT,
    )
    iid = repo.upsert(old)
    assert repo.upsert(XSP_PUT) == iid
    loaded = repo.get(iid)
    assert isinstance(loaded, OptionInstrument)
    assert loaded.symbol == "XSPAM 20DEC14 140.0 P"
    # Series facts are insert-only: the first sighting's underlying stays.
    assert loaded.underlying == "XSP"


def test_list_options_orders_by_symbol_then_expiry_and_filters(db: sqlite3.Connection) -> None:
    repo = InstrumentRepo(db)
    xsp = repo.upsert(XSP_PUT)
    tur = repo.upsert(TUR_PUT)
    xau = repo.upsert(XAU_CALL)
    assert [iid for iid, _ in repo.list_options()] == [tur, xau, xsp]
    assert repo.list_options(symbol="TUR 17MAY19 22.0 P") == [(tur, TUR_PUT)]
    assert repo.find_by_symbol(AssetClass.OPTION, "TUR 17MAY19 22.0 P", "USD") == [(tur, TUR_PUT)]


def test_option_right_is_check_constrained(db: sqlite3.Connection) -> None:
    iid = InstrumentRepo(db).upsert(TUR_PUT)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        db.execute(
            "UPDATE option_instruments SET option_right = 'straddle' WHERE instrument_id = ?",
            (iid,),
        )


def test_view_projects_the_option_arm(db: sqlite3.Connection) -> None:
    InstrumentRepo(db).upsert(TUR_PUT)
    row = db.execute(
        "SELECT asset_class, conid, symbol, currency, underlying, strike, option_right, "
        "contract_multiplier, expiry_date FROM v_instruments"
    ).fetchone()
    assert tuple(row) == (
        "option",
        334765297,
        "TUR 17MAY19 22.0 P",
        "USD",
        "TUR",
        "22",
        "put",
        "100",
        "2019-05-17",
    )


# ---------------------------------------------------------------------------
# option_exercise_links
# ---------------------------------------------------------------------------


def _seed_statement(conn: sqlite3.Connection, statement_hash: str = "hash-opt") -> None:
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash=statement_hash,
        source_path=f"/tmp/{statement_hash}.htm",
        account_id="U1",
        trade_count=0,
        period_start=date(2019, 4, 8),
        period_end=date(2020, 4, 3),
    )


def _seed_exercise_pair(conn: sqlite3.Connection) -> tuple[int, int]:
    """The TUR exercise and its share sale; returns `(option_trade_id, share_trade_id)`."""
    _seed_statement(conn)
    when = at(date(2019, 5, 16), 20, 20)
    TradeRepo(conn).insert_many(
        [
            share_trade(
                TUR, TradeAction.SELL, date(2019, 5, 16), "1500", "22", fees="0.86", when=when
            ),
            option_trade(
                TUR_PUT, TradeAction.EXERCISE_LONG, date(2019, 5, 16), "15", "0", when=when
            ),
        ],
        source_statement_hash="hash-opt",
    )
    ids = TradeRepo(conn).ids_for_rows("hash-opt", [0, 1])
    return ids[1], ids[0]


def test_link_repo_round_trip_and_lookups(db: sqlite3.Connection) -> None:
    option_id, share_id = _seed_exercise_pair(db)
    repo = OptionExerciseLinkRepo(db)
    assert repo.insert_many([(option_id, share_id)]) == 1
    assert repo.insert_many([(option_id, share_id)]) == 0  # retry-safe
    assert repo.for_option_trades([option_id, 999]) == {option_id: share_id}
    assert repo.for_option_trades([]) == {}
    assert repo.all_links() == {option_id: share_id}


def test_link_repo_enforces_the_trade_fks_and_one_share_per_option(db: sqlite3.Connection) -> None:
    option_id, share_id = _seed_exercise_pair(db)
    repo = OptionExerciseLinkRepo(db)
    with pytest.raises(sqlite3.IntegrityError):
        repo.insert_many([(option_id, 12345)])
    repo.insert_many([(option_id, share_id)])
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO option_exercise_links (option_trade_id, share_trade_id) VALUES (?, ?)",
            (share_id, share_id),
        )


def test_links_cascade_from_trades(db: sqlite3.Connection) -> None:
    option_id, share_id = _seed_exercise_pair(db)
    OptionExerciseLinkRepo(db).insert_many([(option_id, share_id)])
    db.execute("DELETE FROM trades WHERE trade_id = ?", (share_id,))
    assert OptionExerciseLinkRepo(db).count() == 0


def test_ids_for_rows_maps_statement_positions_to_ids(db: sqlite3.Connection) -> None:
    option_id, share_id = _seed_exercise_pair(db)
    ids = TradeRepo(db).ids_for_rows("hash-opt", [0, 1, 7])
    assert ids == {0: share_id, 1: option_id}
    assert TradeRepo(db).ids_for_rows("hash-opt", []) == {}


# ---------------------------------------------------------------------------
# option_grants / option_grant_closes
# ---------------------------------------------------------------------------


def _grant(
    *, grant_id: int = 10, quantity: str = "1", closes: tuple[OptionGrantClose, ...] = ()
) -> OptionGrant:
    return OptionGrant(
        grant_trade_id=grant_id,
        instrument=XAU_CALL,
        grant_date=date(2012, 10, 10),
        quantity=Decimal(quantity),
        premium_native=Money.of("770", "USD"),
        grant_fee_native=Money.of("2.45", "USD"),
        grant_fx_rate=Decimal("1.6"),
        proceeds_gbp=Money.gbp("481.25"),
        grant_fee_gbp=Money.gbp("1.53125"),
        closes=closes,
    )


def _close(close_id: int, kind: OptionCloseKind, on: date, cost: str = "0") -> OptionGrantClose:
    return OptionGrantClose(
        close_trade_id=close_id,
        kind=kind,
        close_date=on,
        quantity=Decimal("1"),
        premium_native=Money.of("140" if kind is OptionCloseKind.PURCHASE else "0", "USD"),
        fee_native=Money.of("2.45" if kind is OptionCloseKind.PURCHASE else "0", "USD"),
        fx_rate=Decimal("1.6"),
        cost_gbp=Money.gbp(cost),
    )


def test_grants_round_trip_with_their_closes_in_drain_order(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2012), Money.gbp("0"))
    repo = OptionGrantRepo(db)
    first = _grant(
        grant_id=10,
        quantity="2",
        closes=(
            _close(12, OptionCloseKind.PURCHASE, date(2012, 11, 1), "89.03125"),
            _close(13, OptionCloseKind.LAPSE, date(2012, 12, 21)),
        ),
    )
    later = _grant(grant_id=11)
    assert repo.insert_many(run_id, [later, first]) == 2
    loaded = repo.for_run(run_id)
    # Same grant date: ordered by grant trade id; closes keep their seq.
    assert loaded == [first, later]
    assert loaded[0].closes[0].kind is OptionCloseKind.PURCHASE
    assert loaded[0].premium_native.currency == "USD"
    seqs = db.execute(
        "SELECT close_trade_id, seq FROM option_grant_closes WHERE grant_trade_id = 10 ORDER BY seq"
    ).fetchall()
    assert [(int(r["close_trade_id"]), int(r["seq"])) for r in seqs] == [(12, 0), (13, 1)]
    columns = {r["name"] for r in db.execute("PRAGMA table_info(option_grants)")}
    assert "currency" not in columns


def test_grant_rows_cascade_with_the_run_and_closes_with_the_grant(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    run_id = runs.create(TaxYear(2012), Money.gbp("0"))
    OptionGrantRepo(db).insert_many(
        [run_id][0], [_grant(closes=(_close(12, OptionCloseKind.LAPSE, date(2012, 12, 21)),))]
    )
    assert db.execute("SELECT COUNT(*) FROM option_grant_closes").fetchone()[0] == 1
    runs.replace_for(TaxYear(2012), Money.gbp("1"))
    assert OptionGrantRepo(db).count() == 0
    assert db.execute("SELECT COUNT(*) FROM option_grant_closes").fetchone()[0] == 0


def test_grant_close_kind_is_check_constrained(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2012), Money.gbp("0"))
    OptionGrantRepo(db).insert_many(run_id, [_grant()])
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        db.execute(
            "INSERT INTO option_grant_closes (run_id, grant_trade_id, close_trade_id, kind, "
            "close_date, quantity, premium_native, fee_native, fx_rate, cost_gbp, seq) "
            "VALUES (?, 10, 12, 'novation', '2012-11-01', '1', '0', '0', '1.6', '0', 0)",
            (run_id,),
        )


def test_grant_repo_empty_batch_and_unknown_run(db: sqlite3.Connection) -> None:
    assert OptionGrantRepo(db).insert_many(1, []) == 0
    assert OptionGrantRepo(db).for_run(999) == []
    with pytest.raises(sqlite3.IntegrityError):
        OptionGrantRepo(db).insert_many(999, [_grant()])


# ---------------------------------------------------------------------------
# option_exercise_transfers
# ---------------------------------------------------------------------------


def _transfer(
    option_id: int,
    seq_amount: str,
    *,
    side: Literal["LONG", "SHORT"] = "LONG",
    grant: int | None = None,
) -> OptionExerciseTransfer:
    return OptionExerciseTransfer(
        option_trade_id=option_id,
        share_trade_id=99,
        instrument=TUR_PUT,
        side=side,
        grant_trade_id=grant,
        on=date(2019, 5, 16),
        quantity=Decimal("15"),
        amount_gbp=Money.gbp(seq_amount),
        fees_gbp=Money.gbp("0.408"),
    )


def test_transfers_round_trip_in_date_then_option_then_seq_order(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2019), Money.gbp("0"))
    repo = OptionExerciseTransferRepo(db)
    two_grants = [
        _transfer(20, "100", side="SHORT", grant=1),
        _transfer(20, "50", side="SHORT", grant=2),
    ]
    holder = _transfer(21, "2220.408")
    assert repo.insert_many(run_id, [holder, *two_grants]) == 3
    loaded = repo.for_run(run_id)
    assert loaded == [*two_grants, holder]
    seqs = db.execute(
        "SELECT grant_trade_id, seq FROM option_exercise_transfers WHERE option_trade_id = 20 "
        "ORDER BY seq"
    ).fetchall()
    assert [(int(r["grant_trade_id"]), int(r["seq"])) for r in seqs] == [(1, 0), (2, 1)]


def test_transfer_side_and_grant_agree_in_sql_too(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2019), Money.gbp("0"))
    iid = InstrumentRepo(db).upsert(TUR_PUT)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        db.execute(
            "INSERT INTO option_exercise_transfers (run_id, option_trade_id, share_trade_id, "
            "instrument_id, side, grant_trade_id, on_date, quantity, amount_gbp, fees_gbp, seq) "
            "VALUES (?, 1, 2, ?, 'LONG', 7, '2019-05-16', '1', '0', '0', 0)",
            (run_id, iid),
        )


def test_transfers_cascade_with_the_run(db: sqlite3.Connection) -> None:
    runs = TaxRunRepo(db)
    run_id = runs.create(TaxYear(2019), Money.gbp("0"))
    OptionExerciseTransferRepo(db).insert_many(run_id, [_transfer(21, "1")])
    runs.replace_for(TaxYear(2019), Money.gbp("0"))
    assert OptionExerciseTransferRepo(db).count() == 0


# ---------------------------------------------------------------------------
# Migration 023 — the wipe and the widened CHECKs
# ---------------------------------------------------------------------------


def _apply_through(conn: sqlite3.Connection, last_version: int) -> None:
    """Apply every migration up to `last_version` that the DB has not seen yet."""
    _ensure_bookkeeping_table(conn)
    package = files("ib_cgt.db.migrations")
    for version in range(1, last_version + 1):
        if version in _applied_versions(conn):
            continue
        candidates = [e for e in package.iterdir() if e.name.startswith(f"{version:03d}_")]
        assert len(candidates) == 1, f"expected one migration file for {version=}"
        _apply_one(conn, version, candidates[0].read_text(encoding="utf-8"))


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_023_wipes_statement_rows_instruments_and_runs_but_keeps_accounts_and_rates() -> None:
    conn = open_memory_connection()
    _apply_through(conn, 22)
    AccountRepo(conn).upsert(Account(account_id="U1"))
    StatementRepo(conn).record(
        time_zone=ZoneInfo("America/New_York"),
        statement_hash="h",
        source_path="/tmp/h",
        account_id="U1",
        trade_count=0,
        period_start=date(2024, 4, 6),
        period_end=date(2025, 4, 5),
    )
    cur = conn.execute("INSERT INTO instruments (asset_class) VALUES ('stock')")
    conn.execute(
        "INSERT INTO stock_instruments (instrument_id, conid, symbol, currency) "
        "VALUES (?, 1, 'A', 'USD')",
        (cur.lastrowid,),
    )
    TaxRunRepo(conn).create(TaxYear(2024), Money.gbp("0"))
    conn.execute(
        "INSERT INTO fx_rates (base, quote, rate_date, rate, fetched_at) "
        "VALUES ('GBP', 'USD', '2024-05-01', '1.25', '2024-05-01T00:00:00+00:00')"
    )
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        conn.execute("INSERT INTO instruments (asset_class) VALUES ('option')")

    _apply_through(conn, 23)

    for table in ("statements", "instruments", "stock_instruments", "tax_runs", "tax_run_issues"):
        assert _count(conn, table) == 0, table
    assert _count(conn, "accounts") == 1
    assert _count(conn, "fx_rates") == 1
    # The discriminator now admits options and the new tables exist, empty.
    conn.execute("INSERT INTO instruments (asset_class) VALUES ('option')")
    for table in (
        "option_instruments",
        "option_exercise_links",
        "option_grants",
        "option_grant_closes",
        "option_exercise_transfers",
    ):
        assert _count(conn, table) == 0, table


def test_023_widens_the_issue_kinds(db: sqlite3.Connection) -> None:
    run_id = TaxRunRepo(db).create(TaxYear(2019), Money.gbp("0"))
    iid = InstrumentRepo(db).upsert(TUR_PUT)
    db.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 0, 'option_grant_restated', ?, 'x')",
        (run_id, iid),
    )
    db.execute(
        "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
        "VALUES (?, 1, 'option_exercise_unlinked', ?, 'x')",
        (run_id, iid),
    )
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):
        db.execute(
            "INSERT INTO tax_run_issues (run_id, seq, kind, instrument_id, message) "
            "VALUES (?, 2, 'option_grant_restated', NULL, 'x')",
            (run_id,),
        )
