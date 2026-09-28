"""Tests for `ib_cgt.ingest.ingestor`.

Uses an on-disk tempfile DB (via `conftest.db`) rather than `:memory:`
to exercise the same code path a real `ib-cgt ingest` invocation would.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from ib_cgt.db import (
    CashEventRepo,
    InstrumentRepo,
    StatementPositionRepo,
    StatementRepo,
    TradeRepo,
)
from ib_cgt.domain import FutureInstrument, Money
from ib_cgt.ingest.ingestor import ingest_statement, ingest_statements
from tests.conid import fake_conid


@dataclass
class _FXStub:
    """Minimal FX stub for the merger ingest test.

    The synthesizer only ever calls `convert`. Returning a fixed rate
    is enough to exercise the end-to-end persistence path without
    pulling Frankfurter or seeding the rate cache.
    """

    rate: Decimal
    calls: list[tuple[Money, str, date]] = field(default_factory=list)

    def convert(self, amount: Money, *, target: str, on: date) -> Money:
        self.calls.append((amount, target, on))
        if amount.currency == target:
            return amount
        return Money.of(amount.amount * self.rate, target)


_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "statements"


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """Copy of the DB fixture from `tests/unit/db/conftest.py`.

    We redeclare it locally so the ingest test package doesn't reach
    across into the db tests' conftest — cleaner scoping.
    """
    from ib_cgt.db import apply_migrations, open_connection

    conn = open_connection(tmp_path / "ibcgt.sqlite")
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_ingest_mixed_statement_persists_trades(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    result = ingest_statement(fixture, db)

    # Fixture: 2 stocks + 1 forex + 2 futures = 5 trades.
    assert result.trade_count == 5
    assert result.inserted_count == 5
    assert result.already_imported is False
    assert result.account_id == "U9999999"
    assert TradeRepo(db).count() == 5


def test_reingest_same_statement_is_noop(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "mixed_tiny.htm"

    first = ingest_statement(fixture, db)
    assert first.inserted_count == 5

    second = ingest_statement(fixture, db)
    assert second.already_imported is True
    assert second.inserted_count == 0
    # Hash matches across calls — the short-circuit path.
    assert second.statement_hash == first.statement_hash
    # No duplicates in the DB.
    assert TradeRepo(db).count() == 5


def test_ingest_reordered_headers(db: sqlite3.Connection) -> None:
    """Column drift (reordered headers) must still ingest cleanly."""
    fixture = _FIXTURES / "reordered_headers.htm"
    result = ingest_statement(fixture, db)
    assert result.trade_count == 1
    assert result.inserted_count == 1
    assert result.account_id == "U8888888"


def test_ingest_two_distinct_statements_accumulate(db: sqlite3.Connection) -> None:
    a = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    b = ingest_statement(_FIXTURES / "reordered_headers.htm", db)
    assert a.inserted_count == 5
    assert b.inserted_count == 1
    assert TradeRepo(db).count() == 6


def test_replace_overwrites_existing_statement(db: sqlite3.Connection) -> None:
    """`replace=True` must delete the prior import and re-insert fresh.

    Asserts (a) the trade count after replacement matches the
    fixture (no duplicates from the prior import lingering), (b) the
    `statements.imported_at` timestamp moves forward (a fresh row was
    written), and (c) the result advertises `replaced=True` /
    `already_imported=False`.
    """
    fixture = _FIXTURES / "mixed_tiny.htm"

    first = ingest_statement(fixture, db)
    first_imported_at = db.execute(
        "SELECT imported_at FROM statements WHERE statement_hash = ?",
        (first.statement_hash,),
    ).fetchone()["imported_at"]

    second = ingest_statement(fixture, db, replace=True)

    assert second.replaced is True
    assert second.already_imported is False
    assert second.statement_hash == first.statement_hash
    # Same fixture → same trade_count; the cascade prevented any
    # leftover rows from the first import.
    assert second.trade_count == first.trade_count
    assert TradeRepo(db).count() == first.trade_count

    second_imported_at = db.execute(
        "SELECT imported_at FROM statements WHERE statement_hash = ?",
        (first.statement_hash,),
    ).fetchone()["imported_at"]
    assert second_imported_at >= first_imported_at, (
        "replace should rewrite the statements row with a fresh imported_at"
    )


def test_replace_on_unseen_statement_behaves_like_normal_ingest(
    db: sqlite3.Connection,
) -> None:
    """`replace=True` on a never-seen file is just a normal ingest.

    The flag's contract is "delete prior if any" — when there is no
    prior, it must not error and must not flip `replaced` to True.
    """
    result = ingest_statement(_FIXTURES / "mixed_tiny.htm", db, replace=True)
    assert result.replaced is False
    assert result.already_imported is False
    assert result.inserted_count == result.trade_count > 0


def test_ingest_persists_synthesized_merger_trade(db: sqlite3.Connection) -> None:
    """End-to-end: cash-merger fixture lands as a SELL trade in the DB.

    The fixture has 1 regular buy + 1 cross-currency cash merger.
    With FXService injected, the synthesizer produces a SELL with
    `fees=0` and a `statement_row_index` strictly after the buy's.
    Without FXService injected, only the buy lands.
    """
    fixture = _FIXTURES / "with_cash_merger.htm"
    fx = _FXStub(rate=Decimal("0.74"))

    result = ingest_statement(fixture, db, fx_service=fx)

    assert result.trade_count == 2
    assert result.merger_trade_count == 1
    assert result.inserted_count == 2

    rows = db.execute(
        "SELECT action, fees_amount, fees_currency, statement_row_index "
        "FROM trades ORDER BY statement_row_index"
    ).fetchall()
    assert len(rows) == 2
    buy, sell = rows
    assert buy["action"] == "buy"
    assert sell["action"] == "sell"
    # The synthesized SELL carries no fees.
    assert sell["fees_amount"] == "0"
    assert sell["fees_currency"] == "GBP"
    # And lives at a row_index strictly past the regular trade.
    assert sell["statement_row_index"] > buy["statement_row_index"]


def test_ingest_persists_bond_maturity_as_sell_trade(db: sqlite3.Connection) -> None:
    """End-to-end: bond-maturity fixture lands as a SELL trade at par.

    The fixture has 1 regular bond buy + 1 Bond Maturity Corporate
    Actions row. The maturity synthesizer turns the CA row into a
    SELL with `price=1.00 GBP`, `fees=0`, and a `statement_row_index`
    strictly after the buy's. No FX service is needed (par price is
    in the bond's own currency).
    """
    fixture = _FIXTURES / "with_bond_maturity.htm"
    result = ingest_statement(fixture, db)

    assert result.trade_count == 2
    assert result.maturity_trade_count == 1
    assert result.merger_trade_count == 0
    assert result.inserted_count == 2

    rows = db.execute(
        "SELECT action, price_amount, price_currency, fees_amount, "
        "       quantity, statement_row_index "
        "FROM trades ORDER BY statement_row_index"
    ).fetchall()
    assert len(rows) == 2
    buy, sell = rows

    # Regular bond buy: price rescaled from "98.500" → "0.985".
    assert buy["action"] == "buy"
    assert Decimal(buy["price_amount"]) == Decimal("0.985")
    assert buy["price_currency"] == "GBP"
    assert Decimal(buy["quantity"]) == Decimal("215000")

    # Synthesised maturity SELL: par price 1.00, no fees, strictly after the buy.
    assert sell["action"] == "sell"
    assert Decimal(sell["price_amount"]) == Decimal("1")
    assert sell["price_currency"] == "GBP"
    assert sell["fees_amount"] == "0"
    assert Decimal(sell["quantity"]) == Decimal("215000")
    assert sell["statement_row_index"] > buy["statement_row_index"]

    # The bond is correctly auto-classified as a UK gilt (CGT-exempt).
    bond_row = db.execute(
        "SELECT is_cgt_exempt FROM bond_instruments WHERE symbol = ?",
        ("UKT 0 1/4 01/31/25",),
    ).fetchone()
    assert bool(bond_row["is_cgt_exempt"]) is True


def test_ingest_collapses_yield_suffixed_bond_lots_to_one_instrument(
    db: sqlite3.Connection,
) -> None:
    """Two yield-suffixed BUYs of the same underlying gilt land under one ISIN row.

    The fixture has two trades — `UKT 0 1/4 01/31/25 5.26994388%` and
    `UKT 0 1/4 01/31/25 9.87150193%` — and one bond instrument-info
    row carrying ISIN `GB00BLPK7110`. The post-014 mapper canonicalises
    both trade symbols to `UKT 0 1/4 01/31/25` and resolves both to
    the same ISIN, so the two BUYs collapse to a single
    `bond_instruments` row. This is the user's real-world shape.
    """
    fixture = _FIXTURES / "with_yield_suffixed_bond.htm"
    result = ingest_statement(fixture, db)

    assert result.trade_count == 2
    assert result.inserted_count == 2

    # Single bond_instruments row, ISIN-keyed and with the canonical symbol.
    bond_rows = db.execute(
        "SELECT isin, symbol, currency, is_cgt_exempt FROM bond_instruments"
    ).fetchall()
    assert len(bond_rows) == 1
    [bond] = bond_rows
    assert bond["isin"] == "GB00BLPK7110"
    assert bond["symbol"] == "UKT 0 1/4 01/31/25"
    assert bond["currency"] == "GBP"
    assert bool(bond["is_cgt_exempt"]) is True

    # Both trades reference the same instrument_id.
    trade_rows = db.execute(
        "SELECT instrument_id FROM trades ORDER BY statement_row_index"
    ).fetchall()
    assert len({r["instrument_id"] for r in trade_rows}) == 1


def test_reingest_bond_maturity_is_idempotent(db: sqlite3.Connection) -> None:
    """A second ingest of the same statement does not duplicate the maturity SELL."""
    fixture = _FIXTURES / "with_bond_maturity.htm"

    first = ingest_statement(fixture, db)
    assert first.maturity_trade_count == 1
    assert first.inserted_count == 2

    second = ingest_statement(fixture, db)
    # Hash-level short-circuit returns `already_imported` and does not
    # re-run the parse/map pipeline.
    assert second.already_imported is True
    assert second.inserted_count == 0

    # No duplicates landed.
    assert TradeRepo(db).count() == 2


def test_ingest_persists_dividends(db: sqlite3.Connection) -> None:
    """End-to-end: dividends fixture lands rows in the `dividends` table.

    The fixture has 2 USD cash dividends, 1 EUR payment-in-lieu and
    1 USD withholding-tax row (under IB's real `tblWithholdingTax_`
    div id); the parser → mapper → repo path must produce exactly
    those four rows, distinguishable by `kind` and `currency`. The
    withholding row is parsed after the dividends section, so it has
    the highest id and sorts last among the 15 June rows.
    """
    fixture = _FIXTURES / "with_dividends.htm"
    result = ingest_statement(fixture, db)

    assert result.dividend_count == 4
    assert result.dividends_inserted == 4
    rows = db.execute(
        "SELECT kind, currency, amount_native FROM dividends ORDER BY pay_date, dividend_id"
    ).fetchall()
    kinds = [r["kind"] for r in rows]
    currencies = [r["currency"] for r in rows]
    assert kinds == ["cash_dividend", "withholding_tax", "payment_in_lieu", "cash_dividend"]
    assert currencies == ["USD", "USD", "EUR", "USD"]
    # Withholding rows store the absolute amount; direction lives in `kind`.
    assert rows[1]["amount_native"] == "4.50"


def test_reingest_dividends_idempotent(db: sqlite3.Connection) -> None:
    """A second ingest of the same dividend statement is a no-op."""
    fixture = _FIXTURES / "with_dividends.htm"
    first = ingest_statement(fixture, db)
    second = ingest_statement(fixture, db)
    # Hash short-circuit: second call returns already_imported, doesn't
    # re-parse, doesn't double-insert.
    assert second.already_imported is True
    n = db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"]
    assert n == first.dividend_count


def test_replace_cascades_to_dividends(db: sqlite3.Connection) -> None:
    """`ingest --replace` clears prior dividend rows along with trades."""
    fixture = _FIXTURES / "with_dividends.htm"
    ingest_statement(fixture, db)
    n_before = db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"]
    assert n_before == 4
    result = ingest_statement(fixture, db, replace=True)
    assert result.replaced is True
    n_after = db.execute("SELECT COUNT(*) AS n FROM dividends").fetchone()["n"]
    # Same fixture → same dividends → same count after replace.
    assert n_after == n_before


def test_ingest_without_fx_service_skips_merger(db: sqlite3.Connection) -> None:
    """Without an FXService, Corporate Actions rows are silently skipped.

    Pre-existing tests / call sites that don't pass `fx_service=` keep
    working — the corporate-action synthesizer is opt-in.
    """
    fixture = _FIXTURES / "with_cash_merger.htm"

    result = ingest_statement(fixture, db)

    assert result.trade_count == 1
    assert result.merger_trade_count == 0
    assert result.inserted_count == 1


def test_reingest_with_replace_replays_merger_trade(db: sqlite3.Connection) -> None:
    """`--replace` re-runs CA synthesis at the same statement_row_index.

    Identity stability matters: the audit trail
    `(source_statement_hash, statement_row_index)` must continue to
    point at the same logical event after a replace.
    """
    fixture = _FIXTURES / "with_cash_merger.htm"
    fx = _FXStub(rate=Decimal("0.74"))

    first = ingest_statement(fixture, db, fx_service=fx)
    first_indices = sorted(
        int(r["statement_row_index"])
        for r in db.execute(
            "SELECT statement_row_index FROM trades WHERE source_statement_hash = ?",
            (first.statement_hash,),
        )
    )

    second = ingest_statement(fixture, db, replace=True, fx_service=fx)

    assert second.replaced is True
    assert second.merger_trade_count == first.merger_trade_count == 1
    second_indices = sorted(
        int(r["statement_row_index"])
        for r in db.execute(
            "SELECT statement_row_index FROM trades WHERE source_statement_hash = ?",
            (first.statement_hash,),
        )
    )
    assert second_indices == first_indices


# ---------------------------------------------------------------------------
# Statement period, open positions and cash events
# ---------------------------------------------------------------------------


def test_ingest_records_statement_period(db: sqlite3.Connection) -> None:
    """The `<title>` period lands on the `statements` row."""
    result = ingest_statement(_FIXTURES / "with_open_positions.htm", db)
    row = StatementRepo(db).get(result.statement_hash)
    assert row is not None
    assert row.period_start == date(2025, 4, 7)
    assert row.period_end == date(2026, 4, 3)
    assert row.account_id == "U9999996"


def test_ingest_persists_open_positions_and_cash_events(db: sqlite3.Connection) -> None:
    """Positions and cash events land in their tables; the unresolved row is reported.

    The fixture lists six positions, one of which (`CBK6`) has no
    instrument-information row and no prior contract in the DB, so it
    is reported rather than stored. The eight non-coupon, non-internal
    cash rows become cash events; the coupon goes to `bond_coupons`.
    """
    result = ingest_statement(_FIXTURES / "with_open_positions.htm", db)

    assert result.position_count == 5
    assert result.positions_inserted == 5
    assert result.unresolved_position_symbols == ("CBK6",)
    assert result.cash_event_count == 8
    assert result.cash_events_inserted == 8
    assert result.bond_coupon_count == 1
    assert result.withdrawn_statement_count == 0

    positions = StatementPositionRepo(db).for_statement(result.statement_hash)
    assert [(p.instrument.symbol, str(p.quantity)) for _iid, p in positions] == [
        ("IEAA", "3652"),
        ("IEMI", "100"),
        ("TSLA", "-40"),
        ("UKT 0 3/8 10/22/26", "310000"),
        ("6LK6", "6"),
    ]
    assert all(p.account_id == "U9999996" for _iid, p in positions)

    events = CashEventRepo(db)
    assert events.count() == 8
    assert events.distinct_currencies() == ["GBP", "JPY", "USD"]
    usd = [event for _id, event in events.for_currency("USD")]
    assert [(e.kind.value, str(e.amount.amount)) for e in usd] == [
        ("transfer", "50000.00"),
        ("interest", "12.34"),
        ("interest", "-3.21"),
        ("transfer", "2.50"),
        ("fee", "-1.50"),
    ]


def test_leftover_future_position_resolves_against_known_contract(
    db: sqlite3.Connection,
) -> None:
    """A held-over contract with no FII row resolves via `future_instruments`."""
    cbk6 = FutureInstrument(
        conid=182589266,
        symbol="CBK6",
        currency="USD",
        contract_multiplier=Decimal("1000"),
        expiry_date=date(2026, 5, 15),
    )
    InstrumentRepo(db).upsert(cbk6)

    result = ingest_statement(_FIXTURES / "with_open_positions.htm", db)

    assert result.position_count == 6
    assert result.unresolved_position_symbols == ()
    positions = StatementPositionRepo(db).for_statement(result.statement_hash)
    cbk6_rows = [p for _iid, p in positions if p.instrument.symbol == "CBK6"]
    assert len(cbk6_rows) == 1
    assert cbk6_rows[0].instrument == cbk6
    assert cbk6_rows[0].quantity == Decimal("-3")


def test_ambiguous_leftover_future_position_stays_unresolved(db: sqlite3.Connection) -> None:
    """Two stored contracts sharing the symbol → the row is reported, not guessed."""
    repo = InstrumentRepo(db)
    for expiry in (date(2026, 5, 15), date(2027, 5, 14)):
        repo.upsert(
            FutureInstrument(
                conid=fake_conid("CBK6", "USD", expiry),
                symbol="CBK6",
                currency="USD",
                contract_multiplier=Decimal("1000"),
                expiry_date=expiry,
            )
        )
    result = ingest_statement(_FIXTURES / "with_open_positions.htm", db)
    assert result.position_count == 5
    assert result.unresolved_position_symbols == ("CBK6",)


def test_reingest_positions_and_cash_events_idempotent(db: sqlite3.Connection) -> None:
    fixture = _FIXTURES / "with_open_positions.htm"
    first = ingest_statement(fixture, db)
    second = ingest_statement(fixture, db, replace=True)
    assert second.replaced is True
    assert StatementPositionRepo(db).count() == first.position_count
    assert CashEventRepo(db).count() == first.cash_event_count


def test_replace_withdraws_earlier_version_at_same_path(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """A re-downloaded statement (new bytes, same path) replaces the old one.

    Without `replace` the new hash would be added beside the old
    import and check A10 would only warn; with it the earlier version
    at the same path is withdrawn inside the same transaction.
    """
    original = (_FIXTURES / "with_open_positions.htm").read_bytes()
    path = tmp_path / "25_26.htm"
    path.write_bytes(original)
    first = ingest_statement(path, db)

    path.write_bytes(original.replace(b"</body>", b"<!-- re-downloaded -->\n</body>"))
    second = ingest_statement(path, db, replace=True)

    assert second.statement_hash != first.statement_hash
    assert second.replaced is False  # no prior import of *this* hash
    assert second.withdrawn_statement_count == 1
    rows = db.execute(
        "SELECT statement_hash FROM statements WHERE source_path = ?", (str(path),)
    ).fetchall()
    assert [r["statement_hash"] for r in rows] == [second.statement_hash]
    # The old statement's dependents went with it — no duplicates.
    assert TradeRepo(db).count() == second.trade_count
    assert StatementPositionRepo(db).count() == second.position_count
    assert CashEventRepo(db).count() == second.cash_event_count


def test_modified_file_without_replace_is_added_beside_the_old_import(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """The historical behaviour is unchanged without the flag (A10 catches it)."""
    original = (_FIXTURES / "with_open_positions.htm").read_bytes()
    path = tmp_path / "25_26.htm"
    path.write_bytes(original)
    ingest_statement(path, db)
    path.write_bytes(original.replace(b"</body>", b"<!-- re-downloaded -->\n</body>"))
    second = ingest_statement(path, db)
    assert second.withdrawn_statement_count == 0
    n = db.execute("SELECT COUNT(*) AS n FROM statements WHERE source_path = ?", (str(path),))
    assert n.fetchone()["n"] == 2


# ---------------------------------------------------------------------------
# Overlapping statements — the coverage rule
# ---------------------------------------------------------------------------


def _variant(path: Path, source: Path, *, title_period: str, extra_row: str = "") -> Path:
    """Write a copy of an HTML fixture with a different period and an optional extra trade.

    The extra row is inserted before the Forex block of `mixed_tiny.htm`
    (inside the Stocks / GBP block), so it inherits that block's asset
    class and currency.
    """
    html = source.read_text()
    html = html.replace("April 8, 2024 - April 4, 2025", title_period)
    if extra_row:
        marker = "<!-- Subtotal — must be skipped -->"
        assert marker in html
        html = html.replace(marker, extra_row + marker)
    path.write_text(html)
    return path


def _stock_row(day: str, symbol: str = "CNKY") -> str:
    return (
        f"<tbody><tr><td>{symbol}</td><td>{day}, 11:00:00</td><td align='right'>5</td>"
        "<td align='right'>200.00</td><td align='right'>0</td><td align='right'>-1000.00</td>"
        "<td align='right'>-1.00</td><td align='right'>1001.00</td><td align='right'>0.00</td>"
        "<td align='right'>0.00</td><td align='right'>0.00</td><td align='right'>O</td>"
        "</tr></tbody>\n"
    )


def test_overlapping_statement_adds_only_the_days_it_alone_covers(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """A re-download that runs further than the file on record duplicates nothing."""
    first = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    assert first.inserted_count == 5

    longer = _variant(
        tmp_path / "longer.htm",
        _FIXTURES / "mixed_tiny.htm",
        title_period="April 8, 2024 - May 30, 2025",
        extra_row=_stock_row("2025-05-10"),
    )
    second = ingest_statement(longer, db)

    assert second.trade_count == 6
    assert second.inserted_count == 1
    assert second.covered_trade_count == 5
    assert second.fully_covered is False
    assert TradeRepo(db).count() == 6
    # The surviving row keeps the position it had in the file — it was
    # the third stock row, after the two CNKY fills — so the audit
    # trail still points at the right source row.
    kept = db.execute(
        "SELECT statement_row_index, trade_date FROM trades WHERE source_statement_hash = ?",
        (second.statement_hash,),
    ).fetchall()
    assert [(r["statement_row_index"], r["trade_date"]) for r in kept] == [(2, "2025-05-10")]


def test_statement_with_the_same_period_is_fully_covered(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """A modified file without --replace records the statement but no duplicate rows."""
    ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    same = _variant(
        tmp_path / "same.htm",
        _FIXTURES / "mixed_tiny.htm",
        title_period="April 8, 2024 - April 4, 2025",
        extra_row="<!-- re-downloaded -->",
    )
    second = ingest_statement(same, db)
    assert second.fully_covered is True
    assert second.inserted_count == 0
    assert second.covered_trade_count == 5
    assert TradeRepo(db).count() == 5
    assert StatementRepo(db).get(second.statement_hash) is not None


def test_consecutive_statements_keep_rows_dated_before_their_period(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """IB files a late-evening fill in the next period, printed with the previous date.

    The two periods do not overlap, so the later statement owns every
    row it carries — including the one dated on the earlier
    statement's last day.
    """
    ingest_statement(_FIXTURES / "mixed_tiny.htm", db)  # April 8, 2024 - April 4, 2025
    following = _variant(
        tmp_path / "following.htm",
        _FIXTURES / "mixed_tiny.htm",
        title_period="April 7, 2025 - April 3, 2026",
        extra_row=_stock_row("2025-04-04"),
    )
    second = ingest_statement(following, db)
    # The five copied fixture rows (dated 2024) are not owned by the
    # first statement either: nothing overlaps, nothing is skipped.
    assert second.covered_trade_count == 0
    assert second.inserted_count == 6


def test_first_ingested_owns_the_shared_days_whatever_the_order(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """Ingesting the longer file first makes the shorter one the redundant one."""
    longer = _variant(
        tmp_path / "longer.htm",
        _FIXTURES / "mixed_tiny.htm",
        title_period="April 8, 2024 - May 30, 2025",
        extra_row=_stock_row("2025-05-10"),
    )
    first = ingest_statement(longer, db)
    assert first.inserted_count == 6
    second = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    assert second.fully_covered is True
    assert second.inserted_count == 0
    assert TradeRepo(db).count() == 6


def test_ingest_statements_persists_earliest_period_first(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """A batch is ordered by period, so the shell's expansion order cannot change ownership."""
    longer = _variant(
        tmp_path / "a_longer.htm",
        _FIXTURES / "mixed_tiny.htm",
        title_period="April 8, 2024 - May 30, 2025",
        extra_row=_stock_row("2025-05-10"),
    )
    results = ingest_statements([longer, _FIXTURES / "mixed_tiny.htm"], db)
    # The shorter period sorts first (same start, earlier end) and so owns the shared days.
    assert [p.name for p, _r in results] == ["mixed_tiny.htm", "a_longer.htm"]
    assert results[0][1].inserted_count == 5
    assert results[1][1].inserted_count == 1
    assert results[1][1].covered_trade_count == 5


def test_ingest_statements_short_circuits_files_already_on_record(
    db: sqlite3.Connection,
) -> None:
    ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    results = ingest_statements(
        [_FIXTURES / "mixed_tiny.htm", _FIXTURES / "with_dividends.htm"], db
    )
    assert results[0][1].already_imported is True
    assert results[1][1].already_imported is False
    assert results[1][1].dividends_inserted == 4


def test_replace_reads_coverage_after_withdrawing_the_prior_version(
    db: sqlite3.Connection,
) -> None:
    """A withdrawn version must not count as owning its own days."""
    first = ingest_statement(_FIXTURES / "mixed_tiny.htm", db)
    second = ingest_statement(_FIXTURES / "mixed_tiny.htm", db, replace=True)
    assert second.replaced is True
    assert second.covered_trade_count == 0
    assert second.inserted_count == first.inserted_count
    assert second.withdrawn_overlaps == ()


def test_replace_reports_statements_that_overlapped_the_withdrawn_version(
    db: sqlite3.Connection, tmp_path: Path
) -> None:
    """The overlapping file may have had rows skipped in the withdrawn version's favour."""
    path = tmp_path / "24_25.htm"
    path.write_bytes((_FIXTURES / "mixed_tiny.htm").read_bytes())
    ingest_statement(path, db)
    longer = _variant(
        tmp_path / "longer.htm",
        _FIXTURES / "mixed_tiny.htm",
        title_period="April 8, 2024 - May 30, 2025",
        extra_row=_stock_row("2025-05-10"),
    )
    ingest_statement(longer, db)  # five of its six rows skipped in favour of 24_25.htm

    path.write_bytes(path.read_bytes().replace(b"</body>", b"<!-- v2 -->\n</body>"))
    result = ingest_statement(path, db, replace=True)
    assert result.withdrawn_statement_count == 1
    assert result.withdrawn_overlaps == (str(longer),)
