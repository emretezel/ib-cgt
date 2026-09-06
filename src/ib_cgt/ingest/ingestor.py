"""End-to-end ingestion orchestrator.

Composes the two ingestion primitives (hash → parse + map) with the
existing persistence repositories to turn a path-on-disk into rows in
the database. Everything inside `ingest_statement` runs in one SQLite
transaction, so a failure mid-way leaves the DB exactly as it was at
the start of the call — no half-imported statement rows, no orphaned
instrument upserts.

Idempotency is layered:

1. The statement hash short-circuits a re-import before we even parse:
   `StatementRepo.exists(hash)` → return `already_imported=True`.
2. Even if the file's hash is new but its trades overlap with rows
   already in the DB, the natural-key UNIQUE on `trades` (declared
   by migration 005) catches each duplicate at INSERT time. With
   `INSERT OR IGNORE` the duplicates silently no-op — the counts in
   the result reflect what actually landed.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Final

from ib_cgt.db.connection import transaction
from ib_cgt.db.repos.accounts import AccountRepo
from ib_cgt.db.repos.bond_coupons import BondCouponRepo
from ib_cgt.db.repos.cash_events import CashEventRepo
from ib_cgt.db.repos.dividends import DividendRepo
from ib_cgt.db.repos.instruments import InstrumentRepo
from ib_cgt.db.repos.statement_positions import StatementPositionRepo
from ib_cgt.db.repos.statements import StatementRepo
from ib_cgt.db.repos.trades import TradeRepo
from ib_cgt.domain import Account, AssetClass, BondInstrument, StatementPosition, Trade
from ib_cgt.ingest.bond_coupons import map_bond_coupons
from ib_cgt.ingest.cash_events import map_cash_events
from ib_cgt.ingest.corporate_actions import (
    FXConverter,
    map_bond_maturities,
    map_corporate_actions,
)
from ib_cgt.ingest.dividends import map_dividends
from ib_cgt.ingest.hashing import compute_statement_hash
from ib_cgt.ingest.mapper import map_rows
from ib_cgt.ingest.parser import RawOpenPositionRow, parse_statement
from ib_cgt.ingest.positions import map_open_positions


@dataclass(frozen=True, slots=True, kw_only=True)
class IngestResult:
    """Summary returned by `ingest_statement`.

    Attributes:
        statement_hash: The SHA-256 of the source file (hex).
        account_id: Account the statement belonged to.
        trade_count: Number of trades the parser produced (regular +
            synthesized). Zero is a legal outcome for a statement with
            no activity.
        merger_trade_count: Subset of `trade_count` originating from
            cash-for-shares Corporate Actions rows ("Merged(Acquisition)").
            Reported separately so the CLI can surface them; matching
            treats them identically to ordinary sells.
        maturity_trade_count: Subset of `trade_count` originating from
            Bond Maturity Corporate Actions rows. Reported separately
            so the CLI can surface them; matching treats them
            identically to ordinary sells.
        skipped_maturity_count: Maturity rows the synthesiser produced
            that were dropped because no existing bond instrument
            matches the `(symbol, currency)` IB used in the maturity
            description. The trade-side may have ingested the same
            underlying gilt under a yield-suffixed alias; ISIN-based
            instrument unification is a future enhancement, and until
            it lands these rows are surfaced as a warning rather than
            polluting `bond_instruments`.
        inserted_count: How many of those were new rows. On a repeat
            ingest of a modified statement this can be less than
            `trade_count` if some trades were already present from a
            prior, partially-overlapping import.
        dividend_count: Number of dividend / WHT / payment-in-lieu rows
            the mapper produced from the statement's `tblCombDiv` and
            `tblWithholding` sections. Zero on statements with no
            dividend activity (e.g. futures-only statements).
        dividends_inserted: How many of those were new rows in
            `dividends`. Same idempotency semantics as
            `inserted_count`: `INSERT OR IGNORE` on the
            `(source_statement_hash, statement_row_index)` UNIQUE
            backstops a partial-batch retry.
        bond_coupon_count: Number of bond-coupon rows the mapper
            extracted from the statement's `tblCombInt_*` (Interest)
            sections. Zero on statements with no bonds. Broker
            debit/credit interest rows are silently filtered out
            and never counted here.
        bond_coupons_inserted: How many of those were new rows in
            `bond_coupons`. Same idempotency semantics as
            `dividends_inserted`.
        already_imported: True iff the byte-identical statement had
            been imported before — parse/map were skipped. Mutually
            exclusive with `replaced`.
        replaced: True iff a prior import of this exact hash existed
            and was deleted before this fresh ingest landed (the
            `replace=True` path). Mutually exclusive with
            `already_imported`.
    """

    statement_hash: str
    account_id: str
    trade_count: int
    inserted_count: int
    already_imported: bool
    merger_trade_count: int = 0
    maturity_trade_count: int = 0
    skipped_maturity_count: int = 0
    dividend_count: int = 0
    dividends_inserted: int = 0
    bond_coupon_count: int = 0
    bond_coupons_inserted: int = 0
    position_count: int = 0
    positions_inserted: int = 0
    unresolved_position_symbols: tuple[str, ...] = ()
    cash_event_count: int = 0
    cash_events_inserted: int = 0
    replaced: bool = False
    withdrawn_statement_count: int = 0


def ingest_statement(
    path: Path,
    conn: sqlite3.Connection,
    *,
    replace: bool = False,
    fx_service: FXConverter | None = None,
) -> IngestResult:
    """Parse the file at `path` and persist its trades via `conn`.

    Args:
        path: Absolute or CWD-relative path to an IB HTML `.htm` file.
        conn: An already-open, already-migrated SQLite connection
            (typically from `open_connection` + `apply_migrations`).
        replace: When True, a prior import of the same hash is
            *deleted* (along with its trades, via the migration-004
            cascade) before this fresh ingest runs. The default
            `False` preserves the historical short-circuit behaviour
            — a repeat ingest of an already-imported statement is a
            constant-time no-op.
        fx_service: Optional FX service for synthesizing `SELL` trades
            from cash-for-shares Corporate Actions rows. When omitted,
            corporate-action rows are silently ignored — the CLI's
            `ingest` command always passes one, so production runs
            cover the IEMI-shaped case; tests opt in by injecting a
            mock or seeded service.

    Returns:
        `IngestResult` — see docstring for field semantics.
    """
    # `read_bytes()` handles the file-close for us and avoids the
    # encoding guesswork that `read_text()` would introduce. Hashing
    # happens before the parser runs so a duplicate hash is a constant-
    # time short-circuit.
    source_bytes = path.read_bytes()
    statement_hash = compute_statement_hash(source_bytes)

    statements = StatementRepo(conn)
    prior_existed = statements.exists(statement_hash)

    if prior_existed and not replace:
        # We could re-derive the account id via a secondary SELECT on
        # `statements`, but it's cheaper to reparse only the header —
        # except that hash-based short-circuits exist precisely to avoid
        # re-parse costs. So we return the hash and let the caller
        # treat "already imported" as "nothing more to report".
        # For the account_id in the result we do a tiny SELECT — not
        # expensive, and keeps the result shape consistent.
        row = conn.execute(
            "SELECT account_id, trade_count FROM statements WHERE statement_hash = ?",
            (statement_hash,),
        ).fetchone()
        return IngestResult(
            statement_hash=statement_hash,
            account_id=str(row["account_id"]),
            trade_count=int(row["trade_count"]),
            inserted_count=0,
            already_imported=True,
        )

    parsed = parse_statement(source_bytes)
    regular_trades = map_rows(parsed)

    # Synthesize SELL trades from cash-for-shares Corporate Actions rows
    # (e.g. the IEMI fund-merger that pays cash for the entire position).
    # The synthesized trades are appended *after* the regular ones so
    # `enumerate(...)` in TradeRepo.insert_many gives them statement_row_index
    # values strictly greater than any real-trade index. This keeps re-ingest
    # identities stable as long as the parser's emission order is stable.
    if fx_service is not None:
        merger_trades = map_corporate_actions(parsed, fx_service=fx_service)
    else:
        merger_trades = []

    # Bond maturities — issuer-redemption disposals at par. Always
    # synthesised when present (no FX dependency: par price is in the
    # bond's own currency). Appended after mergers so the per-statement
    # `statement_row_index` enumeration in `TradeRepo.insert_many`
    # produces stable identities across re-ingests.
    #
    # IB's Bond Maturity description carries the bond's *canonical*
    # symbol (e.g. ``"UKT 0 1/4 01/31/25"``), while the trades section
    # may have ingested the same underlying gilt under one or more
    # *yield-suffixed* aliases (``"UKT 0 1/4 01/31/25 5.26994388%"``).
    # Until ISIN-based instrument unification lands, allowing the
    # synthesised SELL through would create an orphan
    # `bond_instruments` row that no `BUY` ever covers, polluting the
    # debug views and surfacing as an `UnmatchedDisposalError` in
    # `match bonds`. The filter below drops any maturity whose
    # `(symbol, currency)` doesn't already match an existing bond
    # instrument; the user is told which were skipped so they can
    # follow up.
    candidate_maturities = map_bond_maturities(parsed)
    maturity_trades, skipped_maturity_trades = _filter_maturities_with_known_instruments(
        candidate_maturities,
        conn,
        in_flight=regular_trades + merger_trades,
    )
    trades = regular_trades + merger_trades + maturity_trades

    # Dividends / WHT / payment-in-lieu — independent event stream from
    # trades. They feed the FX rule engine via the per-currency S.104
    # pool (HMRC CG78315) but are not themselves CGT events and so live
    # in their own table. The dividends-section rows have their own
    # row-index space, independent of trades — `DividendRepo.insert_many`
    # enumerates from zero.
    dividends = map_dividends(parsed)

    # Bond coupon payments — an FX-cashflow source per CG78315.
    # Same independence: own table, own row-index space, mapper
    # silently skips non-coupon rows in the Interest section.
    bond_coupons = map_bond_coupons(parsed)

    # Instrument-less cash movements — broker interest, external
    # transfers, fees — the remaining CG78315 sources. The cash-event
    # mapper takes what the coupon mapper leaves of the Interest
    # section, so the two never double-count a row.
    cash_events = map_cash_events(parsed)

    # Open positions on the period's last day. Rows the statement's own
    # instrument-information section cannot resolve (held-over futures
    # on legacy vintages) are resolved below against instruments already
    # in the DB — including the ones this very ingest is about to add.
    positions, leftover_position_rows = map_open_positions(parsed)

    accounts = AccountRepo(conn)
    trade_repo = TradeRepo(conn)
    dividend_repo = DividendRepo(conn)
    bond_coupon_repo = BondCouponRepo(conn)
    cash_event_repo = CashEventRepo(conn)
    position_repo = StatementPositionRepo(conn)

    # One transaction for everything the parser produced. `transaction()`
    # issues COMMIT on successful exit and ROLLBACK on exception, which
    # is exactly the atomicity the idempotency story relies on (the
    # connection is in autocommit mode, so a bare `with conn:` would
    # commit every statement individually). When `replace` is set, the
    # prior-row delete also lives inside this transaction so a failure
    # between the delete and the re-insert leaves the DB exactly as it
    # was before the call.
    withdrawn = 0
    with transaction(conn):
        if prior_existed and replace:
            # Every dependent table's `source_statement_hash` is
            # ON DELETE CASCADE (migrations 004, 009, 012, 016, 017), so
            # removing the `statements` row atomically removes every
            # trade, dividend, coupon, position and cash event that
            # pointed at it.
            conn.execute(
                "DELETE FROM statements WHERE statement_hash = ?",
                (statement_hash,),
            )
        accounts.upsert(Account(account_id=parsed.account_id))
        if replace:
            # A re-downloaded statement has new bytes (a new hash) but
            # is the same statement: withdraw the earlier version at
            # the same path so its trades don't sit beside the new
            # ones. Same cascade as above.
            withdrawn = statements.delete_by_path(
                parsed.account_id, str(path), except_hash=statement_hash
            )
        statements.record(
            statement_hash=statement_hash,
            source_path=str(path),
            account_id=parsed.account_id,
            trade_count=len(trades),
            period_start=parsed.period_start,
            period_end=parsed.period_end,
        )
        inserted = trade_repo.insert_many(
            trades,
            source_statement_hash=statement_hash,
        )
        dividends_inserted = dividend_repo.insert_many(
            dividends,
            source_statement_hash=statement_hash,
        )
        bond_coupons_inserted = bond_coupon_repo.insert_many(
            bond_coupons,
            source_statement_hash=statement_hash,
        )
        cash_events_inserted = cash_event_repo.insert_many(
            cash_events,
            source_statement_hash=statement_hash,
        )
        # Positions last: the trades above may have created the very
        # instrument rows a leftover position resolves against.
        resolved_leftovers, unresolved = _resolve_leftover_positions(
            leftover_position_rows, parsed.account_id, InstrumentRepo(conn)
        )
        all_positions = positions + resolved_leftovers
        positions_inserted = position_repo.insert_many(
            all_positions,
            source_statement_hash=statement_hash,
        )

    return IngestResult(
        statement_hash=statement_hash,
        account_id=parsed.account_id,
        trade_count=len(trades),
        merger_trade_count=len(merger_trades),
        maturity_trade_count=len(maturity_trades),
        skipped_maturity_count=len(skipped_maturity_trades),
        dividend_count=len(dividends),
        dividends_inserted=dividends_inserted,
        bond_coupon_count=len(bond_coupons),
        bond_coupons_inserted=bond_coupons_inserted,
        position_count=len(all_positions),
        positions_inserted=positions_inserted,
        unresolved_position_symbols=tuple(row.symbol for row in unresolved),
        cash_event_count=len(cash_events),
        cash_events_inserted=cash_events_inserted,
        inserted_count=inserted,
        already_imported=False,
        replaced=prior_existed and replace,
        withdrawn_statement_count=withdrawn,
    )


def _resolve_leftover_positions(
    leftovers: list[RawOpenPositionRow],
    account_id: str,
    instruments: InstrumentRepo,
) -> tuple[list[StatementPosition], list[RawOpenPositionRow]]:
    """Resolve position rows the statement's own instrument table could not.

    A futures contract held over a year end appears in the Open
    Positions section of a legacy statement that lacks an instrument-
    information row for it, because it was not traded in that period.
    The contract was traded — and so stored — in an earlier statement,
    so a `(symbol, currency)` lookup against `future_instruments`
    finds it. Exactly one hit resolves the row; none or several (the
    same root symbol on two expiries) leave it unresolved, and the
    caller reports the symbol rather than guessing.
    """
    resolved: list[StatementPosition] = []
    unresolved: list[RawOpenPositionRow] = []
    for raw in leftovers:
        asset_class = _POSITION_ASSET_CLASSES.get(raw.asset_class)
        hits = (
            instruments.find_by_symbol(asset_class, raw.symbol, raw.currency)
            if asset_class is not None
            else []
        )
        if len(hits) != 1:
            unresolved.append(raw)
            continue
        _iid, instrument = hits[0]
        resolved.append(
            StatementPosition(
                account_id=account_id,
                instrument=instrument,
                quantity=Decimal(raw.quantity_text.replace(",", "")),
            )
        )
    return resolved, unresolved


# Open Positions section labels → the asset class whose child table a
# leftover row is resolved against. Options never reach here (the
# parser drops them) and FX has no positions section.
_POSITION_ASSET_CLASSES: Final[dict[str, AssetClass]] = {
    "Stocks": AssetClass.STOCK,
    "Bonds": AssetClass.BOND,
    "Corporate and Municipal Bonds": AssetClass.BOND,
    "Futures": AssetClass.FUTURE,
}


def _filter_maturities_with_known_instruments(
    candidates: list[Trade],
    conn: sqlite3.Connection,
    *,
    in_flight: list[Trade],
) -> tuple[list[Trade], list[Trade]]:
    """Split synthesised bond-maturity trades into kept vs skipped.

    A maturity is kept only when the `(symbol, currency)` it claims to
    redeem already has at least one BUY trade in the DB — i.e. an open
    holding the redemption could plausibly be settling. Without this
    guard, a maturity row whose IB-rendered symbol differs from the
    trade-side aliases (the user's gilts are stored under yield-
    suffixed symbols like `"UKT 0 1/4 01/31/25 5.26994388%"` while the
    maturity row uses the canonical `"UKT 0 1/4 01/31/25"`) would
    silently create a new orphan `bond_instruments` row that no BUY
    ever covers — polluting `match bonds` and surfacing as an
    `UnmatchedDisposalError`. Skipped maturities are returned so the
    CLI can warn the user; ISIN-level instrument unification is the
    proper long-term reconciliation path.

    Args:
        candidates: Trades produced by `map_bond_maturities`.
        conn: Open SQLite connection — read-only for this lookup.
        in_flight: Trades already mapped this run that haven't yet been
            persisted (regular trades + merger synth). Their
            `(symbol, currency)` BUY pairs count as covered for the
            purpose of the filter, otherwise a fresh statement that
            buys *and* matures the same bond in one ingest would have
            its maturity dropped.

    Returns:
        `(kept, skipped)`. The two lists partition `candidates`. Order
        is preserved within each.
    """
    if not candidates:
        return [], []

    keys: set[tuple[str, str]] = {
        (t.instrument.symbol, t.instrument.currency)
        for t in candidates
        if isinstance(t.instrument, BondInstrument)
    }
    if not keys:
        return [], list(candidates)

    placeholders = ",".join(["(?, ?)"] * len(keys))
    flat: list[str] = [val for pair in keys for val in pair]
    sql = (
        "SELECT DISTINCT b.symbol, b.currency "
        "FROM bond_instruments AS b "
        "JOIN trades AS t ON t.instrument_id = b.instrument_id "
        "WHERE t.action = 'buy' "
        f"AND (b.symbol, b.currency) IN (VALUES {placeholders})"
    )
    rows = conn.execute(sql, flat).fetchall()
    known: set[tuple[str, str]] = {(r["symbol"], r["currency"]) for r in rows}

    # Add this-statement BUYs that haven't been persisted yet.
    for trade in in_flight:
        if isinstance(trade.instrument, BondInstrument) and trade.action.value == "buy":
            known.add((trade.instrument.symbol, trade.instrument.currency))

    kept: list[Trade] = []
    skipped: list[Trade] = []
    for trade in candidates:
        key = (trade.instrument.symbol, trade.instrument.currency)
        if key in known:
            kept.append(trade)
        else:
            skipped.append(trade)
    return kept, skipped


__all__ = ["IngestResult", "ingest_statement"]
