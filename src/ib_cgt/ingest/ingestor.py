"""End-to-end ingestion orchestrator.

Composes the ingestion primitives (hash → parse via the format's
strategy → map) with the persistence repositories to turn a
path-on-disk into rows in the database. Everything `ingest_parsed`
writes goes into one SQLite transaction, so a failure mid-way leaves
the DB exactly as it was at the start of the call — no half-imported
statement rows, no orphaned instrument upserts.

Idempotency is layered:

1. The statement hash short-circuits a re-import before we even parse:
   `StatementRepo.exists(hash)` → return `already_imported=True`.
2. Two *different* statements may cover the same days (a re-download
   that runs a few weeks further, a calendar-year PDF ending on the day
   a tax-year file starts). IB prints no per-row identifier, so the
   overlap is resolved by date through `Coverage`: the statement
   ingested first owns every day of its period, and a later
   overlapping statement contributes only the facts dated on days it
   does not own. The skipped rows are counted on the result, and the
   surviving rows keep the row index they had in the file.
3. Even so, the `(source_statement_hash, statement_row_index)` UNIQUE
   on every child table catches a re-presented row at INSERT time; with
   `INSERT OR IGNORE` it silently no-ops.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
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
from ib_cgt.ingest.coverage import Coverage
from ib_cgt.ingest.dividends import map_dividends
from ib_cgt.ingest.hashing import compute_statement_hash
from ib_cgt.ingest.mapper import map_rows
from ib_cgt.ingest.parsers import StatementFormat, parser_for
from ib_cgt.ingest.positions import map_open_positions
from ib_cgt.ingest.raw import ParsedStatement, RawOpenPositionRow


@dataclass(frozen=True, slots=True, kw_only=True)
class IngestResult:
    """Summary returned by `ingest_statement`.

    Attributes:
        statement_hash: The SHA-256 of the source file (hex).
        account_id: Account the statement belonged to.
        trade_count: Number of trades the parser produced (regular +
            synthesized). Zero is a legal outcome for a statement with
            no activity. On the `already_imported` path it is the
            number of trades on record for the statement instead.
        merger_trade_count: Subset of `trade_count` originating from
            cash-for-shares Corporate Actions rows ("Merged(Acquisition)").
            Reported separately so the CLI can surface them; matching
            treats them identically to ordinary sells.
        maturity_trade_count: Subset of `trade_count` originating from
            Bond Maturity Corporate Actions rows. Reported separately
            so the CLI can surface them; matching treats them
            identically to ordinary sells.
        skipped_maturity_count: Maturity rows the synthesiser produced
            that were dropped because no bond with the ISIN the
            maturity description names has ever been bought — neither
            in the database nor earlier in this statement. A
            redemption with no purchase behind it would create an
            orphan `bond_instruments` row that no BUY covers, so these
            rows are surfaced as a warning instead.
        inserted_count: How many trades were new rows. Less than
            `trade_count` when the coverage rule skipped rows an
            earlier statement already owns (see `covered_trade_count`)
            or a partial-batch retry re-presented rows.
        dividend_count: Number of dividend / WHT / payment-in-lieu rows
            the mapper produced from the statement's Dividends and
            Withholding Tax sections. Zero on statements with no
            dividend activity (e.g. futures-only statements).
        dividends_inserted: How many of those were new rows in
            `dividends`.
        bond_coupon_count: Number of bond-coupon rows the mapper
            extracted from the statement's Interest section. Broker
            debit/credit interest rows are never counted here.
        bond_coupons_inserted: How many of those were new rows in
            `bond_coupons`.
        position_count: Open positions resolved to an instrument.
        positions_inserted: How many of those were new rows.
        unresolved_position_symbols: Open Positions rows that resolved
            to no instrument — reported, never stored.
        cash_event_count: Instrument-less cash rows the mapper kept.
        cash_events_inserted: How many of those were new rows.
        covered_trade_count: Parsed trades skipped because an earlier
            statement of the account already owns their date.
        covered_dividend_count: Dividend rows skipped for the same reason.
        covered_bond_coupon_count: Coupon rows skipped for the same reason.
        covered_cash_event_count: Cash-event rows skipped for the same reason.
        fully_covered: True iff every day of the statement's period was
            already owned by earlier statements — only the open
            positions were new information.
        already_imported: True iff the byte-identical statement had
            been imported before — parse/map were skipped. Mutually
            exclusive with `replaced`.
        replaced: True iff a prior import of this exact hash existed
            and was deleted before this fresh ingest landed (the
            `replace=True` path). Mutually exclusive with
            `already_imported`.
        withdrawn_statement_count: Earlier imports of a *different*
            file at the same path withdrawn by `replace=True`.
        withdrawn_overlaps: Source paths of statements still on file
            whose period overlaps a statement this call withdrew. If
            they were ingested after the withdrawn version, rows on the
            shared days were skipped in its favour and are now gone;
            re-ingesting them with `replace=True` restores them.
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
    covered_trade_count: int = 0
    covered_dividend_count: int = 0
    covered_bond_coupon_count: int = 0
    covered_cash_event_count: int = 0
    fully_covered: bool = False
    replaced: bool = False
    withdrawn_statement_count: int = 0
    withdrawn_overlaps: tuple[str, ...] = ()

    @property
    def covered_count(self) -> int:
        """Every row the coverage rule skipped, across the four dated tables."""
        return (
            self.covered_trade_count
            + self.covered_dividend_count
            + self.covered_bond_coupon_count
            + self.covered_cash_event_count
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class LoadedStatement:
    """A statement read from disk and parsed, not yet persisted.

    Splitting the read from the write lets `ingest_statements` parse a
    whole batch first and persist it in period order, so the coverage
    rule sees the files in a deterministic sequence whatever order the
    shell expanded them in.
    """

    path: Path
    statement_hash: str
    parsed: ParsedStatement


def load_statement(path: Path, *, fmt: StatementFormat = StatementFormat.AUTO) -> LoadedStatement:
    """Read and parse the statement at `path` with the strategy `fmt` selects.

    Raises:
        StatementParseError: The file is not an IB activity statement,
            or its suffix names no known format under `AUTO`.
    """
    # `read_bytes()` handles the file-close for us and avoids the
    # encoding guesswork that `read_text()` would introduce.
    source_bytes = path.read_bytes()
    return LoadedStatement(
        path=path,
        statement_hash=compute_statement_hash(source_bytes),
        parsed=parser_for(fmt, path=path).parse(source_bytes),
    )


def ingest_statement(
    path: Path,
    conn: sqlite3.Connection,
    *,
    replace: bool = False,
    fx_service: FXConverter | None = None,
    fmt: StatementFormat = StatementFormat.AUTO,
) -> IngestResult:
    """Parse the file at `path` and persist its facts via `conn`.

    Args:
        path: Absolute or CWD-relative path to an IB activity statement;
            the file suffix selects the parser (see `ingest.parsers`)
            unless `fmt` names one.
        conn: An already-open, already-migrated SQLite connection
            (typically from `open_connection` + `apply_migrations`).
        replace: When True, a prior import of the same hash is
            *deleted* (along with its rows, via the statement cascade)
            before this fresh ingest runs, and so is any earlier import
            of a different file at the same path. The default `False`
            preserves the historical short-circuit behaviour — a repeat
            ingest of an already-imported statement is a constant-time
            no-op.
        fx_service: Optional FX service for synthesizing `SELL` trades
            from cash-for-shares Corporate Actions rows. When omitted,
            corporate-action rows are silently ignored — the CLI's
            `ingest` command always passes one, so production runs
            cover the IEMI-shaped case; tests opt in by injecting a
            mock or seeded service.
        fmt: The statement format, or `AUTO` to detect it from the
            suffix.

    Returns:
        `IngestResult` — see docstring for field semantics.
    """
    # Hashing happens before the parser runs so a duplicate hash is a
    # constant-time short-circuit.
    source_bytes = path.read_bytes()
    statement_hash = compute_statement_hash(source_bytes)
    statements = StatementRepo(conn)
    if statements.exists(statement_hash) and not replace:
        return _already_imported(conn, statement_hash)
    loaded = LoadedStatement(
        path=path,
        statement_hash=statement_hash,
        parsed=parser_for(fmt, path=path).parse(source_bytes),
    )
    return ingest_parsed(loaded, conn, replace=replace, fx_service=fx_service)


def ingest_statements(
    paths: Sequence[Path],
    conn: sqlite3.Connection,
    *,
    replace: bool = False,
    fx_service: FXConverter | None = None,
    fmt: StatementFormat = StatementFormat.AUTO,
) -> list[tuple[Path, IngestResult]]:
    """Ingest several statements, earliest period first.

    Every file is hashed and parsed before anything is written, so a
    file that is not a statement fails the batch before it changes
    the database. Files already on record (same hash, `replace`
    unset) short-circuit as `ingest_statement` does. The rest are
    persisted in ascending `(account, period_start, period_end)`
    order — the order that makes the coverage rule's "first ingested
    owns the day" independent of how the shell expanded the paths.

    Returns:
        `(path, result)` pairs in the order the files were processed.
    """
    statements = StatementRepo(conn)
    done: list[tuple[Path, IngestResult]] = []
    pending: list[LoadedStatement] = []
    for path in paths:
        source_bytes = path.read_bytes()
        statement_hash = compute_statement_hash(source_bytes)
        if statements.exists(statement_hash) and not replace:
            done.append((path, _already_imported(conn, statement_hash)))
            continue
        pending.append(
            LoadedStatement(
                path=path,
                statement_hash=statement_hash,
                parsed=parser_for(fmt, path=path).parse(source_bytes),
            )
        )
    pending.sort(
        key=lambda s: (s.parsed.account_id, s.parsed.period_start, s.parsed.period_end, str(s.path))
    )
    for loaded in pending:
        done.append(
            (loaded.path, ingest_parsed(loaded, conn, replace=replace, fx_service=fx_service))
        )
    return done


def ingest_parsed(
    loaded: LoadedStatement,
    conn: sqlite3.Connection,
    *,
    replace: bool = False,
    fx_service: FXConverter | None = None,
) -> IngestResult:
    """Map a parsed statement and persist it in one transaction.

    The write half of `ingest_statement`; see there for the `replace`
    and `fx_service` semantics. A statement whose hash is already on
    record short-circuits here too (a batch may list one file twice),
    unless `replace` is set.
    """
    parsed = loaded.parsed
    statement_hash = loaded.statement_hash
    statements = StatementRepo(conn)
    prior_existed = statements.exists(statement_hash)
    if prior_existed and not replace:
        return _already_imported(conn, statement_hash)

    regular_trades = map_rows(parsed)

    # Synthesize SELL trades from cash-for-shares Corporate Actions rows
    # (e.g. the IEMI fund-merger that pays cash for the entire position).
    # The synthesized trades are appended *after* the regular ones so
    # their statement_row_index values are strictly greater than any
    # real-trade index. This keeps re-ingest identities stable as long
    # as the parser's emission order is stable.
    if fx_service is not None:
        merger_trades = map_corporate_actions(parsed, fx_service=fx_service)
    else:
        merger_trades = []

    # Bond maturities — issuer-redemption disposals at par. Always
    # synthesised when present (no FX dependency: par price is in the
    # bond's own currency). Appended after mergers for the same
    # identity-stability reason.
    #
    # A maturity row names its bond by ISIN — the bond's identity — so
    # a redemption of a bond this corpus has never bought is a data
    # gap, not a disposal: letting the synthesised SELL through would
    # create an orphan `bond_instruments` row that no `BUY` covers and
    # surface as an `UnmatchedDisposalError` in `match bonds`. The
    # filter below drops any maturity whose ISIN has no BUY behind it;
    # the user is told which were skipped so they can follow up.
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
    # in their own table, with their own row-index space.
    dividends = map_dividends(parsed)

    # Bond coupon payments — an FX-cashflow source per CG78315. Same
    # independence: own table, own row-index space, mapper silently
    # skips non-coupon rows in the Interest section.
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
    # commit every statement individually). The `replace` deletes live
    # inside it too, so a failure between the delete and the re-insert
    # leaves the DB exactly as it was before the call — and the
    # coverage is read *after* them, so a withdrawn version never
    # counts as owning its own days.
    withdrawn = 0
    withdrawn_periods: list[tuple[date, date]] = []
    with transaction(conn):
        if prior_existed and replace:
            # Every dependent table's `source_statement_hash` is
            # ON DELETE CASCADE (migrations 004, 009, 012, 016, 017), so
            # removing the `statements` row atomically removes every
            # trade, dividend, coupon, position and cash event that
            # pointed at it.
            prior = statements.get(statement_hash)
            if prior is not None:
                withdrawn_periods.append((prior.period_start, prior.period_end))
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
            for earlier in statements.list_by_path(
                parsed.account_id, str(loaded.path), except_hash=statement_hash
            ):
                withdrawn_periods.append((earlier.period_start, earlier.period_end))
            withdrawn = statements.delete_by_path(
                parsed.account_id, str(loaded.path), except_hash=statement_hash
            )

        coverage = Coverage.for_period(
            statements.periods_for_account(parsed.account_id),
            parsed.period_start,
            parsed.period_end,
        )
        kept_trades = _not_owned_elsewhere(trades, coverage, lambda t: t.trade_date)
        kept_dividends = _not_owned_elsewhere(dividends, coverage, lambda d: d.pay_date)
        kept_coupons = _not_owned_elsewhere(bond_coupons, coverage, lambda c: c.pay_date)
        kept_cash_events = _not_owned_elsewhere(cash_events, coverage, lambda e: e.value_date)

        statements.record(
            statement_hash=statement_hash,
            source_path=str(loaded.path),
            account_id=parsed.account_id,
            trade_count=len(kept_trades),
            period_start=parsed.period_start,
            period_end=parsed.period_end,
            time_zone=parsed.time_zone,
        )
        inserted = trade_repo.insert_indexed(kept_trades, source_statement_hash=statement_hash)
        dividends_inserted = dividend_repo.insert_indexed(
            kept_dividends, source_statement_hash=statement_hash
        )
        bond_coupons_inserted = bond_coupon_repo.insert_indexed(
            kept_coupons, source_statement_hash=statement_hash
        )
        cash_events_inserted = cash_event_repo.insert_indexed(
            kept_cash_events, source_statement_hash=statement_hash
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
        withdrawn_overlaps = _overlaps_of_withdrawn(
            statements, parsed.account_id, withdrawn_periods, except_hash=statement_hash
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
        covered_trade_count=len(trades) - len(kept_trades),
        covered_dividend_count=len(dividends) - len(kept_dividends),
        covered_bond_coupon_count=len(bond_coupons) - len(kept_coupons),
        covered_cash_event_count=len(cash_events) - len(kept_cash_events),
        fully_covered=coverage.fully_covered,
        inserted_count=inserted,
        already_imported=False,
        replaced=prior_existed and replace,
        withdrawn_statement_count=withdrawn,
        withdrawn_overlaps=withdrawn_overlaps,
    )


def _already_imported(conn: sqlite3.Connection, statement_hash: str) -> IngestResult:
    """The constant-time result for a hash already on record."""
    # A tiny SELECT for the account id and the trades on record keeps
    # the result shape consistent without re-parsing anything.
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


def _not_owned_elsewhere[T](
    facts: Sequence[T], coverage: Coverage, dated_by: Callable[[T], date]
) -> list[tuple[int, T]]:
    """Pair each fact with its position in the statement and drop the ones owned elsewhere.

    The position — not a fresh dense count — is what the row index
    persists, so the audit trail keeps pointing at the right source
    row whether or not the coverage rule skipped its neighbours.
    """
    return [
        (index, fact)
        for index, fact in enumerate(facts)
        if not coverage.owned_elsewhere(dated_by(fact))
    ]


def _overlaps_of_withdrawn(
    statements: StatementRepo,
    account_id: str,
    withdrawn_periods: Iterable[tuple[date, date]],
    *,
    except_hash: str,
) -> tuple[str, ...]:
    """Source paths of statements still on file that overlap a withdrawn period.

    Those statements may have had rows skipped in the withdrawn
    version's favour when they were ingested; the user is told so
    they can re-ingest them with `replace`. The statement being
    ingested is excluded — it is the withdrawn version's successor.
    """
    paths: list[str] = []
    for start, end in withdrawn_periods:
        for row in statements.overlapping(account_id, start, end):
            if row.statement_hash != except_hash and row.source_path not in paths:
                paths.append(row.source_path)
    return tuple(paths)


def _resolve_leftover_positions(
    leftovers: list[RawOpenPositionRow],
    account_id: str,
    instruments: InstrumentRepo,
) -> tuple[list[StatementPosition], list[RawOpenPositionRow]]:
    """Resolve position rows the statement's own instrument table could not.

    A contract held over a year end can appear in the Open Positions
    section of a legacy statement that lacks an instrument-information
    row for it (and therefore a conid), because it was not traded in
    that period. It was traded — and so stored — in an earlier
    statement, so a `(symbol, currency)` lookup against the child
    table finds it. Exactly one hit resolves the row; none or several
    (the same root symbol on two expiries, or a renamed listing stored
    under its old symbol) leave it unresolved, and the caller reports
    the symbol rather than guessing. This is the one place ingestion
    falls back to display fields; the calculator never does.
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

    A maturity is kept only when the bond it redeems — identified by
    ISIN, the bond's natural key — already has at least one BUY trade
    in the DB or earlier in this statement, i.e. an open holding the
    redemption could plausibly be settling. A redemption with no
    purchase behind it is a data gap (a bond bought before the
    ingested history starts); letting it through would create an
    orphan `bond_instruments` row that no BUY ever covers, polluting
    `match bonds` and surfacing as an `UnmatchedDisposalError`.
    Skipped maturities are returned so the CLI can warn the user.

    Args:
        candidates: Trades produced by `map_bond_maturities`.
        conn: Open SQLite connection — read-only for this lookup.
        in_flight: Trades already mapped this run that haven't yet been
            persisted (regular trades + merger synth). Their bond BUY
            ISINs count as covered for the purpose of the filter,
            otherwise a fresh statement that buys *and* matures the
            same bond in one ingest would have its maturity dropped.

    Returns:
        `(kept, skipped)`. The two lists partition `candidates`. Order
        is preserved within each.
    """
    if not candidates:
        return [], []

    isins: set[str] = {
        t.instrument.isin for t in candidates if isinstance(t.instrument, BondInstrument)
    }
    if not isins:
        return [], list(candidates)

    placeholders = ",".join(["?"] * len(isins))
    sql = (
        "SELECT DISTINCT b.isin "
        "FROM bond_instruments AS b "
        "JOIN trades AS t ON t.instrument_id = b.instrument_id "
        "WHERE t.action = 'buy' "
        f"AND b.isin IN ({placeholders})"
    )
    rows = conn.execute(sql, sorted(isins)).fetchall()
    known: set[str] = {str(r["isin"]) for r in rows}

    # Add this-statement BUYs that haven't been persisted yet.
    for trade in in_flight:
        if isinstance(trade.instrument, BondInstrument) and trade.action.value == "buy":
            known.add(trade.instrument.isin)

    kept: list[Trade] = []
    skipped: list[Trade] = []
    for trade in candidates:
        if isinstance(trade.instrument, BondInstrument) and trade.instrument.isin in known:
            kept.append(trade)
        else:
            skipped.append(trade)
    return kept, skipped


__all__ = [
    "IngestResult",
    "LoadedStatement",
    "ingest_parsed",
    "ingest_statement",
    "ingest_statements",
    "load_statement",
]
