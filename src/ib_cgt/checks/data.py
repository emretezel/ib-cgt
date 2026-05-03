"""Tier A — pre-match data integrity (SQL only).

Each check is a single SQL query that should return zero rows
against a healthy database. A non-zero row count means the
corresponding invariant is broken; the offending rows are surfaced
as the `Finding`'s `evidence`.

The checks run independently of the rule engines — they catch
ingestion bugs, hand-edits, and FX-cache gaps before any matching
work is attempted. Catching these first means a Tier B failure
later in the run is unambiguously an engine concern, not garbled
input.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Final
from zoneinfo import ZoneInfo

from ib_cgt.checks.framework import (
    CheckContext,
    Finding,
    Scope,
    Severity,
    Tier,
    register_check,
)
from ib_cgt.domain.enums import TradeAction

# Cap the number of evidence rows we surface per check. Auditors care
# about the existence and shape of the failure, not the full list when
# a corruption affects thousands of rows. The cap keeps JSON output
# bounded and the Rich renderer readable.
_EVIDENCE_LIMIT: Final = 20

# Valid `trades.action` values — derived from the canonical enum so a
# new TradeAction member automatically widens the allowed set.
_VALID_TRADE_ACTIONS: Final = frozenset(a.value for a in TradeAction)


def _rows_to_evidence(rows: Sequence[sqlite3.Row]) -> tuple[Mapping[str, object], ...]:
    """Convert a SQLite rowset into the immutable mapping shape Finding wants."""
    # `sqlite3.Row.__iter__` yields *values*, not column names — so we
    # zip explicit keys against the row to build a plain dict. Avoids
    # the SIM118 rewrite trap (`for k in r` does not iterate keys).
    return tuple(dict(zip(r.keys(), r, strict=True)) for r in rows[:_EVIDENCE_LIMIT])


# ---------------------------------------------------------------------------
# A1 — every trades.account_id resolves
# ---------------------------------------------------------------------------


@register_check(
    name="A1",
    description="every trades.account_id resolves in accounts",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_trade_account_fk(ctx: CheckContext) -> Finding:
    rows = ctx.conn.execute(
        "SELECT t.trade_id, t.account_id "
        "FROM trades t LEFT JOIN accounts a ON a.account_id = t.account_id "
        "WHERE a.account_id IS NULL"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} trade(s) reference unknown account(s)",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A2 — every trades.instrument_id resolves
# ---------------------------------------------------------------------------


@register_check(
    name="A2",
    description="every trades.instrument_id resolves in instruments",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_trade_instrument_fk(ctx: CheckContext) -> Finding:
    rows = ctx.conn.execute(
        "SELECT t.trade_id, t.instrument_id "
        "FROM trades t LEFT JOIN instruments i ON i.instrument_id = t.instrument_id "
        "WHERE i.instrument_id IS NULL"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} trade(s) reference unknown instrument(s)",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A3 — every instrument has the matching child row for its asset_class
# ---------------------------------------------------------------------------


@register_check(
    name="A3",
    description="every instruments row has a child row matching its asset_class",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_instrument_class_table(ctx: CheckContext) -> Finding:
    # Each parent row must appear in exactly the child table named by
    # its `asset_class`. v_instruments is an UNION of the four joins,
    # so any parent missing its child fails to project — the LEFT JOIN
    # against the view exposes the gap.
    rows = ctx.conn.execute(
        "SELECT i.instrument_id, i.asset_class "
        "FROM instruments i "
        "LEFT JOIN v_instruments v ON v.instrument_id = i.instrument_id "
        "WHERE v.instrument_id IS NULL"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} instrument(s) missing the asset-class child row",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A4 — trades.action is in the canonical enum set
# ---------------------------------------------------------------------------


@register_check(
    name="A4",
    description="trades.action is one of the canonical TradeAction values",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_trade_action_domain(ctx: CheckContext) -> Finding:
    placeholders = ",".join("?" for _ in _VALID_TRADE_ACTIONS)
    rows = ctx.conn.execute(
        f"SELECT trade_id, action FROM trades WHERE action NOT IN ({placeholders})",
        tuple(_VALID_TRADE_ACTIONS),
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} trade(s) carry an unknown action value",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A5 — quantity > 0; price_amount, fees_amount >= 0 (Decimal-aware)
# ---------------------------------------------------------------------------


@register_check(
    name="A5",
    description="trades.quantity > 0 and price_amount, fees_amount >= 0",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_trade_amount_signs(ctx: CheckContext) -> Finding:
    # Decimal codec stores TEXT; SQL numeric comparison is unsafe with
    # TEXT (lexicographic) so we read every row's three numeric columns
    # and check in Python with Decimal. The trades table is small
    # enough that the round-trip is cheap.
    rows = ctx.conn.execute(
        "SELECT trade_id, quantity, price_amount, fees_amount FROM trades"
    ).fetchall()
    bad: list[Mapping[str, object]] = []
    for r in rows:
        try:
            qty = Decimal(str(r["quantity"]))
            price = Decimal(str(r["price_amount"]))
            fees = Decimal(str(r["fees_amount"]))
        except (InvalidOperation, ValueError):
            bad.append(
                {
                    "trade_id": r["trade_id"],
                    "issue": "non-decimal value in numeric column",
                    "quantity": r["quantity"],
                    "price_amount": r["price_amount"],
                    "fees_amount": r["fees_amount"],
                }
            )
            continue
        if qty <= 0 or price < 0 or fees < 0:
            bad.append(
                {
                    "trade_id": r["trade_id"],
                    "quantity": str(qty),
                    "price_amount": str(price),
                    "fees_amount": str(fees),
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} trade(s) have non-positive quantity or negative price/fees",
        evidence=tuple(bad[:_EVIDENCE_LIMIT]),
    )


# ---------------------------------------------------------------------------
# A6 — trade_date <= settlement_date
# ---------------------------------------------------------------------------


@register_check(
    name="A6",
    description="trade_date <= settlement_date",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_settlement_after_trade(ctx: CheckContext) -> Finding:
    # ISO-8601 date strings sort correctly under TEXT comparison.
    rows = ctx.conn.execute(
        "SELECT trade_id, trade_date, settlement_date FROM trades "
        "WHERE settlement_date < trade_date"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} trade(s) settle before their trade date",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A7 — trade_date matches the UK-local projection of trade_datetime
# ---------------------------------------------------------------------------


@register_check(
    name="A7",
    description="trade_date matches the UK-local projection of trade_datetime",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_datetime_date_consistency(ctx: CheckContext) -> Finding:
    # `trade_datetime` is stored UTC but `trade_date` is its
    # **UK-local** date by domain convention (`Trade.uk_date_of`).
    # SQLite's `date(...)` extracts the UTC date and so spuriously
    # flags trades that cross the UK midnight boundary; we reproduce
    # the domain rule in Python instead.
    uk = ZoneInfo("Europe/London")
    rows = ctx.conn.execute("SELECT trade_id, trade_datetime, trade_date FROM trades").fetchall()
    bad: list[Mapping[str, object]] = []
    for r in rows:
        try:
            dt = datetime.fromisoformat(str(r["trade_datetime"]))
        except ValueError:
            bad.append(
                {
                    "trade_id": r["trade_id"],
                    "issue": "unparseable trade_datetime",
                    "trade_datetime": r["trade_datetime"],
                }
            )
            continue
        if dt.tzinfo is None:
            bad.append(
                {
                    "trade_id": r["trade_id"],
                    "issue": "naive trade_datetime",
                    "trade_datetime": r["trade_datetime"],
                }
            )
            continue
        expected = dt.astimezone(uk).date().isoformat()
        if expected != r["trade_date"]:
            bad.append(
                {
                    "trade_id": r["trade_id"],
                    "trade_datetime": r["trade_datetime"],
                    "trade_date": r["trade_date"],
                    "expected_uk_date": expected,
                }
            )
    if not bad:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(bad)} trade(s) have trade_date that doesn't match UK-local of trade_datetime",
        evidence=tuple(bad[:_EVIDENCE_LIMIT]),
    )


# ---------------------------------------------------------------------------
# A8 — for stock/bond/future trades, price_currency matches instrument.currency
# ---------------------------------------------------------------------------


@register_check(
    name="A8",
    description=("stock/bond/future trades have price_currency matching instrument.currency"),
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.WARN,
)
def _check_price_currency_matches_instrument(ctx: CheckContext) -> Finding:
    # FX trades intentionally have a different shape (price quoted in
    # the quote currency of the pair, which equals the trade-leg
    # currency but not always the instrument's `currency` field), so
    # we exclude them here.
    rows = ctx.conn.execute(
        "SELECT t.trade_id, t.price_currency, v.currency AS instrument_currency, "
        "v.asset_class "
        "FROM trades t JOIN v_instruments v ON v.instrument_id = t.instrument_id "
        "WHERE v.asset_class IN ('stock','bond','future') "
        "AND t.price_currency != v.currency"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} trade(s) have a price_currency / instrument-currency mismatch",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A9 — every trades.source_statement_hash resolves
# ---------------------------------------------------------------------------


@register_check(
    name="A9",
    description="every trades.source_statement_hash resolves in statements",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_trade_statement_fk(ctx: CheckContext) -> Finding:
    rows = ctx.conn.execute(
        "SELECT t.trade_id, t.source_statement_hash "
        "FROM trades t LEFT JOIN statements s "
        "ON s.statement_hash = t.source_statement_hash "
        "WHERE s.statement_hash IS NULL"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} trade(s) reference unknown statement hash(es)",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A10 — same (account_id, source_path) seen with two different hashes
# ---------------------------------------------------------------------------


@register_check(
    name="A10",
    description="no two statements share (account_id, source_path) with different hashes",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.WARN,
)
def _check_statement_path_collisions(ctx: CheckContext) -> Finding:
    # Two distinct hashes for one (account, file path) means the same
    # source file was re-ingested after edits. Usually unintentional.
    rows = ctx.conn.execute(
        "SELECT account_id, source_path, COUNT(*) AS hash_count "
        "FROM statements "
        "GROUP BY account_id, source_path "
        "HAVING COUNT(DISTINCT statement_hash) > 1"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=(f"{len(rows)} (account, source_path) tuple(s) seen with multiple hashes"),
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A11 — fx_rates covers every non-GBP price_currency for the trade-date span
# ---------------------------------------------------------------------------


# Mirror `FXService`'s default `fallback_days` window so this check
# matches what the engine actually does at compute time. ECB doesn't
# publish weekend or bank-holiday rates; FX markets trade on those
# days regardless, so the engine rolls back to the most recent
# business-day rate. A11 should only flag dates where even the roll-
# back fails (a real cache hole), not every Sunday/holiday with no
# exact match.
_FX_FALLBACK_DAYS: Final = 10


@register_check(
    name="A11",
    description=(
        "fx_rates resolves (with FXService roll-back fallback) for every non-GBP trade date"
    ),
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.WARN,
)
def _check_fx_coverage(ctx: CheckContext) -> Finding:
    # The engine rolls back up to `_FX_FALLBACK_DAYS` calendar days to
    # the most recent published business-day rate (see
    # `FXService._lookup_rate` / `FXRateRepo.get_latest_on_or_before`).
    # A11 reproduces that semantics: only flag a (currency, date) pair
    # if no rate exists in the trailing window — those are the cases
    # that would actually fail at compute time.
    rows = ctx.conn.execute(
        "SELECT DISTINCT t.price_currency AS currency, t.trade_date "
        "FROM trades t "
        "WHERE t.price_currency != 'GBP' "
        "AND NOT EXISTS ( "
        "  SELECT 1 FROM fx_rates r "
        "  WHERE r.base = 'GBP' AND r.quote = t.price_currency "
        "  AND r.rate_date <= t.trade_date "
        "  AND r.rate_date >= date(t.trade_date, ?) "
        ")",
        (f"-{_FX_FALLBACK_DAYS} days",),
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=(
            f"{len(rows)} (currency, trade_date) pair(s) with no rate within "
            f"{_FX_FALLBACK_DAYS} business-day fallback window"
        ),
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A12 — instruments.symbol non-empty; currency is a 3-letter ISO code
# ---------------------------------------------------------------------------


@register_check(
    name="A12",
    description="instruments.symbol non-empty and currency is a 3-letter ISO code",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.ERROR,
)
def _check_instrument_symbol_currency(ctx: CheckContext) -> Finding:
    rows = ctx.conn.execute(
        "SELECT instrument_id, asset_class, symbol, currency "
        "FROM v_instruments "
        "WHERE symbol IS NULL OR symbol = '' "
        "OR currency IS NULL "
        "OR length(currency) != 3 "
        "OR currency != upper(currency)"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} instrument(s) have an empty symbol or malformed currency",
        evidence=_rows_to_evidence(rows),
    )


# ---------------------------------------------------------------------------
# A13 — dividends FK + sane pay_date
# ---------------------------------------------------------------------------


@register_check(
    name="A13",
    description="dividends reference live instrument/account and have plausible pay_date",
    tier=Tier.A,
    scopes={Scope.ALL, Scope.DATA},
    severity=Severity.WARN,
)
def _check_dividends_referential(ctx: CheckContext) -> Finding:
    rows = ctx.conn.execute(
        "SELECT d.dividend_id, d.account_id, d.instrument_id, d.pay_date "
        "FROM dividends d "
        "LEFT JOIN accounts a ON a.account_id = d.account_id "
        "LEFT JOIN instruments i ON i.instrument_id = d.instrument_id "
        "WHERE a.account_id IS NULL OR i.instrument_id IS NULL "
        "OR d.pay_date IS NULL OR d.pay_date = ''"
    ).fetchall()
    if not rows:
        return Finding(triggered=False)
    return Finding(
        triggered=True,
        detail=f"{len(rows)} dividend row(s) with broken refs or missing pay_date",
        evidence=_rows_to_evidence(rows),
    )
