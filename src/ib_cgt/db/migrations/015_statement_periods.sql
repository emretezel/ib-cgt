-- 015_statement_periods.sql — record the period each statement covers.
--
-- Why this migration exists
-- -------------------------
-- The calculator needs to know how far the ingested history reaches
-- for each account: whether a tax year is fully covered, whether the
-- 30-day look-ahead after the year end is visible yet, and which
-- statement is the *latest* one whose Open Positions section the
-- trade-derived positions must reconcile against. Until now the
-- `statements` row carried no dates at all — the only date signal was
-- the trades themselves, which say nothing about a quiet month.
--
-- Every IB activity statement prints its period in the page title
-- (`"U… Activity Statement April 7, 2025 - April 3, 2026 - …"`), so the
-- parser can supply both bounds for every file.
--
-- Why the table is wiped
-- ----------------------
-- The two new columns are NOT NULL (a statement without a period is
-- not a statement), and nothing already stored can supply them — the
-- period lives in the HTML, not in any row. The only honest option is
-- to rebuild `statements` empty and re-ingest every file; migration
-- 014 set the same precedent for the bond re-keying. The cascades on
-- `trades`, `dividends` and `bond_coupons` clear the dependent rows
-- with the statements, and re-ingesting is idempotent and quick.
-- `tax_runs` is unaffected (it does not reference statements).
--
-- Schema change
-- -------------
--   period_start, period_end   ISO dates (`YYYY-MM-DD`), inclusive,
--                              with `period_start <= period_end`.
--   ix_statements_account_period   serves `StatementRepo.latest_for_
--                              account` ("the newest statement for
--                              this account") — the reconciliation
--                              and coverage read path.

-- 1) Clear dependents first so the parent delete is FK-clean, then
--    the parent. Explicit deletes rather than relying on cascade
--    through a DROP so the intent is visible in the migration itself.
DELETE FROM trades;
DELETE FROM dividends;
DELETE FROM bond_coupons;
DELETE FROM statements;

-- 2) Rebuild the table with the period columns. Same rebuild shape as
--    migrations 005 / 006 (create new, drop old, rename), minus the
--    copy step because there is nothing to copy.
CREATE TABLE statements_new (
    statement_hash TEXT    PRIMARY KEY,
    source_path    TEXT    NOT NULL,
    account_id     TEXT    NOT NULL REFERENCES accounts(account_id),
    imported_at    TEXT    NOT NULL,
    trade_count    INTEGER NOT NULL,
    period_start   TEXT    NOT NULL,
    period_end     TEXT    NOT NULL,
    CHECK (period_start <= period_end)
) STRICT;

DROP TABLE statements;

ALTER TABLE statements_new RENAME TO statements;

-- 3) "Latest statement per account": `WHERE account_id = ? ORDER BY
--    period_end DESC LIMIT 1` walks this index backwards and stops at
--    the first row.
CREATE INDEX ix_statements_account_period ON statements (account_id, period_end);
