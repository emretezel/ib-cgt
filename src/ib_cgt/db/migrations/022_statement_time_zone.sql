-- 022_statement_time_zone.sql — record the zone each statement's clock
-- times are printed in, and re-read every trade instant in it.
--
-- Why this migration exists
-- -------------------------
-- IB prints a trade's `Date/Time` with no offset, and says once per
-- statement which zone it is in: "Trade execution times are displayed
-- in Eastern Time." (Notes/Legal Notes, every vintage, HTML and PDF).
-- Until now the ingest mapper attached Europe/London to that clock, so
-- every stored `trades.trade_datetime` is four to five hours early and
-- — because `trade_date` is the Europe/London date of the instant —
-- every fill printed at 19:00 Eastern or later (20:00 in the weeks the
-- US and UK clocks change on different dates) carries a trade date one
-- day early. In the live history that is 405 of 6,969 trades, and one
-- FX fill at 21:41 Eastern on 5 April 2024 belongs to 2024/25, not
-- 2023/24. Same-day matching, the 30-day rule, the FX rate day and the
-- tax-year cut-off all read `trade_date`, so the instants must be
-- re-read.
--
-- The parser now reads the note and carries the zone; the mapper
-- attaches it; and this table records it, so a trade's time can be
-- shown exactly as the file prints it (`show trade`), and a statement
-- in another zone — should IB ever print one — is read correctly and
-- visibly.
--
-- Why the table is wiped
-- ----------------------
-- Migrations are SQL, and SQLite cannot convert between time zones, so
-- the stored instants cannot be corrected here; nor can the new column
-- be filled — the zone lives in the file, not in any row. The coverage
-- decisions made at ingest ("the first statement ingested owns the
-- day") were also taken on the wrong dates. The honest path, as for
-- migrations 014, 015 and 021, is to wipe every statement-derived row
-- and re-ingest every statement after `db init`. Re-ingest is
-- idempotent on each file's SHA-256. `accounts`, `instruments`,
-- `fx_rates` and `schema_migrations` are untouched; the run tables
-- go because they were computed from the wrong dates.
--
-- Deletion order follows the FK graph:
--   tax_runs   → matched_disposals, future_realisations,
--                fx_event_sources, tax_run_issues        (CASCADE)
--   statements → trades, dividends, bond_coupons,
--                statement_positions, cash_events        (CASCADE)
--
-- Schema change
-- -------------
--   time_zone   IANA zone key (`America/New_York`) of the statement's
--               `Date/Time` cells, NOT NULL and non-empty. It is a fact
--               about the statement — the note applies to the whole
--               file — so it lives here, once, not on every trade.
--   The table is rebuilt (create new, drop old, rename) as 015 did:
--   `ALTER TABLE … ADD COLUMN` would need a DEFAULT for a NOT NULL
--   column on a STRICT table, i.e. a made-up zone.
--   ix_statements_account_period is recreated unchanged.

-- 1) Wipe the derived rows, parents last so every delete is FK-clean.
DELETE FROM tax_runs;
DELETE FROM statements;

-- 2) Rebuild `statements` with the zone column.
CREATE TABLE statements_new (
    statement_hash TEXT    PRIMARY KEY,
    source_path    TEXT    NOT NULL,
    account_id     TEXT    NOT NULL REFERENCES accounts(account_id),
    imported_at    TEXT    NOT NULL,
    trade_count    INTEGER NOT NULL,
    period_start   TEXT    NOT NULL,
    period_end     TEXT    NOT NULL,
    time_zone      TEXT    NOT NULL,
    CHECK (period_start <= period_end),
    CHECK (length(time_zone) > 0)
) STRICT;

DROP TABLE statements;

ALTER TABLE statements_new RENAME TO statements;

-- 3) "Latest statement per account" — `WHERE account_id = ? ORDER BY
--    period_end DESC LIMIT 1` walks this index backwards.
CREATE INDEX ix_statements_account_period ON statements (account_id, period_end);
