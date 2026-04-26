-- 004_cascade_trades_on_statement_delete.sql — let statement deletes cascade to trades.
--
-- Why this migration exists
-- -------------------------
-- During development the iteration loop "edit parser → re-ingest the
-- same statement to see the effect" is the most common workflow. Up
-- to migration 003 the FK on `trades.source_statement_hash` used the
-- default RESTRICT rule, so removing a `statements` row required
-- deleting its dependent `trades` rows by hand first. With CASCADE on,
-- a single `DELETE FROM statements WHERE statement_hash = ?` (or its
-- equivalent inside `ingest_statement(..., replace=True)`) cleans the
-- whole subtree atomically — exactly what dev-time re-ingest wants.
--
-- SQLite cannot alter an FK clause in place (`ALTER TABLE … ALTER
-- CONSTRAINT` does not exist), so the table must be rebuilt:
-- copy → drop → rename. The rebuild is safe to run with FKs ON
-- because no row's reference target changes — we are only altering
-- the cascade rule for *future* deletes, and every existing row's
-- `source_statement_hash` already points at a real `statements` row.

CREATE TABLE trades_new (
    trade_key             TEXT    PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    instrument_id         INTEGER NOT NULL REFERENCES instruments(instrument_id),
    action                TEXT    NOT NULL,
    trade_datetime        TEXT    NOT NULL,
    trade_date            TEXT    NOT NULL,
    settlement_date       TEXT    NOT NULL,
    quantity              TEXT    NOT NULL,
    price_amount          TEXT    NOT NULL,
    price_currency        TEXT    NOT NULL,
    fees_amount           TEXT    NOT NULL,
    fees_currency         TEXT    NOT NULL,
    accrued_amount        TEXT,
    accrued_currency      TEXT,
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE
) STRICT;

-- Copy the existing data verbatim — column order must match the new
-- table, which is identical to the old except for the FK clause.
INSERT INTO trades_new SELECT * FROM trades;

-- Drop the old indexes before dropping the table; SQLite will refuse
-- to drop a table whose dependents are still around, even though
-- indexes specifically aren't blocking. Order is defensive.
DROP INDEX ix_trades_instrument_dt;
DROP INDEX ix_trades_trade_date;
DROP INDEX ix_trades_account_date;
DROP INDEX ix_trades_statement;

DROP TABLE trades;

ALTER TABLE trades_new RENAME TO trades;

-- Recreate every index from migration 001. Names are unchanged so
-- references in `docs/db/trades.md` and EXPLAIN output stay valid.
CREATE INDEX ix_trades_instrument_dt ON trades (instrument_id, trade_datetime);
CREATE INDEX ix_trades_trade_date    ON trades (trade_date);
CREATE INDEX ix_trades_account_date  ON trades (account_id, trade_date);
CREATE INDEX ix_trades_statement     ON trades (source_statement_hash);
