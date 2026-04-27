-- 005_trades_integer_pk.sql — replace SHA `trade_key` with INTEGER surrogate PK.
--
-- Why this migration exists
-- -------------------------
-- The pre-005 schema used a 64-char hex SHA-256 (`trade_key TEXT PK`) over
-- a canonical string of fields that are also stored as columns on the
-- same row. That double-encoded the same facts (mild violation of
-- CLAUDE.md §3 "single source of truth") and bought content-addressable
-- identity that the project does not actually use. The cleaner design is
-- a surrogate `trade_id INTEGER PK` plus a `UNIQUE` on the natural-key
-- columns; `INSERT OR IGNORE` on that UNIQUE preserves the same dedup
-- property the SHA gave us, declaratively.
--
-- The minimal natural key is the smallest set of `trades` columns that
-- distinguish two trades on the same instrument. After migration 003
-- (instruments-as-class-table), that's:
--   (account_id, instrument_id, trade_datetime, action, quantity, price_amount)
-- Asset-class details (symbol, currency, expiry, fx pair) are already
-- implicit in `instrument_id`. Fees, accrued interest, settlement_date,
-- and trade_date are excluded for the same reason the SHA excluded them
-- — cosmetic export differences must not produce duplicate rows.
--
-- Operational note
-- ----------------
-- `matched_disposals` references the old SHA in `disposal_trade_key` and
-- `acquisition_trade_key`. Those values cannot be back-mapped to the
-- freshly-issued INTEGERs, so the migration empties `matched_disposals`
-- (and its parent `tax_runs`, to avoid headerless audit rows) before
-- recreating both tables with the new column types. The compute command
-- is not yet wired up; in every deployed environment these tables hold
-- zero rows, so the truncate has no observable effect. Subsequent
-- `compute --year` invocations repopulate them with integer references.

-- 1) Wipe the audit-trail tables. CASCADE on tax_runs → matched_disposals
--    handles the dependent rows automatically.
DELETE FROM tax_runs;

-- 2) Drop and recreate matched_disposals with INTEGER trade references.
--    PK shape stays composite (run_id, disposal_trade_id, seq) — only
--    the disposal column type changes.
DROP TABLE matched_disposals;

CREATE TABLE matched_disposals (
    run_id                 INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    disposal_trade_id      INTEGER NOT NULL,
    instrument_id          INTEGER NOT NULL REFERENCES instruments(instrument_id),
    disposal_date          TEXT    NOT NULL,
    match_rule             TEXT    NOT NULL,
    matched_quantity       TEXT    NOT NULL,
    matched_proceeds_gbp   TEXT    NOT NULL,
    matched_cost_gbp       TEXT    NOT NULL,
    basis_kind             TEXT    NOT NULL CHECK (basis_kind IN ('DIRECT', 'POOL')),
    acquisition_trade_id   INTEGER,
    pool_quantity_before   TEXT,
    pool_total_cost_before TEXT,
    pool_average_cost      TEXT,
    seq                    INTEGER NOT NULL,
    PRIMARY KEY (run_id, disposal_trade_id, seq)
) STRICT;

CREATE INDEX ix_matched_disposals_run ON matched_disposals (run_id, disposal_date);

-- 3) Rebuild trades with the new PK + UNIQUE. SQLite cannot ALTER a PK
--    in place; the canonical pattern is copy → drop → rename.
CREATE TABLE trades_new (
    trade_id              INTEGER PRIMARY KEY,
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
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (
        account_id, instrument_id, trade_datetime,
        action, quantity, price_amount
    )
) STRICT;

-- Copy every column except `trade_key` (which is gone) and `trade_id`
-- (which the INTEGER PK auto-issues fresh).
INSERT INTO trades_new (
    account_id, instrument_id, action, trade_datetime, trade_date,
    settlement_date, quantity, price_amount, price_currency,
    fees_amount, fees_currency, accrued_amount, accrued_currency,
    source_statement_hash
)
SELECT
    account_id, instrument_id, action, trade_datetime, trade_date,
    settlement_date, quantity, price_amount, price_currency,
    fees_amount, fees_currency, accrued_amount, accrued_currency,
    source_statement_hash
FROM trades;

DROP INDEX ix_trades_instrument_dt;
DROP INDEX ix_trades_trade_date;
DROP INDEX ix_trades_account_date;
DROP INDEX ix_trades_statement;

DROP TABLE trades;

ALTER TABLE trades_new RENAME TO trades;

-- Recreate the four hot-path indexes from migration 001 (names unchanged
-- so EXPLAIN output and docs don't have to be updated).
CREATE INDEX ix_trades_instrument_dt ON trades (instrument_id, trade_datetime);
CREATE INDEX ix_trades_trade_date    ON trades (trade_date);
CREATE INDEX ix_trades_account_date  ON trades (account_id, trade_date);
CREATE INDEX ix_trades_statement     ON trades (source_statement_hash);
