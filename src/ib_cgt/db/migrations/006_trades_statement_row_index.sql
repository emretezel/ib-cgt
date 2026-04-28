-- 006_trades_statement_row_index.sql — restore 1:1 fill ↔ row mapping.
--
-- Why this migration exists
-- -------------------------
-- Migration 005 made the natural-key tuple
--   (account_id, instrument_id, trade_datetime, action, quantity, price_amount)
-- a UNIQUE on `trades`. The intent was a declarative dedup for re-imports:
-- `INSERT OR IGNORE` on that constraint would silently skip rows we had
-- already absorbed.
--
-- Reality: IB's HTML activity statement legitimately emits N distinct
-- `<tr>` rows whose six business columns are identical when one parent
-- order is filled across several ticks at the same wall-clock second.
-- Example observed in `statements/futures/23_24.htm` for FESX DEC 23
-- on 2023-09-12: 8 rows at 05:58:26, each `qty=-1, price=4272.50`. The
-- old UNIQUE collapsed those 8 fills into 1 stored row, dropping 7. The
-- matching engine later saw 4 short slices instead of 15 and raised
-- `InconsistentTradeError` when a 14-contract close drained "more
-- contracts than exist".
--
-- The fix is to identify a `trades` row by its provenance instead of
-- by a synthesized business tuple — every fill in the statement is its
-- own row. Idempotency is now layered as:
--   1. Statement-hash short-circuit on `statements`     — same file, no work.
--   2. `ingest --replace` for re-ingesting a corrected statement.
-- Both already exist; this migration just removes the broken third
-- layer that was masking real data.
--
-- Schema change
-- -------------
--   * Add `statement_row_index INTEGER NOT NULL CHECK (statement_row_index >= 0)`
--     — the zero-based position of this fill within its source statement.
--     Source-of-truth for "which fill am I"; never collides within one
--     `source_statement_hash` because ingestion enumerates densely.
--   * Replace the natural-key UNIQUE with
--     `UNIQUE (source_statement_hash, statement_row_index)` — the new
--     (provenance, position) identity. Composite is unique by
--     construction; `INSERT OR IGNORE` still backstops a partial-batch
--     retry within one ingest.
--
-- SQLite cannot drop an inline UNIQUE in place, so this follows the
-- copy-drop-rename pattern used by migration 005.
--
-- Backfill ordering
-- -----------------
-- For rows already in `trades` we have no record of the original HTML
-- row order. We use `ROW_NUMBER() OVER (PARTITION BY source_statement_hash
-- ORDER BY trade_id) - 1` as the synthetic index. `trade_id` was assigned
-- in parse order at original insert time, so the synthetic index is
-- monotonic-but-not-necessarily-statement-faithful. That's fine: the
-- column's job here is uniqueness, not audit. To recover the *missing*
-- fills the buggy UNIQUE silently dropped, the operator must
-- `ib-cgt ingest <path> --replace` each affected statement after this
-- migration applies — re-ingest issues fresh, statement-faithful indices
-- via `enumerate(map_rows(...))`.

-- 1) Build the new table with the new column and the new UNIQUE.
--    Column order is unchanged from migration 005 except for the new
--    `statement_row_index` slotted right before `source_statement_hash`
--    (the column it's most logically grouped with).
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
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index)
) STRICT;

-- 2) Copy every existing row, synthesising `statement_row_index` per
--    statement via ROW_NUMBER. The window is partitioned by source
--    statement so indices are dense [0..N-1] within each statement.
INSERT INTO trades_new (
    trade_id, account_id, instrument_id, action,
    trade_datetime, trade_date, settlement_date, quantity,
    price_amount, price_currency, fees_amount, fees_currency,
    accrued_amount, accrued_currency,
    statement_row_index, source_statement_hash
)
SELECT
    trade_id, account_id, instrument_id, action,
    trade_datetime, trade_date, settlement_date, quantity,
    price_amount, price_currency, fees_amount, fees_currency,
    accrued_amount, accrued_currency,
    ROW_NUMBER() OVER (
        PARTITION BY source_statement_hash
        ORDER BY trade_id
    ) - 1 AS statement_row_index,
    source_statement_hash
FROM trades;

-- 3) Drop the four hot-path indexes (they're tied to the old `trades`
--    table identity; they'll be recreated against the renamed table
--    below) and the old table itself.
DROP INDEX ix_trades_instrument_dt;
DROP INDEX ix_trades_trade_date;
DROP INDEX ix_trades_account_date;
DROP INDEX ix_trades_statement;

DROP TABLE trades;

ALTER TABLE trades_new RENAME TO trades;

-- 4) Recreate the four hot-path indexes from migration 001/005. Names
--    unchanged so EXPLAIN output and docs/db/trades.md don't have to
--    move with this migration.
CREATE INDEX ix_trades_instrument_dt ON trades (instrument_id, trade_datetime);
CREATE INDEX ix_trades_trade_date    ON trades (trade_date);
CREATE INDEX ix_trades_account_date  ON trades (account_id, trade_date);
CREATE INDEX ix_trades_statement     ON trades (source_statement_hash);
