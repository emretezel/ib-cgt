-- 007_matched_disposals_fees.sql — track buy / sell fees on every match.
--
-- Why this migration exists
-- -------------------------
-- `matched_disposals` previously stored only the post-fee totals
-- `matched_proceeds_gbp` and `matched_cost_gbp`. The fee components
-- were collapsed into those figures via subset semantics:
--
--   * `matched_cost_gbp`   = principal + buy fees (in GBP)
--   * `matched_proceeds_gbp` = principal − sell fees (in GBP)
--
-- Audit reports (`ib-cgt match stocks`, eventually the SA108 export)
-- need to render the buy and sell fee components alongside the totals
-- so the taxpayer can reconcile against IB's per-trade commissions.
-- That requires the fee components to be persisted as their own facts.
--
-- Schema change
-- -------------
--   * `matched_acquisition_fees_gbp` TEXT NOT NULL — the pro-rated
--     buy-side fee share for this chunk (subset of
--     `matched_cost_gbp`). Always set; defaults to '0' for back-fill.
--   * `matched_disposal_fees_gbp` TEXT NOT NULL — the pro-rated
--     sell-side fee share for this chunk. Always set.
--   * `pool_total_fees_before` TEXT NULL — the pool's accumulated
--     buy-side fees at the moment of a S.104 draw. Only set when
--     `basis_kind = 'POOL'`, mirroring the other `pool_*` columns.
--
-- SQLite cannot ALTER STRICT tables to add NOT NULL columns without
-- defaults, and mixing ADD COLUMN with the existing copy/drop/rename
-- pattern would leave the migration history inconsistent. So this
-- follows the same path as migration 005.
--
-- Backfill
-- --------
-- Any existing `matched_disposals` rows pre-date the engine's fee-
-- tracking change — the original computation didn't record fees, so
-- we cannot reconstruct them. The migration writes '0' for both new
-- NOT NULL columns; downstream readers should treat zero on a
-- pre-migration row as "unknown / unreported", not "no fees".
--
-- In practice the table is empty in every active deployment (the
-- calculator orchestrator is not yet built; the read-only `match
-- stocks` CLI does not persist), so the backfill is a no-op there.

CREATE TABLE matched_disposals_new (
    run_id                       INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    disposal_trade_id            INTEGER NOT NULL,
    instrument_id                INTEGER NOT NULL REFERENCES instruments(instrument_id),
    disposal_date                TEXT    NOT NULL,
    match_rule                   TEXT    NOT NULL,
    matched_quantity             TEXT    NOT NULL,
    matched_proceeds_gbp         TEXT    NOT NULL,
    matched_cost_gbp             TEXT    NOT NULL,
    matched_acquisition_fees_gbp TEXT    NOT NULL,
    matched_disposal_fees_gbp    TEXT    NOT NULL,
    basis_kind                   TEXT    NOT NULL CHECK (basis_kind IN ('DIRECT', 'POOL')),
    acquisition_trade_id         INTEGER,
    pool_quantity_before         TEXT,
    pool_total_cost_before       TEXT,
    pool_average_cost            TEXT,
    pool_total_fees_before       TEXT,
    seq                          INTEGER NOT NULL,
    PRIMARY KEY (run_id, disposal_trade_id, seq)
) STRICT;

INSERT INTO matched_disposals_new (
    run_id, disposal_trade_id, instrument_id, disposal_date,
    match_rule, matched_quantity, matched_proceeds_gbp, matched_cost_gbp,
    matched_acquisition_fees_gbp, matched_disposal_fees_gbp,
    basis_kind, acquisition_trade_id,
    pool_quantity_before, pool_total_cost_before, pool_average_cost,
    pool_total_fees_before,
    seq
)
SELECT
    run_id, disposal_trade_id, instrument_id, disposal_date,
    match_rule, matched_quantity, matched_proceeds_gbp, matched_cost_gbp,
    -- New NOT NULL columns: '0' for any pre-existing row. See header.
    '0', '0',
    basis_kind, acquisition_trade_id,
    pool_quantity_before, pool_total_cost_before, pool_average_cost,
    -- New nullable column: NULL for both DIRECT and pre-migration POOL
    -- rows (we cannot reconstruct historical pool fees).
    NULL,
    seq
FROM matched_disposals;

DROP INDEX ix_matched_disposals_run;
DROP TABLE matched_disposals;

ALTER TABLE matched_disposals_new RENAME TO matched_disposals;

-- Recreate the (run_id, disposal_date) index from migration 001/005 —
-- name unchanged so downstream EXPLAIN paths stay readable.
CREATE INDEX ix_matched_disposals_run ON matched_disposals (run_id, disposal_date);
