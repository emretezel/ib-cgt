-- 018_future_realisations.sql — persist closed-out futures contracts per tax run.
--
-- Why this migration exists
-- -------------------------
-- `FutureRuleEngine` produces `FutureRealisation` rows — one per
-- (open slice, close portion) pair under the TCGA92 s.143(5)
-- close-out model — and until now nothing stored them: `tax_runs`
-- and `matched_disposals` only ever modelled the four-rule matching
-- output of stocks, bonds and FX. `ib-cgt compute --year` needs a
-- home for the futures side of the run so the reporting layer can
-- read a whole year back without re-running the engines, and so the
-- Tier D checks can compare persisted rows to a fresh recompute.
--
-- Why a new table and not `matched_disposals`
-- -------------------------------------------
-- A futures close-out is not a matched chunk: there is no matching
-- rule, no pool basis, and the "proceeds" are a signed net cashflow
-- rather than gross consideration (`docs/rules.md` §FutureRuleEngine).
-- Forcing the shape into `matched_disposals` would leave half of its
-- columns meaningless and the CHECK on `basis_kind` unsatisfiable.
-- One table per thing (AGENTS.md §3).
--
-- Why there is no currency column
-- -------------------------------
-- Every native amount on a realisation is in the contract's own
-- currency — `FutureRealisation.__post_init__` pins it — and the
-- contract is `instrument_id`. A `currency` column would be a
-- transitive dependency on `future_instruments.currency` (3NF
-- violation); readers rebuild the `Money` values with the loaded
-- instrument's currency instead.
--
-- Why the trade ids are not foreign keys
-- --------------------------------------
-- Same reasoning as `matched_disposals.disposal_trade_id`: the row
-- is an audit record of what the run computed. If the underlying
-- statement is withdrawn and re-ingested the trade ids change, and
-- the audit row must survive that so a later `check` can report the
-- dangling reference (D6) rather than the row silently vanishing.
--
-- Why `tax_runs` is emptied first
-- -------------------------------
-- No writer for `tax_runs` existed before this migration (`compute`
-- ships with it), so the table is empty on every deployment; the
-- DELETE is a belt-and-braces guarantee that no run header can exist
-- without the futures rows the calculator would have written.
--
-- Schema overview
-- ---------------
--   run_id                FK to the run; cascades with it.
--   open_trade_id,        The realisation's identity within the run
--   close_trade_id        (one open slice is drained at most once per
--                         close trade); UNIQUE per run.
--   instrument_id         FK; the futures contract.
--   side                  'LONG' | 'SHORT'.
--   open_date, close_date ISO dates; close_date is the disposal date.
--   quantity              contracts closed, Decimal-as-TEXT.
--   gross_pnl_native      signed net cashflow in the contract's currency.
--   open_fee_native,      commission shares, non-negative.
--   close_fee_native
--   open_fx_rate,         the "1 GBP = r native" rates applied.
--   close_fx_rate
--   proceeds_gbp          signed; gross_pnl_native at close_fx_rate.
--   cost_gbp              non-negative; both fees at their own rates.
--   seq                   per-close-trade emit order (FIFO slices).

DELETE FROM tax_runs;

CREATE TABLE future_realisations (
    run_id           INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    open_trade_id    INTEGER NOT NULL,
    close_trade_id   INTEGER NOT NULL,
    instrument_id    INTEGER NOT NULL REFERENCES instruments(instrument_id),
    side             TEXT    NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    open_date        TEXT    NOT NULL,
    close_date       TEXT    NOT NULL,
    quantity         TEXT    NOT NULL,
    gross_pnl_native TEXT    NOT NULL,
    open_fee_native  TEXT    NOT NULL,
    close_fee_native TEXT    NOT NULL,
    open_fx_rate     TEXT    NOT NULL,
    close_fx_rate    TEXT    NOT NULL,
    proceeds_gbp     TEXT    NOT NULL,
    cost_gbp         TEXT    NOT NULL,
    seq              INTEGER NOT NULL CHECK (seq >= 0),
    PRIMARY KEY (run_id, close_trade_id, seq),
    UNIQUE (run_id, open_trade_id, close_trade_id),
    CHECK (open_trade_id <> close_trade_id),
    CHECK (open_date <= close_date)
) STRICT;

-- "Every realisation of run N in disposal-date order" — the reporting
-- read path and the D2 net-gain check.
CREATE INDEX ix_future_realisations_run ON future_realisations (run_id, close_date);
