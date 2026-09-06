-- 019_fx_event_sources.sql — resolve a run's synthetic FX event ids to their sources.
--
-- Why this migration exists
-- -------------------------
-- The FX engine consumes cashflows that are not trades — futures
-- realisation P&L, dividends and withholding tax, bond coupons, cash
-- events — and gives each one a synthetic integer id from a disjoint
-- high range (`docs/rules.md` §The engine runner) so it can sit in
-- the same `Acquisition` / `Disposal` streams as the real trades.
-- `matched_disposals` stores those ids verbatim in
-- `disposal_trade_id` / `acquisition_trade_id`. Persisted as-is they
-- resolve to nothing: the ids are allocated per run and mean nothing
-- outside it. This table is the run-scoped map from synthetic id to
-- the real row it stood for, so the audit commands and the Tier D
-- checks (D4) can follow every persisted reference.
--
-- Shape
-- -----
-- A discriminated union in the same style as `matched_disposals.
-- basis_kind`: `kind` says which of the reference columns is set,
-- and the CHECK makes exactly the right one non-NULL. One row per
-- synthetic id per run; the partial unique indexes make every
-- *source* unique per run too (one id per realisation, dividend,
-- coupon or cash event), so the map is a bijection in both
-- directions.
--
-- Like `future_realisations`, the referenced ids are not foreign keys:
-- the row is audit data that must outlive a re-ingest so a dangling
-- reference can be reported rather than silently cascaded away.

CREATE TABLE fx_event_sources (
    run_id         INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    event_id       INTEGER NOT NULL,
    kind           TEXT    NOT NULL
        CHECK (kind IN ('FUTURE_REALISATION', 'DIVIDEND', 'BOND_COUPON', 'CASH_EVENT')),
    open_trade_id  INTEGER,
    close_trade_id INTEGER,
    dividend_id    INTEGER,
    bond_coupon_id INTEGER,
    cash_event_id  INTEGER,
    PRIMARY KEY (run_id, event_id),
    CHECK (
        (kind = 'FUTURE_REALISATION'
            AND open_trade_id IS NOT NULL AND close_trade_id IS NOT NULL
            AND dividend_id IS NULL AND bond_coupon_id IS NULL AND cash_event_id IS NULL)
     OR (kind = 'DIVIDEND'
            AND dividend_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND bond_coupon_id IS NULL AND cash_event_id IS NULL)
     OR (kind = 'BOND_COUPON'
            AND bond_coupon_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND dividend_id IS NULL AND cash_event_id IS NULL)
     OR (kind = 'CASH_EVENT'
            AND cash_event_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND dividend_id IS NULL AND bond_coupon_id IS NULL)
    )
) STRICT;

-- One synthetic id per source per run, in both directions.
CREATE UNIQUE INDEX ux_fx_event_sources_realisation
    ON fx_event_sources (run_id, open_trade_id, close_trade_id)
    WHERE kind = 'FUTURE_REALISATION';
CREATE UNIQUE INDEX ux_fx_event_sources_dividend
    ON fx_event_sources (run_id, dividend_id)
    WHERE kind = 'DIVIDEND';
CREATE UNIQUE INDEX ux_fx_event_sources_coupon
    ON fx_event_sources (run_id, bond_coupon_id)
    WHERE kind = 'BOND_COUPON';
CREATE UNIQUE INDEX ux_fx_event_sources_cash_event
    ON fx_event_sources (run_id, cash_event_id)
    WHERE kind = 'CASH_EVENT';
