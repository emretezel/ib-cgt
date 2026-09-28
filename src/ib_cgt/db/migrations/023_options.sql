-- 023_options.sql — exchange-traded options become the fifth asset class.
--
-- Why this migration exists
-- -------------------------
-- Until now the parser dropped every row under IB's "Equity and Index
-- Options" heading (migration 008 scrubbed the rows an earlier parser
-- had let through as stocks). The taxpayer's history holds four option
-- series — two XAUUSD series written in 2012, an XSP put bought in 2013
-- and sold in 2014, and fifteen TUR puts bought in January 2019 and
-- exercised in May 2019 — and their premiums, commissions and
-- settlements are also foreign-currency cashflows the FX pools never
-- saw (HMRC CG78315). `docs/options.md` records the rules adopted on
-- 2026-09-28 (TCGA 1992 s.144 / s.144A / s.148; HMRC CG12312, CG12317,
-- CG55415, CG55536, CG55545); this migration gives them a home.
--
-- What changes
-- ------------
-- 1. `instruments.asset_class` admits 'option'. SQLite cannot alter a
--    CHECK, so the parent is rebuilt (create new → drop → rename, as
--    022 did for `statements`).
-- 2. `option_instruments` — the fifth asset-class child, keyed by IB's
--    `conid` like stocks and futures, carrying the series facts the
--    rule engine needs: underlying, multiplier, expiry, strike, right.
-- 3. `option_exercise_links` — ingest-time pairing of an exercised or
--    assigned option row with the share trade IB booked for it (one
--    transaction under s.144(2)-(3)). Real FKs to `trades`: the link is
--    a fact of the statement and dies with its rows.
-- 4. Three run tables, keyed like `future_realisations` (no FK to
--    `trades`, so an audit row survives a re-ingest and check D7 can
--    report a dangling id):
--      `option_grants`             one row per written option — the
--                                  disposal constituted by the grant
--                                  (s.144(1)), gross premium and fee;
--      `option_grant_closes`       every later event on a grant — a
--                                  closing purchase (s.148), a lapse,
--                                  an assignment, a cash settlement;
--      `option_exercise_transfers` the amount an exercise moved into
--                                  a share trade (s.144(2)-(3)).
-- 5. `tax_run_issues.kind` admits `option_grant_restated` (a close
--    dated in this year modifies an earlier year's grant — that year
--    must be recomputed and amended) and `option_exercise_unlinked`
--    (an exercise with no share trade beside it was treated as
--    cash-settled under s.144A). The table is dropped and recreated
--    with the wider CHECK; it is empty after the wipe below.
--
-- Why the wipe
-- ------------
-- The option rows were never ingested, so every statement must be read
-- again; adding them shifts the `statement_row_index` of the trades
-- that followed them, which changes trade ids and therefore every
-- persisted run. As with 014, 015, 021 and 022 the honest path is to
-- wipe every statement-derived row and every run and re-ingest after
-- `db init`. Re-ingest is idempotent on each file's SHA-256.
-- `accounts`, `fx_rates` and `schema_migrations` are untouched.
--
-- Deletion order follows the FK graph:
--   tax_runs    → matched_disposals, future_realisations,
--                 fx_event_sources, tax_run_issues              (CASCADE)
--   statements  → trades, dividends, bond_coupons,
--                 statement_positions, cash_events              (CASCADE)
--   instruments → {stock,bond,future,fx}_instruments            (CASCADE)
--
-- Why there is no currency column on the run tables
-- ---------------------------------------------------
-- Every native amount is in the series' own currency (pinned by the
-- domain shapes) and the series is `instrument_id`; a currency column
-- would be a transitive dependency on `option_instruments.currency`
-- (3NF). Readers rebuild the `Money` values from the loaded instrument.

-- 1) Wipe statement-derived rows and the runs computed from them.
DELETE FROM tax_runs;
DELETE FROM statements;
DELETE FROM instruments;

-- 2) Widen the discriminator by rebuilding the parent. The view must go
--    first (it references the table) and is recreated in step 4 with the
--    option arm; the children keep referencing `instruments` by name and
--    are empty after the wipe, so the drop is FK-clean.
DROP VIEW v_instruments;

CREATE TABLE instruments_new (
    instrument_id INTEGER PRIMARY KEY,
    asset_class   TEXT NOT NULL CHECK (asset_class IN ('stock', 'bond', 'future', 'fx', 'option'))
) STRICT;

DROP TABLE instruments;

ALTER TABLE instruments_new RENAME TO instruments;

-- 3) Options: one row per series — one underlying, one expiry, one
--    strike, one right — identified by IB's `conid` (the XSP put kept
--    conid 99465795 when IB renamed its root to XSPAM). `symbol` is IB's
--    display form as the instrument table's Description prints it; the
--    other columns are the series facts the rule engine computes with.
CREATE TABLE option_instruments (
    instrument_id       INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    conid               INTEGER NOT NULL CHECK (conid > 0),
    symbol              TEXT    NOT NULL,
    currency            TEXT    NOT NULL,
    underlying          TEXT    NOT NULL CHECK (length(underlying) > 0),
    contract_multiplier TEXT    NOT NULL,   -- Decimal string (canonical); units per contract.
    expiry_date         TEXT    NOT NULL,   -- YYYY-MM-DD.
    strike              TEXT    NOT NULL,   -- Decimal string; exercise price per unit.
    option_right        TEXT    NOT NULL CHECK (option_right IN ('call', 'put')),
    UNIQUE (conid)
) STRICT;

-- `(symbol, currency)` serves `InstrumentRepo.find_by_symbol` (the
-- ingest fallback for an Open Positions row without an instrument-
-- information row), `list_options(symbol=)` and `TradeRepo.list_filtered`'s
-- symbol join through the view; `(currency)` serves FX-sync's
-- `DISTINCT currency` walk, exactly as for the other children.
CREATE INDEX ix_option_instruments_symbol_currency ON option_instruments (symbol, currency);
CREATE INDEX ix_option_instruments_currency        ON option_instruments (currency);

-- 4) The flat projection over all five children, with the three
--    option-only columns appended (NULL for every other class).
CREATE VIEW v_instruments AS
SELECT i.instrument_id, i.asset_class,
       NULL AS isin,
       s.conid,
       s.symbol, s.currency,
       NULL AS is_cgt_exempt,
       NULL AS contract_multiplier,
       NULL AS expiry_date,
       NULL AS fx_base,
       NULL AS fx_quote,
       NULL AS underlying,
       NULL AS strike,
       NULL AS option_right
FROM instruments i JOIN stock_instruments s USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       b.isin,
       NULL,
       b.symbol, b.currency,
       b.is_cgt_exempt,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM instruments i JOIN bond_instruments b USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       NULL,
       f.conid,
       f.symbol, f.currency,
       NULL,
       f.contract_multiplier,
       f.expiry_date,
       NULL, NULL, NULL, NULL, NULL
FROM instruments i JOIN future_instruments f USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       NULL,
       NULL,
       fx.symbol, fx.currency,
       NULL, NULL, NULL,
       fx.fx_base,
       fx.fx_quote,
       NULL, NULL, NULL
FROM instruments i JOIN fx_instruments fx USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       NULL,
       o.conid,
       o.symbol, o.currency,
       NULL,
       o.contract_multiplier,
       o.expiry_date,
       NULL, NULL,
       o.underlying,
       o.strike,
       o.option_right
FROM instruments i JOIN option_instruments o USING (instrument_id);

-- 5) An exercised (`Ex`) or assigned (`A`) option row and the share
--    trade IB booked at the strike for it, paired at ingest from the
--    same statement. One share trade belongs to one option row and vice
--    versa. Both FKs cascade from `trades`, which cascades from the
--    statement, so a withdrawn statement takes its links with it.
CREATE TABLE option_exercise_links (
    option_trade_id INTEGER PRIMARY KEY REFERENCES trades(trade_id) ON DELETE CASCADE,
    share_trade_id  INTEGER NOT NULL UNIQUE REFERENCES trades(trade_id) ON DELETE CASCADE,
    CHECK (option_trade_id <> share_trade_id)
) STRICT;

-- 6) Written options: the disposal constituted by the grant. Gross
--    figures only — the chargeable premium and the gain are derived
--    from the closes below (assigned contracts leave the grant; closing
--    purchases and cash settlements add to its incidental costs).
CREATE TABLE option_grants (
    run_id           INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    grant_trade_id   INTEGER NOT NULL,   -- the OPEN_SHORT trade; not a FK (audit row).
    instrument_id    INTEGER NOT NULL REFERENCES instruments(instrument_id),
    grant_date       TEXT    NOT NULL,   -- the disposal date.
    quantity         TEXT    NOT NULL,   -- contracts written.
    premium_native   TEXT    NOT NULL,   -- gross premium received, series currency.
    grant_fee_native TEXT    NOT NULL,   -- the grant row's commission, non-negative.
    grant_fx_rate    TEXT    NOT NULL,   -- "1 GBP = r native" on grant_date.
    proceeds_gbp     TEXT    NOT NULL,   -- premium_native at grant_fx_rate (gross).
    grant_fee_gbp    TEXT    NOT NULL,   -- grant_fee_native at grant_fx_rate.
    PRIMARY KEY (run_id, grant_trade_id)
) STRICT;

-- "Every grant of run N in disposal-date order" — the reporting read
-- path and check D2's net-gain sum.
CREATE INDEX ix_option_grants_run ON option_grants (run_id, grant_date);

-- Every later event on a grant, in drain order. `kind` is copied from
-- the closing trade's action so the audit row stands on its own once
-- the trade is gone (the same self-containment `future_realisations`
-- has for `side` and the dates). `cost_gbp` is what the event adds to
-- the grant's incidental costs: premium plus fee for a purchase or a
-- cash settlement, the fee alone for a lapse, zero for an assignment
-- (whose premium share travels to the share trade instead).
CREATE TABLE option_grant_closes (
    run_id          INTEGER NOT NULL,
    grant_trade_id  INTEGER NOT NULL,
    close_trade_id  INTEGER NOT NULL,   -- CLOSE_SHORT / LAPSE_SHORT / ASSIGN_SHORT; not a FK.
    kind            TEXT    NOT NULL
                            CHECK (kind IN ('purchase', 'lapse', 'assignment', 'cash_settlement')),
    close_date      TEXT    NOT NULL,
    quantity        TEXT    NOT NULL,   -- contracts of the grant this event closed.
    premium_native  TEXT    NOT NULL,   -- premium paid on this portion; 0 for lapse / assignment.
    fee_native      TEXT    NOT NULL,
    fx_rate         TEXT    NOT NULL,   -- "1 GBP = r native" on close_date.
    cost_gbp        TEXT    NOT NULL,
    seq             INTEGER NOT NULL CHECK (seq >= 0),   -- drain order within the grant.
    PRIMARY KEY (run_id, grant_trade_id, close_trade_id),
    FOREIGN KEY (run_id, grant_trade_id)
        REFERENCES option_grants(run_id, grant_trade_id) ON DELETE CASCADE,
    CHECK (grant_trade_id <> close_trade_id)
) STRICT;

-- What an exercise or assignment moved into a share trade. `side` says
-- whose option it was: LONG for a holder's exercise (the identified
-- option cost joins the share trade), SHORT for a writer's assignment
-- (the assigned contracts' gross premium joins it), in which case
-- `grant_trade_id` names the grant that was drained — one row per
-- grant a single assignment drains, numbered by `seq`.
CREATE TABLE option_exercise_transfers (
    run_id          INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    option_trade_id INTEGER NOT NULL,   -- EXERCISE_LONG / ASSIGN_SHORT; not a FK.
    share_trade_id  INTEGER NOT NULL,   -- the stock trade at the strike; not a FK.
    instrument_id   INTEGER NOT NULL REFERENCES instruments(instrument_id),
    side            TEXT    NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    grant_trade_id  INTEGER,            -- the drained grant on the SHORT side only.
    on_date         TEXT    NOT NULL,   -- the exercise date = the share trade's date.
    quantity        TEXT    NOT NULL,   -- contracts exercised or assigned in this row.
    amount_gbp      TEXT    NOT NULL,   -- option cost (LONG) or premium share (SHORT) moved.
    fees_gbp        TEXT    NOT NULL,   -- incidental costs riding with it.
    seq             INTEGER NOT NULL CHECK (seq >= 0),
    PRIMARY KEY (run_id, option_trade_id, seq),
    CHECK (option_trade_id <> share_trade_id),
    CHECK ((grant_trade_id IS NULL) = (side = 'LONG'))
) STRICT;

-- "Every transfer of run N in date order" — the reporting read path.
CREATE INDEX ix_option_exercise_transfers_run ON option_exercise_transfers (run_id, on_date);

-- 7) The issue kinds gain the two option warnings. Same shape as 020
--    otherwise; the table is empty after the wipe.
DROP TABLE tax_run_issues;

CREATE TABLE tax_run_issues (
    run_id        INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    seq           INTEGER NOT NULL CHECK (seq >= 0),
    kind          TEXT    NOT NULL CHECK (kind IN (
                      'position_mismatch', 'rate_not_found', 'inconsistent_trades',
                      'engine_failure', 'open_short_position', 'fx_residual',
                      'history_incomplete', 'history_no_lookahead', 'empty_year',
                      'option_grant_restated', 'option_exercise_unlinked')),
    instrument_id INTEGER REFERENCES instruments(instrument_id),
    message       TEXT    NOT NULL,
    PRIMARY KEY (run_id, seq),
    CHECK ((instrument_id IS NULL)
           = (kind IN ('history_incomplete', 'history_no_lookahead', 'empty_year')))
) STRICT;
