-- 024_corporate_actions_and_cash_report.sql — corporate actions become an
-- entity, dividends keep their sign, and the Cash Report is ingested so
-- the FX pools can be reconciled against IB's own cash ledger.
--
-- Why this migration exists
-- -------------------------
-- A reconciliation of the FX-pool balances against IB's Cash Report
-- (every statement 2011-2026, both accounts) found the pools right to
-- the cent everywhere except for three USD gaps, all of them things the
-- schema could not represent:
--
-- 1. A cross-currency corporate action. IEMI (GBP-listed) was cashed
--    out for 14,425.52 USD on 2025-08-22; the ingest squashed the event
--    into a synthetic GBP SELL trade, so the USD never reached the pool
--    and the fact "824 shares for 14,425.52 USD" was stored nowhere.
-- 2. The sign of dividend-section rows. `dividends.amount_native` held
--    the magnitude and `kind` was read as the direction, which booked a
--    payment in lieu PAID on a short (TUR, 2019-06-21, -887.72 USD) as
--    an acquisition and four withholding reversals (January 2017) as
--    disposals.
-- 3. Nothing to check either against: IB's per-currency cash balances
--    were never read.
--
-- What changes
-- ------------
-- 1. `corporate_actions` — one row per event, modelled as up to three
--    legs: security out (negative `quantity`), security in (positive
--    `quantity`) and cash (signed `cash_amount` in `cash_currency`). The
--    sign is the fact, as in `cash_events`. `kind` says whether the
--    engines model the row: `cash_disposal` (one security-out leg plus
--    positive cash, whatever IB's wording — a cash merger, a fund
--    redemption, a tender, a bond maturity, cash in lieu) or
--    `unsupported` (anything else, stored so check A16 can report it
--    rather than dropped). Synthetic SELL trades are no longer written
--    to `trades`; the stock and bond engines read this table instead,
--    and the FX engine projects the cash leg in its own currency.
-- 2. `statement_positions` gains `close_price` (rebuild: a NOT NULL
--    column on a STRICT table cannot be added in place). The
--    reconciliation values the engine's open futures lots at the
--    statement's close so IB's daily variation margin can be compared
--    with the engine's close-out P&L.
-- 3. `statement_cash_balances` — per statement, per currency, IB's
--    Starting Cash and Ending Cash from the Cash Report (Total column).
-- 4. `dividends` is recreated with `CHECK (CAST(amount_native AS REAL)
--    <> 0)`: the amount is now signed as printed and only zero is
--    forbidden. The column set is unchanged from 021.
-- 5. `fx_event_sources` is replaced by `event_sources`. The provenance
--    map now resolves a synthetic id for every engine (a corporate
--    action's id is cited by the stock or bond engine as well as by the
--    FX pools), so the FX-only name was wrong; the table gains the
--    `CORPORATE_ACTION` kind with its `corporate_action_id` column and
--    partial unique index.
-- 6. `tax_run_issues.kind` admits `cash_balance_mismatch` (an error:
--    an account's foreign-currency balance disagrees with IB's ledger,
--    so a pool is missing a source). Dropped and recreated with the
--    wider CHECK; it is empty after the wipe below.
--
-- Why the wipe
-- ------------
-- The dividend signs, the corporate-action legs, the close prices and
-- the cash balances exist only in the statement files, and the trades
-- table must lose the synthetic rows, so every statement is read again
-- and every run recomputed. As with 014, 015, 021, 022 and 023 the
-- honest path is to wipe every statement-derived row and every run and
-- re-ingest after `db init` (`ib-cgt db reset`, `ib-cgt ingest ...`,
-- `ib-cgt fx sync`, `ib-cgt compute --year ...`). Re-ingest is
-- idempotent on each file's SHA-256. `accounts`, `instruments`,
-- `fx_rates` and `schema_migrations` are untouched: instrument identity
-- is a natural key (conid / ISIN) that a re-ingest resolves to the same
-- rows.
--
-- Deletion order follows the FK graph:
--   tax_runs    → matched_disposals, future_realisations, option_*,
--                 fx_event_sources, tax_run_issues                (CASCADE)
--   statements  → trades, option_exercise_links, dividends,
--                 bond_coupons, statement_positions, cash_events  (CASCADE)
--
-- Sign checks on Decimal-as-TEXT columns
-- --------------------------------------
-- Amounts are canonical Decimal strings, on which `<` is lexicographic
-- and unsafe. A sign-only invariant survives `CAST(x AS REAL)` (only
-- precision is lost), so that is how the sign CHECKs below are
-- written; the domain objects enforce the exact Decimal rule.

-- 1) Wipe statement-derived rows and the runs computed from them.
DELETE FROM tax_runs;
DELETE FROM statements;

-- 2) Corporate actions. Every date and amount is a statement fact:
--    `effective_datetime` is IB's Date/Time read in the statement's
--    declared zone (stored UTC), `effective_date` its Europe/London
--    date — the disposal date and the FX-pool date — and `report_date`
--    the day IB booked the event, kept for audit. `instrument_id` is
--    NULL only on an unsupported row whose security the statement's
--    instrument table cannot resolve.
CREATE TABLE corporate_actions (
    corporate_action_id   INTEGER PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    kind                  TEXT    NOT NULL CHECK (kind IN ('cash_disposal', 'unsupported')),
    instrument_id         INTEGER REFERENCES instruments(instrument_id),
    effective_datetime    TEXT    NOT NULL,   -- ISO-8601, UTC.
    effective_date        TEXT    NOT NULL,   -- YYYY-MM-DD, Europe/London date of the instant.
    report_date           TEXT    NOT NULL,   -- YYYY-MM-DD, as printed.
    quantity              TEXT    NOT NULL,   -- signed Decimal; negative = units out.
    cash_amount           TEXT,               -- signed Decimal; NULL when the event moves no cash.
    cash_currency         TEXT,               -- ISO-4217; NULL exactly when cash_amount is.
    description           TEXT    NOT NULL CHECK (length(description) > 0),
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index),
    CHECK ((cash_amount IS NULL) = (cash_currency IS NULL)),
    CHECK (cash_currency IS NULL OR cash_currency GLOB '[A-Z][A-Z][A-Z]'),
    CHECK (kind = 'unsupported' OR instrument_id IS NOT NULL),
    CHECK (kind <> 'cash_disposal'
           OR (cash_amount IS NOT NULL
               AND CAST(quantity AS REAL) < 0
               AND CAST(cash_amount AS REAL) > 0))
) STRICT;

-- `(instrument_id, effective_date)` serves `CorporateActionRepo.
-- for_instrument` — the stock / bond engines' per-instrument load and
-- the position reconciliation's `signed_quantity_by_instrument`.
-- `(cash_currency, effective_date)` serves `list_cash_disposals` and
-- `distinct_cash_currencies` — the FX inputs and `fx sync`'s currency
-- walk. `(source_statement_hash)` serves the statement cascade and the
-- per-statement audit lookups.
CREATE INDEX ix_corporate_actions_instrument_date    ON corporate_actions (instrument_id, effective_date);
CREATE INDEX ix_corporate_actions_cash_currency_date ON corporate_actions (cash_currency, effective_date);
CREATE INDEX ix_corporate_actions_statement          ON corporate_actions (source_statement_hash);

-- 3) Open positions with the statement's Close Price. Same keys as 016;
--    the table is empty after the wipe so the rebuild carries no data.
CREATE TABLE statement_positions_new (
    statement_hash      TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    statement_row_index INTEGER NOT NULL CHECK (statement_row_index >= 0),
    instrument_id       INTEGER NOT NULL REFERENCES instruments(instrument_id),
    quantity            TEXT    NOT NULL,   -- signed Decimal.
    close_price         TEXT    NOT NULL,   -- Decimal, as printed: per unit; bonds as % of par.
    PRIMARY KEY (statement_hash, statement_row_index),
    UNIQUE (statement_hash, instrument_id)
) STRICT;

DROP TABLE statement_positions;

ALTER TABLE statement_positions_new RENAME TO statement_positions;

-- 4) IB's per-currency cash balances at the period's start and end
--    (the Cash Report's Total column, every segment summed). One row
--    per currency per statement; the primary key's leading column is
--    the only read path (`for_statement`).
CREATE TABLE statement_cash_balances (
    statement_hash TEXT NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    currency       TEXT NOT NULL CHECK (currency GLOB '[A-Z][A-Z][A-Z]'),
    starting_cash  TEXT NOT NULL,   -- signed Decimal.
    ending_cash    TEXT NOT NULL,   -- signed Decimal.
    PRIMARY KEY (statement_hash, currency)
) STRICT;

-- 5) Dividends signed as printed. Column set as in 021; the CHECK is
--    new. The table is empty after the wipe.
DROP TABLE dividends;

CREATE TABLE dividends (
    dividend_id           INTEGER PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    symbol                TEXT    NOT NULL CHECK (length(symbol) > 0),
    kind                  TEXT    NOT NULL
                                  CHECK (kind IN ('cash_dividend',
                                                  'withholding_tax',
                                                  'payment_in_lieu')),
    pay_date              TEXT    NOT NULL,
    amount_native         TEXT    NOT NULL CHECK (CAST(amount_native AS REAL) <> 0),
    currency              TEXT    NOT NULL,
    description           TEXT    NOT NULL,
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index)
) STRICT;

CREATE INDEX ix_dividends_pay_currency ON dividends (currency, pay_date);
CREATE INDEX ix_dividends_statement    ON dividends (source_statement_hash);

-- 6) The provenance map, renamed for what it now is. Same shape as 019
--    with a fifth arm. Like the other reference columns,
--    `corporate_action_id` is not a foreign key: the row is audit data
--    that must outlive a re-ingest so a dangling reference can be
--    reported (D4) rather than silently cascaded away.
DROP TABLE fx_event_sources;

CREATE TABLE event_sources (
    run_id              INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    event_id            INTEGER NOT NULL,
    kind                TEXT    NOT NULL
        CHECK (kind IN ('FUTURE_REALISATION', 'DIVIDEND', 'BOND_COUPON',
                        'CASH_EVENT', 'CORPORATE_ACTION')),
    open_trade_id       INTEGER,
    close_trade_id      INTEGER,
    dividend_id         INTEGER,
    bond_coupon_id      INTEGER,
    cash_event_id       INTEGER,
    corporate_action_id INTEGER,
    PRIMARY KEY (run_id, event_id),
    CHECK (
        (kind = 'FUTURE_REALISATION'
            AND open_trade_id IS NOT NULL AND close_trade_id IS NOT NULL
            AND dividend_id IS NULL AND bond_coupon_id IS NULL
            AND cash_event_id IS NULL AND corporate_action_id IS NULL)
     OR (kind = 'DIVIDEND'
            AND dividend_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND bond_coupon_id IS NULL AND cash_event_id IS NULL
            AND corporate_action_id IS NULL)
     OR (kind = 'BOND_COUPON'
            AND bond_coupon_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND dividend_id IS NULL AND cash_event_id IS NULL
            AND corporate_action_id IS NULL)
     OR (kind = 'CASH_EVENT'
            AND cash_event_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND dividend_id IS NULL AND bond_coupon_id IS NULL
            AND corporate_action_id IS NULL)
     OR (kind = 'CORPORATE_ACTION'
            AND corporate_action_id IS NOT NULL
            AND open_trade_id IS NULL AND close_trade_id IS NULL
            AND dividend_id IS NULL AND bond_coupon_id IS NULL
            AND cash_event_id IS NULL)
    )
) STRICT;

-- One synthetic id per source per run, in both directions.
CREATE UNIQUE INDEX ux_event_sources_realisation
    ON event_sources (run_id, open_trade_id, close_trade_id)
    WHERE kind = 'FUTURE_REALISATION';
CREATE UNIQUE INDEX ux_event_sources_dividend
    ON event_sources (run_id, dividend_id)
    WHERE kind = 'DIVIDEND';
CREATE UNIQUE INDEX ux_event_sources_coupon
    ON event_sources (run_id, bond_coupon_id)
    WHERE kind = 'BOND_COUPON';
CREATE UNIQUE INDEX ux_event_sources_cash_event
    ON event_sources (run_id, cash_event_id)
    WHERE kind = 'CASH_EVENT';
CREATE UNIQUE INDEX ux_event_sources_corporate_action
    ON event_sources (run_id, corporate_action_id)
    WHERE kind = 'CORPORATE_ACTION';

-- 7) The issue kinds gain the cash-balance error. Same shape as 023
--    otherwise; the table is empty after the wipe.
DROP TABLE tax_run_issues;

CREATE TABLE tax_run_issues (
    run_id        INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    seq           INTEGER NOT NULL CHECK (seq >= 0),
    kind          TEXT    NOT NULL CHECK (kind IN (
                      'position_mismatch', 'rate_not_found', 'inconsistent_trades',
                      'engine_failure', 'open_short_position', 'fx_residual',
                      'history_incomplete', 'history_no_lookahead', 'empty_year',
                      'option_grant_restated', 'option_exercise_unlinked',
                      'cash_balance_mismatch')),
    instrument_id INTEGER REFERENCES instruments(instrument_id),
    message       TEXT    NOT NULL,
    PRIMARY KEY (run_id, seq),
    CHECK ((instrument_id IS NULL)
           = (kind IN ('history_incomplete', 'history_no_lookahead', 'empty_year')))
) STRICT;
