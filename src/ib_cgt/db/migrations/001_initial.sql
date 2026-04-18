-- 001_initial.sql — the full initial schema for ib-cgt.
--
-- Hot query paths this schema is optimised for (architecture.md §Component
-- map, item 2):
--   1. Compute path: load all trades for instrument X across all accounts,
--      ordered by trade_datetime — served by ix_trades_instrument_dt.
--   2. FX lookup:  convert(amount, ccy, date) → GBP — served by the
--      primary key on (base, quote, rate_date) plus ix_fx_rates_quote_date.
--   3. Ingestion idempotency: statement-hash + trade_key uniqueness
--      (both enforced as primary keys).
--
-- All monetary amounts are stored as TEXT holding the canonical Decimal
-- string (see src/ib_cgt/db/codecs.py). SQLite's NUMERIC affinity cannot
-- preserve penny-level precision on fractions.

-- accounts ------------------------------------------------------------------
CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    label      TEXT
) STRICT;

-- instruments ---------------------------------------------------------------
-- Single table with nullable sub-class columns, discriminated by
-- `asset_class`. The UNIQUE constraint is the natural key: two futures with
-- different expiries are distinct instruments, and FX pairs are identified
-- by (base, quote). Stock/bond rows set expiry/fx columns to NULL.
CREATE TABLE instruments (
    instrument_id        INTEGER PRIMARY KEY,
    asset_class          TEXT    NOT NULL CHECK (asset_class IN ('stock','bond','future','fx')),
    symbol               TEXT    NOT NULL,
    currency             TEXT    NOT NULL,
    isin                 TEXT,
    is_cgt_exempt        INTEGER CHECK (is_cgt_exempt IN (0, 1)),
    contract_multiplier  TEXT,
    expiry_date          TEXT,
    fx_base              TEXT,
    fx_quote             TEXT,
    UNIQUE (asset_class, symbol, currency, expiry_date, fx_base, fx_quote)
) STRICT;
CREATE INDEX ix_instruments_symbol ON instruments (symbol);

-- statements ----------------------------------------------------------------
-- Declared before `trades` because `trades.source_statement_hash` references it.
CREATE TABLE statements (
    statement_hash TEXT PRIMARY KEY,
    source_path    TEXT    NOT NULL,
    account_id     TEXT    NOT NULL REFERENCES accounts(account_id),
    imported_at    TEXT    NOT NULL,
    trade_count    INTEGER NOT NULL
) STRICT;

-- trades --------------------------------------------------------------------
-- `trade_key` is the composite business key produced by ingestion
-- (IB has no per-trade ID). `INSERT OR IGNORE` on this PK makes statement
-- re-imports idempotent. `source_statement_hash` lets us answer
-- "which statement did this trade come from?" and withdraw-then-reimport
-- workflows if a statement is superseded.
CREATE TABLE trades (
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
    source_statement_hash TEXT    NOT NULL REFERENCES statements(statement_hash)
) STRICT;
-- Hot query: chronological trades for one instrument across every account.
CREATE INDEX ix_trades_instrument_dt ON trades (instrument_id, trade_datetime);
-- Tax-year cut-off scans.
CREATE INDEX ix_trades_trade_date    ON trades (trade_date);
-- Per-account reporting.
CREATE INDEX ix_trades_account_date  ON trades (account_id, trade_date);
-- Provenance + withdraw-and-reimport support.
CREATE INDEX ix_trades_statement     ON trades (source_statement_hash);

-- fx_rates ------------------------------------------------------------------
-- Rates are stored with explicit (base, quote). For UK CGT the FX service
-- always calls with base = 'GBP', but storing explicitly keeps the cache
-- useful for ad-hoc cross-rate lookups.
CREATE TABLE fx_rates (
    base       TEXT NOT NULL,
    quote      TEXT NOT NULL,
    rate_date  TEXT NOT NULL,
    rate       TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (base, quote, rate_date)
) STRICT;
-- Fast lookups when the base is fixed and we scan by (quote, date).
CREATE INDEX ix_fx_rates_quote_date ON fx_rates (quote, rate_date);

-- tax_runs ------------------------------------------------------------------
-- One row per compute invocation. `replace_for(tax_year)` deletes any
-- existing row for the year (CASCADE cleans matched_disposals) before
-- inserting a fresh one.
CREATE TABLE tax_runs (
    run_id      INTEGER PRIMARY KEY,
    tax_year    INTEGER NOT NULL,
    computed_at TEXT    NOT NULL,
    net_gbp     TEXT    NOT NULL
) STRICT;
CREATE INDEX ix_tax_runs_year ON tax_runs (tax_year, computed_at);

-- matched_disposals ---------------------------------------------------------
-- Basis columns encode the domain `MatchBasis` union:
--   basis_kind = 'DIRECT' → acquisition_trade_key is set, pool_* are NULL.
--   basis_kind = 'POOL'   → pool_* are set, acquisition_trade_key is NULL.
-- A disposal chunk can have `acquisition_trade_key IS NULL` (pool draws),
-- so we include `match_rule` in the PK to disambiguate and we rely on the
-- application layer (MatchedDisposalRepo) to enforce the union invariant.
CREATE TABLE matched_disposals (
    run_id                 INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    disposal_trade_key     TEXT    NOT NULL,
    instrument_id          INTEGER NOT NULL REFERENCES instruments(instrument_id),
    disposal_date          TEXT    NOT NULL,
    match_rule             TEXT    NOT NULL,
    matched_quantity       TEXT    NOT NULL,
    matched_proceeds_gbp   TEXT    NOT NULL,
    matched_cost_gbp       TEXT    NOT NULL,
    basis_kind             TEXT    NOT NULL CHECK (basis_kind IN ('DIRECT','POOL')),
    acquisition_trade_key  TEXT,
    pool_quantity_before   TEXT,
    pool_total_cost_before TEXT,
    pool_average_cost      TEXT,
    seq                    INTEGER NOT NULL,
    PRIMARY KEY (run_id, disposal_trade_key, seq)
) STRICT;
CREATE INDEX ix_matched_disposals_run ON matched_disposals (run_id, disposal_date);
