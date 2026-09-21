-- 021_instrument_conid_identity.sql
--
-- Rekey `stock_instruments` and `future_instruments` on IB's `conid`,
-- drop the `isin` column from the parent `instruments` table, and
-- detach `dividends` from `instruments`.
--
-- Why this migration exists
-- -------------------------
-- 1. IB renames symbols between statements. The ETF listed as `JNKEz`
--    in the 2022-2025 statements is printed as `JNKE` from 2025/26,
--    with the same IB contract id (`conid` 102048570) throughout. With
--    `(symbol, currency)` as the stock natural key the live DB held
--    three rows for one listing and position reconciliation reported a
--    phantom mismatch. The same failure mode exists for futures, keyed
--    `(symbol, currency, expiry_date)`. IB's `conid` is stable for the
--    life of a contract and is printed in every Financial Instrument
--    Information table of every statement vintage, so it is the
--    correct natural key for both classes. Forex pairs get no conid
--    from IB, so `fx_instruments` keeps its pair-based key; bonds keep
--    the ISIN (migration 014).
--
-- 2. `instruments.isin` was a nullable column on the parent although
--    only bonds have an ISIN, and `bond_instruments.isin` has been the
--    authoritative `NOT NULL UNIQUE` copy since 014. Storing the fact
--    twice violates single-source-of-truth; the parent now holds only
--    the surrogate id and the asset-class discriminator.
--
-- 3. A dividend / withholding / payment-in-lieu row is only ever an FX
--    pool acquisition or disposal (HMRC CG78315); nothing in the
--    calculator needs the stock it was paid on. Worse, IB pays some
--    ETF distributions in a currency other than the trade currency
--    (IEMI trades GBP, pays USD), so a `(symbol, dividend-currency)`
--    keyed stock row was a phantom instrument with no trades. The
--    `dividends` table therefore loses `instrument_id` and keeps the IB
--    security tag as plain `symbol` text (the `SYMBOL(SECID)` prefix of
--    the description) for audit labels — the same instrument-less shape
--    `cash_events` already has.
--
-- Wipe + re-ingest path
-- ---------------------
-- `conid` exists only in the statements, so existing rows cannot be
-- backfilled. As with migrations 014 and 015 this migration wipes
-- every statement-derived row and the user re-ingests every statement
-- after `db init`. Re-ingest is idempotent on each file's SHA-256.
-- `accounts`, `fx_rates` and `schema_migrations` are untouched.
--
-- Deletion order follows the FK graph (every `instrument_id` FK is
-- RESTRICT; the run tables cascade from `tax_runs`, the statement
-- tables from `statements`, the children from `instruments`):
--   tax_runs   → matched_disposals, future_realisations,
--                fx_event_sources, tax_run_issues        (CASCADE)
--   statements → trades, dividends, bond_coupons,
--                statement_positions, cash_events        (CASCADE)
--   instruments → {stock,bond,future,fx}_instruments     (CASCADE)
--
-- DDL notes
-- ---------
-- `ALTER TABLE … DROP COLUMN` (SQLite ≥ 3.35) refuses while a view still
-- references the column, so `v_instruments` is dropped first and
-- recreated last with the new `conid` column. The two rebuilt child
-- tables are simply dropped and recreated (they are empty after the
-- wipe). `dividends` is rebuilt the same way.

-- 1. Wipe statement-derived data in FK order.

DELETE FROM tax_runs;
DELETE FROM statements;
DELETE FROM instruments;

-- 2. Drop the view that projects `instruments.isin` and the two child
--    tables whose natural key changes.

DROP VIEW v_instruments;
DROP TABLE stock_instruments;
DROP TABLE future_instruments;

-- 3. The parent keeps only identity + discriminator.

ALTER TABLE instruments DROP COLUMN isin;

-- 4. Stocks: `conid` is the natural key. `symbol` and `currency` are
--    display / trade-currency attributes refreshed from the latest
--    statement on every upsert (IB renames symbols; the conid does not
--    change).

CREATE TABLE stock_instruments (
    instrument_id INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    conid         INTEGER NOT NULL CHECK (conid > 0),
    symbol        TEXT    NOT NULL,
    currency      TEXT    NOT NULL,
    UNIQUE (conid)
) STRICT;

-- 5. Futures: `conid` identifies one contract (root × expiry × venue).
--    Multiplier and expiry are contract facts carried for the rule
--    engine; `symbol` is display text.

CREATE TABLE future_instruments (
    instrument_id        INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    conid                INTEGER NOT NULL CHECK (conid > 0),
    symbol               TEXT    NOT NULL,
    currency             TEXT    NOT NULL,
    contract_multiplier  TEXT    NOT NULL,   -- Decimal string (canonical).
    expiry_date          TEXT    NOT NULL,   -- YYYY-MM-DD.
    UNIQUE (conid)
) STRICT;

-- 6. Indexes. The `UNIQUE (conid)` constraints above are the natural-key
--    probes `InstrumentRepo.upsert` issues. The `(symbol, currency)`
--    indexes serve `InstrumentRepo.find_by_symbol` (the ingest-time
--    fallback for an Open Positions row with no instrument-information
--    row), the `--symbol` filters of `list_stocks` / `list_futures` and
--    `TradeRepo.list_filtered`'s symbol join through the view. The
--    `(currency)` indexes serve FX-sync's `DISTINCT currency` walk, which
--    a `(symbol, currency)` index cannot (currency is not its leading
--    column).

CREATE INDEX ix_stock_instruments_symbol_currency  ON stock_instruments  (symbol, currency);
CREATE INDEX ix_stock_instruments_currency         ON stock_instruments  (currency);
CREATE INDEX ix_future_instruments_symbol_currency ON future_instruments (symbol, currency);
CREATE INDEX ix_future_instruments_currency        ON future_instruments (currency);

-- 7. Dividends without an instrument link. `symbol` is the IB security
--    tag as printed on the row, kept so audit output can still say
--    "dividend IEMI cash_dividend". Same column set as migration 009
--    otherwise; `ix_dividends_instrument_pay` goes with the column.

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
    amount_native         TEXT    NOT NULL,
    currency              TEXT    NOT NULL,
    description           TEXT    NOT NULL,
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index)
) STRICT;

CREATE INDEX ix_dividends_pay_currency ON dividends (currency, pay_date);
CREATE INDEX ix_dividends_statement    ON dividends (source_statement_hash);

-- 8. Recreate the convenience view with the new column set: `isin` is
--    sourced from `bond_instruments` only, `conid` from the stock and
--    future children.

CREATE VIEW v_instruments AS
SELECT i.instrument_id, i.asset_class,
       NULL AS isin,
       s.conid,
       s.symbol, s.currency,
       NULL AS is_cgt_exempt,
       NULL AS contract_multiplier,
       NULL AS expiry_date,
       NULL AS fx_base,
       NULL AS fx_quote
FROM instruments i JOIN stock_instruments s USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       b.isin,
       NULL,
       b.symbol, b.currency,
       b.is_cgt_exempt,
       NULL, NULL, NULL, NULL
FROM instruments i JOIN bond_instruments b USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       NULL,
       f.conid,
       f.symbol, f.currency,
       NULL,
       f.contract_multiplier,
       f.expiry_date,
       NULL, NULL
FROM instruments i JOIN future_instruments f USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class,
       NULL,
       NULL,
       fx.symbol, fx.currency,
       NULL, NULL, NULL,
       fx.fx_base,
       fx.fx_quote
FROM instruments i JOIN fx_instruments fx USING (instrument_id);
