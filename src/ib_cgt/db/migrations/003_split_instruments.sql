-- 003_split_instruments.sql — class-table inheritance for instruments.
--
-- Why this migration exists
-- -------------------------
-- The 001 schema flattened four asset-class subtypes into one `instruments`
-- table with nullable subclass-specific columns and a `UNIQUE` covering all
-- of them. SQLite (per ANSI SQL) treats `NULL` as distinct in UNIQUE
-- constraints, so for futures (which have NULL `fx_base` / `fx_quote`) the
-- `INSERT OR IGNORE` upsert path never saw a conflict and the live DB
-- accumulated duplicate rows for the same future. Patching this with a
-- COALESCE-sentinel index would violate CLAUDE.md §3 ("no magic values"),
-- and leaving the columns nullable conflates four entity types in one
-- table ("each table represents one thing").
--
-- The fix is class-table inheritance: keep `instruments` as a thin parent
-- table holding only the truly-shared identity, and split each asset
-- class into its own child table whose natural-key columns are NOT NULL
-- and covered by a UNIQUE that the constraint can actually enforce.
--
-- Operational note
-- ----------------
-- This migration drops the old `instruments` table. SQLite will refuse if
-- any row in `trades` still references it (FKs are ON), so the operator
-- must wipe the local database before applying. The project has a single
-- ingested statement and ingestion is idempotent on the file's SHA-256;
-- re-running `ib-cgt ingest` after `ib-cgt db init` reconstructs all
-- trades and instrument rows deterministically.

-- 1) Tear down the old single-table schema. The two indexes go first so
--    SQLite doesn't complain about dependents during the table drop.
DROP INDEX  IF EXISTS ix_instruments_currency;
DROP INDEX  IF EXISTS ix_instruments_symbol;
DROP TABLE  IF EXISTS instruments;

-- 2) Parent table — identity, discriminator, and the only attribute that
--    is truly shared and class-independent (ISIN). Keeping the parent
--    intentionally thin means every other column lives in exactly one
--    place (the appropriate child table) — single source of truth.
CREATE TABLE instruments (
    instrument_id INTEGER PRIMARY KEY,
    asset_class   TEXT NOT NULL CHECK (asset_class IN ('stock','bond','future','fx')),
    isin          TEXT
) STRICT;

-- 3) Child tables. Each owns its asset-class-specific natural key with no
--    nullable columns, so the per-class UNIQUE actually enforces dedup.
--    The child's PK is the parent's surrogate id (PK = FK), creating a
--    one-to-one relationship that the FK CASCADE keeps in sync on delete.
CREATE TABLE stock_instruments (
    instrument_id INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    symbol        TEXT NOT NULL,
    currency      TEXT NOT NULL,
    UNIQUE (symbol, currency)
) STRICT;

CREATE TABLE bond_instruments (
    instrument_id INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    symbol        TEXT    NOT NULL,
    currency      TEXT    NOT NULL,
    is_cgt_exempt INTEGER NOT NULL CHECK (is_cgt_exempt IN (0, 1)),
    UNIQUE (symbol, currency)
) STRICT;

CREATE TABLE future_instruments (
    instrument_id        INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    symbol               TEXT NOT NULL,
    currency             TEXT NOT NULL,
    contract_multiplier  TEXT NOT NULL,                -- Decimal string (canonical).
    expiry_date          TEXT NOT NULL,                -- YYYY-MM-DD.
    UNIQUE (symbol, currency, expiry_date)
) STRICT;

CREATE TABLE fx_instruments (
    instrument_id INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    symbol        TEXT NOT NULL,
    currency      TEXT NOT NULL,
    fx_base       TEXT NOT NULL,
    fx_quote      TEXT NOT NULL,
    UNIQUE (symbol, currency, fx_base, fx_quote)
) STRICT;

-- 4) Per-child currency indexes. The FX-sync command issues
--    `SELECT DISTINCT currency FROM v_instruments WHERE currency != 'GBP'`,
--    which fans out into four child scans via the view below; an index on
--    `currency` lets each scan walk the index instead of the table heap.
--    The leading column of each child's UNIQUE is `symbol`, so symbol
--    filters are already served — we don't add a redundant symbol index.
CREATE INDEX ix_stock_instruments_currency  ON stock_instruments  (currency);
CREATE INDEX ix_bond_instruments_currency   ON bond_instruments   (currency);
CREATE INDEX ix_future_instruments_currency ON future_instruments (currency);
CREATE INDEX ix_fx_instruments_currency     ON fx_instruments     (currency);

-- 5) Convenience view. External queries that only care about a unified
--    "instrument" shape (CLI's DISTINCT-currency query, TradeRepo's
--    symbol-filter join) target the view; they don't have to know about
--    the per-class split. Per CLAUDE.md §3, this is exactly the kind of
--    "common joins or projections" a view is appropriate for — no truth
--    duplication, just a read-time projection.
CREATE VIEW v_instruments AS
SELECT i.instrument_id, i.asset_class, i.isin,
       s.symbol, s.currency,
       NULL AS is_cgt_exempt,
       NULL AS contract_multiplier,
       NULL AS expiry_date,
       NULL AS fx_base,
       NULL AS fx_quote
FROM instruments i JOIN stock_instruments s USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class, i.isin,
       b.symbol, b.currency,
       b.is_cgt_exempt,
       NULL, NULL, NULL, NULL
FROM instruments i JOIN bond_instruments b USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class, i.isin,
       f.symbol, f.currency,
       NULL,
       f.contract_multiplier,
       f.expiry_date,
       NULL, NULL
FROM instruments i JOIN future_instruments f USING (instrument_id)
UNION ALL
SELECT i.instrument_id, i.asset_class, i.isin,
       fx.symbol, fx.currency,
       NULL, NULL, NULL,
       fx.fx_base,
       fx.fx_quote
FROM instruments i JOIN fx_instruments fx USING (instrument_id);
