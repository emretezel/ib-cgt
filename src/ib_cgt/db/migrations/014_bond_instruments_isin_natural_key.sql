-- 014_bond_instruments_isin_natural_key.sql
--
-- Rekey `bond_instruments` from `(symbol, currency)` to `(isin)`.
--
-- Why this migration exists
-- -------------------------
-- IB renders the same underlying gilt under multiple symbols across
-- one statement: yield-suffixed `UKT 2 3/4 09/07/24 4.56970771%` for
-- one BUY lot, `UKT 2 3/4 09/07/24 4.73704309%` for another lot, and
-- the canonical `UKT 2 3/4 09/07/24 FH45` (or just `UKT 2 3/4 09/07/24`)
-- for the maturity row. With a `(symbol, currency)` natural key these
-- aliases each became a separate `bond_instruments` row, so the
-- maturity SELL never matched the BUYs.
--
-- IB always prints the bond's ISIN (Security ID) in the bonds-shaped
-- Financial Instrument Information table, and embeds it inline in
-- every Bond Maturity description. ISIN is globally unique by ISO
-- standard, so it is the correct natural key.
--
-- Wipe + re-ingest path
-- ---------------------
-- The pre-014 schema cannot retrofit ISINs onto existing bond rows
-- (the data isn't there until the new parser populates it). So this
-- migration deletes every bond-related row in dependency order; the
-- user re-ingests every statement after `db init` to repopulate.
-- Re-ingest is idempotent on the SHA-256 of each `.htm` file.
--
-- The cascade graph for bonds:
--   matched_disposals.instrument_id  → instruments.instrument_id
--   bond_coupons.instrument_id       → instruments.instrument_id
--   dividends.instrument_id          → instruments.instrument_id
--   trades.instrument_id             → instruments.instrument_id
--   bond_instruments.instrument_id   → instruments.instrument_id  (CASCADE)
-- Without ON DELETE CASCADE on the first four, deleting `instruments`
-- directly would fail; we delete by predicate in dependency order
-- before the parent.

-- 1. Wipe all bond data, in dependency order.

DELETE FROM matched_disposals
 WHERE instrument_id IN (SELECT instrument_id FROM bond_instruments);

DELETE FROM bond_coupons
 WHERE instrument_id IN (SELECT instrument_id FROM bond_instruments);

DELETE FROM dividends
 WHERE instrument_id IN (SELECT instrument_id FROM bond_instruments);

DELETE FROM trades
 WHERE instrument_id IN (SELECT instrument_id FROM bond_instruments);

DELETE FROM instruments
 WHERE instrument_id IN (SELECT instrument_id FROM bond_instruments);
-- bond_instruments rows go via the parent's ON DELETE CASCADE.

-- 2. Drop the convenience view that fans out to bond_instruments.
-- SQLite leaves a view dangling when its underlying table is
-- dropped; re-creating both keeps the view definition aligned with
-- the new bond_instruments shape.

DROP VIEW IF EXISTS v_instruments;

-- 3. Recreate bond_instruments with ISIN as the natural key.
--
-- ISIN is the primary identity (NOT NULL UNIQUE). `symbol` and
-- `currency` are display fields — `symbol` carries the canonical
-- form (e.g. `UKT 2 3/4 09/07/24`) sourced from the instrument-info
-- Description column. `is_cgt_exempt` carries forward unchanged from
-- the pre-014 schema (gilt classifier verdict at ingest time).

DROP TABLE bond_instruments;

CREATE TABLE bond_instruments (
    instrument_id INTEGER PRIMARY KEY
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    isin          TEXT    NOT NULL,
    symbol        TEXT    NOT NULL,
    currency      TEXT    NOT NULL,
    is_cgt_exempt INTEGER NOT NULL CHECK (is_cgt_exempt IN (0, 1)),
    UNIQUE (isin)
) STRICT;

-- 4. Currency index — required for the cross-class FX-sync DISTINCT
-- query to walk an index rather than the table heap. Symmetric with
-- the `ix_*_instruments_currency` indexes migration 003 created on
-- the other child tables.

CREATE INDEX ix_bond_instruments_currency
    ON bond_instruments (currency);

-- 5. Display index on (symbol, currency) — kept as an index, not a
-- UNIQUE: the same canonical symbol could legitimately appear under
-- two different ISINs (re-issuance under a new ISIN with the same
-- description is permitted by gilt naming conventions). Used by the
-- `bonds list` CLI for the symbol-ordered output.

CREATE INDEX ix_bond_instruments_symbol_currency
    ON bond_instruments (symbol, currency);

-- 6. Recreate v_instruments — same projection as migration 003 but
-- with the bond branch sourcing `b.isin` from the child column
-- (post-014 the child holds the authoritative ISIN). Stocks /
-- futures / FX branches still source `i.isin` from the parent.

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
SELECT i.instrument_id, i.asset_class, b.isin,
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
