-- 013_rescale_bond_trade_prices.sql — divide existing bond trade prices by 100.
--
-- Why this migration exists
-- -------------------------
-- IB quotes bond `T. Price` as a percentage of par (e.g. `98.602`
-- means 98.602% of face value), with `Quantity` carrying the face-
-- value nominal. The post-mapper invariant — shared with stocks,
-- futures, and FX — is that `Trade.price.amount * Trade.quantity`
-- equals settlement cash. The mapper now divides bond prices by 100
-- at ingest time so that invariant holds; this migration brings
-- already-ingested rows in line with the new convention without
-- forcing a re-ingest of every historical statement.
--
-- Effect: every bond trade's `price_amount` is divided by 100. Stocks,
-- futures, and FX rows are untouched. The migration is gated on
-- `instrument_id IN (SELECT instrument_id FROM bond_instruments)` so
-- it is asset-class-precise.
--
-- Idempotency
-- -----------
-- The migrator's `schema_migrations` table guarantees this script
-- runs at most once per database, so re-running `db init` is safe.
--
-- Encoding
-- --------
-- `trades.price_amount` is `TEXT` containing the canonical Decimal
-- string form (see `db/codecs.py`). Bond prices are 0–200 with at
-- most ~6 significant digits; SQLite double precision gives ~15
-- significant digits, leaving plenty of margin. The `printf('%.10f',
-- ...)` formats to 10 decimal places, then a pair of `RTRIM`s strips
-- trailing zeros and any orphan trailing dot so the stored text stays
-- a canonical Decimal representation (e.g. `"0.98602"`, not
-- `"0.9860200000"`).

UPDATE trades
   SET price_amount = RTRIM(
         RTRIM(printf('%.10f', CAST(price_amount AS REAL) / 100.0), '0'),
         '.'
       )
 WHERE instrument_id IN (SELECT instrument_id FROM bond_instruments);
