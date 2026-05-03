-- 010_fx_fees_to_gbp.sql — retag FX-trade fees as GBP-denominated.
--
-- Why this migration exists
-- -------------------------
-- Up to and including the previous mapper revision, Forex rows had
-- their fee `Money` constructed with `instrument.currency` (the pair's
-- base — EUR for EUR.GBP, USD for USD.JPY, ...). Interactive Brokers
-- in fact denominates the Comm/Fee column in GBP for *every* Forex
-- row regardless of the pair traded. The mapper's `Trade` invariant
-- now requires `fees.currency == "GBP"` for FX trades; existing rows
-- in the database carry the wrong tag and would fail to deserialise
-- through `TradeRepo._row_to_trade`.
--
-- The amount in the database is the raw GBP figure that IB printed
-- on the statement — the mapper only ever recorded the magnitude
-- (`abs(fees)`) and slapped a currency label on it. So this is a
-- pure relabelling: rewrite `fees_currency` to `'GBP'` for every
-- row whose instrument is an FX pair, leaving `fees_amount`
-- untouched.
--
-- The matched_disposals audit trail produced by previous tax-run
-- invocations is *not* updated here — those rows are a forensic
-- record of what the engine computed at run time and are
-- intentionally immutable. To recompute the GBP figures with the
-- corrected fee tagging, run `ib-cgt compute --year ...` again.

UPDATE trades
   SET fees_currency = 'GBP'
 WHERE instrument_id IN (SELECT instrument_id FROM fx_instruments)
   AND fees_currency != 'GBP';
