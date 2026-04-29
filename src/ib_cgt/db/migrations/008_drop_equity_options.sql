-- 008_drop_equity_options.sql — purge stock-options that pre-date the
-- parser's options-section filter.
--
-- Why this migration exists
-- -------------------------
-- Up to and including the previous parser revision, rows under the
-- IB statement section header "Equity and Index Options" were
-- routed into the stocks pipeline. This project does not handle
-- stock options, so the parser now drops that section at parse
-- time (see `ingest/parser.py::_IGNORED_ASSET_CLASSES`). However,
-- the live database still carries any option rows that were
-- ingested before the filter landed.
--
-- Concretely, on the developer's local DB this is a single
-- instrument (`TUR 17MAY19 22.0 P`, USD) with two attached trades.
-- Pattern-matching on the OCC option-symbol shape lets the
-- migration scrub *any* such instrument generically, so it remains
-- correct on a fresh install or a database that later acquires
-- additional option rows from a re-ingest of an older release.
--
-- The OCC format is `<UNDERLYING> <DDMMMYY> <STRIKE> <C|P>` — three
-- space-separated parts after the underlying ticker. Plain stock
-- symbols never carry that pattern (even unusual ones like `BRK.B`,
-- `EOLU B` or `3M`), so the GLOB is a reliable filter.
--
-- Order of operations matters: `trades.instrument_id` is a plain
-- (non-cascading) FK to `instruments`, so the dependent trades must
-- be deleted before the parent instrument row. Removing
-- `instruments` then cascades to `stock_instruments` via the
-- `ON DELETE CASCADE` rule from migration 003.
--
-- The audit-trail tables (`tax_runs`, `matched_disposals`) are
-- intentionally untouched: they have no FK to `trades` and represent
-- a frozen record of what the engine saw at run time. Today they
-- are empty, but if a future re-run repopulates them we still want
-- this migration to leave them alone.

DELETE FROM trades
WHERE instrument_id IN (
    SELECT instrument_id
    FROM stock_instruments
    WHERE symbol GLOB '* [0-3][0-9][A-Z][A-Z][A-Z][0-9][0-9] *[CP]'
);

DELETE FROM instruments
WHERE instrument_id IN (
    SELECT instrument_id
    FROM stock_instruments
    WHERE symbol GLOB '* [0-3][0-9][A-Z][A-Z][A-Z][0-9][0-9] *[CP]'
);
