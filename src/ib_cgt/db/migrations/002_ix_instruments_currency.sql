-- 002_ix_instruments_currency.sql — fast enumeration of observed currencies.
--
-- The FX sync command needs to answer "which currencies do we need rates for?"
-- and it answers via `SELECT DISTINCT currency FROM instruments WHERE currency != 'GBP'`.
-- Without an index on `currency`, that query scans the whole instruments table.
-- With this index, SQLite walks the index (one entry per unique currency
-- repeated per instrument) and short-circuits duplicates, turning it into
-- near-O(#distinct currencies).
--
-- `instruments.currency` is NOT NULL (schema 001), so the index has no NULL
-- tail to worry about. It is also immutable per row (a US-listed stock is
-- always USD), which means the index has near-zero write churn.

CREATE INDEX ix_instruments_currency ON instruments (currency);
