# `fx_rates`

## Purpose

Local cache of daily Frankfurter (ECB) FX rates, addressed by
`(base, quote, rate_date)`. The rate is interpreted as
`1 unit of base = rate × units of quote`. The cache is a strict cache
— it holds no truth that can't be re-fetched from Frankfurter — but
the calculator depends on it being warm so a tax-year computation
doesn't issue thousands of HTTP calls. For UK CGT the `base` column is
always `GBP`, but the schema does not constrain it: an FX-instrument
trade priced in `EUR` may need a `USD/EUR` cross via this table at a
later date.

This table is **not** linked to `trades` or `instruments` by foreign
key. It is read at compute time by date, never by trade id.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `base` | `TEXT` | No (PK) | ISO-4217 base currency. |
| `quote` | `TEXT` | No (PK) | ISO-4217 quote currency. |
| `rate_date` | `TEXT` | No (PK) | Publication date, `YYYY-MM-DD`. |
| `rate` | `TEXT` | No | Decimal string — `1 base = rate * quote` on `rate_date`. |
| `fetched_at` | `TEXT` | No | ISO-8601 UTC datetime; refreshed on every upsert so cache age is observable. |

See [`index.md`](./index.md#encoding-conventions) for decimal and
datetime encoding conventions.

## Primary key

`(base, quote, rate_date)` — composite natural key. The triple is the
only meaningful identity for a cached rate; a surrogate would add
nothing. Storing the columns in this order gives the index that
order, which the FX read path
[`FXRateRepo.get_latest_on_or_before`](../../src/ib_cgt/fx/service.py)
exploits directly: a range-scan on `rate_date` within a fixed
`(base, quote)` prefix is served by the PK alone, with no sort step.

## Foreign keys

None. The cache stands alone.

## Uniqueness constraints

None beyond the primary key.

## CHECK constraints

None at the database level. ISO-4217 validity is enforced upstream
in the domain layer.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| `ix_fx_rates_quote_date` | `(quote, rate_date)` | Reverse-direction lookup: when `base` is fixed but the search needs a `quote`-first prefix (e.g. listing every rate for `USD` regardless of base). |

The primary key already serves the dominant `(base, quote, rate_date)`
access pattern; this secondary index is for the less-common
"what's cached for currency X?" use case.

## Views

None.

## Read paths

- [`FXRateRepo.get(base, quote, on)`](../../src/ib_cgt/db/repos/fx_rates.py)
  — single-date lookup, returns `None` on miss.
- [`FXRateRepo.dates_present(base, quote, start, end)`](../../src/ib_cgt/db/repos/fx_rates.py)
  — set of cached `rate_date`s in `[start, end]`, used by the FX sync
  to compute the missing set before hitting Frankfurter.
- [`FXRateRepo.max_rate_date(base, quote)`](../../src/ib_cgt/db/repos/fx_rates.py)
  — newest cached date for a pair; the incremental-sync resume
  point.
- [`FXRateRepo.get_latest_on_or_before(base, quote, on, *, max_lookback_days=10)`](../../src/ib_cgt/db/repos/fx_rates.py)
  — business-day fallback for ECB non-publication dates (weekends,
  TARGET holidays); returns `(rate_date, rate)` or `None`.

## Write paths

- [`FXRateRepo.upsert_many(rates)`](../../src/ib_cgt/db/repos/fx_rates.py)
  — batched `INSERT … ON CONFLICT(base, quote, rate_date) DO UPDATE
  SET rate = excluded.rate, fetched_at = excluded.fetched_at`. The
  upsert path overwrites stale entries so a Frankfurter correction for
  a past date is honoured.

## CLI commands that touch this table

- `ib-cgt fx sync [--currency CODE]…`
  ([`src/ib_cgt/cli.py`](../../src/ib_cgt/cli.py),
  [`src/ib_cgt/fx/service.py`](../../src/ib_cgt/fx/service.py)) —
  resumes from `max_rate_date` per currency and inserts the missing
  range.

## Sample (first 5 rows)

Table is currently empty.
