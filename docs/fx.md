# FX service

How `ib-cgt` turns native-currency trades into GBP for UK CGT.

## Why FX is both a converter *and* an asset class

UK CGT requires every disposal to be reported in GBP, converted at the
spot rate on the transaction date. That makes the FX service a
**converter** for stocks, bonds, and futures denominated in non-GBP
currencies.

It is *also* an **asset class**: HMRC treats non-GBP cash balances as
chargeable assets, pooled per currency pair against GBP under the same
same-day / 30-day / S.104 rules as shares. The rate cache we build for
conversion is reused by the FX rule engine (`ib_cgt.rules.fx`) when
that lands.

## Cache model

- **Table**: `fx_rates(base TEXT, quote TEXT, rate_date TEXT, rate TEXT,
  fetched_at TEXT)`, PK `(base, quote, rate_date)`, secondary index on
  `(quote, rate_date)`. See
  [`architecture.md`](./architecture.md#component-map) for the full
  schema story.
- **Storage convention**: `base = 'GBP'` for every cached row. A row
  `(GBP, USD, 2025-01-02, 1.2550)` means **`1 GBP = 1.2550 USD`** on
  that date — Frankfurter's default convention when the caller passes
  `base=GBP`.
- **Precision**: rate stored as a canonical `Decimal` string; floats
  never appear on the Python side either (the client parses JSON
  numbers through `Decimal(str(raw))`).
- **Publication dates only**: weekends and TARGET holidays are *not*
  represented in the cache — the upstream ECB feed does not publish
  rates on non-working days, and synthesising placeholders would make
  "is this cached?" ambiguous. Fallback happens at read time instead.

## Business-day fallback

When CGT needs a rate for date `D`:

1. Look up the most recent `rate_date ≤ D` within a fixed window of
   `fallback_days` calendar days.
2. If found, use it (this is the "previous working day" convention).
3. If not found, raise `RateNotFoundError` — never silently serve a
   rate from weeks ago.

Default window: **10 calendar days**. Long enough to cover an Easter
weekend plus a single missed publication; short enough that a badly
populated cache fails loudly rather than misreporting a tax figure.

The lookup is a single SQLite query using the primary key
`(base, quote, rate_date)`:

```sql
SELECT rate_date, rate
  FROM fx_rates
 WHERE base = ? AND quote = ?
   AND rate_date <= ?
   AND rate_date >= ?       -- D − fallback_days
 ORDER BY rate_date DESC
 LIMIT 1;
```

`EXPLAIN QUERY PLAN` confirms SQLite serves this from the PK; there is
no sort step.

## `convert(amount, target, on)` — arithmetic

Given the `base = GBP` storage convention:

- **native → GBP**: `amount / r` where `r` is the cached
  `(GBP, native)` rate.
- **GBP → native**: `amount * r`.
- **cross-currency** (neither leg is GBP): two lookups, pivoting
  through GBP — `(amount / r_from) * r_to`.

Same-currency conversions (including GBP → GBP) short-circuit and do
not touch the DB.

All arithmetic stays in `Decimal`; the result's precision is whatever
falls out of Python's default decimal context (28 digits). Rounding to
pennies is a reporting concern, not a conversion concern.

## `ib-cgt fx sync --year YYYY`

Bulk-fills the cache for a UK tax year (6 Apr `YYYY` → 5 Apr `YYYY+1`),
padded on the front by `fallback_days` so a 6 April Monday lookup can
still fall back to Good Friday.

```text
ib-cgt fx sync --year 2024
# → fetches every ECB publication inside [2024-03-27, 2025-04-05]
#   for every non-GBP currency that appears on a 2024/25 trade.
```

Currency set, in order:

1. If `--currency` (`-c`) is passed one or more times, use exactly that
   set (upper-cased, deduplicated).
2. Otherwise, auto-detect:
   ```sql
   SELECT DISTINCT i.currency
     FROM trades t
     JOIN instruments i ON i.instrument_id = t.instrument_id
    WHERE t.trade_date BETWEEN <start> AND <end>
      AND i.currency != 'GBP'
    ORDER BY i.currency;
   ```
   This uses the `ix_trades_trade_date` index.

The command is idempotent: dates already cached are not re-fetched, and
the "rows written" figure in the summary reflects genuinely new rows.

### Environment variables

| Variable        | Default                          | Purpose                                  |
|-----------------|----------------------------------|------------------------------------------|
| `IB_CGT_DB`     | `~/.ib-cgt/ibcgt.sqlite`         | SQLite file location.                    |
| `IB_CGT_FX_URL` | `https://api.frankfurter.dev`    | Override for tests / local Frankfurter.  |

## Error model

| Exception            | Raised when                                                         |
|----------------------|---------------------------------------------------------------------|
| `RateNotFoundError`  | The cache has no rate within `fallback_days` of the requested date. |
| `FrankfurterError`   | Upstream HTTP failure or malformed response.                        |
| `FXServiceError`     | Base class for the two above — catch this for "anything FX-shaped". |

All three live in `ib_cgt.fx`.
