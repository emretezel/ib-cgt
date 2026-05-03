# `dividends`

## Purpose

One row per non-trade cash distribution parsed from an IB statement —
cash dividends, payment-in-lieu-of-dividend, and withholding tax.
Dividends themselves are **income**, not CGT events; this table
exists so the FX rule engine can fold the foreign-currency cash leg
into the per-currency S.104 pool (HMRC CG78315 — "foreign currency
arising from any source"). The CGT / income-tax treatment of the
dividend itself is out of scope; only the cash inflow/outflow is
modelled here.

A separate table from `trades` rather than synthesised `Trade` rows
because dividends are a structurally different event: the holding
survives, no quantity of shares is transacted, and there is no
per-unit price being negotiated. Reusing `trades` would mean
loosening `Trade._check_action_vs_instrument` invariants and forcing
every BUY/SELL consumer to filter out a non-trade action — an
AGENTS.md rule 3 violation. Cash-for-shares mergers go into `trades`
because they *are* forced sales (the instrument vanishes); dividends
do not.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `dividend_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by `INTEGER PRIMARY KEY`. |
| `account_id` | `TEXT` | No (FK) | Owning IB account. |
| `instrument_id` | `INTEGER` | No (FK) | The stock the dividend was paid on. |
| `kind` | `TEXT` | No | One of `cash_dividend`, `withholding_tax`, `payment_in_lieu` (CHECK-constrained). |
| `pay_date` | `TEXT` | No | `YYYY-MM-DD` payment date — when the cash hits the foreign-currency balance. The FX rate at this date is what the projector applies for GBP conversion. |
| `amount_native` | `TEXT` | No | Decimal string, **strictly positive**. Direction of cash movement is encoded in `kind`, never via a sign on the amount (mirrors the `Trade.quantity > 0` / sign-on-`action` convention). |
| `currency` | `TEXT` | No | ISO-4217 currency of `amount_native`. |
| `description` | `TEXT` | No | Raw IB description string verbatim (e.g. `"AAPL(US0378331005) Cash Dividend USD 0.24 per Share (Mixed Income)"`). The forensic anchor that lets a future `show dividend <id>` audit command reconcile a stored row to its source HTML row. |
| `statement_row_index` | `INTEGER` | No | Zero-based offset within the dividend section of the source statement; assigned at ingest from `enumerate(map_dividends(...))`. Independent of the `trades.statement_row_index` space — each table owns its own row-index counter. |
| `source_statement_hash` | `TEXT` | No (FK) | Provenance — the statement this row was parsed from. |

See [`index.md`](./index.md#encoding-conventions) for decimal, money,
and date encoding conventions.

## Primary key

`dividend_id` — surrogate (matches the `trades` precedent post-005).

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md) — default `RESTRICT`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.
- `source_statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.

The cascade is what makes `ib-cgt ingest --replace` and
`ib-cgt db reset` clean: deleting a `statements` row also removes
every dividend that pointed at it.

## Uniqueness constraints

- `UNIQUE (source_statement_hash, statement_row_index)` — the
  provenance-based identity (mirrors `trades` migration 006). Every
  row in the parsed dividends section gets its own zero-based
  offset, so the composite is dense and unique within one ingest
  call by construction. `INSERT OR IGNORE` on this constraint
  backstops a partial-batch retry; the hash-level short-circuit on
  `statements` handles the "byte-identical re-import" case before
  this constraint is reached.

## CHECK constraints

- `kind IN ('cash_dividend', 'withholding_tax', 'payment_in_lieu')`
  — the documented row variants. A future variant (e.g. dividend
  reclassified as return-of-capital) requires a migration that
  expands the CHECK list.
- `statement_row_index >= 0` — sanity guard.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_dividends_instrument_pay` | `(instrument_id, pay_date)` | Hot path for any future per-instrument dividend audit command (mirrors `ix_trades_instrument_dt`). |
| `ix_dividends_pay_currency` | `(currency, pay_date)` | Drives `DividendRepo.for_currency` — the bulk filter the FX cashflow projector uses to pull every USD / EUR / etc. dividend in chronological order. |
| `ix_dividends_statement` | `(source_statement_hash)` | Lets `ingest --replace` find rows to cascade-delete by statement hash without scanning the table. |

## Read paths

- `DividendRepo.for_currency(currency, since=, until=)` — primary
  consumer is the FX rule engine via the CLI's `_load_fx_audit_data`
  helper. Returns `(dividend_id, Dividend)` pairs ordered by
  `pay_date` ASC.
- `DividendRepo.get(dividend_id)` — single-row audit lookup.
  Returns a `StoredDividend` DTO carrying the dividend plus its
  statement provenance.
- `DividendRepo.count()` — test-support helper.

## Write paths

- `DividendRepo.insert_many(dividends, source_statement_hash=...)`
  — called by `ingest_statement` after `map_dividends(parsed)`.
  `INSERT OR IGNORE` semantics; idempotent under the
  `(source_statement_hash, statement_row_index)` UNIQUE.

## CLI commands

- `ib-cgt ingest <path>` — the only producer. The CLI summary now
  prints "N new / M dividend cashflows" alongside the trade count.
- `ib-cgt match fx` — primary consumer. Cash-dividend events show
  up in the Disp ID / Acq ID column as `Div #N`, withholding-tax
  events as `WHT #N`, where `N` is the real `dividend_id`.

## Sample (first 5 rows)

```
SELECT dividend_id, account_id, instrument_id, kind, pay_date,
       amount_native, currency, description, statement_row_index
FROM dividends
LIMIT 5;
```

(empty on a fresh DB; populated after the first ingest of a
statement carrying a `tblCombDiv_<acct>Body` section.)
