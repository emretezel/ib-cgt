# `dividends`

## Purpose

One row per non-trade cash distribution parsed from an IB statement —
cash dividends, payment-in-lieu-of-dividend, and withholding tax.
Withholding rows come from IB's `tblWithholdingTax_<acct>Body` section
(the parser originally keyed on a shorter `tblWithholding_` prefix
IB never emits, so no withholding row was ingested before the fix).
Only withholding rows that **name a stock** land here; the section's
instrument-less rows — tax withheld on broker interest and its
cancellation — are [`cash_events`](./cash_events.md) of kind
`withholding`, and the two mappers partition the section through
`has_instrument_prefix`.
Dividends themselves are **income**, not CGT events; this table
exists so the FX rule engine can fold the foreign-currency cash leg
into the per-currency S.104 pool (HMRC CG78315 — "foreign currency
arising from any source"). The CGT / income-tax treatment of the
dividend itself is out of scope; only the cash inflow/outflow is
modelled here.

Because only the cash leg matters, a dividend is **not linked to an
instrument** (migration `021`), exactly like a cash event. The IB
security tag (`SYMBOL` from the `SYMBOL(SECID)` description prefix)
is kept as plain `symbol` text for audit labels. This also removes a
modelling trap: IB pays some ETF distributions in a currency other
than the one it prices the listing's trades in (IEMI trades in GBP
and pays USD), and under the old `(symbol, currency)` stock key each
such dividend created a phantom instrument with no trades.

A separate table from `trades` rather than synthesised `Trade` rows
because dividends are a structurally different event: the holding
survives, no quantity of shares is transacted, and there is no
per-unit price being negotiated. Reusing `trades` would mean
loosening `Trade._check_action_vs_instrument` invariants and forcing
every BUY/SELL consumer to filter out a non-trade action — an
AGENTS.md rule 3 violation. Cash-for-shares mergers go into
[`corporate_actions`](./corporate_actions.md) because they *are*
forced disposals (the instrument vanishes); dividends do not.

## The signed-amount convention

Since migration `024` `amount_native` is **signed as printed**, like
[`cash_events.amount_native`](./cash_events.md). Direction is the
sign: positive means currency arrived in the balance (an FX-pool
**acquisition**), negative means it left (a **disposal** of the
absolute amount). A cash dividend or a payment in lieu is normally
positive and withholding tax normally negative — but a payment in
lieu *paid* on a short position is negative (TUR, −887.72 USD on
2019-06-21) and a withholding refund is positive (four reversals in
January 2017), and the old mapper's `abs()` booked each of those the
wrong way round, leaving the USD pool 516.35 USD out against IB's
Cash Report. `kind` therefore records the row's **nature** and is
never consulted for direction. A zero row is rejected by the domain
object and by the schema.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `dividend_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by `INTEGER PRIMARY KEY`. |
| `account_id` | `TEXT` | No (FK) | Owning IB account. |
| `symbol` | `TEXT` | No | The IB security tag printed on the row (`AAPL` from `AAPL(US0378331005) Cash Dividend …`). An audit label, not a reference: nothing joins it to `stock_instruments`. CHECK-constrained non-empty. |
| `kind` | `TEXT` | No | One of `cash_dividend`, `withholding_tax`, `payment_in_lieu` (CHECK-constrained). |
| `pay_date` | `TEXT` | No | `YYYY-MM-DD` payment date — when the cash hits the foreign-currency balance. The FX rate at this date is what the projector applies for GBP conversion. |
| `amount_native` | `TEXT` | No | **Signed** Decimal string, never zero (see above). Positive = an FX-pool acquisition on `pay_date`, negative = a disposal of the absolute amount. |
| `currency` | `TEXT` | No | ISO-4217 currency of `amount_native` — the payment currency, which may differ from the currency the stock trades in. |
| `description` | `TEXT` | No | Raw IB description string verbatim (e.g. `"AAPL(US0378331005) Cash Dividend USD 0.24 per Share (Mixed Income)"`). The forensic anchor that lets a future `show dividend <id>` audit command reconcile a stored row to its source HTML row. |
| `statement_row_index` | `INTEGER` | No | Zero-based offset within the dividend section of the source statement; assigned at ingest from `enumerate(map_dividends(...))`. Independent of the `trades.statement_row_index` space — each table owns its own row-index counter. |
| `source_statement_hash` | `TEXT` | No (FK) | Provenance — the statement this row was parsed from. |

See [`index.md`](./index.md#encoding-conventions) for decimal, money,
and date encoding conventions.

## Primary key

`dividend_id` — surrogate (matches the `trades` precedent post-005).

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md) — default `RESTRICT`.
- `source_statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.

No `instrument_id` (since migration `021`): see the purpose section.

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
- `CAST(amount_native AS REAL) <> 0` (migration `024`) — a row that
  moves nothing is a parse error; check **A13** repeats the test.
- `statement_row_index >= 0` — sanity guard.
- `length(symbol) > 0` — the security tag is never blank.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_dividends_pay_currency` | `(currency, pay_date)` | Drives `DividendRepo.for_currency` — the bulk filter the FX cashflow projector uses to pull every USD / EUR / etc. dividend in chronological order. |
| `ix_dividends_statement` | `(source_statement_hash)` | Lets `ingest --replace` find rows to cascade-delete by statement hash without scanning the table. |

## Read paths

- `DividendRepo.for_currency(currency, since=, until=)` — primary
  consumer is the FX rule engine via the calculator's engine runner
  (`load_fx_inputs`). Returns `(dividend_id, Dividend)` pairs ordered
  by `pay_date` ASC.
- `DividendRepo.distinct_currencies()` — sorted list of every
  currency with at least one row; the runner unions it into the FX
  pool list so a dividend-only currency still gets a pool.
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
- `ib-cgt check data` — check **A13** (referential integrity, date,
  non-zero amount).

## Sample (first 5 rows)

Captured after the migration `024` re-ingest with
`SELECT dividend_id, account_id, symbol, kind, pay_date, amount_native, currency, description, statement_row_index FROM dividends LIMIT 5;`
from the live DB, rendered as `column = value` blocks. The first rows
are the 2013 PDF statement's; its withholding rows (ids 7–9) carry
negative amounts (`-13.73`, `-29.74`, `-20.93` USD).

```
        dividend_id = 1
         account_id = U1004320
             symbol = AAPL
               kind = cash_dividend
           pay_date = 2013-05-16
      amount_native = 91.51
           currency = USD
        description = AAPL Dividend 3.05 USD per Share (Ordinary Dividend)
statement_row_index = 0

        dividend_id = 2
         account_id = U1004320
             symbol = AAPL
               kind = cash_dividend
           pay_date = 2013-08-15
      amount_native = 198.25
           currency = USD
        description = AAPL Dividend 3.05 USD per Share (Ordinary Dividend)
statement_row_index = 1

        dividend_id = 3
         account_id = U1004320
             symbol = INTC
               kind = cash_dividend
           pay_date = 2013-09-01
      amount_native = 139.51
           currency = USD
        description = INTC Dividend .225 USD per Share (Ordinary Dividend)
statement_row_index = 2

        dividend_id = 4
         account_id = U1004320
             symbol = AAPL
               kind = cash_dividend
           pay_date = 2013-11-14
      amount_native = 198.25
           currency = USD
        description = AAPL Dividend 3.05 USD per Share (Ordinary Dividend)
statement_row_index = 3

        dividend_id = 5
         account_id = U1004320
             symbol = INTC
               kind = payment_in_lieu
           pay_date = 2013-12-01
      amount_native = 8.10
           currency = USD
        description = INTC Payment in Lieu of Dividend (Ordinary Dividend)
statement_row_index = 4
```
