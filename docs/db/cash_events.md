# `cash_events`

## Purpose

One row per **instrument-less cash movement** on an account, parsed
from the statement sections that carry them: broker interest
(`tblCombInt_`), deposits and withdrawals (`tblCombDepWith_`), fees
(`tblCombFees_`), and the withholding-tax rows that name no stock
(`tblWithholdingTax_`). None of these is a CGT event in itself; the
table exists so the FX rule engine can treat every foreign-currency
movement as currency arising from a source (HMRC CG78315): a dollar
of credit interest is a dollar acquired, a dollar of fees is a dollar
spent, an external USD deposit is dollars acquired at the spot rate on
the day they arrived.

What is **not** here, and why:

- **Bond coupons** — the Interest section's `Bond Coupon Payment`
  rows belong to an instrument and live in
  [`bond_coupons`](./bond_coupons.md). The two mappers partition the
  section through `is_coupon_description`, so a row is never counted
  twice.
- **Withholding on dividends** — names the stock and lives in
  [`dividends`](./dividends.md) as `kind = withholding_tax`. Only the
  withholding rows with no instrument behind them (tax withheld on
  broker interest, and its cancellation) are cash events; the two
  mappers partition that section through `has_instrument_prefix`.
- **Internal transfers** between the taxpayer's own IB accounts
  (`Internal Transfer In From Account U…` / `… Out To Account U…`).
  Both legs appear, one per account, and the pools already span
  every account, so they net to zero and are skipped at ingest.
- **Trade cash legs** — settlement cash on stock, bond and forex
  trades is projected from `trades` itself.

GBP rows are stored like any other (the audit trail is complete) but
the FX projector ignores them: no FX pool exists for sterling.

## The signed-amount convention

`amount_native` is **signed** — the only monetary column in the
schema that is. Direction is the sign: positive means currency
arrived in the balance (an FX-pool **acquisition**), negative means
currency left it (a **disposal** of the absolute amount). The sibling
tables carry direction in a `kind` column and store magnitudes;
that does not work here because the same kind goes either way — a
fee can be refunded, and IB printed negative `JPY Credit Interest`
throughout the negative-rate years. A description-based rule would
misread those rows; the sign never does.

`kind` therefore records the **statement section of origin**, not
the direction, and is purely descriptive (it labels the row in
`match fx` output and lets audit queries group by source).

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `cash_event_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by `INTEGER PRIMARY KEY`. The `Cash #N` label in `match fx` output. |
| `account_id` | `TEXT` | No (FK) | The IB account whose balance moved. |
| `kind` | `TEXT` | No | Section of origin, CHECK-closed: `interest`, `transfer`, `fee`, `withholding`. |
| `value_date` | `TEXT` | No | `YYYY-MM-DD` date the cash hit or left the balance — the FX-rate date the projector applies. |
| `amount_native` | `TEXT` | No | **Signed** Decimal string (see above). Never zero: the mapper raises on a zero row. |
| `currency` | `TEXT` | No | ISO-4217 currency of `amount_native`. GBP rows are stored but never feed a pool. |
| `description` | `TEXT` | No | Raw IB description verbatim (`USD Credit Interest for May-2025`, `Electronic Fund Transfer`, `Snapshot Market Data Fee for Apr-2025`, …) — the audit anchor back to the source HTML row. |
| `statement_row_index` | `INTEGER` | No | Zero-based offset within the statement's **cash-event stream**: parser emit order across the sections (interest, then deposits and withdrawals, then fees, then the instrument-less withholding rows), after the coupon and internal-transfer rows have been dropped. Independent of every other table's row-index space. |
| `source_statement_hash` | `TEXT` | No (FK) | Provenance — the statement the row was parsed from. |

See [`index.md`](./index.md#encoding-conventions) for decimal and
date encoding.

## Primary key

`cash_event_id` — surrogate, mirroring `dividend_id` /
`bond_coupon_id`.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md) — default `RESTRICT`.
- `source_statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.

No `instrument_id`: by definition nothing instrument-shaped is behind
these rows.

## Uniqueness constraints

- `UNIQUE (source_statement_hash, statement_row_index)` — the
  provenance identity, same shape as `dividends` / `bond_coupons`.
  `INSERT OR IGNORE` on it backstops a partial-batch retry.

## CHECK constraints

- `kind IN ('interest', 'transfer', 'fee', 'withholding')`.
- `statement_row_index >= 0`.

A non-zero amount is enforced by the domain object
(`CashEvent.__post_init__`) rather than the schema; check **A14**
flags any zero that a hand-edit lets through.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_cash_events_currency_date` | `(currency, value_date)` | Drives `CashEventRepo.for_currency` — the FX projector's chronological pull of every USD / EUR / JPY row. |
| `ix_cash_events_statement` | `(source_statement_hash)` | Lets the statement cascade and provenance lookups find rows without a table scan. |

## Views

None.

## Read paths

- [`CashEventRepo.for_currency(currency, since=, until=)`](../../src/ib_cgt/db/repos/cash_events.py)
  — `(cash_event_id, CashEvent)` pairs ordered by `value_date`, then
  id. Consumed by the engine runner (`load_fx_inputs`) for every
  non-GBP currency, which allocates each row a synthetic id from
  `4 * 10**12` and records a `CashEventRef` in `FXInputs.sources`.
- [`CashEventRepo.distinct_currencies()`](../../src/ib_cgt/db/repos/cash_events.py)
  — sorted currencies with at least one row (GBP included; the
  runner drops it). Unioned into the FX pool list so a currency that
  appears only as, say, JPY interest still gets a pool.
- [`CashEventRepo.get(cash_event_id)`](../../src/ib_cgt/db/repos/cash_events.py)
  — single-row audit lookup returning a `StoredCashEvent` with its
  statement provenance.
- [`CashEventRepo.latest_value_date()`](../../src/ib_cgt/db/repos/cash_events.py),
  [`CashEventRepo.count()`](../../src/ib_cgt/db/repos/cash_events.py)
  — support helpers.

## Write paths

- [`CashEventRepo.insert_many(events, *, source_statement_hash)`](../../src/ib_cgt/db/repos/cash_events.py)
  — called by `ingest_statement` after `map_cash_events(parsed)`,
  inside the one ingest transaction.

## CLI commands that touch this table

- `ib-cgt ingest PATH` — the only producer. The summary line prints
  `N new / M cash events`.
- `ib-cgt match fx` / `show match` — consumers via the runner. Rows
  surface in the Disp ID / Acq ID column as `Cash #N` with the
  description `<kind>: <IB description>`.
- `ib-cgt check data` — check **A14** (referential integrity, date,
  non-zero amount).
- `ib-cgt db reset` — clears the table.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
first full re-ingest following migration 017.

```
        cash_event_id = 1
           account_id = U1004320
                 kind = interest
           value_date = 2018-03-05
        amount_native = -0.37
             currency = EUR
          description = EUR Debit Interest for Feb-2018
  statement_row_index = 0
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

        cash_event_id = 2
           account_id = U1004320
                 kind = interest
           value_date = 2018-04-04
        amount_native = -2.98
             currency = EUR
          description = EUR Debit Interest for Mar-2018
  statement_row_index = 1
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

        cash_event_id = 3
           account_id = U1004320
                 kind = interest
           value_date = 2018-01-04
        amount_native = 1.29
             currency = GBP
          description = GBP Credit Interest for Dec-2017
  statement_row_index = 2
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

        cash_event_id = 4
           account_id = U1004320
                 kind = interest
           value_date = 2018-04-04
        amount_native = 0.15
             currency = GBP
          description = GBP Credit Interest for Mar-2018
  statement_row_index = 3
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7

        cash_event_id = 5
           account_id = U1004320
                 kind = interest
           value_date = 2017-12-05
        amount_native = -1.63
             currency = USD
          description = USD Debit Interest for Nov-2017
  statement_row_index = 4
source_statement_hash = a7d240d88f17027046f6725b3f5c916663342dc94eaa0b2c45ff4dc699f122b7
```
