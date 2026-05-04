# `bond_coupons`

## Purpose

One row per bond coupon payment parsed from an IB statement's
`Interest` section (`tblCombInt_<acct>Body`). Coupons are
**income**, not CGT events; this table exists so the FX rule engine
can fold the foreign-currency cash leg into the per-currency S.104
pool (HMRC CG78315 — "foreign currency arising from any source").
The income-tax treatment of the coupon itself (interest savings
allowance, accrued-income-scheme interactions) is out of scope; only
the cash inflow is modelled here.

GBP-denominated coupons are still persisted (for audit completeness)
but the FX projector skips them — no FX trade is realised on a GBP
cashflow for a UK taxpayer.

A separate table from `dividends` rather than a `kind="bond_coupon"`
extension because coupons and equity dividends are structurally
distinct: different IB statement sections, different income-tax
treatment, and a `Dividend.instrument` typed narrowly as
`StockInstrument` would have to broaden to a polymorphic instrument
reference. Per AGENTS.md §3 ("each table represents one thing"), a
dedicated table is the cleaner choice.

A separate table from `trades` for the same reason `dividends` is —
no quantity transacts, no per-unit price negotiates, the holding
survives.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `bond_coupon_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by `INTEGER PRIMARY KEY`. |
| `account_id` | `TEXT` | No (FK) | Owning IB account. |
| `instrument_id` | `INTEGER` | No (FK) | The bond the coupon was paid on. |
| `pay_date` | `TEXT` | No | `YYYY-MM-DD` payment date — when the cash hits the foreign-currency balance. The FX rate at this date is what the projector applies for GBP conversion. |
| `amount_native` | `TEXT` | No | Decimal string, **strictly positive**. Coupons are credits — there is no withholding-tax variant on bond interest in any corpus seen so far, and no signed-amount convention. |
| `currency` | `TEXT` | No | ISO-4217 currency of `amount_native`. |
| `description` | `TEXT` | No | Raw IB description string verbatim (e.g. `"Bond Coupon Payment (UKT 0 1/8 01/30/26 - United Kingdom Gilt UKT 0 1/8 01/30/26)"`). The forensic anchor that lets a future `show coupon <id>` audit command reconcile a stored row to its source HTML row. |
| `statement_row_index` | `INTEGER` | No | Zero-based offset within the **bond-coupon stream** of the source statement; assigned at ingest from `enumerate(map_bond_coupons(...))`. Independent of the `trades`, `dividends`, and broker-interest spaces — each table owns its own row-index counter. Broker debit/credit interest rows are filtered before this index is assigned. |
| `source_statement_hash` | `TEXT` | No (FK) | Provenance — the statement this row was parsed from. |

See [`index.md`](./index.md#encoding-conventions) for decimal, money,
and date encoding conventions.

## Primary key

`bond_coupon_id` — surrogate, mirroring `dividend_id` on the
sibling table.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md) — default `RESTRICT`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.
- `source_statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.

The cascade is what makes `ib-cgt ingest --replace` and
`ib-cgt db reset` clean: deleting a `statements` row also removes
every coupon that pointed at it.

## Uniqueness constraints

- `UNIQUE (source_statement_hash, statement_row_index)` — the
  provenance-based identity, same shape as `dividends`. The
  bond-coupon mapper enumerates over the filtered set (broker
  interest already dropped), so the index is dense and unique
  within one ingest call by construction. `INSERT OR IGNORE` on
  this constraint backstops a partial-batch retry; the hash-level
  short-circuit on `statements` handles the "byte-identical
  re-import" case before this constraint is reached.

## CHECK constraints

- `statement_row_index >= 0` — sanity guard.
- `CAST(amount_native AS REAL) > 0` — coupons are credits;
  zero-magnitude rows are mapped as `MappingError` upstream and
  cannot reach the table.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_bond_coupons_instrument_pay` | `(instrument_id, pay_date)` | Hot path for a future per-bond coupon audit command (mirrors `ix_dividends_instrument_pay`). |
| `ix_bond_coupons_pay_currency` | `(currency, pay_date)` | Drives `BondCouponRepo.for_currency` — the bulk filter the FX cashflow projector uses to pull every USD / EUR / JPY coupon in chronological order. |
| `ix_bond_coupons_statement` | `(source_statement_hash)` | Lets `ingest --replace` find rows to cascade-delete by statement hash without scanning the table. |

## Read paths

- `BondCouponRepo.for_currency(currency, since=, until=)` — primary
  consumer is the FX rule engine. Returns `(bond_coupon_id,
  BondCoupon)` pairs ordered by `pay_date` ASC. GBP coupons are
  retrievable via `currency="GBP"` for audit but the FX projector
  skips them.
- `BondCouponRepo.get(bond_coupon_id)` — single-row audit lookup.
  Returns a `StoredBondCoupon` DTO carrying the coupon plus its
  statement provenance.
- `BondCouponRepo.count()` — test-support helper.

## Write paths

- `BondCouponRepo.insert_many(coupons, source_statement_hash=...)`
  — called by `ingest_statement` after `map_bond_coupons(parsed)`.
  `INSERT OR IGNORE` semantics; idempotent under the
  `(source_statement_hash, statement_row_index)` UNIQUE.

## CLI commands

- `ib-cgt ingest <path>` — the only producer. The CLI summary now
  prints "N new / M bond coupons" alongside the trade and dividend
  counts when the source statement contains any.
- `ib-cgt match fx` — eventual consumer (sixth FX-cashflow source).
  Coupon events will surface in the Disp ID / Acq ID column with a
  `Cpn #N` label, where `N` is the real `bond_coupon_id`.

## Sample (first 5 rows)

```
SELECT bond_coupon_id, account_id, instrument_id, pay_date,
       amount_native, currency, description, statement_row_index
FROM bond_coupons
LIMIT 5;
```

(empty on a fresh DB; populated after the first ingest of a
statement carrying a `tblCombInt_<acct>Body` section with
`Bond Coupon Payment (...)` rows.)
