# `corporate_actions`

## Purpose

One row per corporate action IB printed in a statement's **Corporate
Actions** section (`tblCorporateActions_<acct>Body`), or one row per
printed line when the event is one the model does not classify. A
corporate action is not a trade — nothing was bought or sold at a
negotiated price — but it can still be a CGT disposal (a cash-for-
shares merger, a bond redemption) and it can still move foreign
currency into an account (IEMI, a GBP-listed fund, was cashed out for
14,425.52 USD). Until migration `024` the ingest squashed such events
into synthesised `SELL` trades, which lost the cash leg's currency:
the USD never reached the USD pool and the fact "824 units for
14,425.52 USD" was stored nowhere.

The row models an event as **up to three legs**: a security leg out
(negative `quantity`), a security leg in (positive `quantity`) and a
cash leg (signed `cash_amount` in `cash_currency`). `kind` says
whether the engines model the row:

| Kind | Meaning | What the engines do |
|---|---|---|
| `cash_disposal` | Units left the account and cash arrived — a cash-for-shares merger, a bond maturity, a tender for cash. Always one negative quantity and one positive cash amount. | The stock or bond engine books a disposal of `|quantity|` on `effective_date` with proceeds = the cash converted at that date; the FX engine books the cash into its currency's pool on the same date. Both cite the same synthetic event id, `5·10**12 + corporate_action_id`, printed as `CA #N`. |
| `unsupported` | Anything else — a split, a spin-off, a share-for-share merger, a return of capital, cash in lieu of fractions, or a disposal-shaped event whose security could not be resolved. Stored as printed, one row per statement line. | Nothing. Check **A16** reports every such row that moves units or cash so it can be modelled by hand; the position reconciliation (C7) still nets its quantity when it has an instrument. |

Classification is by the **legs**, never by IB's wording: a group of
statement rows sharing a `Date/Time` and a description is a
`cash_disposal` iff it has exactly one row with a non-zero quantity,
that quantity is negative, exactly one row with non-zero cash, that
cash is positive (the same row for a same-currency event, a second
zero-quantity row in the cash currency for a cross-currency one) and
nothing else. Ingest never fails on a corporate action: an
unclassifiable group, or one whose security has no instrument, is
stored as `unsupported` rows and flagged.

`effective_datetime` is the statement's `Date/Time` in the zone the
statement declares (IB prints a batch stamp, `20:25:00` Eastern);
`effective_date` is the Europe/London date of that instant — the
disposal date under TCGA 1992 s.28 and the FX-pool date for the cash
leg, on the same trade-date basis every other event uses. IB's
`Report Date` (often days later, when the action was processed) is
kept for audit only.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `corporate_action_id` | `INTEGER` | No (PK) | Surrogate id; auto-issued by `INTEGER PRIMARY KEY`. The `N` in `CA #N`, and the low part of the synthetic event id `5·10**12 + N` the engines cite. |
| `account_id` | `TEXT` | No (FK) | The IB account the event happened in. |
| `kind` | `TEXT` | No | `cash_disposal` or `unsupported` (CHECK-constrained). |
| `instrument_id` | `INTEGER` | Yes (FK) | The security the row moves, resolved to the same `instruments` row the trades use (stocks by conid through the `SYMBOL(ISIN)` tag, bonds by the leading `(ISIN)`). `NULL` only on `unsupported` rows whose security could not be resolved. |
| `effective_datetime` | `TEXT` | No | The statement's `Date/Time` as an ISO-8601 UTC instant. |
| `effective_date` | `TEXT` | No | `YYYY-MM-DD`, the Europe/London date of `effective_datetime` — the disposal and FX-pool date. |
| `report_date` | `TEXT` | No | IB's `Report Date`, audit only. |
| `quantity` | `TEXT` | No | **Signed** Decimal string in the instrument's unit; negative = units out, positive = units in, zero = a row that moves no units (a cash-only line). |
| `cash_amount` | `TEXT` | Yes | **Signed** Decimal string; `NULL` (together with `cash_currency`) when the row has no cash leg. Never zero. |
| `cash_currency` | `TEXT` | Yes | ISO-4217 currency of `cash_amount` — the currency the cash arrived in, which need not be the security's (IEMI: GBP listing, USD cash). |
| `description` | `TEXT` | No | IB's description verbatim — the audit anchor back to the statement row. CHECK-constrained non-empty. |
| `statement_row_index` | `INTEGER` | No | Zero-based position within the statement's corporate-action stream (mapper emit order: groups by instant and description, rows in statement order within a group). Independent of every other table's row-index space. |
| `source_statement_hash` | `TEXT` | No (FK) | Provenance — the statement the row was parsed from. |

See [`index.md`](./index.md#encoding-conventions) for decimal, money
and date encoding.

## Primary key

`corporate_action_id` — surrogate, mirroring `dividend_id` /
`cash_event_id`.

## Foreign keys

- `account_id` → [`accounts.account_id`](./accounts.md) — default `RESTRICT`.
- `instrument_id` → [`instruments.instrument_id`](./instruments.md) — default `RESTRICT`.
- `source_statement_hash` → [`statements.statement_hash`](./statements.md) — `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (source_statement_hash, statement_row_index)` — the
  provenance identity every statement-derived table shares.
  `INSERT OR IGNORE` on it makes a re-presented row a no-op.

## CHECK constraints

- `kind IN ('cash_disposal', 'unsupported')`.
- `(cash_amount IS NULL) = (cash_currency IS NULL)` — a cash leg is
  an amount *and* a currency, or nothing.
- `cash_currency IS NULL OR cash_currency GLOB '[A-Z][A-Z][A-Z]'`.
- `kind = 'unsupported' OR instrument_id IS NOT NULL` — a modelled
  disposal names its security.
- `kind <> 'cash_disposal' OR (cash_amount IS NOT NULL AND CAST(quantity AS REAL) < 0 AND CAST(cash_amount AS REAL) > 0)`
  — the legs of a cash disposal. The sign tests cast the Decimal text
  to `REAL`: the sign survives the cast, only precision is lost, and
  the domain object (`CorporateAction.__post_init__`) enforces the
  exact rule.
- `length(description) > 0`, `statement_row_index >= 0`.

## Indexes

| Name | Columns | Query pattern |
|---|---|---|
| `ix_corporate_actions_instrument_date` | `(instrument_id, effective_date)` | `CorporateActionRepo.for_instrument` (the stock and bond engines' per-instrument load) and `signed_quantity_by_instrument` (the position reconciliation). |
| `ix_corporate_actions_cash_currency_date` | `(cash_currency, effective_date)` | `list_cash_disposals` / `distinct_cash_currencies` — the FX runner's chronological pull per pool currency, and `fx sync`'s currency discovery. |
| `ix_corporate_actions_statement` | `(source_statement_hash)` | The statement cascade and per-statement audit reads. |

## Views

None.

## Read paths

- [`CorporateActionRepo.for_instrument(instrument_id, *, since, until)`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — every kind, ordered by `effective_date` then id; the runner hands
  them to `StockRuleEngine.compute` / `BondRuleEngine.compute`, which
  act on the `cash_disposal` rows only.
- [`CorporateActionRepo.list_cash_disposals(*, since, until)`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — every `cash_disposal` in date order; `load_fx_inputs` registers
  each one in `FXInputs.sources` as a `CorporateActionRef` (GBP cash
  included, so a stock or bond citation of a GBP redemption still
  resolves) and the FX projector books the non-GBP ones.
- [`CorporateActionRepo.distinct_cash_currencies()`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — unioned into the FX pool list, so a currency that arrives only
  through a corporate action still gets a pool.
- [`CorporateActionRepo.signed_quantity_by_instrument(account_id, *, up_to)`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — net units per instrument, any kind, for `reconcile_positions`.
- [`CorporateActionRepo.get(corporate_action_id)`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — single-row audit lookup (`show match --corporate-action`, the
  report's event resolver) returning a `StoredCorporateAction` with
  its provenance.
- [`CorporateActionRepo.count()`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — test-support helper.

## Write paths

- [`CorporateActionRepo.insert_indexed(rows, *, source_statement_hash)`](../../src/ib_cgt/db/repos/corporate_actions.py)
  — called by `ingest_statement` after `map_corporate_actions(parsed)`
  and the coverage filter (by `effective_date`), inside the one
  ingest transaction; upserts the instrument when the row has one.

## CLI commands that touch this table

- `ib-cgt ingest PATH` — the only producer. The summary line prints
  `N new / M corporate actions`, and a yellow note when any row is
  `unsupported`.
- `ib-cgt match stocks` / `match bonds` — the disposal row prints as
  `CA #N`; `ib-cgt match fx` shows the cash leg as `acq CA #N`
  (`corporate action <symbol> cash_disposal`).
- `ib-cgt show match --corporate-action N` (or `--disposal` with the
  synthetic event id) — the chunk audit of the cash leg.
- `ib-cgt check data` — **A16** lists `unsupported` rows that move
  units or cash; **A15** covers the table's dates.
- `ib-cgt check all` — **C7** nets the quantities into the position
  reconciliation; **C11** sees the cash legs through the pools;
  **D4** resolves persisted `CA` citations through
  [`event_sources`](./event_sources.md).
- `ib-cgt db reset` — clears the table.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration 024 (`SELECT * FROM
corporate_actions LIMIT 5`, no ordering; the system `sqlite3` binary
predates STRICT tables). The live table holds six rows: five gilt
redemptions at par in GBP and the IEMI cash-out in USD (id 6).

```
  corporate_action_id = 1
           account_id = U1004320
                 kind = cash_disposal
        instrument_id = 302
   effective_datetime = 2023-09-07T00:25:00+00:00
       effective_date = 2023-09-07
          report_date = 2023-09-07
             quantity = -170000
          cash_amount = 170000.00
        cash_currency = GBP
          description = (GB00B7Z53659)  Bond Maturity FOR GBP 1.00 PER BOND (UKT 2 1/4 09/07/23, UKT 2 1/4 09/07/23, GB00B7Z53659)
  statement_row_index = 0
source_statement_hash = 2a8bab1ee64bc7b1a139782b717c71f25ebaa2e2799b9d53f60e0397a9681821

  corporate_action_id = 2
           account_id = U1004320
                 kind = cash_disposal
        instrument_id = 454
   effective_datetime = 2024-01-31T01:25:00+00:00
       effective_date = 2024-01-31
          report_date = 2024-01-31
             quantity = -200000
          cash_amount = 200000.00
        cash_currency = GBP
          description = (GB00BMGR2791)  Bond Maturity FOR GBP 1.00 PER BOND (UKT 0 1/8 01/31/24, UKT 0 1/8 01/31/24, GB00BMGR2791)
  statement_row_index = 1
source_statement_hash = 2a8bab1ee64bc7b1a139782b717c71f25ebaa2e2799b9d53f60e0397a9681821

  corporate_action_id = 3
           account_id = U1004320
                 kind = cash_disposal
        instrument_id = 455
   effective_datetime = 2024-09-07T00:25:00+00:00
       effective_date = 2024-09-07
          report_date = 2024-09-09
             quantity = -250000
          cash_amount = 250000.00
        cash_currency = GBP
          description = (GB00BHBFH458)  Bond Maturity FOR GBP 1.00 PER BOND (UKT 2 3/4 09/07/24 FH45, UKT 2 3/4 09/07/24, GB00BHBFH458)
  statement_row_index = 0
source_statement_hash = 7e21d25e360788cb192430058c52fdd935f51bf73691943d61734a9ee189e74a

  corporate_action_id = 4
           account_id = U1004320
                 kind = cash_disposal
        instrument_id = 586
   effective_datetime = 2025-01-31T01:25:00+00:00
       effective_date = 2025-01-31
          report_date = 2025-01-31
             quantity = -215000
          cash_amount = 215000.00
        cash_currency = GBP
          description = (GB00BLPK7110)  Bond Maturity FOR GBP 1.00 PER BOND (UKT 0 1/4 01/31/25, UKT 0 1/4 01/31/25, GB00BLPK7110)
  statement_row_index = 1
source_statement_hash = 7e21d25e360788cb192430058c52fdd935f51bf73691943d61734a9ee189e74a

  corporate_action_id = 5
           account_id = U1004320
                 kind = cash_disposal
        instrument_id = 587
   effective_datetime = 2026-01-30T01:25:00+00:00
       effective_date = 2026-01-30
          report_date = 2026-01-30
             quantity = -310000
          cash_amount = 310000.00
        cash_currency = GBP
          description = (GB00BL68HJ26)  Bond Maturity FOR GBP 1.00 PER BOND (UKT 0 1/8 01/30/26, UKT 0 1/8 01/30/26, GB00BL68HJ26)
  statement_row_index = 0
source_statement_hash = e418350673e03c4079333e63bbfadb64f2e1f317f8ae630a3f1b87cdc811c894
```
