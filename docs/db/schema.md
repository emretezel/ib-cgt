# Schema

Effective schema after migrations `001`–`021`
([`src/ib_cgt/db/migrations/`](../../src/ib_cgt/db/migrations)). Every table
is `STRICT`. Columns are `NOT NULL` unless marked `null`. `PK` = primary key.
`FK → t.c` = foreign key, `RESTRICT` unless marked `cascade` (`ON DELETE
CASCADE`). Composite keys, table-level `CHECK`s and indexes are listed under
each table. Purpose, read/write paths and sample rows are on the per-table
pages linked from [`index.md`](./index.md).

Trade-id columns on run-scoped tables (`matched_disposals`,
`future_realisations`, `fx_event_sources`) deliberately have no FK to
`trades`, so audit rows survive a re-ingest.

## Reference

### `accounts`

| Column | Type | Constraints |
|---|---|---|
| `account_id` | TEXT | PK |
| `label` | TEXT | null |

### `instruments`

| Column | Type | Constraints |
|---|---|---|
| `instrument_id` | INTEGER | PK |
| `asset_class` | TEXT | CHECK IN (`'stock'`, `'bond'`, `'future'`, `'fx'`) |

Identity per class (the natural key `InstrumentRepo.upsert` recognises an
instrument by): stocks and futures — IB's `conid`; bonds — ISIN; FX pairs —
the pair columns. `symbol` is display text everywhere; the calculator only
ever compares `instrument_id`.

### `stock_instruments`

| Column | Type | Constraints |
|---|---|---|
| `instrument_id` | INTEGER | PK, FK → `instruments.instrument_id` cascade |
| `conid` | INTEGER | UNIQUE, CHECK `> 0` |
| `symbol` | TEXT | |
| `currency` | TEXT | |

- INDEX `ix_stock_instruments_symbol_currency` `(symbol, currency)`
- INDEX `ix_stock_instruments_currency` `(currency)`

### `bond_instruments`

| Column | Type | Constraints |
|---|---|---|
| `instrument_id` | INTEGER | PK, FK → `instruments.instrument_id` cascade |
| `isin` | TEXT | UNIQUE |
| `symbol` | TEXT | |
| `currency` | TEXT | |
| `is_cgt_exempt` | INTEGER | CHECK IN (0, 1) |

- INDEX `ix_bond_instruments_currency` `(currency)`
- INDEX `ix_bond_instruments_symbol_currency` `(symbol, currency)`

### `future_instruments`

| Column | Type | Constraints |
|---|---|---|
| `instrument_id` | INTEGER | PK, FK → `instruments.instrument_id` cascade |
| `conid` | INTEGER | UNIQUE, CHECK `> 0` |
| `symbol` | TEXT | |
| `currency` | TEXT | |
| `contract_multiplier` | TEXT | |
| `expiry_date` | TEXT | |

- INDEX `ix_future_instruments_symbol_currency` `(symbol, currency)`
- INDEX `ix_future_instruments_currency` `(currency)`

### `fx_instruments`

| Column | Type | Constraints |
|---|---|---|
| `instrument_id` | INTEGER | PK, FK → `instruments.instrument_id` cascade |
| `symbol` | TEXT | |
| `currency` | TEXT | |
| `fx_base` | TEXT | |
| `fx_quote` | TEXT | |

- UNIQUE `(symbol, currency, fx_base, fx_quote)`
- INDEX `ix_fx_instruments_currency` `(currency)`

## Ingested statement data

### `statements`

| Column | Type | Constraints |
|---|---|---|
| `statement_hash` | TEXT | PK |
| `source_path` | TEXT | |
| `account_id` | TEXT | FK → `accounts.account_id` |
| `imported_at` | TEXT | |
| `trade_count` | INTEGER | |
| `period_start` | TEXT | |
| `period_end` | TEXT | |
| `time_zone` | TEXT | IANA zone key of the statement's `Date/Time` cells |

- CHECK `(period_start <= period_end)`
- CHECK `(length(time_zone) > 0)`
- INDEX `ix_statements_account_period` `(account_id, period_end)`

### `statement_positions`

| Column | Type | Constraints |
|---|---|---|
| `statement_hash` | TEXT | FK → `statements.statement_hash` cascade |
| `statement_row_index` | INTEGER | CHECK `>= 0` |
| `instrument_id` | INTEGER | FK → `instruments.instrument_id` |
| `quantity` | TEXT | |

- PK `(statement_hash, statement_row_index)`
- UNIQUE `(statement_hash, instrument_id)`

### `trades`

| Column | Type | Constraints |
|---|---|---|
| `trade_id` | INTEGER | PK |
| `account_id` | TEXT | FK → `accounts.account_id` |
| `instrument_id` | INTEGER | FK → `instruments.instrument_id` |
| `action` | TEXT | |
| `trade_datetime` | TEXT | |
| `trade_date` | TEXT | |
| `settlement_date` | TEXT | |
| `quantity` | TEXT | |
| `price_amount` | TEXT | |
| `price_currency` | TEXT | |
| `fees_amount` | TEXT | |
| `fees_currency` | TEXT | |
| `accrued_amount` | TEXT | null |
| `accrued_currency` | TEXT | null |
| `statement_row_index` | INTEGER | CHECK `>= 0` |
| `source_statement_hash` | TEXT | FK → `statements.statement_hash` cascade |

- UNIQUE `(source_statement_hash, statement_row_index)`
- INDEX `ix_trades_instrument_dt` `(instrument_id, trade_datetime)`
- INDEX `ix_trades_trade_date` `(trade_date)`
- INDEX `ix_trades_account_date` `(account_id, trade_date)`
- INDEX `ix_trades_statement` `(source_statement_hash)`

### `dividends`

| Column | Type | Constraints |
|---|---|---|
| `dividend_id` | INTEGER | PK |
| `account_id` | TEXT | FK → `accounts.account_id` |
| `symbol` | TEXT | CHECK `length > 0` |
| `kind` | TEXT | CHECK IN (`'cash_dividend'`, `'withholding_tax'`, `'payment_in_lieu'`) |
| `pay_date` | TEXT | |
| `amount_native` | TEXT | |
| `currency` | TEXT | |
| `description` | TEXT | |
| `statement_row_index` | INTEGER | CHECK `>= 0` |
| `source_statement_hash` | TEXT | FK → `statements.statement_hash` cascade |

- UNIQUE `(source_statement_hash, statement_row_index)`
- INDEX `ix_dividends_pay_currency` `(currency, pay_date)`
- INDEX `ix_dividends_statement` `(source_statement_hash)`

### `bond_coupons`

| Column | Type | Constraints |
|---|---|---|
| `bond_coupon_id` | INTEGER | PK |
| `account_id` | TEXT | FK → `accounts.account_id` |
| `instrument_id` | INTEGER | FK → `instruments.instrument_id` |
| `pay_date` | TEXT | |
| `amount_native` | TEXT | |
| `currency` | TEXT | |
| `description` | TEXT | |
| `statement_row_index` | INTEGER | CHECK `>= 0` |
| `source_statement_hash` | TEXT | FK → `statements.statement_hash` cascade |

- UNIQUE `(source_statement_hash, statement_row_index)`
- INDEX `ix_bond_coupons_instrument_pay` `(instrument_id, pay_date)`
- INDEX `ix_bond_coupons_pay_currency` `(currency, pay_date)`
- INDEX `ix_bond_coupons_statement` `(source_statement_hash)`

### `cash_events`

| Column | Type | Constraints |
|---|---|---|
| `cash_event_id` | INTEGER | PK |
| `account_id` | TEXT | FK → `accounts.account_id` |
| `kind` | TEXT | CHECK IN (`'interest'`, `'transfer'`, `'fee'`, `'withholding'`) |
| `value_date` | TEXT | |
| `amount_native` | TEXT | |
| `currency` | TEXT | |
| `description` | TEXT | |
| `statement_row_index` | INTEGER | CHECK `>= 0` |
| `source_statement_hash` | TEXT | FK → `statements.statement_hash` cascade |

- UNIQUE `(source_statement_hash, statement_row_index)`
- INDEX `ix_cash_events_currency_date` `(currency, value_date)`
- INDEX `ix_cash_events_statement` `(source_statement_hash)`

## FX cache

### `fx_rates`

| Column | Type | Constraints |
|---|---|---|
| `base` | TEXT | |
| `quote` | TEXT | |
| `rate_date` | TEXT | |
| `rate` | TEXT | |
| `fetched_at` | TEXT | |

- PK `(base, quote, rate_date)`
- INDEX `ix_fx_rates_quote_date` `(quote, rate_date)`

## Tax runs

### `tax_runs`

| Column | Type | Constraints |
|---|---|---|
| `run_id` | INTEGER | PK |
| `tax_year` | INTEGER | |
| `computed_at` | TEXT | |
| `net_gbp` | TEXT | |

- INDEX `ix_tax_runs_year` `(tax_year, computed_at)`

### `matched_disposals`

| Column | Type | Constraints |
|---|---|---|
| `run_id` | INTEGER | FK → `tax_runs.run_id` cascade |
| `disposal_trade_id` | INTEGER | |
| `instrument_id` | INTEGER | FK → `instruments.instrument_id` |
| `disposal_date` | TEXT | |
| `match_rule` | TEXT | |
| `matched_quantity` | TEXT | |
| `matched_proceeds_gbp` | TEXT | |
| `matched_cost_gbp` | TEXT | |
| `matched_acquisition_fees_gbp` | TEXT | |
| `matched_disposal_fees_gbp` | TEXT | |
| `basis_kind` | TEXT | CHECK IN (`'DIRECT'`, `'POOL'`) |
| `acquisition_trade_id` | INTEGER | null |
| `pool_quantity_before` | TEXT | null |
| `pool_total_cost_before` | TEXT | null |
| `pool_average_cost` | TEXT | null |
| `pool_total_fees_before` | TEXT | null |
| `seq` | INTEGER | |

- PK `(run_id, disposal_trade_id, seq)`
- INDEX `ix_matched_disposals_run` `(run_id, disposal_date)`

### `future_realisations`

| Column | Type | Constraints |
|---|---|---|
| `run_id` | INTEGER | FK → `tax_runs.run_id` cascade |
| `open_trade_id` | INTEGER | |
| `close_trade_id` | INTEGER | |
| `instrument_id` | INTEGER | FK → `instruments.instrument_id` |
| `side` | TEXT | CHECK IN (`'LONG'`, `'SHORT'`) |
| `open_date` | TEXT | |
| `close_date` | TEXT | |
| `quantity` | TEXT | |
| `gross_pnl_native` | TEXT | |
| `open_fee_native` | TEXT | |
| `close_fee_native` | TEXT | |
| `open_fx_rate` | TEXT | |
| `close_fx_rate` | TEXT | |
| `proceeds_gbp` | TEXT | |
| `cost_gbp` | TEXT | |
| `seq` | INTEGER | CHECK `>= 0` |

- PK `(run_id, close_trade_id, seq)`
- UNIQUE `(run_id, open_trade_id, close_trade_id)`
- CHECK `(open_trade_id <> close_trade_id)`
- CHECK `(open_date <= close_date)`
- INDEX `ix_future_realisations_run` `(run_id, close_date)`

### `fx_event_sources`

| Column | Type | Constraints |
|---|---|---|
| `run_id` | INTEGER | FK → `tax_runs.run_id` cascade |
| `event_id` | INTEGER | |
| `kind` | TEXT | CHECK IN (`'FUTURE_REALISATION'`, `'DIVIDEND'`, `'BOND_COUPON'`, `'CASH_EVENT'`) |
| `open_trade_id` | INTEGER | null |
| `close_trade_id` | INTEGER | null |
| `dividend_id` | INTEGER | null |
| `bond_coupon_id` | INTEGER | null |
| `cash_event_id` | INTEGER | null |

- PK `(run_id, event_id)`
- CHECK exactly the id columns for `kind` are non-null
  (`'FUTURE_REALISATION'` → `open_trade_id`, `close_trade_id`;
  `'DIVIDEND'` → `dividend_id`; `'BOND_COUPON'` → `bond_coupon_id`;
  `'CASH_EVENT'` → `cash_event_id`), all others NULL
- UNIQUE INDEX `ux_fx_event_sources_realisation` `(run_id, open_trade_id, close_trade_id)` WHERE `kind = 'FUTURE_REALISATION'`
- UNIQUE INDEX `ux_fx_event_sources_dividend` `(run_id, dividend_id)` WHERE `kind = 'DIVIDEND'`
- UNIQUE INDEX `ux_fx_event_sources_coupon` `(run_id, bond_coupon_id)` WHERE `kind = 'BOND_COUPON'`
- UNIQUE INDEX `ux_fx_event_sources_cash_event` `(run_id, cash_event_id)` WHERE `kind = 'CASH_EVENT'`

### `tax_run_issues`

| Column | Type | Constraints |
|---|---|---|
| `run_id` | INTEGER | FK → `tax_runs.run_id` cascade |
| `seq` | INTEGER | CHECK `>= 0` |
| `kind` | TEXT | CHECK IN (`'position_mismatch'`, `'rate_not_found'`, `'inconsistent_trades'`, `'engine_failure'`, `'open_short_position'`, `'fx_residual'`, `'history_incomplete'`, `'history_no_lookahead'`, `'empty_year'`) |
| `instrument_id` | INTEGER | null, FK → `instruments.instrument_id` |
| `message` | TEXT | |

- PK `(run_id, seq)`
- CHECK `instrument_id IS NULL` iff `kind` IN (`'history_incomplete'`, `'history_no_lookahead'`, `'empty_year'`)

## Bookkeeping

### `schema_migrations`

| Column | Type | Constraints |
|---|---|---|
| `version` | INTEGER | PK |
| `applied_at` | TEXT | |

## Views

- `v_instruments` — `UNION ALL` of the four `*_instruments` children joined
  to `instruments`; columns `instrument_id`, `asset_class`, `isin` (bonds
  only), `conid` (stocks and futures only), `symbol`, `currency`,
  `is_cgt_exempt`, `contract_multiplier`, `expiry_date`, `fx_base`,
  `fx_quote` (class-specific columns are NULL for the other classes).
