# `option_instruments`

## Purpose

Asset-class child table for exchange-traded options — the fifth
child of [`instruments`](./instruments.md), added by migration `023`.
One row per **series**: one underlying, one expiry, one strike, one
right (call or put), identified by IB's `conid` exactly as stocks and
futures are. The `instrument_id` is shared with the parent row (where
`asset_class = 'option'`).

The conid is the natural key because IB renders a series' symbol in
several forms across statements and sections (`C OGFX DEC 12 1920`
in a 2012 Financial Instrument Information row, `XAUUSD 21DEC12
1920.0 C` in its Description and in the Trades section, OCC codes
such as `XSPAM 141220P00140000, XSP 141220P00140000` for the same
series after a root rename) and even renames the root — the XSP put
became XSPAM in 2014 — while the conid never changes. `symbol` is
therefore display text refreshed to the latest statement's Description
on every upsert; every other column is a contract fact fixed at insert.

The four fact columns are what `OptionRuleEngine` computes with
(`docs/options.md`): `contract_multiplier` turns a per-unit premium
into cash, `strike` and `underlying` let the ingest linker recognise
the share trade an exercise produced, `option_right` decides which
side of that share trade the premium moves to (TCGA 1992
s.144(2)–(3)), and `expiry_date` dates a lapse.

## Columns

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `instrument_id` | `INTEGER` | No (PK, FK) | The parent's id; one-to-one with `instruments.instrument_id`. |
| `conid` | `INTEGER` | No | IB's contract id for the series, from the `Conid` column of the statement's Financial Instrument Information table. The natural key. |
| `symbol` | `TEXT` | No | IB's display form `ROOT DDMMMYY STRIKE C\|P` (e.g. `TUR 17MAY19 22.0 P`), as the latest ingested statement prints it — display only. |
| `currency` | `TEXT` | No | ISO-4217 code the premium and strike are quoted in. |
| `underlying` | `TEXT` | No | The underlying's symbol (`XAUUSD`, `XSPAM`, `TUR`); the stock symbol an exercise's share trade must carry. |
| `contract_multiplier` | `TEXT` | No | Decimal string; units per contract (100 for every series in the history). Premium cash = price × multiplier × contracts. |
| `expiry_date` | `TEXT` | No | `YYYY-MM-DD`. |
| `strike` | `TEXT` | No | Decimal string; exercise price per unit — the price IB books the exercise share trade at. |
| `option_right` | `TEXT` | No | `call` or `put` (CHECK-constrained); the domain `OptionRight` enum's value. |

See [`index.md`](./index.md#encoding-conventions) for the decimal-as-
text and date encoding conventions.

## Primary key

`instrument_id`. Doubles as the FK to `instruments(instrument_id)`,
making the parent / child relationship strictly one-to-one.

## Foreign keys

- `instrument_id` → [`instruments.instrument_id`](./instruments.md)
  `ON DELETE CASCADE`.

## Uniqueness constraints

- `UNIQUE (conid)` — natural key. Two strikes or two expiries on one
  underlying are two conids and therefore two series.

## CHECK constraints

- `CHECK (conid > 0)` — IB contract ids are positive integers.
- `CHECK (length(underlying) > 0)`.
- `CHECK (option_right IN ('call', 'put'))`.
- `contract_multiplier > 0` and `strike > 0` are enforced by the
  domain layer (`OptionInstrument.__post_init__`) — decimals are
  stored as text, so the database cannot compare them numerically.

## Indexes

| Name | Columns | Query pattern served |
|---|---|---|
| (implicit, via `UNIQUE (conid)`) | `(conid)` | Natural-key lookup in `InstrumentRepo._find_id_by_natural_key` on every upsert. |
| `ix_option_instruments_symbol_currency` | `(symbol, currency)` | `InstrumentRepo.find_by_symbol` (the ingest fallback for an Open Positions row without an instrument-information row), `list_options(symbol=)` behind `ib-cgt match options --symbol`, and `TradeRepo.list_filtered`'s symbol join through `v_instruments`. |
| `ix_option_instruments_currency` | `(currency)` | FX-sync's `DISTINCT currency` walk over `v_instruments`, so every currency a series is quoted in gets a rate cache. |

## Views

Surfaced through [`v_instruments`](./index.md#views) — the option arm
of the UNION ALL exposes `conid`, `contract_multiplier`, `expiry_date`
plus the three columns migration `023` appended to the view for this
class alone: `underlying`, `strike`, `option_right` (NULL for every
other class).

## Read paths

- [`InstrumentRepo.get(instrument_id)`](../../src/ib_cgt/db/repos/instruments.py)
  — dispatched here when `asset_class = 'option'`; rebuilds an
  `OptionInstrument` via `_option_from_row`.
- [`InstrumentRepo._find_id_by_natural_key(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — `conid` UNIQUE lookup.
- [`InstrumentRepo.list_options(symbol=)`](../../src/ib_cgt/db/repos/instruments.py)
  — drives `match options`, the calculator's option pass and check
  C8–C10; ordered by `(symbol, expiry_date)`.
- [`InstrumentRepo.find_by_symbol(AssetClass.OPTION, symbol, currency)`](../../src/ib_cgt/db/repos/instruments.py)
  — the positions mapper's fallback.

## Write paths

- [`InstrumentRepo._insert_child(instrument_id, instrument)`](../../src/ib_cgt/db/repos/instruments.py)
  — inserts here when `instrument` is an `OptionInstrument`.
- [`InstrumentRepo.upsert(...)`](../../src/ib_cgt/db/repos/instruments.py)
  — on a conid hit, refreshes `symbol` only; underlying, multiplier,
  expiry, strike and right are series facts.

## CLI commands that touch this table

- `ib-cgt ingest PATH` (transitively, via `InstrumentRepo.upsert` for
  every option trade and every option Open Positions row).
- `ib-cgt match options [--symbol]` (read, via `list_options`).
- `ib-cgt compute --year` (read, the option pass; and the persist
  step resolves every grant and transfer to its `instrument_id`).
- `ib-cgt check options` / `check all` — C8, C9, C10 iterate the
  series; C7 reconciles option positions.
- `ib-cgt show trade <id>` — prints the series facts for an option row.

## Sample (first 5 rows)

Captured via the Python `sqlite3` module in `-line` style after the
full re-ingest following migration `023` (`SELECT * FROM
option_instruments LIMIT 5`, no ordering; the system `sqlite3` binary
predates STRICT tables). The table holds exactly the four series the
2011–2026 history contains.

```
      instrument_id = 5
              conid = 92738240
             symbol = XAUUSD 21DEC12 1920.0 C
           currency = USD
         underlying = XAUUSD
contract_multiplier = 100
        expiry_date = 2012-12-21
             strike = 1920
       option_right = call

      instrument_id = 6
              conid = 86616752
             symbol = XAUUSD 21DEC12 1600.0 P
           currency = USD
         underlying = XAUUSD
contract_multiplier = 100
        expiry_date = 2012-12-21
             strike = 1600
       option_right = put

      instrument_id = 29
              conid = 99465795
             symbol = XSPAM 20DEC14 140.0 P
           currency = USD
         underlying = XSPAM
contract_multiplier = 100
        expiry_date = 2014-12-20
             strike = 140
       option_right = put

      instrument_id = 77
              conid = 334765297
             symbol = TUR 17MAY19 22.0 P
           currency = USD
         underlying = TUR
contract_multiplier = 100
        expiry_date = 2019-05-17
             strike = 22
       option_right = put
```
