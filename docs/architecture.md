# Architecture

## Purpose

`ib-cgt` computes UK Capital Gains Tax from Interactive Brokers activity
statements, HTML or PDF. This page is the evergreen map of the library: what the components
are, how they depend on each other, where the code lives, and the order in which
they get built. Every future change that touches cross-component structure —
a new module, a new dependency arrow, a reordering of steps — must update this
page in the same commit.

## Scope (confirmed decisions)

- **Asset classes (v1)**: stocks, bonds, futures, FX.
- **FX treatment**: tracked as its own CGT asset class with full UK matching
  rules (same-day / 30-day / S.104 per currency pair vs GBP base), not just a
  rate-conversion mechanism.
- **Accounts**: multiple IB accounts belonging to a single UK taxpayer. Trades
  carry `account_id`; S.104 pools span all accounts (UK CGT applies per
  taxpayer, not per account).
- **FX rates**: [Frankfurter](https://frankfurter.dev) (free, ECB-backed),
  cached locally in SQLite.
- **Persistence**: SQLite — single-user desktop tool; rule 5–7 in `AGENTS.md`
  expects an explicit, indexed SQL design.
- **Python**: 3.12, `ib-cgt` conda env, `pyproject.toml`-driven build with
  exact versions locked in `requirements.lock`, ruff + mypy (strict) + pytest
  gates.

## UK CGT rules the design honours

Locked in so component responsibilities are unambiguous:

- **Tax year**: 6 April → 5 April.
- **Share matching order** (TCGA 1992 s.104 / s.105 / s.106A):
  1. **Same-day** (s.105(1)(b)) — disposals first matched with
     acquisitions on the same day.
  2. **Bed-and-breakfast** (s.106A) — next matched with acquisitions
     in the next 30 days.
  3. **Section 104 pool** (s.104) — remaining matched against the
     pooled holding (weighted-average cost basis).
  4. **Later acquisitions** (s.105(2)) — final residual matched
     against acquisitions made *after* the 30-day window, earliest
     first. Covers a sell-short followed by a buy-to-cover more than
     30 days later, and any disposal that runs past an under-sized
     S.104 pool.
  Applies to stocks, non-exempt bonds, and — per the FX scope
  decision above — FX holdings per currency pair.
- **GBP requirement**: every disposal's proceeds and cost must be in GBP,
  converted at the spot rate on the transaction date.
- **Futures**: individual-investor treatment — gain/loss realised on contract
  close-out (or expiry / physical delivery), no mark-to-market pooling. Each
  closed contract is its own disposal.
- **Bonds**: UK gilts and Qualifying Corporate Bonds are CGT-exempt; other
  bonds fall under standard pooling rules. Per-instrument exempt flag required.
  Purchase / sale accrued interest adjusts cost basis / proceeds for bonds.

## Component map

Eleven components. Each lives under `src/ib_cgt/` as its own package except
where noted.

1. **Domain model** — `ib_cgt.domain` — pure, framework-free types: `Trade`,
   `Instrument`, `Account`, `Money`, `CurrencyPair`, `TaxYear`, `Disposal`,
   `MatchedDisposal`, `TaxLot`, `TaxYearReport`, and enums `AssetClass`,
   `MatchRule`. Frozen dataclasses, no I/O, no third-party deps. Raw
   (native-currency) shapes separate from derived (GBP) shapes.

2. **Persistence layer** — `ib_cgt.db` — SQLite schema, hand-rolled
   migrations under `db/migrations/NNN_*.sql`, repository classes per
   aggregate (`TradeRepo`, `FXRateRepo`, …). Schema designed around hot
   queries — trades for instrument X across accounts in tax year Y, FX rate
   for (ccy, date), statement-hash idempotency.

3. **Statement ingestion** — `ib_cgt.ingest` — parses IB activity
   statements into canonical `Trade` / `CorporateAction` / `Cashflow`
   records. The parser is a **strategy** (`ingest/parsers/`): one adapter
   per file format — HTML (BeautifulSoup / lxml) and PDF (pdfplumber's
   rectangle grid) — each producing the same neutral table model, and one
   assembler that owns what the columns mean, so a PDF and an HTML
   statement of the same period yield the same `ParsedStatement`. The
   format is picked from the file suffix (`ib-cgt ingest --format`
   overrides it). Overlapping statements are resolved by **coverage**: the
   first statement ingested for an account owns every day of its period
   and a later overlapping one contributes only the days it alone covers
   (IB has no per-row ID and distinct fills can be byte-identical, so
   content cannot be the key). See [`ingestion.md`](./ingestion.md).
   The assembler drops rows in the "Equity and Index Options" section
   (`assemble._IGNORED_ASSET_CLASSES`): options are out of scope until
   the [options proposal](./options.md) is adopted, and the OCC-formatted
   symbols would otherwise be ingested as stocks. Bond `T. Price` is rescaled by 1/100 at the mapper boundary
   so the unit invariant `price * quantity == settlement cash` holds —
   IB quotes bond prices as a percentage of par (`98.602` = 98.602% of
   face value) while every other asset class uses true per-unit prices.
   **Instrument identity at ingest is the class's natural key, and in
   the calculator it is `instruments.instrument_id`.** IB renames
   symbols between statements (`JNKEz` became `JNKE`), so symbol is
   display text everywhere; the rule engines never compare symbol,
   ISIN, conid or expiry — they process whatever trades the runner
   loaded for one `instrument_id`.
   * **Stocks and futures are keyed by IB's `conid`** (migration 021),
     read from the `Conid` column that every Financial Instrument
     Information table carries. The mapper resolves each trade-row
     symbol to its instrument-information row (through one
     `InstrumentInfoIndex` per statement) and rejects with
     `MappingError` any stock or future with no such row.
   * **Bond identity is the ISIN** (migration 014): the mapper
     resolves every trade-row symbol against the bonds-shaped table's
     `Security ID` column, canonicalises the display symbol via
     `_canonicalise_gilt_symbol` (strips yield-percent and IB-code
     suffixes from gilt symbols so all lots of one gilt collapse to
     one instrument), and rejects with `MappingError` any bond row
     that has no resolvable ISIN. The `Issuer` column carries the
     gilt-classifier signal (`"United Kingdom Gilt …"`);
     `Description` is the canonical name.
   * **FX pairs** get no conid from IB and stay keyed by the pair.
   The parser also walks `tblCorporateActions_*Body` divs;
   `ingest/corporate_actions.py` materialises two CGT-relevant shapes
   into synthesized `SELL` trades that the rule engines treat as
   ordinary disposals:
   * **Cash-for-shares mergers** (Stocks):
     `Merged(Acquisition) for <CCY> <PRICE> per Share` — cross-currency
     proceeds FX-converted to the stock's listing currency at the
     disposal-date spot rate.
   * **Bond maturities** (Bonds):
     `(<ISIN>) Bond Maturity FOR <CCY> <PRICE> PER BOND
     (<symbol>, <long_desc>, <isin>)` — redemption at par on the
     maturity date, in the bond's own currency. The gilt classifier
     re-runs against the synthesised instrument so an exempt UKT
     bond still routes through `ExemptBondResult` rather than the
     S.104 path.

     IB renders the same gilt under multiple trade-side aliases
     (`UKT 0 1/4 01/31/25 5.26994388%` for one yield lot,
     `… 9.87150193%` for another, `UKT 2 3/4 09/07/24 FH45` for the
     maturity-row form). With ISIN as the bond's natural key
     (migration 014) every alias collapses to one
     `bond_instruments` row, so trade BUYs and maturity SELLs
     reconcile naturally without any orchestrator-side filtering.
     A defensive `_filter_maturities_with_known_instruments`
     remains as a backstop for degenerate cases where a maturity
     row appears for a bond that has no prior BUY in the corpus.
   Dividends-as-corporate-action, splits, spin-offs, share-for-share
   mergers, and tendered-to-other-stock rows are silently ignored.

   Beyond trades, every ingest records (all inside one transaction,
   all cascading from the `statements` row):
   * the **statement period** from the page `<title>` — the only
     place every vintage prints the range in a fixed shape;
   * the **Open Positions** section as `statement_positions` — the
     broker's own end-of-period view, resolved to the same
     instruments the trades use (a held-over stock or futures
     contract with no instrument-information row, hence no conid, is
     resolved by symbol against instruments already in the DB, and
     reported if that fails);
   * **dividends and withholding tax** under IB's real
     `tblWithholdingTax_` div id (an earlier prefix mismatch silently
     dropped every withholding row). Dividend rows are
     instrument-less: only their cash leg feeds the FX pools, the IB
     security tag is kept as text, and the payment currency is
     whatever the section says (IEMI trades in GBP but pays USD);
   * the **cash-shaped sections** — Interest (minus the bond coupons
     `bond_coupons` owns), Deposits & Withdrawals (minus transfers
     between the taxpayer's own accounts), Fees, and the
     instrument-less withholding rows — as signed `cash_events`.
   `ingest --replace` also withdraws an earlier import of a
   *different* file at the same path, so a re-downloaded statement
   replaces the old version rather than sitting beside it.

4. **FX rate service** — `ib_cgt.fx` — Frankfurter HTTP client (date-range
   batched), SQLite-backed cache, previous-business-day fallback for
   weekends/holidays (ECB publishes on TARGET business days), bulk preload
   before a tax-year computation, `convert(amount, ccy, date) → GBP` utility.

5. **Asset-class rule engines** — `ib_cgt.rules` — strategy pattern, one
   engine per asset class registered by `AssetClass`:
   - `StockRuleEngine` — four-rule UK matching (same-day / 30-day /
     S.104 / s.105(2)) via the shared matching engine; direction-
     agnostic so short round-trips fall through to s.105(2) when
     their cover buy is more than 30 days later.
   - `BondRuleEngine` — four-rule matching; skips QCB/gilt-exempt
     instruments; attaches purchase/sale accrued interest to
     cost/proceeds. Sees bond maturities as ordinary SELL trades
     (the synthesis is upstream in `ingest/corporate_actions.py`),
     so an exempt gilt's maturity rolls into `ExemptBondResult`
     and a non-exempt bond's maturity triggers a real S.104
     disposal at par.
   - `FutureRuleEngine` — per-contract realised-gain on close-out; no
     pooling. Emits a separate `FutureRealisation` shape because UK
     share-matching rules (s.104 / s.105 / s.106A) do not apply to
     individual-investor futures (TCGA 1992 s.143(5)–(6), HMRC CG56079) — `MatchedDisposal` and
     `MatchRule` stay strictly for the share-matching engines.
   - `FXRuleEngine` — four-rule matching per currency pair vs GBP.
   A shared `MatchingEngine` implements the generic same-day /
   30-day / S.104 / s.105(2) algorithm reused by Stock, Bond, and
   FX engines. See [`rules.md`](./rules.md) for the per-engine
   details.

6. **CGT calculator / orchestrator** — `ib_cgt.calculator` — entry point
   for a tax-year computation. Its `runner` module is the single canonical
   loader: it reads the trade / dividend / coupon history from the DB and
   drives the four rule engines in the one order that works (futures before
   FX, because realised futures P&L is an FX cashflow), with stocks and
   bonds in soft-residual mode so an open short is reported rather than
   raised. The `match` commands, the `check` tiers and the calculator all
   consume that runner. `Calculator` on top runs the whole history once,
   filters chunks and futures realisations into the target tax year
   (6 Apr → 5 Apr), reconciles trade-derived positions with the latest
   statements per taxpayer (`calculator/positions.py`), derives the run's
   issues and persists everything in one transaction — five tables, "save
   what worked". See [`rules.md`](./rules.md#persistence) and
   [`rules.md`](./rules.md#the-engine-runner).

7. **Reporting** — `ib_cgt.report` — consumes a persisted tax run
   (`calculator.load_persisted_run`) and renders the SA108 view of it:
   the five box figures per form section ("Listed shares and
   securities" for stocks and non-exempt bonds, boxes 23–27; "Other
   property, assets and gains" for futures close-outs and currency
   pools, boxes 14–19), split by asset class, then one computation per
   HMRC disposal (one instrument, one day) in the working-sheet layout
   of the SA108 notes (A proceeds, B incidental costs of disposal, C,
   D cost, E incidental costs of acquisition, G, H). `model` is the
   report shape, `builder` the only arithmetic (gross proceeds and fee
   un-folding, the futures proceeds / cost convention, per-line
   gain / loss classification), `sources` / `labels` resolve engine ids
   to the citeable labels of `docs/audit.md`, `document` / `layout`
   form the page, and `render` emits console (rich), Markdown, JSON and
   CSV. Pure formatting on top of persisted results — no engine pass,
   no FX lookups. See [`reporting.md`](./reporting.md).

8. **CLI** — `ib_cgt.cli` — Typer app, laid out as a package with one
   module per command or command group. `cli/app.py` owns the root
   `app` and the six sub-group Typers (`db`, `fx`, `match`, `show`,
   `check`, `bonds`) and nothing else; each command module registers
   its commands on those Typers at import time, and the package
   `__init__` imports the modules in `--help` order and re-exports
   `app` for the `ib-cgt` console script. Helpers used by two or more
   command modules live in `cli/common.py` (console, FX-service
   factory, option parsers, number formatters), `cli/matching_render.py`
   (share-matching tables shared by `match stocks` / `match bonds`) and
   `cli/fx_labels.py` (FX provenance labels shared by `match fx` /
   `show match`); a helper used by one command stays private to that
   command's module. `tests/unit/cli/test_app_tree.py` pins the
   registered command tree so a module dropped from `__init__` fails
   loudly rather than silently removing its commands.

9. **Configuration** — `ib_cgt.config` — defaults (DB path, data dir,
   Frankfurter URL, log level), overridable via `ib-cgt.toml` in the repo
   root or `IB_CGT_*` env vars.

10. **Testing & fixtures** — `tests/` — `tests/fixtures/statements/` with
    sanitised IB HTML; `tests/fixtures/fx/` with recorded Frankfurter
    responses replayed via a stub client; `tests/unit/` per component;
    `tests/integration/` for end-to-end (ingest → compute → report) with
    golden reports.

11. **Documentation** — `docs/` — multi-page (`cgt-rules.md`,
    `ingestion.md`, `fx.md`, `rules.md`, `cli.md`, this page). Per
    `AGENTS.md` rule 18, no single-page README.

## Dependency graph

```
                   CLI
                    │
       ┌────────────┼────────────┐
       │            │            │
   Ingestion   Calculator    Reporting
       │            │            │
       │       RuleEngines       │
       │       ┌────┴─────┐      │
       │       │          │      │
       │       │    MatchingEngine
       │       │          │      │
       │    FXService     │      │
       │       │          │      │
       └───────┴──── DB ──┴──────┘
                    │
                Domain model
```

Rules:

- `Domain` imports nothing from this library — leaf.
- No upward imports. `Reporting` may not import from `CLI`; `Rules` may not
  import from `Calculator`; etc.
- `Checks` (the `ib-cgt check` facility, `ib_cgt.checks`) sits beside `CLI`
  and consumes `Calculator`'s engine runner; nothing below it imports it.
- `Config` is injected into components at composition time (in `CLI`), not
  imported downward.

## Folder layout

```
ib-cgt/
├── pyproject.toml
├── environment.yml
├── requirements.lock
├── AGENTS.md  /  CLAUDE.md  (mirror)
├── LICENSE
├── .gitignore
├── docs/
│   ├── index.md
│   ├── architecture.md          ← this page
│   ├── reporting.md             ← SA108 report: box mapping, conventions, formats
│   ├── ingestion.md             ← parser strategy, PDF grid, coverage rule
│   ├── options.md               ← options: proposed CGT treatment (not implemented)
│   ├── cgt-rules.md             (planned)
│   ├── fx.md                    ← Frankfurter caching, business-day fallback
│   └── cli.md                   (planned)
├── src/
│   └── ib_cgt/
│       ├── __init__.py
│       ├── py.typed
│       ├── config.py            ← env-var knobs (full TOML config planned)
│       ├── domain/
│       ├── db/
│       │   ├── migrations/      ← `NNN_*.sql`, applied by `migrator.py`
│       │   └── repos/           ← one repository class per table
│       ├── ingest/
│       │   ├── raw.py           ← format-neutral ParsedStatement + Raw*Row containers
│       │   ├── parsers/         ← StatementParser strategy: tables (neutral model),
│       │   │                       assemble (semantics), html, pdf, and the registry
│       │   ├── coverage.py      ← the day-ownership rule between overlapping statements
│       │   ├── ingestor.py      ← hash → parse → map → coverage filter → one transaction
│       │   └── mapper.py, dividends.py, cash_events.py, bond_coupons.py, …
│       ├── fx/
│       ├── rules/
│       ├── calculator/          ← engine runner (`runner.py`, `runs.py`)
│       ├── checks/              ← `ib-cgt check` facility
│       ├── cli/                 ← Typer package (component 8)
│       │   ├── __init__.py      ← composition root; imports the command modules, exports `app`
│       │   ├── __main__.py      ← `python -m ib_cgt.cli`
│       │   ├── app.py           ← root `app` + the six sub-group Typers
│       │   ├── common.py        ← console, FX-service factory, parsers, formatters
│       │   ├── matching_render.py ← share-matching tables (match stocks / bonds)
│       │   ├── fx_labels.py     ← FX provenance labels (match fx / show match)
│       │   ├── db.py            ← `db init`, `db reset`
│       │   ├── ingest.py        ← `ingest`
│       │   ├── trades.py        ← `trades`
│       │   ├── fx.py            ← `fx sync`
│       │   ├── match_futures.py / match_stocks.py / match_fx.py / match_bonds.py
│       │   ├── show_trade.py / show_realisation.py / show_match.py
│       │   ├── check.py         ← `check` callback + six subcommands
│       │   ├── bonds.py         ← `bonds list`
│       │   ├── compute.py       ← `compute --year`
│       │   └── report.py        ← `report --year`
│       └── report/              ← SA108 reporting (component 7)
│           ├── model.py         ← report shape: sections, boxes, computations
│           ├── builder.py       ← persisted run → `Sa108Report` (the only arithmetic)
│           ├── sources.py       ← engine ids → `EventRef` through the repos
│           ├── labels.py        ← citeable label vocabulary (docs/audit.md)
│           ├── document.py      ← format-neutral page AST
│           ├── layout.py        ← `Sa108Report` → `Document`
│           └── render/          ← to_console / to_markdown / to_json / to_csv
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_smoke.py
    ├── fixtures/                (planned)
    ├── unit/
    │   └── domain/
    └── integration/             (planned)
```

## Implementation order

Each step has its own per-component plan with detailed design, tests, and
verification. Items marked ✅ are in `main`; items marked 🟡 are in progress;
items marked ⬜ are pending.

1. ✅ **Project skeleton** — `pyproject.toml`, src layout, conda deps,
   ruff / mypy / pytest configured, smoke test green.
2. ✅ **Domain model** — dataclasses / enums; no I/O.
3. ✅ **DB schema + migrations + repos** — tables, indexes, repositories
   with unit tests.
4. ✅ **FX service** — Frankfurter client, cache, business-day fallback.
   See [`fx.md`](./fx.md).
5. ✅ **Statement ingestion** — HTML and PDF parsers behind one assembler,
   canonical mapping, coverage-based overlap safety, CLI `ingest`.
6. ✅ **Matching engine** — generic same-day / 30-day / S.104 /
   s.105(2) mechanics in isolation; FX-free; itemised pool
   residuals via pro-rata attribution. Consumed by `StockRuleEngine`
   today; will also be consumed by Bond / FX engines later.
7. ✅ **FutureRuleEngine** — per-contract close-out model (TCGA 1992 s.143(5)–(6), HMRC CG56079),
   FIFO long/short queues, `FutureRealisation` output (separate from
   `MatchedDisposal`).
8. ✅ **StockRuleEngine** — four-rule UK matching (same-day / 30-day
   / S.104 / s.105(2)) using the matching engine. Direction-agnostic
   so short round-trips fall through to s.105(2) when their cover
   buy is more than 30 days later.
9. ✅ **BondRuleEngine** — auto-detects UK gilts at ingest (description
   prefix `"United Kingdom Gilt"` with `UKT `+GBP fallback;
   `IB_CGT_BONDS_EXEMPT` allowlist for QCBs and edge cases). Returns
   `ExemptBondResult` for exempt bonds (no S.104 pool); applies the
   shared four-rule matcher to non-exempt bonds with purchase / sale
   accrued interest folded into cost / proceeds.
10. ✅ **FXRuleEngine** — four-rule UK matching per non-GBP currency
    pool (one EUR-vs-GBP pool, one USD-vs-GBP pool, …), reusing the
    shared matching engine. A cross-currency trade (e.g. `EUR.USD`)
    feeds two pools at once with independent per-leg GBP conversion.
    The engine consumes **eight cashflow sources** per HMRC CG78315
    ("foreign currency arising from any source"): forex trades,
    non-GBP stock trades' settlement cash, non-GBP dividends (cash
    dividends, payment-in-lieu, withholding tax — see the
    `dividends` table at [`docs/db/dividends.md`](db/dividends.md)),
    non-GBP futures trade fees, futures realisation P&L, non-GBP
    bond coupon payments (extracted from IB's `Interest` section —
    always inflows), non-GBP **bond trades'** settlement cash, and
    non-GBP **cash events** (broker interest, external deposits
    booked at spot, fees — see
    [`docs/db/cash_events.md`](db/cash_events.md)).
    Soft-residual mode surfaces any leftover shortfall (e.g. opening
    balance pre-dating the IB history) as a yellow warning rather
    than blanking the pool. Per-currency CLI:
    `ib-cgt match fx [--currency CCY]`.
11. ✅ **Calculator orchestrator** — the engine runner
    (`ib_cgt.calculator.runner`) is the single loader every `match`
    command and `check` tier runs on; open positions reconcile against
    the latest statements per taxpayer (`ib_cgt.calculator.positions`,
    check C7); `Calculator` computes one tax year over the whole
    history, records issues and persists five tables; CLI
    `compute --year [--dry-run]`; Tier D checks D1–D6 verify the
    persisted runs.
12. ✅ **Reporting** — `ib-cgt report --year`: SA108 box figures per
    form section with a per-class split, one computation per HMRC
    disposal in the working-sheet layout, console / Markdown / JSON /
    CSV renderers. See [`reporting.md`](./reporting.md).
13. ⬜ **End-to-end tests + docs** — golden-report integration tests;
    fill out remaining `docs/` pages.

## Status

| # | Component | Package | Status |
|---|-----------|---------|--------|
| 1 | Project skeleton | — (build / tooling) | ✅ Done |
| 2 | Domain model | `ib_cgt.domain` | ✅ Done |
| 3 | Persistence | `ib_cgt.db` | ✅ Done |
| 4 | Ingestion | `ib_cgt.ingest` | ✅ HTML + PDF adapters, coverage rule |
| 5 | FX service | `ib_cgt.fx` | ✅ Done |
| 6 | Rule engines | `ib_cgt.rules` | ✅ `MatchingEngine` (four-rule) + `FutureRuleEngine` + `StockRuleEngine` + `BondRuleEngine` + `FXRuleEngine` |
| 7 | Calculator | `ib_cgt.calculator` | ✅ engine runner, open-position reconciliation, `Calculator.compute` / `persist` / `load` |
| 8 | Reporting | `ib_cgt.report` | ✅ SA108 model + builder, console / Markdown / JSON / CSV renderers |
| 9 | CLI | `ib_cgt.cli` | 🟡 `db init` / `db reset` / `ingest` / `trades` / `fx sync` / `bonds list` / `match futures` / `match stocks` / `match fx` / `match bonds` / `show trade` / `show realisation` / `show match` / `check` / `compute` / `report` |
| 10 | Configuration | `ib_cgt.config` | ⬜ Pending |
| 11 | Tests & fixtures | `tests/` | 🟡 Smoke + domain unit tests |
| 11 | Documentation | `docs/` | 🟡 `index.md`, `architecture.md`, `ingestion.md`, `fx.md`, `rules.md`, `reporting.md`, `options.md`, `db/` |

## How to keep this in sync

This page is authoritative for cross-component structure. Any change that:

- adds or renames a component (new Python package under `src/ib_cgt/`),
- changes a dependency arrow (a component gains or loses a downstream
  consumer or upstream dependency),
- reorders the implementation sequence, or
- flips a status column

must be made to this page in the **same commit** as the code change. Component-
internal details (which classes exist, which SQL indexes, which HTTP headers)
belong in each component's dedicated docs page (`ingestion.md`, `fx.md`, etc.),
not here.
