# Architecture

## Purpose

`ib-cgt` computes UK Capital Gains Tax from Interactive Brokers HTML activity
statements. This page is the evergreen map of the library: what the components
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
- **Python**: 3.12, `ib-cgt` conda env, `pyproject.toml`-driven build, ruff +
  mypy (strict) + pytest gates.

## UK CGT rules the design honours

Locked in so component responsibilities are unambiguous:

- **Tax year**: 6 April → 5 April.
- **Share matching order** (TCGA 1992 s.104 / s.105 / s.106A):
  1. **Same-day** — disposals first matched with acquisitions on the same day.
  2. **Bed-and-breakfast** — next matched with acquisitions in the next 30 days.
  3. **Section 104 pool** — remaining matched against the pooled holding
     (weighted-average cost basis).
  Applies to stocks, non-exempt bonds, and — per the FX scope decision above —
  FX holdings per currency pair.
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

3. **Statement ingestion** — `ib_cgt.ingest` — parses IB HTML statements
   (BeautifulSoup / lxml) into canonical `Trade` / `CorporateAction` /
   `Cashflow` records, handles format drift across 2017→2025 statements,
   deduplicates using a composite business key (IB has no per-trade ID).
   CLI-driven, idempotent.

4. **FX rate service** — `ib_cgt.fx` — Frankfurter HTTP client (date-range
   batched), SQLite-backed cache, previous-business-day fallback for
   weekends/holidays (ECB publishes on TARGET business days), bulk preload
   before a tax-year computation, `convert(amount, ccy, date) → GBP` utility.

5. **Asset-class rule engines** — `ib_cgt.rules` — strategy pattern, one
   engine per asset class registered by `AssetClass`:
   - `StockRuleEngine` — full S.104 matching.
   - `BondRuleEngine` — S.104 matching; skips QCB/gilt-exempt instruments;
     attaches purchase/sale accrued interest to cost/proceeds.
   - `FutureRuleEngine` — per-contract realised-gain on close-out; no pooling.
   - `FXRuleEngine` — S.104-style matching per currency pair vs GBP.
   A shared `MatchingEngine` implements the generic same-day / 30-day / S.104
   algorithm reused by Stock, Bond, and FX engines.

6. **CGT calculator / orchestrator** — `ib_cgt.calculator` — entry point
   for a tax-year computation. Loads trades from the DB, routes each to the
   right rule engine, calls the FX service for GBP conversions, filters
   disposals into the target tax year (6 Apr → 5 Apr), persists the run plus
   `MatchedDisposal` rows for audit.

7. **Reporting** — `ib_cgt.report` — consumes a tax run; renders per-asset-
   class summary (count of disposals, total proceeds, cost, gains, losses,
   net) and a disposal-by-disposal detail (HMRC / SA108 evidence). Output
   formats: console (rich tables), CSV, JSON. Pure formatting — no tax logic.

8. **CLI** — `ib_cgt.cli` — Typer app. Commands: `db init`, `ingest`,
   `fx sync --year`, `compute --year`, `report --year`.

9. **Configuration** — `ib_cgt.config` — defaults (DB path, data dir,
   Frankfurter URL, log level), overridable via `ib-cgt.toml` in the repo
   root or `IB_CGT_*` env vars.

10. **Testing & fixtures** — `tests/` — `tests/fixtures/statements/` with
    sanitised IB HTML; `tests/fixtures/fx/` with recorded Frankfurter
    responses replayed via a stub client; `tests/unit/` per component;
    `tests/integration/` for end-to-end (ingest → compute → report) with
    golden reports.

11. **Documentation** — `docs/` — multi-page (`cgt-rules.md`,
    `ingestion.md`, `fx.md`, `cli.md`, this page). Per `AGENTS.md` rule 18,
    no single-page README.

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
- `Config` is injected into components at composition time (in `CLI`), not
  imported downward.

## Folder layout

```
ib-cgt/
├── pyproject.toml
├── environment.yml
├── AGENTS.md  /  CLAUDE.md  (mirror)
├── LICENSE
├── .gitignore
├── docs/
│   ├── index.md
│   ├── architecture.md          ← this page
│   ├── cgt-rules.md             (planned)
│   ├── ingestion.md             (planned)
│   ├── fx.md                    (planned)
│   └── cli.md                   (planned)
├── src/
│   └── ib_cgt/
│       ├── __init__.py
│       ├── py.typed
│       ├── cli.py               (planned)
│       ├── config.py            (planned)
│       ├── domain/
│       ├── db/
│       │   └── migrations/      (planned)
│       ├── ingest/              (planned)
│       ├── fx/                  (planned)
│       ├── rules/               (planned)
│       ├── calculator/          (planned)
│       ├── report/              (planned)
│       └── utils/               (planned, if needed)
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
3. ⬜ **DB schema + migrations + repos** — tables, indexes, repositories
   with unit tests.
4. ⬜ **FX service** — Frankfurter client, cache, business-day fallback.
5. ⬜ **Statement ingestion** — HTML parser, canonical mapping, dedup,
   CLI `ingest`.
6. ⬜ **Matching engine + StockRuleEngine** — same-day / 30-day / S.104
   mechanics on the simplest case.
7. ⬜ **BondRuleEngine** — add QCB / gilt exempt handling; attach accrued
   interest; reuse matching engine.
8. ⬜ **FutureRuleEngine** — per-contract close-out model.
9. ⬜ **FXRuleEngine** — S.104-style matching per currency pair.
10. ⬜ **Calculator orchestrator** — wire engines together, tax-year
    filtering, persist `tax_run`.
11. ⬜ **Reporting** — console + CSV + JSON renderers.
12. ⬜ **End-to-end tests + docs** — golden-report integration tests;
    fill out remaining `docs/` pages.

## Status

| # | Component | Package | Status |
|---|-----------|---------|--------|
| 1 | Project skeleton | — (build / tooling) | ✅ Done |
| 2 | Domain model | `ib_cgt.domain` | ✅ Done |
| 3 | Persistence | `ib_cgt.db` | ⬜ Pending |
| 4 | Ingestion | `ib_cgt.ingest` | ⬜ Pending |
| 5 | FX service | `ib_cgt.fx` | ⬜ Pending |
| 6 | Rule engines | `ib_cgt.rules` | ⬜ Pending |
| 7 | Calculator | `ib_cgt.calculator` | ⬜ Pending |
| 8 | Reporting | `ib_cgt.report` | ⬜ Pending |
| 9 | CLI | `ib_cgt.cli` | ⬜ Pending |
| 10 | Configuration | `ib_cgt.config` | ⬜ Pending |
| 11 | Tests & fixtures | `tests/` | 🟡 Smoke + domain unit tests |
| 11 | Documentation | `docs/` | 🟡 `index.md` + this page |

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
