# ib-cgt

UK Capital Gains Tax from Interactive Brokers statements.

`ib-cgt` reads the activity statements you download from Interactive
Brokers (HTML or PDF), applies the UK CGT rules to every trade since your
account was opened, and produces what the SA108 *Capital Gains Tax
summary* pages ask for: the box figures per section and one working-sheet
computation per disposal.

- **Asset classes** — stocks and ETFs, bonds (gilts and qualifying
  corporate bonds are flagged exempt), futures, exchange-traded options,
  and foreign-currency cash, which HMRC treats as a chargeable asset in
  its own right.
- **Rules** — same-day, 30-day and Section 104 pooling for shares, bonds
  and currency (TCGA 1992 s.104 / s.105 / s.106A); close-out treatment
  for futures (s.143); s.144 / s.148 for options.
- **GBP conversion** at the ECB reference rate on the transaction date,
  fetched from [Frankfurter](https://frankfurter.dev) and cached locally.
- **Output** — a PDF to keep with your return, or Markdown, JSON, CSV and
  console; every figure can be traced back to the statement row it came
  from with `ib-cgt show`.
- **Storage** — one SQLite file, `~/.ib-cgt/ibcgt.sqlite` by default
  (override with `IB_CGT_DB`). Several IB accounts of one taxpayer share
  one set of pools.

This is a calculator, not tax advice: check the output before you file
(see the [disclaimer](#disclaimer)).

## Install

You need [conda](https://github.com/conda-forge/miniforge) (Miniforge or
Miniconda). The environment file pins Python 3.12 and the exact package
versions in `requirements.lock`, then installs `ib-cgt` in editable mode.

```bash
git clone https://github.com/emretezel/ib-cgt.git
cd ib-cgt
conda env create -f environment.yml
conda activate ib-cgt
ib-cgt --help
```

After pulling changes, refresh the environment with
`conda env update -f environment.yml`.

## Run

### 1. Download your statements

In IB Client Portal go to **Performance & Reports → Statements**, pick
**Activity**, a **Custom Date Range** and **HTML** or **PDF**. IB caps a
statement at one year, so download one per year.

**Download every statement from the day the account was opened**, for
every IB account you hold. UK matching is path-dependent: the allowable
cost of what you sell this year depends on every acquisition before it,
so the calculator runs over your whole history each time. Client Portal
only offers the last few years online; older statements can be requested
from IB Client Services. Put the files in `statements/`, which git
ignores.

### 2. Ingest, fetch FX rates, compute, report

```bash
ib-cgt ingest statements/*.htm statements/*.pdf   # creates the database on first use
ib-cgt fx sync                                    # ECB rates for every currency seen
ib-cgt compute --year 2025/26                     # runs the rule engines, saves the year
ib-cgt report  --year 2025/26 --out 2025-26.pdf   # SA108 figures and computations
```

Overlapping statements are safe: the first file ingested owns its days
and a later file adds only the days it does not cover. `compute` exits 1
and lists the issues when something could not be included, such as a
missing FX rate or an open position the latest statement does not
confirm. `report` also takes `--format markdown|json|csv` and
`--summary-only` for the box figures alone.

When a new statement arrives, ingest it and run `compute` again for each
year you need; a run replaces the earlier one for the same year.

## Documentation

The full documentation lives under [`docs/`](./docs/index.md):
[ingestion](./docs/ingestion.md), the [rule engines](./docs/rules.md),
[options](./docs/options.md), [FX](./docs/fx.md),
[reporting](./docs/reporting.md), [auditing a figure](./docs/audit.md),
the [architecture](./docs/architecture.md) and the
[database reference](./docs/db/index.md).

## Development

```bash
ruff format && ruff check && mypy && pytest
```

Contributor and AI-assistant instructions are in [`AGENTS.md`](./AGENTS.md)
(mirrored to [`CLAUDE.md`](./CLAUDE.md)).

## Disclaimer

`ib-cgt` is provided "as is", without warranty of any kind, and its
output is not tax, legal or financial advice. The author is not a tax
adviser. HMRC's rules change, IB's statement formats change, and this
software may contain errors: you are responsible for checking every
figure against your own records and for the accuracy of your tax return.
To the fullest extent permitted by law, the author accepts no liability
for any loss, penalty, interest or other consequence arising from the use
of this software or reliance on its output. Sections 15 and 16 of the
[licence](./LICENSE) set out the full disclaimer of warranty and
limitation of liability.

## Licence

[GPL-3.0-or-later](./LICENSE).
