# Reporting — the SA108 view of a tax year

`ib-cgt report --year 2025/26` turns the run that `compute` persisted
into what the SA108 "Capital Gains Tax summary" pages ask for and what
HMRC expects to see attached to them: the box figures per section of
the form, and one computation per disposal laid out like the working
sheet in the SA108 notes. The command only reads — no engine pass, no
FX lookups — so it needs nothing but the database.

```bash
ib-cgt compute --year 2025/26                       # once, after ingesting
ib-cgt report  --year 2025/26                       # console, summary + computations
ib-cgt report  --year 2025/26 --summary-only        # just the boxes
ib-cgt report  --year 2025/26 --out 2025-26.md      # Markdown, inferred from the suffix
ib-cgt report  --year 2025/26 --format json         # to stdout
ib-cgt report  --year 2025/26 --format csv -o lines.csv
```

The package is `ib_cgt.report` (component 7 of
[`architecture.md`](./architecture.md)): `model` is the report shape,
`builder` the only arithmetic, `sources` / `labels` the id resolution
and vocabulary, `document` / `layout` the page, `render` the four
outputs.

## Which section of the form

| Asset class | SA108 section | Boxes (disposals / proceeds / costs / gains / losses) |
|---|---|---|
| Stocks, ETFs | Listed shares and securities | 23 / 24 / 25 / 26 / 27 |
| Bonds that are not CGT-exempt | Listed shares and securities | 23 / 24 / 25 / 26 / 27 |
| Futures close-outs (TCGA 1992 s.143) | Other property, assets and gains | 14 / 15 / 16 / 17 / 19 |
| Foreign-currency pools (CG78315) | Other property, assets and gains | 14 / 15 / 16 / 17 / 19 |

Exempt gilts and qualifying corporate bonds are never persisted by
`compute` and never appear. IB's bonds are exchange-listed, which is
why non-exempt bonds sit with listed shares; an unlisted bond would
belong in "Other property" and would need its own classification.

Both sections always print, with zeros when empty, so every box the
form has can be copied. The report also prints the year totals across
both sections and a "Not included in the figures above" block — the
run's warnings and errors — so the return can be completed knowingly.

## What one disposal is

The notes for box 23 say: "Count all disposals of the same class of
share or security in the same company made on the same day as a
single disposal." The report applies that rule to every asset class:
**one HMRC disposal is one instrument on one day** — however many
sell trades, accounts or matched chunks fed it. A day's close-outs of
one futures contract are one disposal, and a day's disposals from one
currency pool are one disposal.

Gains and losses, on the other hand, are classified **per computation
line**: each identification (same-day, 30-day, S.104 holding, later
acquisition, close-out) is its own computation under HMRC's rules and
its own row in `matched_disposals`, so a same-day gain and a pool loss
inside one day's disposal count once in box 23 but land in boxes 26
*and* 27 rather than netting. This is the same split the `compute`
summary table prints.

## The working-sheet conventions

Every line carries the letters of the working sheet on page CGN 15 of
the SA108 notes:

| Letter | Meaning | Share-matched line (stock, bond, currency) | Futures close-out |
|---|---|---|---|
| A | Disposal proceeds, gross | `matched_proceeds_gbp + matched_disposal_fees_gbp` | `max(proceeds_gbp, 0)` |
| B | Incidental costs of disposal | `matched_disposal_fees_gbp` | 0 |
| C | Net proceeds | A − B (= the engine's net proceeds) | A |
| D | Cost | `matched_cost_gbp − matched_acquisition_fees_gbp` | `max(−proceeds_gbp, 0)` — the payment on a losing close-out |
| E | Incidental costs of acquisition | `matched_acquisition_fees_gbp` | `cost_gbp` — both commissions |
| G | Allowable costs | D + E (= the engine's cost) | D + E |
| H | Gain or loss | C − G | C − G |

The engines carry proceeds *net* of the sale fee and cost *inclusive*
of the purchase fee, with each fee stored as its own fact
([`rules.md` — fee tracking](./rules.md#fee-tracking)). The form wants
proceeds gross and every incidental cost on the allowable-costs side,
so the builder un-folds the fees: box 24 / 15 is ΣA and box 25 / 16 is
ΣB + ΣG. Nothing changes hands — H on every line is exactly the
engine's `gain_gbp`, and per section `proceeds − allowable costs ==
gains − losses` to the penny (the model refuses figures for which it
does not hold).

Three consequences worth knowing:

- **A currency line can carry B and E.** A forex trade with a GBP
  leg attaches its commission to the pool event, so a `USD held vs
  GBP` line may show incidental costs; stock, bond, dividend and cash
  events feeding a pool never do.
- **Bond accrued interest needs no separate line.** The mapper leaves
  `Trade.accrued_interest` empty (the accrued cash reaches the pools
  as interest cash events), and were it ever populated the bond
  engine folds it into D on a purchase and A on a sale.
- **A losing futures close-out is a cost, not negative proceeds.**
  The persisted `proceeds_gbp` is the signed net cashflow of the
  close-out (s.143(5)); the form's boxes are non-negative, so a loss
  is reported as D with A = 0. H is unchanged either way.

Money is carried unrounded through the model; the console and
Markdown renderers show pennies, JSON and CSV carry full precision. A
rendered line may therefore differ from a rendered box total by a
penny of rounding; the underlying figures reconcile exactly.

## What the report does not fill in

The tool has no view of the taxpayer's other affairs, so these boxes
are left to the filer:

- 26.1 / 17.0 — amounts claimed under the foreign income and gains
  regime.
- 28 / 20 — claim or election codes.
- 29, 30 / 21, 22 — figures already reported on real-time transaction
  returns.
- 45 — losses brought forward and used in-year; 47 — losses available
  to carry forward. The report prints the year's net gain or loss;
  what is carried forward depends on earlier years' losses, which the
  tool does not hold.

The notes say not to deduct the annual exempt amount — HMRC applies
it — so the report never does.

## The computations

One block per disposal, numbered through the report, ordered section
→ asset class → date → symbol. The header is the working sheet's
"description of asset" plus the disposal's A, B, C, G and H; the
lines beneath show, per identification:

- the disposal event and the rule (`same-day (s.105(1)(b))`, `30-day
  (s.106A)`, `S.104 holding`, `later acquisition (s.105(2))`,
  `close-out (s.143)`);
- the acquisition — trade label, date and description for a direct
  match; the holding's units, cost and average cost for a pool draw;
  the open trade, side, gross P&L, commissions and both FX rates for
  a close-out;
- D, E, G, A, B, C and H.

Every event is cited in the notation of [`audit.md`](./audit.md) —
`#N` for a trade, `Div #N` / `WHT #N` / `Cpn #N` / `Cash #N` for the
cashflows a currency pool consumes, `P&L #A→#B` for a futures
close-out — resolved from the run's own `fx_event_sources` rows, so
`ib-cgt show trade N` takes the reader from any line back to the IB
statement. A row whose statement was withdrawn after the run is
printed as `unresolved` rather than invented; re-run `compute` to
refresh the run.

## Formats

| `--format` | Goes to | Contents | Use |
|---|---|---|---|
| `console` (default) | the terminal | summary + computations (rich tables) | looking up box values |
| `markdown` | stdout or `--out` | the same page as Markdown | the document to keep and to print to PDF as the "computations" attached to the return |
| `json` | stdout or `--out` | the full model: sections, boxes, totals, issues, every line and basis | scripts, golden tests, a future form-filler |
| `csv` | stdout or `--out` | one row per computation line with the disposal's identity repeated | spreadsheet reconciliation |

`--out` alone picks the format from the suffix (`.md`, `.json`,
`.csv`); `--summary-only` drops the computations (not available for
CSV, which is the computations). Console output cannot be written to
a file.

Exit codes: `0` when the run was clean; `1` when the year has never
been computed (`No computed run for 2025/26 — run ib-cgt compute
--year 2025/26 first`) or when the run it reads carries
error-severity issues — the report is still rendered, with the run
marked `INCOMPLETE`, so a script cannot silently consume figures the
calculator itself called incomplete; `2` for a usage error.

## Worked example

The calculator test scenario (`tests/unit/calculator/conftest.py`)
sells 10 ISF for 120 GBP a share on 5 April 2025 with a 3 GBP
commission, having bought them the same day for 100 GBP a share with
a 2 GBP commission. The engine records the chunk as proceeds 1,197
(net) and cost 1,002 (inclusive); the report shows:

| A | B | C | D | E | G | H |
|---:|---:|---:|---:|---:|---:|---:|
| 1,200.00 | 3.00 | 1,197.00 | 1,000.00 | 2.00 | 1,002.00 | 195.00 |

and, for a year containing only that disposal, box 23 = 1, box 24 =
1,200.00, box 25 = 1,005.00, box 26 = 195.00, box 27 = 0.00.
