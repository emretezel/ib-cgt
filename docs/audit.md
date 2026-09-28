# Auditing matched output

The `ib-cgt match fx` (and `match stocks` / `match futures`) tables
print one row per matched chunk. Each row carries the trade ids
that drove it; the `show` subcommands let you drill from any of
those ids back to the original IB statement, the native amounts,
the FX rate the engine applied, and any other chunks that
matched against the same disposal.

## Identifier conventions

In `match fx` output:

- `#5685` — a real `trades.trade_id` (forex / stock / futures-fee
  trade). Citeable via `show trade <id>`.
- `acq #7043` — same as above, prefixed with `acq` because the row
  is a basis for a matched disposal.
- `P&L #5421→#8732` — a futures realisation. Stable across runs;
  `5421` is the `OPEN_LONG`/`OPEN_SHORT` trade id, `8732` is the
  `CLOSE_*` trade id.
- `P&L #5421→#8732[i]` — slice index added when a single close
  trade drained more than one open slice (FIFO multi-slice
  closeout).
- `acq #N→...` reads as: this acquisition came from a futures P&L
  cashflow.
- `Div #N` / `WHT #N` — a cash dividend (or payment-in-lieu) /
  withholding-tax row, cited by its `dividends.dividend_id`.
- `Cpn #N` — a bond coupon, cited by its `bond_coupons.bond_coupon_id`.
  Coupons reach the pools through the shared engine runner, so they
  appear in `match fx` exactly as `compute` will see them.
- `Cash #N` — an instrument-less cash movement (broker interest, an
  external deposit or withdrawal, a fee, interest withholding),
  cited by its `cash_events.cash_event_id`. The description column
  prints `<kind>: <IB description>`, e.g.
  `transfer: Electronic Fund Transfer`.

The synthetic integer ids the FX engine works with internally are
never printed; the runner's provenance map
(`FXInputs.sources`, see `docs/rules.md#the-engine-runner`) resolves
each one back to the citeable label above.

## `ib-cgt show trade <trade_id>`

Single-trade audit dossier. Resolves the trade through
`TradeRepo.get`, looks the source statement up via
`StatementRepo.get`, and prints:

- Account, statement file path, statement row index.
- Trade datetime (UTC), the time **as printed** in the statement's own
  zone (`statements.time_zone` — `2024-05-01, 09:00:00 (America/New_York)`),
  the UK-local trade date and the settlement date.
- Native qty / price / fees (and accrued interest for bonds).
- For non-GBP trades: the cached `1 GBP = r native` rate at
  `trade_date` — exactly what the rule engines feed into their
  GBP arithmetic.
- Asset-class-specific rows: stock cost / proceeds in native +
  GBP; FX trade per-leg breakdown; futures contract metadata
  + a pointer to `show realisation`.

## `ib-cgt show realisation --close <close_trade_id>`

Re-runs `FutureRuleEngine` for the futures instrument owning the
close trade and prints one panel per realisation produced from
that close — full P&L computation, open / close FX rates, GBP
proceeds / cost / gain. Multi-slice closes show each slice with
its `[i]` index matching the `match fx` notation.

Optional `--open <open_trade_id>` to narrow to a single
(open, close) pair when a close drained several opens.

## `ib-cgt show match --disposal <disposal_trade_id>`

Per-disposal chunk audit. Determines which currency pool(s) the
disposal touches, re-runs the FX engine for those pools, and
prints every matched chunk attached to the disposal in match
order with running residual:

```
Disposal #5685 — forex GBP.CHF buy on 2018-02-21

CHF vs GBP
┏━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ # ┃ Rule       ┃       Qty ┃ Cost (GBP)┃ Proceeds  ┃ Basis     ┃ Residual  ┃
┃   ┃            ┃           ┃           ┃    (GBP)  ┃           ┃ after     ┃
┡━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━┩
│ 1 │ later_acq  │ 4.5193    │      3.70 │      3.39 │ acq #7043 │ 88.23     │
│ — │ UNMATCHED  │ 88.2316   │         — │     66.18 │ shortfall │      0    │
└───┴────────────┴───────────┴───────────┴───────────┴───────────┴───────────┘
```

The `Residual after` column lets you see at a glance whether a
small chunk is the entire disposal or the tail of a larger one
matched in pieces. Any `UnmatchedDisposalChunk` (soft-residual
mode) appears in a yellow `UNMATCHED` row at the bottom.

## End-to-end verification flow

1. Run `ib-cgt match fx` and identify a row to verify.
2. For each `Disp ID` / `Acq ID` cell:
   - `#N` → `ib-cgt show trade N`
   - `P&L #A→#B[i]` → `ib-cgt show realisation --close B`
3. Open the IB statement file from each dossier, find the row,
   confirm the native amounts.
4. Multiply native amount × cached FX rate, confirm the GBP
   figure matches the `match fx` row.
5. If the matched quantity looks small, run `ib-cgt show match
   --disposal <id>` to see the full chunk sequence and any
   un-covered residual.

## `ib-cgt compute --year 2024/25 [--dry-run]`

The persisted counterpart of the `match` dry runs. It computes the
year from the same engine pass the `match` commands render, so every
chunk in `matched_disposals` and every row in `future_realisations`
can be found in `match stocks` / `match bonds` / `match fx` /
`match futures` output with the same ids and the same labels — the
`Cpn #N` / `Cash #N` / `P&L #A→#B` notation is resolved for a
persisted run through `fx_event_sources`. The command prints the
per-class summary, the warnings and (in red) the errors, then either
`Persisted run #N …` or `Dry run — nothing persisted`, and exits 1
iff an error-severity issue exists. The issues themselves are in
`tax_run_issues` and come back with `Calculator.load`.

`ib-cgt check all` then runs Tier D: D1 recomputes every persisted
year from a fresh engine pass and compares chunks, realisations, the
net gain and the recorded engine failures; D2 re-adds the header from
its rows; D4 / D6 confirm every trade id (or synthetic id) still
resolves. A `FAIL` there means the database changed under a run —
re-run `compute` for the year.

## `ib-cgt report --year 2025/26 [--format console|markdown|json|csv] [--out FILE] [--summary-only]`

The SA108 view of a persisted run — see [`reporting.md`](./reporting.md)
for the box mapping and the working-sheet conventions. Every line of
every computation cites its events in the notation above, resolved
from the run's `fx_event_sources` rows rather than a live engine pass:
`#N` for a trade (`show trade N`), `Div #N` / `WHT #N` / `Cpn #N` /
`Cash #N` for the non-trade cashflows, and `P&L #A→#B` for a futures
close-out (`show realisation --close B`). The one difference from
`match fx` is that the `[i]` slice suffix is not printed — a persisted
run cannot tell how many slices a close drained in *other* years, and
`(open, close)` is unique within a run anyway.
