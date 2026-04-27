# Rule engines

How `ib-cgt` turns raw trades into UK-CGT tax events. The package
under `src/ib_cgt/rules/` holds the algorithm-bearing tier of the
calculator: matching, per-asset-class quirks, fee allocation,
reconciliation lists. It depends only on `ib_cgt.domain` and
`ib_cgt.fx`; it knows nothing about HTTP, SQLite, or the CLI.

## Why this layer exists

UK CGT applies different rules to different asset classes, and the
rules don't reduce to a common algorithm:

- **Stocks, bonds, FX** — UK share-matching applies (TCGA 1992 s.105 /
  s.106A / s.104). A disposal can be split across same-day matches,
  30-day forward matches, and the pooled holding, with a strict
  precedence order. The shared `MatchingEngine` implements this
  algorithm once for all three.
- **Futures** — individual-investor treatment per HMRC HS292: each
  closed contract is its own disposal, paired with the trade that
  opened it. There is no pool, no same-day rule, no 30-day rule. The
  `FutureRuleEngine` handles this independently.

Per-asset-class quirks (bond accrued interest, FX rate-on-rate, the
futures multiplier) live in the asset-class engine, not in the matching
algorithm. The matching engine consumes already-GBP `Acquisition` and
`Disposal` records — it is FX-free and asset-class-agnostic.

## The three UK matching rules

When matching a disposal `D` of N units of an instrument:

1. **Same-day** (TCGA s.105). Match against acquisitions of the same
   class on the same date as `D`. Cost basis is the actual price paid
   on those acquisitions, FIFO within the day across multiple buys.
2. **Bed-and-Breakfast** (TCGA s.106A, the "30-day rule"). Any
   residual matches against acquisitions in the **30 days following**
   `D`'s date, FIFO by acquisition date. The window is `(D, D+30]`
   inclusive of day +30. Designed to neutralise wash-sale style
   schemes that briefly close and re-open a position around a tax
   year-end.
3. **Section 104 pool**. Any residual after rules 1 and 2 draws from
   the pool — the running aggregate of every acquisition that was not
   fully consumed by rules 1 or 2. Cost basis is the pool's
   weighted-average cost at the moment of the draw.

Same-day takes priority **across disposals**, not just within one
disposal: if two disposals on different dates both want the same
acquisition, the one that can claim it under the same-day rule gets
it before the 30-day rule kicks in.

### Worked example

```
Acq A on 2024-01-01: 50 units, cost £500   (£10/unit)
Acq B on 2024-05-10: 20 units, cost £400   (£20/unit)   ← same-day with the disp
Acq C on 2024-05-15: 30 units, cost £900   (£30/unit)   ← 30-day forward
Disp on 2024-05-10: 100 units, proceeds £2,000
```

The disposal is matched in three chunks, in priority order:

| Rule              | Qty | Cost  | Proceeds | Basis              |
|-------------------|-----|-------|----------|--------------------|
| SAME_DAY          | 20  | £400  | £400     | DirectAcq(B)       |
| BED_AND_BREAKFAST | 30  | £900  | £600     | DirectAcq(C)       |
| SECTION_104       | 50  | £500  | £1,000   | TaxLotSnapshot(A)  |

Proceeds are split pro-rata by quantity (£20/unit × matched qty), so
the three chunks sum back to the disposal's original £2,000. The pool
contained only A at the moment of the draw (B was consumed same-day,
C was forward), so the snapshot's average cost equals A's £10/unit.
After the draw, A is fully consumed and the pool is empty.

## The shared `MatchingEngine`

```python
from ib_cgt.rules import MatchingEngine

engine = MatchingEngine()
result = engine.match(
    instrument=instrument,
    acquisitions=[...],  # Sequence[Acquisition], GBP, any order
    disposals=[...],     # Sequence[Disposal], GBP, any order
)
```

### Inputs

`Acquisition` and `Disposal` are the *derived* shapes from
`ib_cgt.domain.disposal` — both are GBP-denominated. The engine takes
unsorted input and sorts internally, so the caller does not have to
order chronologically. Instrument identity is required separately so
the engine can validate every input belongs to the named instrument
and so an empty input still returns a sensible `final_pool`.

### Output: `MatchingResult`

| Field                    | Type                         | Description                                                    |
|--------------------------|------------------------------|----------------------------------------------------------------|
| `matched_disposals`      | `tuple[MatchedDisposal, ...]`| One row per disposal-chunk-rule triple.                        |
| `unmatched_acquisitions` | `tuple[UnmatchedAcquisition, ...]` | Itemised pool residuals at end of run.                |
| `final_pool`             | `TaxLot`                     | Aggregate pool state at end of run.                            |

Matched disposals are emitted in **(disposal-chronological,
rule-priority)** order: for each disposal, SAME_DAY chunks first,
then BED_AND_BREAKFAST, then SECTION_104. This matches the order an
audit report wants and the order the `matched_disposals` table's
`seq` column persists.

### Algorithm

The engine runs in three passes over the data:

1. **Pass 1 — same-day, across all disposals.** This pass is what makes
   "same-day takes priority across disposals" work. If we processed
   disposals chronologically and applied all three rules per
   disposal, an earlier disposal would 30-day-match an acquisition
   before a later disposal could same-day-match it.
2. **Pass 2 — 30-day forward, across all remaining residuals.**
3. **Pass 3 — S.104 pool draws, across remaining residuals.**

Within each pass, lots and disposals are visited in chronological
order (date, then trade id) so FIFO behaviour is deterministic.

### Coverage shortfall

If a disposal has un-matched residual after exhausting all three
rules — typically because the user loaded an incomplete trade history
— the engine raises `UnmatchedDisposalError`. The error carries the
disposal's trade id, the instrument's symbol, and the residual
quantity at the moment the pool came up short. No partial pool draw
is performed before the error: this keeps the engine's emit set
internally consistent and makes the error message accurate.

The intent is for the calculator to fail loudly: a half-matched
disposal would silently distort the tax report.

### Itemised pool residuals: pro-rata attribution

`MatchingResult.unmatched_acquisitions` shows which buys still sit in
the pool at end of run, keyed by acquisition trade id. The list
**reconciles exactly** to the aggregate `final_pool`: sum of
`quantity_remaining` equals `final_pool.quantity`; sum of
`cost_remaining_gbp` equals `final_pool.total_cost_gbp`.

To preserve that invariant, S.104 pool draws are attributed to lots
**pro-rata across the pool**: a draw of fraction `f` of the pool's
quantity reduces every lot's quantity *and* cost by `f`. A
side-effect of pro-rata attribution is that each lot's lot-local
cost-per-unit is preserved through draws — the audit list still
reports each acquisition's original cost basis, just at a smaller
size after later draws.

UK CGT treats the pool as fungible, so any consistent attribution is
permissible. Pro-rata is one valid presentation; FIFO-by-date would
be another, but it does not reconcile to the aggregate when the pool
draws at average cost. Pro-rata wins on reconciliation.

### Worked example, continued

Take just two acquisitions with no same-day or 30-day matches:

```
Acq A on 2024-01-01: 100 units @ £10  (cost £1,000)
Acq B on 2024-01-15: 100 units @ £20  (cost £2,000)
Disp on 2024-06-01: 50 units, S.104 only
```

Pool at draw time: 200 units, £3,000, average £15. The disposal
draws 50 units at £15/unit → cost £750, drawn fraction 25 %.

The matched disposal:

```
match_rule=SECTION_104, matched_quantity=50,
matched_cost_gbp=£750, basis=TaxLotSnapshot(qty_before=200, ...)
```

Pro-rata attribution drains 25 % of every lot:

| Acq | qty before | qty after | cost before | cost after | cost/unit |
|-----|-----------:|----------:|------------:|-----------:|----------:|
| A   |        100 |        75 |      £1,000 |       £750 |       £10 |
| B   |        100 |        75 |      £2,000 |     £1,500 |       £20 |

`final_pool` = 150 units / £2,250 (15 average), and the two
`UnmatchedAcquisition` rows sum to exactly that.

## `FutureRuleEngine`

```python
from ib_cgt.fx import FXService
from ib_cgt.rules import FutureRuleEngine

engine = FutureRuleEngine(fx)            # fx implements FXConverter Protocol
result = engine.compute(instrument, trades)
```

### Why futures don't fit `MatchedDisposal`

UK CGT for individual-investor futures (HMRC HS292) doesn't apply
same-day, 30-day, or S.104 matching. Each closed contract is a
standalone disposal, paired one-to-one (or one-to-many on partial
closes) with the open trade that established it. The notional gain is
realised on the close date, in the contract's native currency,
converted to GBP at the open-date and close-date spots independently.

To reflect that, the engine emits a separate `FutureRealisation`
shape rather than `MatchedDisposal`. The match-rule enum
(`MatchRule`) stays strictly the three UK share-matching values, and
the per-contract-closeout case is type-distinct.

### FIFO opens-vs-closes per side

The engine maintains two FIFO deques per instrument: `long_q` for
slices opened via `OPEN_LONG` and `short_q` for `OPEN_SHORT`. A
`CLOSE_LONG` drains from `long_q` until its quantity is satisfied;
`CLOSE_SHORT` drains from `short_q`. The two queues never cross —
closing a long never touches a short slice, and vice versa.

A single close trade may close N open slices: the engine emits N
`FutureRealisation` rows, one per drained slice.

### Long vs short legs

| Side  | Disposal date  | proceeds_gbp leg               | cost_gbp leg                  |
|-------|----------------|--------------------------------|-------------------------------|
| LONG  | close trade    | close-leg @ close-date FX rate | open-leg @ open-date FX rate  |
| SHORT | close trade    | open-leg @ open-date FX rate   | close-leg @ close-date FX rate|

Either way, `gain_gbp = proceeds_gbp − cost_gbp` reads naturally and
the disposal date for tax purposes is `close_date` (HS292: the gain
crystallises at closeout).

### Fee allocation

Fees are allocated **pro-rata by quantity** across drains. The "last
drain on a slice" (or "last drain on a close trade") consumes the
exact fee residual that's left, so multiple partial drains across a
single slice or close don't accumulate 1-cent precision drift.

A partially-closed slice carries its residual fee forward in the
queue until it's fully drained or surfaces as an `OpenPosition`.

### Output: `FutureResult`

| Field            | Type                            | Description                                              |
|------------------|---------------------------------|----------------------------------------------------------|
| `realisations`   | `tuple[FutureRealisation, ...]` | One row per (open-slice, close-portion) pair.            |
| `open_positions` | `tuple[OpenPosition, ...]`      | Slices still un-closed at end of input — *not* tax events. |

`OpenPosition.open_price` and `OpenPosition.fees_remaining` are in
the contract's **native currency**, not GBP — there is no GBP
conversion until the position eventually closes.

### Inconsistent inputs

The engine raises `InconsistentTradeError` when the trade stream
cannot be processed: `CLOSE_LONG` with no open long, close quantity
exceeding available open quantity, or a `BUY`/`SELL` action on a
future (the domain layer should reject the latter at construction
time, but the engine guards against malformed in-memory inputs too).

## Persistence

UK matching outputs persist to `matched_disposals` (one row per
chunk, basis-discriminated DIRECT vs POOL). See
[`docs/db/matched_disposals.md`](./db/matched_disposals.md).

`FutureRealisation` does not yet have a SQL table. The persistence
story for futures will come with the calculator orchestrator
(component 11): a parallel `future_realisations` table is the
expected shape, but it isn't built in this milestone.

`UnmatchedAcquisition` and `OpenPosition` are not persisted at all —
they are computed fresh on every engine call. Persisting them is
deferred until the reporting layer needs them across runs.

## Error model

| Exception                | Raised when                                                                          |
|--------------------------|--------------------------------------------------------------------------------------|
| `UnmatchedDisposalError` | A disposal cannot be fully covered by acquisitions or the pool (data incomplete).    |
| `WrongAssetClassError`   | An engine is handed an instrument outside its asset class (programming error).       |
| `InconsistentTradeError` | A trade is internally inconsistent for its asset class (CLOSE with no open, etc.).   |
| `RuleEngineError`        | Base class for environmental engine failures — catch for "anything engine-shaped".   |

All four live in `ib_cgt.rules`.

## What's not implemented yet

`StockRuleEngine`, `BondRuleEngine`, and `FXRuleEngine` are pending
(see [`architecture.md`](./architecture.md#implementation-order),
steps 8–10). Each will consume `MatchingEngine` as its internal
matching primitive, layering on the asset-class quirks (bond accrued
interest, FX rate-on-rate, etc.) on the way in. The strategy-pattern
abstract base class will be introduced when the second engine lands —
having only one concrete engine today doesn't justify the boilerplate.
