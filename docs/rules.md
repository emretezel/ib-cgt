# Rule engines

How `ib-cgt` turns raw trades into UK-CGT tax events. The package
under `src/ib_cgt/rules/` holds the algorithm-bearing tier of the
calculator: matching, per-asset-class quirks, fee allocation,
reconciliation lists. It depends only on `ib_cgt.domain` and
`ib_cgt.fx`; it knows nothing about HTTP, SQLite, or the CLI.

## Why this layer exists

UK CGT applies different rules to different asset classes, and the
rules don't reduce to a common algorithm:

- **Stocks, bonds, FX** — UK share-matching applies (TCGA 1992 s.104
  / s.105 / s.106A). A disposal can be split across same-day
  matches, 30-day forward matches, the pooled holding, and any
  later acquisitions, with a strict precedence order. The shared
  `MatchingEngine` implements this algorithm once for all three.
- **Futures** — individual-investor treatment per TCGA 1992 s.143(5)–(6) (HMRC CG56079): each
  closed contract is its own disposal, paired with the trade that
  opened it. There is no pool, no same-day rule, no 30-day rule. The
  `FutureRuleEngine` handles this independently.

Per-asset-class quirks (bond accrued interest, FX rate-on-rate, the
futures multiplier) live in the asset-class engine, not in the matching
algorithm. The matching engine consumes already-GBP `Acquisition` and
`Disposal` records — it is FX-free and asset-class-agnostic.

## The four UK matching rules

When matching a disposal `D` of N units of an instrument:

1. **Same-day** (TCGA92/S105(1)(b)). Match against acquisitions of
   the same class on the same date as `D`. Cost basis is the actual
   price paid on those acquisitions, FIFO within the day across
   multiple buys.
2. **Bed-and-Breakfast** (TCGA92/S106A, the "30-day rule"). Any
   residual matches against acquisitions in the **30 days following**
   `D`'s date, FIFO by acquisition date. The window is `(D, D+30]`
   inclusive of day +30. Designed to neutralise wash-sale style
   schemes that briefly close and re-open a position around a tax
   year-end.
3. **Section 104 pool** (TCGA92/S104). Any residual after rules 1
   and 2 draws from the pool — the running aggregate of every
   acquisition before `D`'s date that was not fully consumed by
   rules 1 or 2. Cost basis is the pool's weighted-average cost at
   the moment of the draw. The pool can partially cover a disposal
   — the engine takes whatever the pool has and leaves the rest for
   rule 4.
4. **Later acquisitions** (TCGA92/S105(2)). Any residual after rules
   1–3 matches against acquisitions made *after* the 30-day window
   ("not already identified under stage 2 above"), taking the
   **earliest** such acquisition first. This is what covers a
   sell-short followed by a buy-to-cover more than 30 days later
   (the buy-to-cover IS the acquisition under HMRC's date semantic
   for shorts: disposal date = sell-short date, acquisition date =
   buy-to-cover date), and any disposal that runs past an
   under-sized S.104 pool.

Across disposals the statute decides the order. TCGA92/S106A(4):
"Securities disposed of on an earlier date shall be identified before
securities disposed of on a later date; and, accordingly, securities
disposed of by a later disposal shall not be identified with
securities already identified as disposed of by an earlier disposal."
So each disposal is identified in full, earliest first, and a lot an
earlier disposal took — under whichever rule — is never available to
a later one. The one exception is the same-day rule: s.106A is subject
to s.105(1) (s.106A(9); HMRC [CG51560](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg51560):
the 30-day rule "has priority over all other identification rules
except the 'same day' rule"), so an acquisition that is same-day to
one disposal and within 30 days of an earlier one belongs to the
same-day disposal. Statute text:
[legislation.gov.uk, s.106A](https://www.legislation.gov.uk/ukpga/1992/12/section/106A).

### Which date is a trade's date

Every rule above compares **dates**, and a tax year runs from 6 April to
5 April. A trade's date is the **UK-local (Europe/London) date of its
execution instant**, not the date printed on the statement. IB prints its
clock in the account's display zone — Eastern Time, declared in every
statement's notes (see [`ingestion.md`](./ingestion.md#time-zones)) — so a
fill at 21:41 Eastern on 5 April was made at 02:41 BST on 6 April and belongs
to the new tax year. The contract is made at an instant; the UK calendar date
of that instant is the date UK CGT works with, whatever zone the broker
chose to display it in. Decided on 2026-09-28, when ingestion was corrected
to read the statement's declared zone (405 of the 6,969 trades then on file
moved one day later; one crossed a tax-year boundary). The domain enforces it
(`Trade._check_datetime_fields`) and check A7 polices the stored rows.

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
    disposals=[...],  # Sequence[Disposal], GBP, any order
)
```

### Inputs

`Acquisition` and `Disposal` are the *derived* shapes from
`ib_cgt.domain.disposal` — both are GBP-denominated. The engine takes
unsorted input and sorts internally, so the caller does not have to
order chronologically. The instrument is passed separately so an
empty input still returns a sensible `final_pool`; it is **not** used
to validate the inputs. Identity is the caller's responsibility: the
runner loads every acquisition and disposal for one
`instruments.instrument_id`, and the engine never compares symbol,
ISIN, conid or expiry — IB renames symbols between statements, and
those fields are display or ingest-time data.

### Output: `MatchingResult`

| Field                    | Type                         | Description                                                    |
|--------------------------|------------------------------|----------------------------------------------------------------|
| `matched_disposals`      | `tuple[MatchedDisposal, ...]`| One row per disposal-chunk-rule triple.                        |
| `unmatched_acquisitions` | `tuple[UnmatchedAcquisition, ...]` | Itemised pool residuals at end of run.                |
| `final_pool`             | `TaxLot`                     | Aggregate pool state at end of run.                            |
| `unmatched_disposals`    | `tuple[UnmatchedDisposalChunk, ...]` | Disposal residuals nothing could cover; only populated in soft-residual mode. |

Matched disposals are emitted in **(disposal-chronological,
rule-priority)** order: for each disposal, SAME_DAY chunks first,
then BED_AND_BREAKFAST, then SECTION_104, then LATER_ACQUISITION.
This matches the order an audit report wants and the order the
`matched_disposals` table's `seq` column persists.

### Algorithm

The engine has two steps:

1. **Same-day pairing, day by day (s.105(1)).** Every disposal on a
   day is identified with that day's acquisitions, FIFO, before
   anything else. This is what makes the same-day rule prevail
   across disposals: a loop that finished an earlier disposal first
   would 30-day-match an acquisition that a disposal on the
   acquisition's own day is entitled to.
2. **One chronological walk (s.106A(4)).** Disposals are visited in
   (date, trade id) order and each is identified in full before the
   next is looked at:
   - **30-day forward** — acquisitions strictly after the disposal
     day and within 30 days, earliest first (s.106A(5)).
   - **S.104 pool draw** — the pool is every lot dated before the
     disposal that no earlier disposal has identified, drawn at
     weighted-average cost with pro-rata attribution. Partial
     coverage is allowed.
   - **Later acquisitions (s.105(2))** — for any remainder, the
     earliest not-yet-identified acquisitions strictly after the
     30-day window. Acquisitions inside the window are excluded even
     if the 30-day step only partially consumed them — the statutory
     "not already identified under stage 2 above" clause.

Within both steps, lots and disposals are visited in chronological
order (date, then trade id) so FIFO behaviour is deterministic.

After the walk, any disposal still carrying residual quantity is
reported as `UnmatchedDisposalError` — the trade history is
genuinely incomplete (typically a still-open short with no
buy-to-cover anywhere in the input).

### Why earlier years stay put

Because a disposal is finished before the next one is looked at, its
chunks depend only on: acquisitions on its own day; acquisitions in
the 30 days after it that no same-day disposal or earlier disposal
took; the pool of unidentified lots before it, which is fixed by
earlier events; and, only if it is still uncovered, the earliest
unidentified acquisitions after the window. A statement ingested
later can therefore change a disposal in exactly two cases: it lies
within 30 days before the new period (the 30-day rule reaching
forward), or it was left uncovered by everything on record and the
first new acquisition now covers it — a short or missing-history
residual the engine already reports (`unmatched_disposals`, the
`OPEN_SHORT_POSITION` and `FX_RESIDUAL` warnings).

This replaced a rule-by-rule sweep on 2026-09-28. That sweep ran every
pool draw in the history before any later-acquisition match, so
ingesting the 2026/27 statements let 2026 EUR sales draw from the pool
the 2014 purchase that the uncovered EUR.CAD sale of 20 Feb 2012 had
identified under s.105(2), and rewrote 2011/12 by £146. Check **D1**
(stored runs equal a fresh recompute) is what surfaces such a rewrite.

### Coverage shortfall

If a disposal has un-matched residual after exhausting all four
rules — typically because the user loaded an incomplete trade
history (e.g. a still-open short with no buy-to-cover anywhere in
the input) — the engine raises `UnmatchedDisposalError`. The error
carries the disposal's trade id, the instrument's symbol, and the
final residual quantity. The two steps always emit whatever
matches they can; the residual sweep is a single check after the
walk that surfaces what could not be covered.

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

## `StockRuleEngine`

```python
from ib_cgt.fx import FXService
from ib_cgt.rules import StockRuleEngine

engine = StockRuleEngine(fx)  # fx implements FXConverter Protocol
result = engine.compute(instrument, trades)
```

`StockRuleEngine` is a thin strategy on top of `MatchingEngine`. It
projects raw `Trade` rows into GBP-denominated `Acquisition` and
`Disposal` records via the FX service, then delegates the match.

### Per-trade projection

The engine is **direction-agnostic**: every `BUY` becomes an
`Acquisition` and every `SELL` becomes a `Disposal`, regardless of
whether the running balance is long or short.

| Action | Native math                            | Domain shape  | Date carried into match shape         |
|--------|----------------------------------------|---------------|---------------------------------------|
| `BUY`  | `cost_native = price * qty + fees`     | `Acquisition` | `acquisition_date = trade.trade_date` |
| `SELL` | `proceeds_native = price * qty - fees` | `Disposal`    | `disposal_date = trade.trade_date`    |

Both legs of a single trade settle on the same date, so a single
trade-date FX rate covers the whole trade.

### Fee tracking

Fees are carried through the pipeline as a **separate fact**
alongside cost and proceeds, with **subset semantics**:

- `Acquisition.fees_gbp` is the buy-side fee component already
  inside `Acquisition.cost_gbp` (`cost_gbp = price * qty + fees`,
  all in GBP at the trade-date spot rate).
- `Disposal.fees_gbp` is the sell-side fee amount already deducted
  from `Disposal.proceeds_gbp` (`proceeds_gbp = price * qty − fees`).

The matching engine drains each lot's `fees_remaining` in lockstep
with `cost_remaining` so that `MatchedDisposal` carries
`matched_acquisition_fees_gbp` and `matched_disposal_fees_gbp` for
every chunk. For S.104 chunks the pool's accumulated buy-side fees
also appear on the basis snapshot
(`TaxLotSnapshot.total_fees_gbp_before`). The reported gain is
unchanged — fees have always been part of allowable cost / disposal
cost — the split is purely for audit clarity.

### Cross-account history

S.104 pools span every account belonging to the taxpayer (per
[`docs/architecture.md §Scope — Accounts`](./architecture.md)). The
engine does not partition by `account_id` — the caller is expected
to feed in the trade history for an instrument across **all**
accounts. The persistence layer's natural-key UNIQUE on each child
table (IB `conid` for stocks and futures, ISIN for bonds) resolves
cross-account history to a single instrument id automatically, even
when IB renders the listing under a different symbol in one
account's statements.

### Short positions

The four-rule order covers short round-trips for free without any
short-aware branching:

| Scenario                                   | Rule applied                                                          |
|--------------------------------------------|------------------------------------------------------------------------|
| Sell-short and buy-to-cover same date      | `SAME_DAY` — buy-to-cover as the same-day acquisition                  |
| Sell-short, buy-to-cover within 30 days    | `BED_AND_BREAKFAST` — buy-to-cover as the 30-day forward acquisition    |
| Sell-short, buy-to-cover after 30 days     | `LATER_ACQUISITION` — buy-to-cover as the s.105(2) later acquisition    |
| Sell-short with no buy-to-cover            | strict: `UnmatchedDisposalError`; soft-residual mode (the runner's default): an `UnmatchedDisposalChunk` in `unmatched_disposals` |

For shorts, `MatchedDisposal.disposal_date` is the sell-short trade
date (the date the borrowed shares are disposed of); the basis
`DirectAcquisition` points to the buy-to-cover trade, whose date
is rendered separately in the audit output (it is implicit in the
basis trade id at the domain level).

### Exercise transfers (s.144)

`compute(instrument, trades, transfers=...)` also takes the
`OptionExerciseTransfer` records the option engine produced for this
stock's trades (the runner distributes them by `share_trade_id`). Each
one modifies the projection of the share trade IB booked at the
strike, in the direction TCGA 1992 s.144(2)–(3) prescribes
([`options.md`](./options.md#the-four-s144-directions-in-stockruleengine)):

| Transfer | Right | Share trade | Projection change |
|---|---|---|---|
| `LONG` (holder exercised) | call | `BUY`  | `cost_gbp += amount`, `fees_gbp += fees` |
| `LONG` (holder exercised) | put  | `SELL` | `proceeds_gbp -= amount`, `fees_gbp += amount` |
| `SHORT` (writer assigned) | call | `SELL` | `proceeds_gbp += amount − fees`, `fees_gbp += fees` |
| `SHORT` (writer assigned) | put  | `BUY`  | `cost_gbp -= amount − fees`, `fees_gbp += fees` |

Two things are refused with `InconsistentTradeError`: a transfer whose
`share_trade_id` is not among this stock's trades, and a transfer whose
option implies the other direction (a holder's call pointing at a
sale) — either means the exercise link is wrong.

### Corporate actions (cash disposals)

`compute(instrument, trades, corporate_actions=...)` also takes the
stock's rows from
[`corporate_actions`](./db/corporate_actions.md). Each `cash_disposal`
row — a cash-for-shares merger, a tender for cash — becomes a
`Disposal` of `|quantity|` units on `effective_date` (the London date
of the statement's `Date/Time`; TCGA 1992 s.28), with proceeds = the
cash the issuer paid converted to GBP at that date's spot rate and no
fees (IB charges no commission on a corporate action). It is then
matched like any sale: same-day, 30-day, S.104, s.105(2). The
disposal's `trade_id` is the synthetic event id `5 * 10**12 +
corporate_action_id`, printed as `CA #N`, and the FX engine books the
cash leg into its currency's pool under the same id — so the IEMI
cash-out (824 units of a GBP-listed fund for 14,425.52 USD) is one
event cited twice: a stock disposal of 10,641.43 GBP and a USD-pool
acquisition of 14,425.52 USD, both on 2025-08-16. `unsupported` rows
are ignored here; check A16 reports them.

### Errors

| Exception                | When                                                                                              |
|--------------------------|---------------------------------------------------------------------------------------------------|
| `WrongAssetClassError`   | The engine was handed a non-`StockInstrument`.                                                    |
| `InconsistentTradeError` | A trade carries an action other than `BUY`/`SELL` (defensive — `Trade.__post_init__` rejects this). |
| `ValueError`             | A trade's `instrument` doesn't match the engine call's `instrument`.                              |
| `UnmatchedDisposalError` | Propagated from `MatchingEngine` when a disposal can't be covered by all four rules — strict mode only; `compute(..., soft_residuals=True)` reports the remainder instead (see *Soft-residual mode* under `FXRuleEngine`). |

## `FutureRuleEngine`

```python
from ib_cgt.fx import FXService
from ib_cgt.rules import FutureRuleEngine

engine = FutureRuleEngine(fx)  # fx implements FXConverter Protocol
result = engine.compute(instrument, trades)
```

### Why futures don't fit `MatchedDisposal`

UK CGT for individual-investor futures (TCGA 1992 s.143(5)–(6), HMRC CG56079) doesn't apply
same-day, 30-day, or S.104 matching. Each closed contract is a
standalone disposal, paired one-to-one (or one-to-many on partial
closes) with the open trade that established it. The notional gain is
realised on the close date, in the contract's native currency,
converted to GBP at the open-date and close-date spots independently.

To reflect that, the engine emits a separate `FutureRealisation`
shape rather than `MatchedDisposal`. The match-rule enum
(`MatchRule`) stays strictly the four UK share-matching values, and
the per-contract-closeout case is type-distinct.

### FIFO opens-vs-closes per side

The engine maintains two FIFO deques per instrument: `long_q` for
slices opened via `OPEN_LONG` and `short_q` for `OPEN_SHORT`. A
`CLOSE_LONG` drains from `long_q` until its quantity is satisfied;
`CLOSE_SHORT` drains from `short_q`. The two queues never cross —
closing a long never touches a short slice, and vice versa.

A single close trade may close N open slices: the engine emits N
`FutureRealisation` rows, one per drained slice.

### Why FIFO rather than pooling (decision)

Which open slice a close-out is identified against is a judgement
call: the futures chapter of the HMRC manual (CG56000P) does not
settle it. TCGA 1992 s.104(3)(b) ("any other assets ... dealt in
without identifying the particular assets") together with s.106A(10)
extends same-day / 30-day / pooling to fungible non-share assets, and
HMRC applies that reading to crypto tokens (CRYPTO22200). No page in
the futures chapter applies it to contract close-outs, and CG56081's
"pooling rules will apply" refers to the delivered underlying asset,
not to the contracts themselves. The choice only moves gain between
close-outs, and therefore between tax years; the total over the life
of a position is identical under either model.

**Decision (2026-09-21): identify close-outs against opens first-in,
first-out, per side.** FIFO keeps every `FutureRealisation` traceable
to exactly one open trade (which `show realisation` relies on), it is
the ordering IB's own statements present, and it needs no second
identification model in the domain. A pooled-average alternative is
not planned.

### Long vs short legs

| Side  | Disposal date  | proceeds_gbp leg               | cost_gbp leg                  |
|-------|----------------|--------------------------------|-------------------------------|
| LONG  | close trade    | close-leg @ close-date FX rate | open-leg @ open-date FX rate  |
| SHORT | close trade    | open-leg @ open-date FX rate   | close-leg @ close-date FX rate|

Either way, `gain_gbp = proceeds_gbp − cost_gbp` reads naturally and
the disposal date for tax purposes is `close_date` (CG56079: the gain
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

## `OptionRuleEngine`

```python
from ib_cgt.fx import FXService
from ib_cgt.rules import OptionRuleEngine

engine = OptionRuleEngine(fx)  # fx implements FXConverter Protocol
result = engine.compute(
    instrument,
    trades,
    exercise_links={exercise_trade_id: share_trade_id},  # from option_exercise_links
    soft_residuals=True,
)
```

One call per option series (one underlying, expiry, strike and right —
one IB conid), cross-account like stocks. The series' trade history has
two sides that UK CGT taxes in two different ways
([`options.md`](./options.md) has the rules and the HMRC citations);
the engine keeps them strictly apart — a bought put and a written put
on the same series never net.

### The holder's side — a pooled asset

A bought option is an asset pooled by series (CG55536), so the long
side is projected into `Acquisition` / `Disposal` records and handed
to the shared `MatchingEngine` exactly as `StockRuleEngine` does, with
`price × contract_multiplier × quantity` as the cash and the fee a
separate fact:

| Action | Projection |
|---|---|
| `OPEN_LONG` | acquisition: premium paid + commission |
| `CLOSE_LONG` | disposal: premium received − commission |
| `LAPSE_LONG` | disposal for **nil** on the lapse date (s.144(4), CG55415) — the identified cost is the loss |
| `EXERCISE_LONG`, linked | **not a disposal** (s.144(3)): put through the matcher as a zero-proceeds disposal so the cost is identified by the same rules, then its chunks are lifted out of `matched_disposals` into one `OptionExerciseTransfer(side="LONG")` — `amount_gbp` = identified cost + the exercise row's fee, `fees_gbp` = identified acquisition fees + that fee |
| `EXERCISE_LONG`, not linked | cash-settled (s.144A): an ordinary disposal at the row's price; the trade id goes into `cash_settled_trade_ids` |

Same-day pairing, the 30-day rule, the S.104 pool and later
acquisitions therefore all apply to a series; residuals behave as for
stocks (`soft_residuals=True` in the runner).

### The writer's side — a FIFO ledger of grants

The grant is the disposal (s.144(1)): "the full amount of the premium
less any incidental cost of disposal are assessable as a gain arising
when the option is written" (CG55536). Every later event adjusts *that*
disposal, so the short side is a deque of `_GrantSlice` records per
series, drained first-in, first-out — the same identification the
futures engine uses, **decided for options on 2026-09-28** (s.148 gives
no rule; FIFO keeps each close traceable to one grant and only moves
gain between grants). Each `OPEN_SHORT` becomes one `OptionGrant`
carrying its closes:

| Action | `OptionGrantClose.kind` | Effect on the grant |
|---|---|---|
| `OPEN_SHORT` | — | a grant: gross premium at the grant-date spot as `proceeds_gbp`, the commission as `grant_fee_gbp` |
| `CLOSE_SHORT` | `PURCHASE` | s.148(3): premium + commission at the close-date spot is `cost_gbp`, added to the grant's incidental costs |
| `LAPSE_SHORT` | `LAPSE` | nothing (CG55536); recorded with zero cost so the grant is seen to be closed |
| `ASSIGN_SHORT`, linked | `ASSIGNMENT` | zero cost on the grant; the assigned contracts' share of the gross premium and grant fee (plus the assignment row's fee) leaves as an `OptionExerciseTransfer(side="SHORT", grant_trade_id=…)`, and `chargeable_quantity` drops by the assigned contracts (s.144(2), CG12317) |
| `ASSIGN_SHORT`, not linked | `CASH_SETTLEMENT` | s.144A: the cash paid is a cost of the grant, like a closing purchase; the trade id goes into `cash_settled_trade_ids` |

`OptionGrant.gain_gbp` is `chargeable_proceeds − chargeable_fee −
closing_costs`, where the chargeable amounts are the grant's proceeds
and fee scaled to the contracts still charged on it. A grant fully
assigned has `is_chargeable == False` and is not a disposal of the
year. Slices still open at the end of input come back as `OpenGrant`
records (native premium price and fee remaining) — not tax events.

Fees are allocated pro-rata by quantity with the last drain on a slice
(or on a closing trade) taking the exact residual, as in
`FutureRuleEngine`; both engines multiply before dividing
(`fee × qty_step / total`), so a third of a 3.00 fee is exactly 1.00.
FX follows CG78310: every cashflow at the spot on its own date — the
grant at the grant date, each close at its own.

### Restatement

The engine does not know about tax years. A close dated in a later
year than its grant simply sits on the grant; the calculator, filtering
grants by `grant_date` and seeing a close dated in the year being
computed on a grant of an earlier year, records an
`option_grant_restated` warning naming the grant and its year, so the
earlier year is recomputed (its stored run is stale — check D1 says so).
A zero-cost lapse restates nothing and is not reported.

### Output: `OptionResult`

| Field | Type | Description |
|---|---|---|
| `matched` | `MatchingResult` | The holder's side, exercise chunks already lifted out; `unmatched_disposals` carries soft residuals. |
| `grants` | `tuple[OptionGrant, ...]` | One per `OPEN_SHORT`, in trade order, each with every close on record. |
| `open_grants` | `tuple[OpenGrant, ...]` | Written contracts still open at end of input — not tax events. |
| `transfers` | `tuple[OptionExerciseTransfer, ...]` | Every amount an exercise or assignment moved into a share trade, for `StockRuleEngine`. |
| `cash_settled_trade_ids` | `tuple[int, ...]` | Exercise / assignment rows with no linked share trade, treated under s.144A. |

### Errors

| Exception | When |
|---|---|
| `WrongAssetClassError` | The engine was handed a non-`OptionInstrument`. |
| `InconsistentTradeError` | A `CLOSE_SHORT` / `LAPSE_SHORT` / `ASSIGN_SHORT` with **no open grant** (or more contracts than the open grants hold); a `BUY` / `SELL` action on an option (the domain rejects it first). |
| `UnmatchedDisposalError` | Propagated from `MatchingEngine` on the holder's side in strict mode; the runner uses soft residuals. |

## Persistence

`ib-cgt compute --year` (`ib_cgt.calculator.Calculator`) runs every
engine over the **whole** history, keeps the chunks and futures
realisations, option grants and exercise transfers dated inside the
year, and writes one run in a single transaction, replacing any
earlier run for the same year:

| Table | Rows |
|---|---|
| [`tax_runs`](./db/tax_runs.md) | The header: year, timestamp, net gain over the chunk, realisation and grant rows. |
| [`matched_disposals`](./db/matched_disposals.md) | One row per chunk (stocks, non-exempt bonds, bought options, FX pools), DIRECT vs POOL basis. |
| [`future_realisations`](./db/future_realisations.md) | One row per closed-out futures slice. |
| [`option_grants`](./db/option_grants.md) / [`option_grant_closes`](./db/option_grant_closes.md) | One row per written option's grant dated in the year, with every later close on record (whatever its year). |
| [`option_exercise_transfers`](./db/option_exercise_transfers.md) | One row per amount an exercise or assignment dated in the year moved into a share trade. |
| [`event_sources`](./db/event_sources.md) | The synthetic event ids the chunks cite, resolved to their dividend / coupon / cash event / realisation / corporate action. |
| [`tax_run_issues`](./db/tax_run_issues.md) | What the run could not do (errors) and what it wants noticed (warnings). |

Whole history, then filter: UK matching is path-dependent (a S.104
average cost on a 2025 disposal depends on every acquisition since the
first statement, and the 30-day rule reaches past the year end), so
the engines are never fed a single year's trades. The rows go into
the report in a canonical order — chunks by disposal id, realisations
by close date then close trade, grants by grant date then grant trade,
transfers by exercise date then option trade — which is also the order the repos
read them back in, so `Calculator.load(year)` reproduces
`compute(year)` exactly and check D1 can compare the two.

**"Save what worked."** One instrument or currency pool failing does
not blank the run: its rows are absent and the failure is an
error-severity issue. So is every position the latest statements do
not confirm (§Open positions and residuals). The CLI exits 1 iff an
error-severity issue exists; warnings — a confirmed open short, an FX
pool residual, a history that stops before the year end or inside the
30-day look-ahead, an empty year — never fail a run. There is no
`--strict`.

`UnmatchedAcquisition`, `OpenPosition` and `OpenGrant` are not
persisted — they are computed fresh on every engine call and the
`match` commands render them from the live pass.

## Error model

| Exception                | Raised when                                                                          |
|--------------------------|--------------------------------------------------------------------------------------|
| `UnmatchedDisposalError` | A disposal cannot be fully covered by acquisitions or the pool (data incomplete).    |
| `WrongAssetClassError`   | An engine is handed an instrument outside its asset class (programming error).       |
| `InconsistentTradeError` | A trade is internally inconsistent for its asset class (CLOSE with no open, etc.).   |
| `RuleEngineError`        | Base class for environmental engine failures — catch for "anything engine-shaped".   |

All four live in `ib_cgt.rules`.

## `FXRuleEngine`

```python
from ib_cgt.fx import FXService
from ib_cgt.rules import FXRuleEngine

engine = FXRuleEngine(fx)  # fx implements FXConverter Protocol
result = engine.compute(
    "USD",
    forex_trades=forex_trades,
    stock_trades=non_gbp_stock_trades,
    future_trades=non_gbp_future_trades,
    future_realisations=realisations,  # from FutureRuleEngine
    dividends=non_gbp_dividends,  # from DividendRepo.for_currency
    bond_coupons=non_gbp_bond_coupons,  # from BondCouponRepo.for_currency
    bond_trades=non_gbp_bond_trades,  # real trade ids, like stocks
    cash_events=non_gbp_cash_events,  # from CashEventRepo.for_currency
    option_trades=non_gbp_option_trades,  # real trade ids, like stocks
    corporate_actions=cash_disposals,  # from CorporateActionRepo.list_cash_disposals
)
```

`FXRuleEngine` is a thin strategy on top of `MatchingEngine`. It
projects events from **ten sources** into GBP-denominated
`Acquisition` and `Disposal` records via the FX service
(`project`), then delegates the match (`compute`). The projection is
exposed on its own because the cash-balance reconciliation sums it
without matching. Unlike the stock engine its API is
**per-currency** rather than per-instrument, because UK CGT pools FX
per single non-GBP currency vs GBP — and a single `EUR.USD` trade
therefore touches *two* pools (one EUR, one USD).

The ten cashflow sources implement HMRC CG78315 — "foreign currency
arising from any source" — so the per-currency pool reflects every
foreign-cash movement IB reports:

1. **Forex trades** — explicit `Forex` rows. Same projection as
   the v1 engine.
2. **Non-GBP stock trades** — a USD-listed stock BUY spends USD
   from the pool (a disposal of `(price*qty + fees)` USD); a SELL
   brings USD in (an acquisition of `(price*qty − fees)` USD). GBP
   value is the cash amount converted at trade-date spot.
3. **Non-GBP dividends** — cash dividends, payments in lieu and
   withholding tax on `pay_date`. **Direction is the sign of the
   amount**, exactly as for cash events: a cash dividend or a
   payment in lieu is normally positive (an acquisition) and
   withholding tax negative (a disposal), but a payment in lieu
   *paid* on a short position is negative and a withholding refund
   positive, and each goes the other way — the row's `kind` is never
   consulted for direction. Stored as their own table `dividends`
   rather than synthesised `Trade` rows because a distribution does
   not transact a quantity of the underlying stock — see
   [`docs/db/dividends.md`](db/dividends.md).
4. **Non-GBP futures trade fees** — every OPEN/CLOSE leg pays a
   commission in the contract's native currency at trade_date,
   regardless of whether the position eventually realises a gain
   or loss. Always a disposal.
5. **Futures realisations** — gross P&L on closed contracts settles
   in the contract's native currency at close_date. Winning trades
   emit acquisitions; losing trades emit disposals. The orchestrator
   runs `FutureRuleEngine` first to produce the realisation list.
6. **Non-GBP bond coupons** — coupon payments on foreign-currency
   bond holdings credit the pool on `pay_date`. Always
   acquisitions (coupons are credits — there is no withholding-tax
   variant on bond interest in the corpora seen so far). Stored as
   their own table `bond_coupons`, separate from `dividends`
   because IB emits them in a different statement section
   (`tblCombInt_*`) and the income-tax treatment differs (interest
   savings allowance vs dividend allowance). See
   [`docs/db/bond_coupons.md`](db/bond_coupons.md).
7. **Non-GBP bond trades** — the mirror of the stock projection: a
   BUY spends `(price*qty + accrued + fees)` of the bond's currency
   (a disposal), a SELL brings `(price*qty + accrued − fees)` in (an
   acquisition), GBP value at trade-date spot. A redemption is not a
   trade; its cash arrives through source 10. Whether the bond is CGT-exempt is irrelevant to
   the cash leg. `accrued` is `Trade.accrued_interest` when set and
   zero otherwise — today always zero, see the accrued-interest
   invariant below.
8. **Non-GBP cash events** — broker interest, external deposits and
   withdrawals, fee rows and interest withholding
   ([`docs/db/cash_events.md`](db/cash_events.md)). **Direction is
   the sign of the amount**: positive rows are acquisitions at the
   value-date spot, negative rows are disposals of the absolute
   amount. IB's descriptions are never consulted for direction — it
   printed negative `JPY Credit Interest` throughout the negative-
   rate years, and a fee can be refunded.
9. **Non-GBP option trades** — every premium, commission and
   settlement amount of an option series priced in a foreign
   currency, on the trade date (`fx_cashflow.from_option_trade`).
   The net cash is `±premium − fee` with premium =
   `price × multiplier × quantity`: buying an option (`OPEN_LONG`)
   or closing a written one (`CLOSE_SHORT`, `LAPSE_SHORT`,
   `ASSIGN_SHORT`) spends the currency, writing one (`OPEN_SHORT`)
   or selling a bought one (`CLOSE_LONG`, `LAPSE_LONG`,
   `EXERCISE_LONG`) brings it in. A positive net is an acquisition,
   a negative net a disposal, zero no event — so a lapse or a linked
   exercise, which IB prints at price 0, moves only its fee, and a
   cash-settled exercise moves the settlement. The share leg of an
   exercise is an ordinary stock trade (source 2). See
   [`options.md`](./options.md).
10. **Corporate-action cash** — the cash leg of a `cash_disposal`
    corporate action (a cash-for-shares merger, a bond redemption)
    in the pool's currency, on `effective_date`
    (`fx_cashflow.from_corporate_action`): an acquisition of the
    amount received, whatever currency the security traded in —
    IEMI trades in GBP and was cashed out in USD, so the USD pool
    acquires 14,425.52 USD on 2025-08-16 under the same `CA #N` id
    the stock engine cites for the disposal of the units. The amount
    is statement-sourced and posted as printed. A GBP cash leg
    touches no pool; `unsupported` rows project nothing. See
    [`docs/db/corporate_actions.md`](db/corporate_actions.md).

Three conventions behind the cash-event source are user decisions rather
than HMRC guidance, and are recorded here as such:

- **External deposits are booked at spot** on the day they arrive.
  The statements cannot show what the currency cost when it was
  bought elsewhere, so the GBP value at the deposit date stands in
  for the true acquisition cost — chosen so a pool is not left
  permanently short of dollars that are plainly there.
- **Transfers between the taxpayer's own accounts are ignored.**
  Both legs appear (one per account) and the pools already span every
  account, so they net to zero.
- **Accrued interest reaches a pool by exactly one route.** The
  statement's `Purchase / Sale Accrued Interest` lines are ordinary
  interest cash events because the trade mapper never populates
  `Trade.accrued_interest`. If a later change populates the trade
  field, the cash-event mapper must start excluding those lines in
  the same change — otherwise the accrued cash would hit the pool
  twice (once through `from_bond_trade`, once as a cash event).

The cashflow projection helpers live in
[`src/ib_cgt/rules/fx_cashflow.py`](../src/ib_cgt/rules/fx_cashflow.py).
Each is a pure function returning `Acquisition | Disposal | None`.

### Sources the pools do not see yet

Every description IB has printed in this taxpayer's 2011–2026 history
falls into one of the ten sources above, and since 2026-09-29 that is
no longer taken on trust: the pools are reconciled against IB's own
Cash Report for every account and currency (see *Cash balances*
below), and the reconciliation is what found the two gaps the tenth
source and the signed dividends closed. What the model still does
**not** carry is:

| Missing source | Why it matters | Status |
|---|---|---|
| Corporate actions the mapper does not classify (splits, spin-offs, share-for-share mergers, returns of capital, cash in lieu of fractional shares) | Units or cash moving with no modelled disposal behind them | Stored as `unsupported` rows in `corporate_actions` and reported by check A16; none in the history so far |
| Position transfers in or out of IB with a cash component (ACATS, FOP) | Cash moving without a trade | None in the history (the 2022 move between the taxpayer's own accounts was positions only); C11 would show it |

Every amount a projector computes (a forex quote leg, a stock or bond
principal, a futures realisation) is posted to the cent, as IB's own
ledger posts it — the statement prints Proceeds 0.60 for 0.0657 ×
9.08615 SEK. Until 2026-09-28 the pools summed full-precision products
instead, and the sub-cent dust surfaced as phantom residual warnings on
the DKK, NOK, CHF and SEK pools once their balances returned to zero.

### Per-currency pool model

The CGT pool for a UK taxpayer is per single non-GBP currency vs
GBP — there is one USD pool, one EUR pool, etc. — irrespective of
which counter-currency the trade was struck against. Every projected
event needs an instrument to carry, so the engine synthesises an
`FXInstrument(symbol=<ccy>, currency=<ccy>, currency_pair=(<ccy>,
GBP))` once per `compute()` call and stamps it onto every projected
event; the matching engine itself never inspects it for identity. The persisted `FXInstrument` rows in
`fx_instruments` stay keyed by traded pair (`EUR.USD`, `USD.GBP`,
…) — that's the ingestion shape, distinct from the matching shape.

The CLI / orchestrator passes the **full** forex-trade list per
call; the engine projects only the legs that touch the requested
pool currency and silently skips the rest. That keeps the IO
model simple — load forex trades once — while still letting the
engine decide per-trade which pools each fill belongs to.

### Per-trade projection

For an FX `Trade` whose pair is `BASE.QUOTE` and whose target pool
is `C`:

| Trade            | `target == BASE`                                      | `target == QUOTE`                                       |
|------------------|-------------------------------------------------------|---------------------------------------------------------|
| `BUY BASE.QUOTE` | acquire `qty` of C (paid `qty*price` of QUOTE)        | dispose `qty*price` of C (received `qty` of BASE)       |
| `SELL BASE.QUOTE`| dispose `qty` of C (received `qty*price` of QUOTE)    | acquire `qty*price` of C (paid `qty` of BASE)           |

Trades whose pair touches neither `target` nor GBP-pivoted-to-target
are skipped without an FX call.

The mapper (`ingest/mapper.py`) tags `Trade.price.currency` with
the pair's *base* currency (the domain invariant), even though the
printed amount represents quote-per-base. Multiplying
`qty * price.amount` therefore always yields the quote-currency
total, which is then posted to the cent as IB's ledger does.

### GBP conversion — independent per leg

The OTHER-leg amount (in OTHER currency) becomes the GBP cost or
proceeds via `FXService.convert_with_rate(amount, target=GBP,
on=trade_date)`. When OTHER is GBP the value is already in GBP and
the engine short-circuits without touching the FX cache.

A cross-currency trade (both legs non-GBP) produces TWO events under
TWO pools, **independently** GBP-converted using each leg's own spot
rate — those are different rates and the two GBP figures will not
reconcile to a single trade-value GBP figure. That is correct under
HMRC: each leg is an independent tax event and uses its own spot
(CG78310 — Bentley v Pike, Capcount Trading v Evans).

Worked examples (all converted at the trade-date spot):

| Trade                     | Pool | Event       | Units | GBP value                                      |
|---------------------------|------|-------------|-------|------------------------------------------------|
| `BUY USD.GBP 100 @0.79`   | USD  | acquire     | 100   | 79 GBP (no FX call)                            |
| `SELL USD.GBP 100 @0.79`  | USD  | dispose     | 100   | 79 GBP (no FX call)                            |
| `BUY GBP.EUR 100 @0.85`   | EUR  | dispose     | 85    | 100 GBP (no FX call — GBP is base here)        |
| `SELL GBP.EUR 100 @0.85`  | EUR  | acquire     | 85    | 100 GBP (no FX call)                           |
| `BUY EUR.USD 100 @1.10`   | EUR  | acquire     | 100   | `(100*1.10) USD → GBP` at trade-date USD/GBP   |
| `BUY EUR.USD 100 @1.10`   | USD  | dispose     | 110   | `100 EUR → GBP` at trade-date EUR/GBP          |
| `SELL EUR.USD 100 @1.10`  | EUR  | dispose     | 100   | `(100*1.10) USD → GBP` at trade-date USD/GBP   |
| `SELL EUR.USD 100 @1.10`  | USD  | acquire     | 110   | `100 EUR → GBP` at trade-date EUR/GBP          |

### Fees — subset semantics, single-event allocation

Fees are tracked as a **separate fact** alongside cost / proceeds
with subset semantics, mirroring `StockRuleEngine`:

- `Acquisition.fees_gbp` is the buy-side fee component already
  inside `Acquisition.cost_gbp`.
- `Disposal.fees_gbp` is the sell-side fee amount already deducted
  from `Disposal.proceeds_gbp`.

The mapper tags `Trade.fees.currency` with the pair's base
currency; the engine converts whatever the tag says at the
trade-date spot.

A **single-leg** trade (one side of the pair is GBP, only one event
is generated) attaches fees to that single event in the usual way.
A **cross-currency** trade splits into two events but is one fee
charge — the engine attaches it entirely to the **acquisition leg**
(`fees_gbp = 0` on the disposal leg). Allocating a single fee
charge to exactly one event keeps the audit reconciliation honest;
HMRC treats fees as incidental costs of acquisition, which makes
the acquisition leg the right home.

### Cross-account history

S.104 pools span every account belonging to the taxpayer (per
[`docs/architecture.md §Scope — Accounts`](./architecture.md)), and
the same applies to FX pools. The CLI's `match fx` command does not
expose an `--account` flag for the same reason `match stocks`
doesn't.

### Soft-residual mode

The FX engine calls
`MatchingEngine.match(..., soft_residuals=True)`. Long-running IB
accounts may carry a foreign-currency opening balance that
pre-dates the ingested statements (a wire-in, prior-statement
activity, etc.). Under strict matching that surfaces as
`UnmatchedDisposalError` for the whole pool, blanking the audit
output — not useful when most of the pool *is* matched.

Soft mode collects the residual into
`MatchingResult.unmatched_disposals` (a tuple of
`UnmatchedDisposalChunk`) so the renderer can show every matched
chunk plus a clearly-flagged warning for the un-covered remainder.
The CLI's `match fx` command surfaces this as a yellow "Unmatched
disposals" section.

`StockRuleEngine.compute` and `BondRuleEngine.compute` take the
same switch as a keyword (`soft_residuals=False` by default, so
direct callers keep strict semantics). The engine runner (below)
always passes `True`: a still-open short, or a sale of units the
history never saw bought, is then a residual chunk the `match
stocks` / `match bonds` commands render in their own yellow block,
and the tax-year calculator decides whether it is expected by
reconciling the instrument against the open positions on the
account's latest statement. Futures never match, so the switch does
not apply to them.

### Errors

| Exception                | When                                                                                          |
|--------------------------|-----------------------------------------------------------------------------------------------|
| `WrongAssetClassError`   | An input trade's instrument is not the expected asset class for its source list.              |
| `InconsistentTradeError` | A forex / stock trade carries an action other than `BUY`/`SELL`.                              |
| `ValueError`             | `currency` is `"GBP"`, empty, or not an ISO-4217 code.                                        |
| `RateNotFoundError`      | Propagated from `FXService` when no spot rate is available within the cache fallback window.  |

`UnmatchedDisposalError` is **not** raised from the FX engine;
residuals surface in `MatchingResult.unmatched_disposals` instead
(soft-residual mode).

## `BondRuleEngine`

```python
from ib_cgt.rules import BondRuleEngine, BondResult, ExemptBondResult

engine = BondRuleEngine(fx=fx_service)
result: BondResult = engine.compute(bond_instrument, trades_for_bond)
```

Bonds split into two regimes under UK CGT:

* **CGT-exempt** — UK gilts and Qualifying Corporate Bonds. No
  matching, no S.104 pool, no `MatchedDisposal` rows.
* **Non-exempt** — corporate / foreign-issuer bonds that don't
  qualify. Standard four-rule matching (same-day → 30-day → S.104
  → s.105(2)) with purchase / sale accrued interest folded into
  cost / proceeds.

The engine branches on `BondInstrument.is_cgt_exempt`, which the
ingest mapper sets at trade-ingestion time via
`_classify_bond_exempt` in
[`src/ib_cgt/ingest/mapper.py`](../src/ib_cgt/ingest/mapper.py):

1. **Description match** (primary): the IB Financial Instrument
   Information description starts with `"United Kingdom Gilt"`
   (case-insensitive). Authoritative for UK gilts — IB prints the
   issuer name verbatim.
2. **Symbol-prefix fallback**: when no instrument-info description
   is available, a symbol of `UKT …` on a GBP-denominated bond is
   classified as a gilt. The GBP gate avoids a false positive on
   any foreign-currency listing that happens to share the prefix.
3. **User allowlist**: the env var `IB_CGT_BONDS_EXEMPT="SYM1,SYM2"`
   marks any additional bonds CGT-exempt — covers QCBs and any
   exempt bond the heuristics miss.

Run `ib-cgt bonds list` to inspect the inferred classification and
verify that every gilt / QCB has been correctly flagged before
running the calculator.

### Result shape

`BondResult` is a sealed union:

| Branch              | Returned for            | Carries                                                           |
|---------------------|-------------------------|-------------------------------------------------------------------|
| `ExemptBondResult`  | `is_cgt_exempt=True`    | Aggregate buy / sell counts and native-currency totals for audit. |
| `MatchingResult`    | `is_cgt_exempt=False`   | The four-rule matched disposals, residuals, and final pool.       |

The exempt branch performs no FX conversion (none is required —
the bond does not produce any CGT event), so an exempt-bonds-only
run can be made before the FX cache is populated.

A **redemption** reaches the engine as a `cash_disposal` row of
[`corporate_actions`](./db/corporate_actions.md)
(`compute(instrument, trades, corporate_actions=...)`), never as a
synthesised trade. On the exempt branch it counts as one more sale in
`ExemptBondResult`'s totals (a cash leg in another currency is
converted through GBP in two steps, since the FX service refuses a
direct cross rate); on the non-exempt branch it is a real S.104
disposal at the redemption cash on `effective_date`, under the
`CA #N` event id the FX engine also cites for the cash.

### Per-trade projection (non-exempt branch)

Both legs use the trade-date spot rate; native-currency arithmetic
folds in accrued interest:

```
cost_native     = price * quantity + accrued + fees   (BUY)
proceeds_native = price * quantity + accrued - fees   (SELL)
```

The accrued-interest field on `Trade` is `Money | None`. When
present it is the cash side of the accrued portion of the
last-coupon-to-trade-date interest, in the bond's native currency.
The mapper does not yet extract this column from IB statements
(gilts are exempt → no accrued path is exercised today); the
engine tolerates `None` as zero so the projection is forward-
compatible with a future ingestion change.

### Errors

| Exception                | When                                                                                          |
|--------------------------|-----------------------------------------------------------------------------------------------|
| `WrongAssetClassError`   | The instrument passed to `compute` is not a `BondInstrument`.                                 |
| `InconsistentTradeError` | A bond trade carries an action other than `BUY` / `SELL`.                                     |
| `ValueError`             | A trade in the input list references a different bond than the engine was called with.        |
| `UnmatchedDisposalError` | Non-exempt branch only. A disposal still has residual quantity after all four matching passes (typically a buy-to-cover never appearing in the input). |

## The engine runner

`ib_cgt.calculator.runner` is the one place that loads the persisted
history and drives the five engines. The `match` commands, the
`check` tiers, and the tax-year calculator all consume it, so every
command sees the same inputs and the same engine behaviour.

| Entry point                                                | What it does                                                                                   |
|------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `run_option_engine(conn, fx, *, symbol, since, until)`     | One `OptionEngineRun` per option series, cross-account, soft-residual mode; loads the series' exercise links from `option_exercise_links`. |
| `run_stock_engine(conn, fx, *, symbol, since, until, option_runs)` | One `StockEngineRun` per stock, cross-account, soft-residual mode. Hands each stock the exercise transfers whose share trade it owns and its corporate actions; runs its own option pass when `option_runs` is `None`. |
| `run_bond_engine(conn, fx, *, symbol, since, until)`       | One `BondEngineRun` per bond (sealed `BondResult` union), soft-residual mode, with the bond's corporate actions (redemptions). |
| `run_future_engine(conn, fx, *, symbol, account_id, …)`    | One `FutureEngineRun` per contract, GBP contracts included.                                     |
| `load_fx_inputs(conn, *, future_runs, since, until)`       | The shared `FXInputs` bundle: forex / non-GBP stock / non-GBP bond / non-GBP futures / non-GBP option trades, futures realisations, dividends, coupons, cash events, `cash_disposal` corporate actions, provenance map, pool list. |
| `project_pool(fx, inputs, currency)`                       | One pool's acquisition and disposal streams, projected but not matched — what the cash-balance reconciliation sums. |
| `run_fx_engine(conn, fx, *, future_runs, currency, …)`     | One `FXEngineRun` per non-GBP pool; runs its own futures pass when none is supplied.            |
| `run_engines(conn, fx)`                                    | The whole-history pass: futures → options → stocks (with the transfers) → bonds → FX.           |

Each run record carries the instrument (or currency), the trades
that fed the engine, and *either* the result *or* the captured
exception — a single bad instrument never blanks the pass.

**Ordering.** The FX engine consumes `FutureRealisation` objects
(a closed contract settles its P&L in the contract's currency, an
acquisition or disposal of that currency on the close date), and
only the futures engine produces them. `load_fx_inputs` therefore
takes the futures runs as a required argument and `run_engines`
fixes the order outright. The stock engine consumes the option
engine's `OptionExerciseTransfer` records (an exercised or assigned
option modifies the share trade it produced, s.144(2)–(3)), so options
run before stocks. Stock, bond and option cash legs come from the
trades themselves, not from those engines' results, so their order
relative to FX is immaterial; FX is a pure sink.

**Synthetic ids and provenance.** Non-trade events get integer ids
from disjoint high ranges — realisations from `10**12`, dividends
from `2 * 10**12`, coupons from `3 * 10**12`, cash events from
`4 * 10**12` — allocated in a deterministic order (futures in
`list_futures` order and engine emit order, then dividends, coupons
and cash events by currency and date). A corporate action's id is
different in kind: `5 * 10**12 + corporate_action_id`
(`corporate_action_event_id`), a pure function of the row, because
the stock or bond engine and the FX engine cite the same event
without ever seeing each other's output. `FXInputs.sources` maps
every synthetic id to a `FutureRealisationRef` / `DividendRef` /
`BondCouponRef` / `CashEventRef` / `CorporateActionRef`
(`ib_cgt.domain.event_sources`), which is how the audit output prints
`P&L #A→#B`, `Div #N`, `WHT #N`, `Cpn #N`, `Cash #N` and `CA #N`
instead of the ids. Bond and option trades, like stock trades, carry
their real `trades` ids and need no provenance entry.

## Open positions and residuals

Since the matching engines run in soft-residual mode, a disposal
with nothing to match against — a sale of shares bought before the
earliest statement, a short still open, a futures contract opened
but never closed, a written option whose grant precedes the history
— no longer stops a run. What decides whether such
a residual is *fine* or a *data gap* is the broker's own view of the
book: the Open Positions section of each account's latest statement
([`docs/db/statement_positions.md`](db/statement_positions.md)).

`ib_cgt.calculator.positions.reconcile_positions` nets every
account's trades and corporate-action quantities up to its latest
statement's `period_end` (`TradeRepo.signed_quantity_by_instrument`
plus `CorporateActionRepo.signed_quantity_by_instrument` — a merger
or a redemption removes the units whether or not its tax side is
modelled) and compares the result with that statement's positions,
**summed across accounts**. UK CGT
pools are per taxpayer, and IB position transfers between the
taxpayer's own accounts are not trades and are never ingested, so a
per-account comparison would flag every transferred holding twice;
the taxpayer-level totals are what the pools see. Each instrument
gets one `PositionReconciliation` whose status is:

| Status             | Meaning                                                                                   |
|--------------------|-------------------------------------------------------------------------------------------|
| `match`            | Both totals agree — including two accounts whose legs net to zero, or a flat instrument no statement lists. |
| `mismatch`         | Both sides carry a quantity and they differ.                                              |
| `not_on_statement` | The trades net to a non-zero holding no latest statement lists (a sale never ingested, an over-sold stock, a futures OPEN whose contract the statement no longer carries). |
| `no_trades`        | A statement lists a holding the trades never built (bought before the earliest statement — cost basis unknown). |

Check **C7** reports every non-`match` row (ERROR severity) and the
tax-year calculator turns the same rows into `position_mismatch`
issues. A residual the statement *confirms* — an open short it lists,
an open futures contract it lists — is not a gap. FX pools are not
reconciled by position: the earliest statement is the origin of every
pool and pre-history balances are unknowable by design, so an FX
residual is only ever a warning. They are reconciled by **movement**
instead — the next section.

## Cash balances

The FX pools have a second yardstick, independent of every trade
and dividend: the **Cash Report** of each statement, which states
the account's holding of every currency at the start and end of the
period ([`docs/db/statement_cash_balances.md`](db/statement_cash_balances.md)).
Every projected pool event is a signed movement of one currency in
one account, so summed they must reproduce IB's movement — or a
pool is missing a source, and every later disposal of that currency
is matched on a wrong cost. This is how the IEMI cash (14,425.52 USD
that never reached the USD pool) and the mis-signed payments in lieu
and withholding reversals (516.35 USD booked the wrong way) were
found; before 2026-09-29 nothing could have caught either, because
the futures account's dollars covered the stock account's shortfall
silently.

`ib_cgt.calculator.cash_balances.reconcile_cash_balances` compares,
**per account** (IB keeps cash per account, and so does every
projected event) and per non-GBP currency:

```
IB movement   = ending cash on the account's latest statement
              − starting cash on its earliest statement          (pre-history cash the pools never saw)
engine total  = Σ acquisitions − Σ disposals of the currency in the account, dated ≤ the latest period_end
              + Σ over the engine's open futures lots of (close_price − open_price) × multiplier × signed quantity
```

The futures term is the one systematic difference between the two
views: IB settles variation margin daily, so its cash already holds
every open contract's unrealised P&L, while the engine posts a
contract's P&L only on close. Marking the **engine's own** FIFO lots
(`run_future_engine(..., account_id, until=period_end)`) at the
statement's Close Price (`statement_positions.close_price`) is what
makes the two agree to the cent; IB's own unrealised-P&L figure,
computed from an average cost, does not. A lot whose contract the
statement no longer lists cannot be priced — it is counted
(`unpriced_open_lots`) and left to C7.

The tolerance is **one unit of the currency** (`CASH_TOLERANCE`):
IB posts each ledger line to the cent independently, so over fifteen
years the pennies can add up to a few cents, never to a pound. Each
`CashBalanceReconciliation` is `match` or `mismatch`; check **C11**
reports every mismatch (ERROR severity) and the calculator records
the same rows as `cash_balance_mismatch` issues on the run. An
account whose statements carry no Cash Report is skipped; a currency
only IB's balances name is still compared (a mismatch, not silence).
Live, after the migration 024 re-ingest, every account and currency
reconciles: EUR, JPY and USD in the futures account, USD in the
stock account.

## What's not implemented yet

A strategy-pattern abstract base class for the five engines is
deliberately deferred — the per-engine APIs are not identical (FX is
per-currency, Stock / Bond / Future / Option are per-instrument; Bond
returns a sealed union, Future and Option return their own shapes,
Option also takes the exercise links). With five real implementations
in place, the right abstraction is easier to see; revisit if a sixth
class arrives.
