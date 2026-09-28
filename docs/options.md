# Options — the CGT rules the engine applies

Exchange-traded options are the fifth asset class the calculator models
(migration `023_options.sql`, 2026-09-28). IB prints them under
`Equity and Index Options` (and `Options On Futures`) in Trades, Open
Positions and Financial Instrument Information; every row is ingested,
each series is one instrument, and `OptionRuleEngine` applies TCGA 1992
s.144 / s.144A / s.148 to it. This page sets out the rules, the decisions
taken, how IB prints the rows, and what the history contains.

The full IB history (2011 onwards) holds four option series:

| Series | Side | What happened |
|---|---|---|
| `XAUUSD 21DEC12 1920.0 C` (OGFX, 2012) | written | 1 contract granted 2012-10-10 for 770 USD, bought back 2012-11-01 for 140 USD |
| `XAUUSD 21DEC12 1600.0 P` (OGFX, 2012) | written | 1 contract granted 2012-10-15 for 450 USD, lapsed 2012-12-21 |
| `XSPAM 20DEC14 140.0 P` (2013–14; IB renamed the root from `XSP`) | bought | 1 contract bought 2013-05-03 for 785 USD, sold 2014-01-14 for 182 USD |
| `TUR 17MAY19 22.0 P` (2019) | bought | 15 contracts bought 2019-01-03 for 1.85 each, exercised 2019-05-16 into a sale of 1,500 TUR at 22 |

An earlier version of this page said options existed only in 2012–2014;
the 2019 TUR exercise was found when the parser stopped dropping the
section, and it is the one exercise in the history.

## The short answer to "treat them like futures?"

No — only the arithmetic of a closed round-trip looks the same. The statute
treats an option as an asset in its own right (TCGA 1992 s.144) with rules
for each of the things that can happen to it, and three of those rules
differ from the futures close-out model in [`rules.md`](./rules.md). All
of the rows below are implemented.

| Event | Futures (s.143(5)–(6)) | Options (s.144 / s.148) |
|---|---|---|
| Open a long | Nothing until close-out | **Acquisition** of an asset: premium + commission is allowable cost |
| Close a long by selling | Disposal on close-out, FIFO per side | **Disposal** of the option under normal CG rules — same-day / 30-day / s.104 **pooled by series** (HMRC CG55536), not FIFO |
| Long lapses unexercised | (cash-settled at expiry, s.143(6)) | **Disposal for nil** on the lapse date (s.144(4)): allowable loss = premium + costs (CG55415) |
| Long exercised | (delivery, s.143(8)) | No separate disposal: the identified option cost is **added to the cost of the shares** bought under a call, or is a **cost of disposal** of the shares sold under a put (s.144(3), CG55536); with no share trade the option is cash-settled under s.144A |
| Write (sell to open) | Nothing until close-out | The premium less costs is a **chargeable gain at the date of grant** (s.144(1), CG12312, CG55536) |
| Close a short by buying back | Disposal on close-out | **Not a disposal**: the cost of the closing purchase (plus its commission) is added to the incidental costs of the original grant (s.148(3), CG55545) — the grant-date gain is reduced, and no new event arises |
| Short lapses unexercised | — | Nothing further: the grant charge stands (CG55536) |
| Short assigned | — | Grant and the resulting share trade are **one transaction** (s.144(2)): the premium is added to the sale proceeds of shares delivered under a call, or deducted from the cost of shares bought under a put; the grant charge on the assigned contracts is unwound, and a grant in an earlier tax year is restated (CG12317) |

Statute: TCGA 1992 [s.143](https://www.legislation.gov.uk/ukpga/1992/12/section/143)
(qualifying options are assets, s.143(1)–(2)),
[s.144](https://www.legislation.gov.uk/ukpga/1992/12/section/144) (grant, exercise,
abandonment), [s.144A](https://www.legislation.gov.uk/ukpga/1992/12/section/144A)
(cash-settled options), [s.148](https://www.legislation.gov.uk/ukpga/1992/12/section/148)
(traded options: closing purchases). HMRC Capital Gains Manual:
[CG12312](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg12312) (grant),
[CG12317](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg12317)
(exercise adjusts the grant charge),
[CG55400](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg55400) (traded
and financial options: introduction — no wasting-asset restriction on the premium),
[CG55415](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg55415)
(loss on lapse), [CG55536](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg55536)
(traded options: the summary table above is drawn from it),
[CG55545](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg55545)
(writer closes out),
[CG78315](https://www.gov.uk/hmrc-internal-manuals/capital-gains-manual/cg78315)
(foreign currency arising from any source).

Everything IB lets a retail account trade — exchange-listed equity, index and
futures options — is a "traded option" or "financial option" under s.144(8),
so the same rules apply to options on futures.

## The rules, one per event

1. **Identity.** An option series (one underlying, one expiry, one strike,
   one right) is one instrument, keyed by IB's conid exactly as stocks and
   futures are ([`db/option_instruments.md`](./db/option_instruments.md)).
   The domain shape is `OptionInstrument(conid, symbol, currency,
   underlying, contract_multiplier, expiry_date, strike, right)`;
   `AssetClass.OPTION`; `OptionRight` is `CALL` / `PUT`. The symbol is
   display text (IB renamed `XSP` to `XSPAM` under one conid; the newer
   symbol wins, the series facts never change).
2. **Long side — buy to open, sell to close.** Acquisitions and disposals
   of the series, matched by the shared `MatchingEngine` (same-day, 30-day,
   s.104 pool, later acquisitions) exactly as a stock. Cost = premium paid ×
   multiplier + commission; proceeds = premium received × multiplier −
   commission. GBP at the trade-date spot. Actions `OPEN_LONG` / `CLOSE_LONG`.
3. **Long side — lapse** (`LAPSE_LONG`; IB code `C;Ep`, price 0). A
   disposal for nil proceeds on the expiry date, so the whole identified
   cost of the lapsed quantity is a loss.
4. **Long side — exercise** (`EXERCISE_LONG`; IB code `C;Ex`). *Not* a
   disposal. The engine puts the exercise through the matcher as a
   zero-proceeds disposal so the cost is identified by the same rules as
   any other disposal of the series, then lifts those chunks out of the
   result into one `OptionExerciseTransfer(side="LONG")` whose
   `amount_gbp` is the identified cost plus the exercise row's fee and
   whose `fees_gbp` is the identified acquisition fees plus that fee. The
   stock engine folds it into the share trade IB booked at the strike:
   added to the cost of shares bought under a call, an incidental cost of
   the disposal of shares sold under a put. With no linked share trade the
   option was cash-settled (s.144A): an ordinary disposal at the row's
   price, and the run carries an `option_exercise_unlinked` warning.
5. **Short side — sell to open** (`OPEN_SHORT`). A disposal at the grant
   date: proceeds = gross premium × multiplier at the grant-date spot,
   the grant commission an incidental cost of disposal, no acquisition
   cost. Reported in the tax year of the grant, as one `OptionGrant`.
6. **Short side — buy to close** (`CLOSE_SHORT`). No disposal. The premium
   paid × multiplier + commission, at the close-date spot, is an
   `OptionGrantClose(kind=PURCHASE)` on the grant it closes and joins that
   grant's incidental costs (s.148(3)). **Decision (2026-09-28): closing
   purchases are identified against open grants first-in, first-out per
   series** — s.148 gives no identification rule; FIFO matches the futures
   decision in `rules.md`, keeps each purchase traceable to one grant, and
   moves gain only between grants, never in total.
7. **Short side — lapse** (`LAPSE_SHORT`; code `C;Ep`). Nothing further;
   recorded as an `OptionGrantClose(kind=LAPSE)` with zero cost so the
   grant is seen to be closed.
8. **Short side — assignment** (`ASSIGN_SHORT`; code `A`). The assigned
   contracts' share of the gross premium (pro rata by quantity) leaves the
   grant as an `OptionExerciseTransfer(side="SHORT", grant_trade_id=…)`
   and is folded into the share trade: added to the proceeds of shares
   delivered under a call, deducted from the cost of shares bought under a
   put. The grant keeps only the contracts not assigned
   (`OptionGrant.chargeable_quantity`), so the charge on the assigned
   contracts is unwound (CG12317). A grant fully assigned is not a
   disposal of the year at all. With no linked share trade the assignment
   was cash-settled (s.144A): the cash paid is an
   `OptionGrantClose(kind=CASH_SETTLEMENT)` cost of the grant, like a
   closing purchase, and the run warns.
9. **Restatement.** A closing purchase, assignment or cash settlement dated
   in tax year Y on a grant charged in an earlier year X changes X's
   figures. The grant realisation always carries every later close, so
   recomputing X gives the amended figure; computing Y records an
   `option_grant_restated` warning naming the grant and X. Check D1 flags
   X's stored run as stale until it is recomputed. A lapse (zero cost)
   restates nothing and is not reported.
10. **Cash legs.** Every premium, commission and settlement amount in a
    non-GBP currency feeds that currency's FX pool at the day's spot, as
    the ninth cashflow source of [`rules.md`](./rules.md#fxruleengine)
    (`fx_cashflow.from_option_trade`, CG78315): buying an option or
    closing a written one spends the currency, writing an option or
    selling a bought one brings it in, and a lapse or a linked exercise
    moves only its fee (IB prints those rows at price 0). The share leg of
    an exercise is an ordinary stock trade and reaches the pool through
    the stock projection.
11. **SA108 placement.** Options over listed shares and indices are not
    themselves "listed shares and securities", so they report under *Other
    property, assets and gains* (boxes 14–19) alongside futures and
    currency pools ([`reporting.md`](./reporting.md)). The
    exercise/assignment adjustments flow into the share disposals they
    modify, in the *Listed shares* section.

### The four s.144 directions in `StockRuleEngine`

`StockRuleEngine.compute(instrument, trades, transfers=…)` applies each
transfer to the share trade it names (`OptionExerciseTransfer.share_trade_id`):

| Transfer | Right | Share trade | Effect on the share trade's projection |
|---|---|---|---|
| `LONG` (holder exercised) | call | BUY | `cost_gbp += amount`, `fees_gbp += fees` |
| `LONG` (holder exercised) | put | SELL | `proceeds_gbp -= amount`, `fees_gbp += amount` (the option cost is an incidental cost of disposal) |
| `SHORT` (writer assigned) | call | SELL | `proceeds_gbp += amount − fees`, `fees_gbp += fees` |
| `SHORT` (writer assigned) | put | BUY | `cost_gbp -= amount − fees`, `fees_gbp += fees` |

A transfer whose share trade is not among the stock's trades, or whose
option implies the other direction (a holder's call points at a sale),
is an `InconsistentTradeError` — the link was wrong and guessing would
mis-tax both legs.

## Worked example — the live figures

After the 2026-09-28 re-ingest and recompute (`ib-cgt match options`,
`ib-cgt report --year 2012/13`):

**2012/13 — two grants, 669.79 GBP net (USD 1,072.65).**

| Grant | Premium | Fee | FX (grant) | A Proceeds | Later event | B Incidental costs | H Gain |
|---|---:|---:|---:|---:|---|---:|---:|
| `XAUUSD 21DEC12 1920.0 C` #5, 2012-10-10 | 770.00 USD | 2.45 USD | 1.6012 | 480.89 | closing purchase #6 on 2012-11-01: 140.00 + 2.45 USD at 1.6155 = 88.18 GBP | 89.71 (1.53 + 88.18) | 391.18 |
| `XAUUSD 21DEC12 1600.0 P` #7, 2012-10-15 | 450.00 USD | 2.45 USD | 1.6064 | 280.13 | lapsed #8 on 2012-12-21 | 1.53 | 278.60 |

**2013/14 — one pooled disposal, a 395.39 GBP loss.** `XSPAM 20DEC14
140.0 P`: bought 2013-05-03 for 785.00 + 1.07 USD (S.104 pool of one
contract, cost 505.35 GBP), sold 2014-01-14 for 182.00 − 1.25 USD
(proceeds 109.96 GBP net of the 0.76 GBP fee).

**2019/20 — one exercise, no option disposal.** 15 `TUR 17MAY19 22.0 P`
bought 2019-01-03 for 1.85 each (2,775.00 + 0.51 USD) and exercised
2019-05-16 (`C;Ex`) into the sale of 1,500 TUR at 22 (`Ex;O`, share trade
#311). The identified cost, 2,208.92 GBP, plus the 0.41 GBP fee become
the incidental cost of that share disposal: A = 25,763.14, B = 2,209.59
(0.67 of sale commission + 2,208.92), and the share line's description
reads `…; s.144: option #313 exercised, 2,208.92 GBP cost of disposal`.
The option series itself has no disposal in 2019/20.

## IB statement shapes

- **Asset label**: `Equity and Index Options` (every file seen); `Options On
  Futures` is accepted too. The legacy custodian suffix is stripped as for
  stocks.
- **Financial Instrument Information** columns for options:
  `Symbol | Description | Conid | Underlying | Listing Exch | Multiplier |
  Expiry | Delivery Month | Type | Strike | Code`. `Type` is `C` / `P`
  (also `CALL` / `PUT`), `Strike` a decimal, `Expiry` `YYYY-MM-DD`.
- **The trade row's symbol** is IB's display form `ROOT DDMMMYY STRIKE C|P`
  (`TUR 17MAY19 22.0 P`). The instrument table's `Symbol` cell may instead
  hold one or more OCC codes, comma-separated after a root rename
  (`XSPAM 141220P00140000, XSP 141220P00140000`), and its `Description`
  the display form under the table's own root. The mapper
  (`mapper.resolve_option_info`) resolves a trade symbol by **exact
  symbol**, then **exact description**, then a parsed **series key**
  (root, expiry, right, strike) matched against the row's description, any
  OCC code in its `Symbol` cell, and its `Underlying` / `Expiry` / `Type` /
  `Strike` columns. Two rows carrying the same key is a `MappingError`; a
  series with no row at all is one too.
- **Codes**: `O` open, `C` close, `C;Ep` lapse (price 0), `C;Ex` exercise
  by the holder (price 0), `A` assignment of the writer, `C;O` a reversal
  through zero (split into a close and an open by the running position, as
  for futures), `C;L` the liquidation flag (ignored). `Ep` / `Ex` / `A` on
  an opening row, on a reversal, or on the wrong side of the position is a
  `MappingError`.
- **The exercise share leg** is a Stocks row at the same instant, at the
  strike, for `contracts × multiplier` shares, code `Ex;O` — an ordinary
  stock trade to everything but the link. `ingest/option_exercises.py`
  pairs it with the option row (same instant, symbol == underlying, same
  currency, `quantity == contracts × multiplier`, `price == strike`, the
  direction the right implies); exactly one candidate is a link, none
  means cash-settled, several is a `MappingError`. Links persist in
  [`db/option_exercise_links.md`](./db/option_exercise_links.md) and
  `ib-cgt ingest` reports `N option exercise(s) linked to a share trade`
  and any unlinked ones.
- **Open Positions** rows under the options label are ingested as
  `statement_positions` like every other class, so check C7 and the
  calculator reconcile option holdings against the statements.

## What the code looks like

| Layer | Where | What |
|---|---|---|
| Domain | `ib_cgt.domain` | `OptionInstrument`, `OptionRight`, `OptionCloseKind` (`PURCHASE`, `LAPSE`, `ASSIGNMENT`, `CASH_SETTLEMENT`), `TradeAction.LAPSE_LONG` / `EXERCISE_LONG` / `LAPSE_SHORT` / `ASSIGN_SHORT`, `OptionGrant` (with `closes`, `chargeable_quantity`, `incidental_costs_gbp`, `gain_gbp`), `OptionGrantClose`, `OpenGrant`, `OptionExerciseTransfer`, `option_share_action(right, side)`; `RunIssueKind.OPTION_GRANT_RESTATED` / `OPTION_EXERCISE_UNLINKED` |
| Ingestion | `ingest/parsers/assemble.py`, `ingest/mapper.py`, `ingest/option_exercises.py`, `ingest/positions.py` | Option rows read in every section; `build_option_instrument`, `resolve_option_info`, `_derive_option_events`; exercise links |
| Schema | `db/migrations/023_options.sql` | [`option_instruments`](./db/option_instruments.md), [`option_exercise_links`](./db/option_exercise_links.md), [`option_grants`](./db/option_grants.md), [`option_grant_closes`](./db/option_grant_closes.md), [`option_exercise_transfers`](./db/option_exercise_transfers.md); `instruments` and `tax_run_issues` CHECKs widened; `v_instruments` gains the option arm |
| Rules | `rules/options.py` | `OptionRuleEngine(fx).compute(instrument, trades, *, exercise_links, soft_residuals)` → `OptionResult(matched, grants, open_grants, transfers, cash_settled_trade_ids)`; `rules/stocks.py` takes `transfers`; `rules/fx_cashflow.from_option_trade` |
| Calculator | `calculator/runner.py`, `calculator/calculator.py` | `run_option_engine`; the runner order futures → options → stocks (with the transfers) → bonds → FX; grants filtered by grant date, transfers by exercise date; the two warnings; persistence of the three run tables |
| Report | `report/` | `GrantBasis` / `GrantCloseRef`; the grants working-sheet table; the s.144 note on share lines |
| CLI / checks | `cli/match_options.py`, `cli/check.py`, `cli/show_trade.py`, `checks/` | `ib-cgt match options [--symbol --since --until]`; `ib-cgt check options` (C8 every series runs cleanly, C9 grant closure, C10 exercise links pair the right rows; D7 run-table trade ids resolve; D1 / D2 extended to grants and transfers); `ib-cgt show trade <id>` prints the series facts, the premium (price × multiplier × quantity) and the exercise linkage |

See [`rules.md`](./rules.md#optionruleengine) for the engine,
[`ingestion.md`](./ingestion.md) for the parser and mapper,
[`reporting.md`](./reporting.md) for the SA108 view and
[`audit.md`](./audit.md) for the dry-run and drill-down commands.
