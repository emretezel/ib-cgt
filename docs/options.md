# Options — proposal for the CGT rules (not yet implemented)

The full IB history (2011 onwards) contains one product type the calculator
does not model: **Equity and Index Options**, traded in 2012 (two XAUUSD
call/put series on the OGFX venue, both closed in-year — one bought back, one
expired) and 2013–2014 (one XSP put, bought May 2013 and sold in 2014). None
are open today; the parser drops the rows in every section, so they affect
neither positions nor pools. This page proposes how they should be taxed and
what implementing that would take, so the rules can be agreed before any code
is written. Nothing here is applied yet.

## The short answer to "treat them like futures?"

No — only the arithmetic of a closed round-trip looks the same. The statute
treats an option as an asset in its own right (TCGA 1992 s.144) with rules
for each of the four things that can happen to it, and three of those rules
differ from the futures close-out model in [`rules.md`](./rules.md):

| Event | Futures (s.143(5)–(6), implemented) | Options (s.144 / s.148, proposed) |
|---|---|---|
| Open a long | Nothing until close-out | **Acquisition** of an asset: premium + commission is allowable cost |
| Close a long by selling | Disposal on close-out, FIFO per side | **Disposal** of the option under normal CG rules — same-day / 30-day / s.104 **pooled by series** (HMRC CG55536: "options pooled by series"), not FIFO |
| Long lapses unexercised | (cash-settled at expiry, s.143(6)) | **Disposal for nil** on the lapse date (s.144(4)): allowable loss = premium + costs (CG55415) |
| Long exercised | (delivery, s.143(8)) | No separate disposal: the premium is **added to the cost of the shares** bought under a call, or is a **cost of disposal** of the shares sold under a put (s.144(3), CG55536); a cash-settled index option is s.144A — the premium is allowed against the cash received |
| Write (sell to open) | Nothing until close-out | The premium net of costs is a **chargeable gain at the date of grant** (s.144(1), CG12312, CG55536) |
| Close a short by buying back | Disposal on close-out | **Not a disposal**: the cost of the closing purchase (plus its commission) is added to the incidental costs of the original grant (s.148, CG55545) — the grant-date gain is reduced, and no new event arises |
| Short lapses unexercised | — | Nothing further: the grant charge stands (CG55536) |
| Short assigned | — | Grant and the resulting share sale/purchase are **one transaction** (s.144(2)): the premium is added to the sale proceeds of shares delivered under a call, or deducted from the cost of shares bought under a put; if grant and exercise fall in different tax years the earlier charge is adjusted (CG12317) |

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
(writer closes out).

Everything IB lets a retail account trade — exchange-listed equity, index and
futures options — is a "traded option" or "financial option" under s.144(8),
so the same rules apply to options on futures.

## The proposed rules, one per event

1. **Identity.** An option series (one underlying, one expiry, one strike,
   one right) is one instrument, keyed by IB's conid exactly as stocks and
   futures are. IB prints the series under `Equity and Index Options` in
   Trades, Open Positions and Financial Instrument Information; the trades
   symbol equals the instrument table's *Description* on the 2012–2014
   files, not its *Symbol*, so resolution must try both.
2. **Long side — buy to open, sell to close.** Acquisitions and disposals
   of the series, matched by the shared `MatchingEngine` (same-day, 30-day,
   s.104 pool, later acquisitions) exactly as a stock. Cost = premium paid ×
   multiplier + commission; proceeds = premium received × multiplier −
   commission. GBP at the trade-date spot.
3. **Long side — lapse.** IB prints the expiry as a trade with code `Ep`
   and price 0 (`C;Ep`). It is a disposal for nil proceeds on the expiry
   date, so the whole pooled cost of the lapsed quantity is a loss.
4. **Long side — exercise (`Ex` code).** The option leg is *not* a disposal.
   Its pooled cost is carried into the share trade IB books on the same
   timestamp with an `Ex`/`A` code: added to the cost of the shares bought
   under a call, deducted from the proceeds of the shares sold under a put.
   For a cash-settled index option (XSP) there is no share trade: the cash
   IB books is the disposal consideration and the pooled cost is allowed
   against it (s.144A).
5. **Short side — sell to open.** A disposal at the grant date: proceeds =
   premium received × multiplier − commission, cost = nil. Reported in the
   tax year of the grant.
6. **Short side — buy to close.** No disposal. The premium paid × multiplier
   + commission is added to the incidental costs of the grant it closes,
   identified FIFO against open grants of the series (s.148 gives no
   identification rule; FIFO matches the futures decision in `rules.md` and
   keeps each closing purchase traceable to one grant). When the closing
   purchase falls in a later tax year than the grant, the grant year's
   figures are restated — the same mechanism CG12317 describes for
   exercise.
7. **Short side — lapse.** Nothing further.
8. **Short side — assignment (`A` code).** The premium is folded into the
   share trade IB books on the same timestamp: added to the proceeds of
   shares delivered under a call, deducted from the cost of shares bought
   under a put. As in 6, a grant in an earlier year is restated.
9. **Cash legs.** Every premium, commission and settlement amount in a
   non-GBP currency feeds that currency's FX pool at the day's spot, as the
   ninth cashflow source of [`rules.md`](./rules.md#fxruleengine) (HMRC
   CG78315 — foreign currency arising from any source).
10. **SA108 placement.** To confirm: options over listed shares and indices
    are not themselves "listed shares and securities"; the working
    assumption is the *Other property, assets and gains* section (boxes
    14–19) alongside futures, with the exercise/assignment adjustments
    flowing into the share disposals they modify.

## What implementing it takes

- **Domain**: `AssetClass.OPTION`, an `OptionInstrument` (conid, underlying,
  multiplier, expiry, strike, right, cash-settled flag) and `TradeAction`
  values for the eight events above, or the existing open/close pairs plus an
  `event` qualifier (lapse, exercise, assignment).
- **Schema**: an `option_instruments` child table keyed by conid, a link from
  a share trade to the option event that modifies it (exercise/assignment),
  and a run table for grant realisations and their later restatements.
- **Ingestion**: the assembler stops dropping `Equity and Index Options` (and
  `Options On Futures`), the mapper resolves the series by description as
  well as symbol, and the `Ep` / `Ex` / `A` codes become event kinds.
- **Rules**: an `OptionRuleEngine` — the long side is `MatchingEngine` with
  lapses as nil-proceeds disposals and exercises as cost transfers; the short
  side is a per-grant ledger (premium, closing purchases, assignment) that
  emits one realisation per grant with restatement records for later years.
- **Calculator / report**: grant realisations and restatements alongside
  futures realisations; restatements need an "amended" marker on the
  earlier year's persisted run.
- **Checks**: option positions reconcile against the statements' Open
  Positions like every other class.

## Decisions needed before coding

1. Identification for closing purchases against open grants: FIFO (as
   proposed, consistent with futures) or pooling by series.
2. How to present a prior-year restatement (an amended SA108 for that year,
   or an adjustment carried into the current year — HMRC expects the former).
3. Whether to backfill the 2012–2014 option trades at all: every series is
   closed, the amounts are small (the 2012 XAUUSD pair realised about USD
   1,073 net; the XSP put closed in 2014), and those years are outside any
   return still open.
