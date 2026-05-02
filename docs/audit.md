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

## `ib-cgt show trade <trade_id>`

Single-trade audit dossier. Resolves the trade through
`TradeRepo.get`, looks the source statement up via
`StatementRepo.get`, and prints:

- Account, statement file path, statement row index.
- Trade datetime / trade date / settlement date.
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
