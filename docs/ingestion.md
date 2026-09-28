# Ingestion

How `ib-cgt ingest` turns an Interactive Brokers activity statement — HTML
or PDF — into rows of the database. Component 3 of
[`architecture.md`](./architecture.md); the package is `src/ib_cgt/ingest/`.

## The pipeline

```
file bytes ──hash──▶ already imported?  ──yes──▶ stop (constant time)
     │
     └─▶ parser strategy (by suffix, or --format) ──▶ RawDocument (tables of classified rows)
                                                          │
                                                   assembler ──▶ ParsedStatement (raw string rows)
                                                          │
                            mappers: trades, corporate actions, dividends, coupons, cash events, positions
                                                          │
                                      exercise links (option row ↔ share trade at the strike)
                                                          │
                                        coverage filter (days an earlier statement owns)
                                                          │
                                one transaction: statements + trades + option_exercise_links
                                                 + dividends + bond_coupons + cash_events
                                                 + statement_positions
```

Every stage below the parser is format-blind: a PDF and an HTML statement of
the same period produce the same `ParsedStatement` and therefore the same
rows.

## Formats and the parser strategy

`ib_cgt.ingest.parsers` holds one adapter per file format and one assembler
behind both:

| Module | Role |
|---|---|
| `parsers/tables.py` | The neutral table model: `RawDocument` (account, period, tables), `RawTable` (section + rows), `TableRow` (a `RowKind` and its cell texts). Also the shared header parsing — the statement period every vintage prints as `Month D, YYYY - Month D, YYYY`, and the `U…` account code. |
| `parsers/html.py` | `.htm` / `.html`: BeautifulSoup + lxml. A section is a `<div id="tbl<Section>_…Body">`; a `<th>` row is a header, a `header-asset` / `header-currency` cell marks a sub-header, `subtotal` / `total` rows are aggregates, everything else is data. Cell line breaks (`<br/>`) are kept. |
| `parsers/pdf.py` | `.pdf`: pdfplumber. See *The PDF grid* below. |
| `parsers/assemble.py` | Turns a `RawDocument` into a `ParsedStatement`. Owns everything about what the columns *mean*. |
| `parsers/__init__.py` | `StatementFormat` (`auto`, `html`, `pdf`), the `StatementParser` protocol, the two strategies, and `parser_for(fmt, path=…)`. |

`ib-cgt ingest PATH...` detects the format from each file's suffix;
`--format html|pdf` overrides a misleading suffix.

### The assembler's rules (both formats)

- **Columns are resolved by label, never by position.** Each section has an
  alias table (`Comm/Fee` and `Comm in GBP` are both the fee column;
  `Proceeds` versus `Notional Value` is irrelevant) and a required set. Every
  header row that resolves the required labels becomes the current column
  map — IB prints a fresh header when the column set changes mid-section (the
  Forex sub-table of Trades, the bonds sub-table of Open Positions, which
  prints `Accrued Int` where `Mult` sits). A one-cell header (`Carried by …`)
  is ignored. A multi-cell header missing a required label fails loudly:
  reading its rows with the previous header's positions would put values in
  the wrong fields.
- **Asset-class and currency sub-headers** stamp the rows below them. The
  legacy custodian suffix (`Stocks - Held with Interactive Brokers …`) is
  stripped. Every asset class IB prints is read — stocks, bonds, futures,
  forex and, since migration 023, `Equity and Index Options` / `Options On
  Futures` (see [`options.md`](./options.md)). The Financial Instrument
  Information aliases include the option columns `Underlying`, `Type` and
  `Strike` (the stocks table also has a `Type` column — `ETF`, `COMMON` — so
  the raw field is `type_text` and only the option mapper reads it).
- **Aggregates are skipped**: total rows, rows too short to carry every
  required column, and rows whose marker cell (the date, the symbol, the
  quantity) is blank. A real row before any header or outside any currency
  block fails loudly — that is an adapter fault, and dropping it would drop a
  trade.
- **Emit order is fixed per section family**, whatever the file order:
  dividends then withholding; interest, then deposits and withdrawals, then
  fees. The order fixes the per-statement row index the child tables are keyed
  on.
- **Wrapped cells are one value** (`2012-12-19,` over `09:41:00` is one
  timestamp), except the Open Positions symbol cell of a bond, whose last line
  is the symbol and whose earlier lines are the description.

### Time zones

IB prints every `Date/Time` cell without an offset and declares the zone once
per file, in the Notes/Legal Notes section: *"Trade execution times are
displayed in Eastern Time."* Both adapters read that note
(`tables.time_zone_from_text`; the PDF prints it on the last page) and the
assembler carries it as `ParsedStatement.time_zone`. The zone is an account
display setting, so it is one per statement, not one per exchange — a Eurex
fill at 09:00 Frankfurt is printed as 03:00. A file without the note, or with
a phrase the parser does not know, is rejected: the statement's own
declaration is the only honest source, and nothing is assumed.

The mappers attach the zone to the printed clock
(`mapper.parse_statement_datetime`), which gives the execution instant;
`trades.trade_datetime` stores it in UTC. A trade's date for CGT —
`trades.trade_date` — is the **Europe/London date of that instant**
(`Trade.uk_date_of`; see
[`rules.md`](./rules.md#which-date-is-a-trades-date)). Eastern is four or five
hours behind London, so a fill printed from 19:00 Eastern (20:00 in the weeks
the US and UK clocks change on different dates) is dated the next day: the
Globex evening session, Asian-hours futures and late FX fills. The zone is
recorded on the statement row (`statements.time_zone`, migration 022), and
`ib-cgt show trade` prints the time as the statement prints it, so a row can
be found in the file by eye.

### The PDF grid

IB draws its PDF statement as a grid, which is all the adapter reads:

- every table cell is a filled rectangle; cells sharing a top and bottom edge
  form a **band**, and a band with a horizontal gap wider than a hairline is
  two rows, one per page column (the gutter of a two-column page);
- a row whose words are all 10 pt is a **title** — it opens the section of the
  rows that follow in its column, closes the column when it is a section we do
  not read (`Cash Report`, `Interest Accruals`, `Forex Balances`, `Codes`, …),
  and, when it repeats the section already open there, is a page-break
  continuation (IB repeats the title but not always the header);
- a one-cell row is a sub-header: a three-letter upper-case code is the
  currency, `Carried by …` and `Total …` are noise, anything else is the
  asset class;
- a multi-cell row whose first cell starts with `Total` is an aggregate; one
  whose first cell is `Symbol`, `Date`, `Date/Time`, `Description`, `Code` or
  `Report Date` is a header; everything else is data;
- a word belongs to the cell that contains its centre on **both** axes — on a
  two-column page the left and right rows have different heights, so a band's
  vertical extent says nothing about the other column. Words inside a cell are
  grouped into lines by their vertical position and joined with newlines.

The pure layer (`build_document`) takes typed `Cell` / `Word` records and is
tested on hand-built geometry; `read_pages` is the only pdfplumber call.
Against the seven real 2011–2017 files, every printed per-block total (trade
quantities per symbol, amounts per currency block of every cash and dividend
section) equals the sum of the rows the adapter produced.

### Options: series resolution, event codes, exercise links

An option row's symbol is IB's display form `ROOT DDMMMYY STRIKE C|P`
(`TUR 17MAY19 22.0 P`). The instrument table may print the same series
quite differently — its `Symbol` cell as one or more OCC codes
(`XSPAM 141220P00140000, XSP 141220P00140000` after IB renamed the root),
its `Description` as the display form under the table's own root. The
mapper (`mapper.resolve_option_info`, shared with the Open Positions
mapper) tries an **exact symbol** match, then an **exact description**
match, then parses the trade symbol to a **series key** (root, expiry,
right, strike) and matches it against the row's description, every OCC
code in its `Symbol` cell, and its `Underlying` / `Expiry` / `Type` /
`Strike` columns. Two rows with one key, or no row at all, is a
`MappingError` — the conid comes from that row and nothing is guessed.
`build_option_instrument` then reads conid, underlying, multiplier,
expiry, strike and right (the columns first, the parsed key as fallback
on the 2012 vintage, whose table lacks `Type` and `Strike`).

Option rows open and close like futures rows (`O` / `C`, and the `C;O`
reversal split by the statement-local running position — the shared
`_derive_open_close_events`), plus one qualifier token that names *how*
a close happened (`_derive_option_events`):

| Code | Long close | Short close |
|---|---|---|
| `C;Ep` (lapse, price 0) | `LAPSE_LONG` | `LAPSE_SHORT` |
| `C;Ex` (exercise, price 0) | `EXERCISE_LONG` | error — IB marks the writer with `A` |
| `A` (assignment) | error — IB marks the holder with `Ex` | `ASSIGN_SHORT` |

A qualifier on an opening row, on a reversal, or more than one qualifier
on a row is a `MappingError`. The liquidation flag `L` is ignored as it is
for futures.

When an option is exercised or assigned IB books the share leg at the
same instant as a Stocks row at the strike for `contracts × multiplier`
shares (code `Ex;O`). `ingest/option_exercises.py` pairs the two on the
mapped trades, before anything is persisted: same `trade_datetime`, a
`StockInstrument` whose symbol is the option's underlying in the option's
currency, `quantity == contracts × multiplier`, `price == strike`, and
the direction the right implies (a holder's call or a writer's put buys
shares; a holder's put or a writer's call sells them). Exactly one
candidate is a link; none means IB booked no share trade (a cash-settled
option, s.144A) and the row is counted as unlinked so the calculator can
warn; several candidates, or one share trade claimed twice, is a
`MappingError`. The ingestor resolves the pair's row positions to trade
ids after the insert (`TradeRepo.ids_for_rows`) and writes
[`option_exercise_links`](./db/option_exercise_links.md); both rows share
a date, so the coverage rule keeps or skips them together. `ib-cgt ingest`
prints the counts (`1 option exercise linked to a share trade` on
`statements/futures/19_20.htm`; `N exercise(s) with no share trade`
otherwise).

Option rows in Open Positions are ingested as `statement_positions`
through the same resolution, so option holdings reconcile against the
statements like stocks and futures (check C7).

### Vintage quirks the mappers absorb

- Forex trades print `Comm in GBP` (with a non-breaking space in HTML) rather
  than `Comm/Fee`; both are the fee column, and IB denominates every forex
  commission in GBP either way.
- 2013–2014 dividend rows carry no `(SECID)` tag and say `Dividend` rather than
  `Cash Dividend`: `AAPL Dividend 3.05 USD per Share (Ordinary Dividend)`,
  `INTC Payment in Lieu of Dividend (Ordinary Dividend)`, withholding
  `AAPL Dividend 3.05 USD per Share - US Tax`. The tag is optional in the
  dividend grammar and the kind phrase is anchored right after the symbol,
  which is what keeps interest-withholding rows (`Withholding @ 30% on Credit
  Interest`) out of the dividend mapper.
- IB's Change in Dividend Accruals section is never read: an accrual
  adjustment moves no cash, and its table has a different column set.

## Overlapping statements: the coverage rule

IB lets you download any date range, so two statements for one account can
cover the same days — a re-downloaded statement that runs a few weeks past
the file on record, a calendar-year PDF that ends on the day a tax-year HTML
file starts. IB prints no per-row identifier, and genuinely distinct fills
can share every visible field (see `tests/integration/test_ingest_real.py`,
the FESX fifteen-fill case), so the overlap cannot be de-duplicated by
content. It is de-duplicated by **date** (`ingest/coverage.py`):

> The first statement ingested for an account owns every day of its period.
> A later statement whose period overlaps it contributes only the facts dated
> on days the earlier one does not cover.

Two refinements make that exact for IB's files:

- **Only overlapping periods take part.** IB assigns a transaction to a
  statement by the exchange trading date, so a fill executed late on the
  evening before a period starts is printed with the previous calendar date
  while belonging to the later statement (a 17:06 ET CME fill on 5 April
  2022 sits in the statement that starts on 6 April). Two consecutive,
  non-overlapping statements never share a fact, so nothing of the later one
  is ever skipped, whatever its rows are dated.
- **Two files that start on the same day** carry the same evening-before
  rows, so the new statement's rows dated before its own period are skipped
  as well.

What this means in practice:

| You ingest | Result |
|---|---|
| The next tax year's statement | Every row lands, including rows printed with the previous period's last date. |
| A re-download of the latest statement that runs to a later end date | Only the rows past the old end date land (plus the newer open positions). |
| The same period again from a modified file, without `--replace` | Nothing lands but the statement row and its positions; the CLI says the period was already covered. |
| `--replace` | The prior version (same hash, or an earlier file at the same path) is withdrawn first and the coverage is read after that, so it never owns its own days. The CLI lists other statements that overlapped the withdrawn one: if they were ingested after it, rows on the shared days were skipped in its favour and are restored by re-ingesting them with `--replace`. |

Open positions are never filtered: they are a snapshot as of the period's
last day, and the calculator reads the latest statement's. Rows the rule
skips keep their neighbours' positions — the persisted `statement_row_index`
is the row's position in the file, so `show trade` still points at the right
source row.

`ib-cgt ingest` with several paths parses them all first (a file that is not
a statement fails the batch before anything is written) and persists them
earliest period first, so the rule does not depend on the order the shell
expanded the paths. Check **A15** warns when a fact is dated more than a month
outside its own statement's period — the symptom of a mis-read period.

## Idempotency, in layers

1. The SHA-256 of the file bytes: a repeat ingest of an identical file is a
   constant-time no-op (`Already imported`).
2. The coverage rule above, for a different file over the same days.
3. The `(source_statement_hash, statement_row_index)` UNIQUE on every child
   table, for a re-presented row within one statement.

## What the tables get

| Section | Table | Notes |
|---|---|---|
| Trades | `trades` | Stocks, bonds, futures, options, forex. Corporate-action cash mergers and bond maturities are synthesised as SELL trades after the regular rows. |
| Trades (options with `Ex` / `A`) | `option_exercise_links` | The option row and the share trade IB booked for it at the strike, when exactly one matches. |
| Financial Instrument Information | `*_instruments` | The conid (stocks, futures, options) and ISIN (bonds) that key instruments; for options also underlying, multiplier, expiry, strike and right. |
| Dividends, Withholding Tax | `dividends` | Instrument-less; withholding rows with no stock tag are cash events. |
| Interest | `bond_coupons` / `cash_events` | Coupon payments to the former, everything else to the latter. |
| Deposits & Withdrawals, Fees | `cash_events` | Internal transfers between the taxpayer's own accounts are dropped. |
| Open Positions | `statement_positions` | As of the period's last day; resolved through the same instrument identities. |

See [`db/statements.md`](./db/statements.md) for the statement row itself.
