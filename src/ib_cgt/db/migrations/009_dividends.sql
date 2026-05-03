-- 009_dividends.sql — store IB dividend cashflows for FX-pool matching.
--
-- Why this migration exists
-- -------------------------
-- The FX rule engine consumes four cashflow sources today: forex trades,
-- non-GBP stock-trade cash legs, non-GBP futures-trade fees, and futures
-- realisations. Per HMRC CG78315 ("foreign currency arising from any
-- source") a dividend in USD / EUR / SEK is just as much an inflow into
-- the per-currency S.104 pool as a stock-sale receipt is — but the
-- ingest layer was silently dropping the IB `tblCombDiv_<acct>Body`
-- section, leaving every FX pool short of its real opening cover.
--
-- This migration adds a dedicated `dividends` table so the ingest layer
-- can persist each dividend / withholding / payment-in-lieu row and the
-- FX cashflow projector can fold them into the matching engine.
--
-- Why a new table and not a new `TradeAction`
-- -------------------------------------------
-- Cash-for-shares mergers ARE forced sales (the instrument vanishes,
-- cash arrives) and therefore project cleanly onto a `Trade(SELL)`.
-- Dividends do not: the holding survives, no quantity of shares is
-- transacted, and there is no per-unit "price" being negotiated. Forcing
-- them into `trades` would mean loosening `Trade._check_action_vs_instrument`
-- (every BUY/SELL consumer would then have to filter DIVIDEND), and
-- repurposing `quantity` / `price` away from their docstring meanings —
-- a direct AGENTS.md rule 3 violation ("each table represents one thing").
--
-- Schema overview
-- ---------------
--   dividend_id          surrogate PK (matches the `trades.trade_id`
--                        precedent — INTEGER PRIMARY KEY).
--   account_id, instrument_id  FKs identical to `trades` so the same
--                              cascade / audit story works.
--   kind                 'cash_dividend'  — gross dividend received
--                                         (FX-pool acquisition).
--                        'withholding_tax' — US/foreign WHT debited
--                                          (FX-pool disposal).
--                        'payment_in_lieu' — cash-in-lieu on lent stock
--                                          (acquisition; kept distinct
--                                          for income-side audit).
--   pay_date             ISO-8601 date the cash hits the foreign-
--                        currency balance — that is the date the FX
--                        rate applies for GBP conversion.
--   amount_native        Decimal-as-TEXT, strictly positive. Direction
--                        is encoded in `kind`, never via a sign on the
--                        amount (mirrors `Trade.quantity` >= 0 with
--                        sign in `action`).
--   currency             ISO-4217 of the cash leg.
--   description          Raw IB description verbatim — the forensic
--                        audit anchor that lets `show dividend <id>`
--                        match a stored row to the source HTML row.
--   statement_row_index  Zero-based position within the dividend
--                        section of the source statement. Independent
--                        of `trades.statement_row_index` (each table
--                        owns its own index space).
--   source_statement_hash  Provenance FK with ON DELETE CASCADE so
--                          `ingest --replace` clears these rows
--                          atomically with the trades.

CREATE TABLE dividends (
    dividend_id           INTEGER PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    instrument_id         INTEGER NOT NULL REFERENCES instruments(instrument_id),
    kind                  TEXT    NOT NULL
                                  CHECK (kind IN ('cash_dividend',
                                                  'withholding_tax',
                                                  'payment_in_lieu')),
    pay_date              TEXT    NOT NULL,
    amount_native         TEXT    NOT NULL,
    currency              TEXT    NOT NULL,
    description           TEXT    NOT NULL,
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index)
) STRICT;

-- Hot path: chronological dividend stream for one instrument. Matches
-- the `ix_trades_instrument_dt` shape so audit commands like
-- `show dividend --instrument <id>` (future) get an index-only scan.
CREATE INDEX ix_dividends_instrument_pay ON dividends (instrument_id, pay_date);

-- FX projector: load every dividend that touches a given currency
-- between two dates. The composite (currency, pay_date) is what
-- `DividendRepo.for_currency` filters on at SQL level, narrowing the
-- result before Python gets involved.
CREATE INDEX ix_dividends_pay_currency ON dividends (currency, pay_date);

-- Provenance / withdraw-and-reimport: same role as the matching index
-- on `trades`. Lets `ingest --replace` find rows to cascade-delete via
-- `statement_hash` without scanning the full table.
CREATE INDEX ix_dividends_statement ON dividends (source_statement_hash);
