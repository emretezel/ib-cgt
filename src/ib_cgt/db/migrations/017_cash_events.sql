-- 017_cash_events.sql — store instrument-less cash movements for FX-pool matching.
--
-- Why this migration exists
-- -------------------------
-- Per HMRC CG78315, foreign currency arising from *any* source feeds
-- the same per-currency S.104 pool. Three IB statement sections carry
-- cash movements that have no instrument behind them and were, until
-- now, dropped on the floor:
--
--   * Interest (`tblCombInt_`)      — broker credit / debit interest,
--     stock-lending income, short-stock interest, and the accrued-
--     interest lines on bond purchases and sales. (Bond coupons live
--     in the same section but belong to an instrument, so they keep
--     their own table, `bond_coupons`.)
--   * Deposits & Withdrawals (`tblCombDepWith_`) — money moved in
--     from or out to the outside world. Transfers between the
--     taxpayer's own accounts are not stored: the pools span every
--     account, so they net to zero.
--   * Fees (`tblCombFees_`)         — platform / data / cancellation
--     charges and their refunds.
--
-- A USD credit-interest line is a dollar acquired; a USD fee is a
-- dollar spent. Each becomes an FX-pool acquisition or disposal at the
-- spot rate on its value date — including external deposits, which the
-- user has chosen to book at spot as a documented simplification
-- (the true purchase cost of externally sourced currency is unknowable
-- from the statements).
--
-- Why one table and not three
-- ---------------------------
-- The three sections describe the same thing — a dated, signed cash
-- movement on an account with no instrument — and differ only in
-- origin, which `kind` records. Splitting them would triple the repo,
-- projector and provenance surface for no relational gain.
--
-- Why the amount is signed
-- ------------------------
-- Direction is intrinsic to the row, not to its kind: interest is
-- credited *or* debited, transfers arrive *or* leave, fees are charged
-- *or* refunded. IB's descriptions cannot be trusted for it (negative
-- "Credit Interest" appears in JPY's negative-rate years), so the sign
-- of `amount_native` is the fact: > 0 currency acquired, < 0 spent.
-- Zero is rejected at the domain boundary. This is a deliberate
-- departure from `dividends` / `bond_coupons`, whose direction is fixed
-- by `kind` and whose amounts are therefore stored positive.
--
-- Schema overview
-- ---------------
--   cash_event_id          surrogate PK (INTEGER PRIMARY KEY), the
--                          precedent of `dividend_id` / `bond_coupon_id`.
--   account_id             FK; the balance the cash moved on.
--   kind                   CHECK-closed: 'interest' | 'transfer' | 'fee' | 'withholding'.
--   value_date             ISO date the cash moved — the FX-rate date.
--   amount_native          signed Decimal-as-TEXT (see above).
--   currency               ISO-4217 of the balance. GBP rows are stored
--                          for audit but never touch a pool.
--   description            raw IB description verbatim (audit anchor).
--   statement_row_index    zero-based position across the three cash
--                          sections in parser emit order; its own
--                          row-index space.
--   source_statement_hash  provenance FK, ON DELETE CASCADE.

CREATE TABLE cash_events (
    cash_event_id         INTEGER PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    kind                  TEXT    NOT NULL
        CHECK (kind IN ('interest', 'transfer', 'fee', 'withholding')),
    value_date            TEXT    NOT NULL,
    amount_native         TEXT    NOT NULL,
    currency              TEXT    NOT NULL,
    description           TEXT    NOT NULL,
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index)
) STRICT;

-- FX projector: load every movement in one currency between two dates
-- in value-date order — `CashEventRepo.for_currency`, the same shape as
-- `ix_dividends_pay_currency` / `ix_bond_coupons_pay_currency`.
CREATE INDEX ix_cash_events_currency_date ON cash_events (currency, value_date);

-- Provenance / withdraw-and-reimport: lets the statement cascade and
-- any per-statement audit find the rows without a table scan.
CREATE INDEX ix_cash_events_statement ON cash_events (source_statement_hash);
