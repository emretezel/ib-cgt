-- 012_bond_coupons.sql — store IB bond coupon payments for FX-pool matching.
--
-- Why this migration exists
-- -------------------------
-- IB statements expose bond coupon payments inside the "Interest"
-- section (`tblCombInt_<acct>Body`) as rows whose description matches
-- `"Bond Coupon Payment (<symbol> - <long_description>)"`. Up to and
-- including the previous ingest revision the entire Interest section
-- was silently dropped.
--
-- Per HMRC CG78315, "foreign currency arising from any source" feeds
-- the same per-currency S.104 pool the FXRuleEngine already computes
-- against. A coupon paid in USD / EUR / JPY is therefore a sixth
-- inflow source on top of the five already wired (forex trades,
-- non-GBP stock-trade cash, dividends, futures fees, futures realised
-- P&L). GBP coupons are not pooled (no FX trade is realised) but are
-- still persisted here for audit symmetry and so the user can see
-- every coupon they received in one place.
--
-- Why a new table and not extending `dividends`
-- ---------------------------------------------
-- AGENTS.md §3 ("each table represents one thing") forbids
-- conflating equity dividends with bond coupons:
--   * Different IB sections (`tblCombDiv` vs `tblCombInt`).
--   * Different income-tax treatment (dividend allowance vs interest
--     savings allowance / accrued-income-scheme interactions).
--   * Different shape (no withholding-tax variant on bond coupons,
--     no `kind` discriminator needed).
-- Forcing them into the same table would require a `kind` value the
-- WHT/PIL CHECK constraint cannot accept, plus a polymorphic
-- `instrument` foreign key the Dividend domain object refuses by
-- design (typed narrowly as `StockInstrument`). A dedicated table is
-- structurally cleaner.
--
-- Schema overview
-- ---------------
--   bond_coupon_id       surrogate PK — matches `dividend_id`
--                        precedent (INTEGER PRIMARY KEY).
--   account_id, instrument_id  FKs identical to `trades` /
--                              `dividends` for cascade / audit
--                              symmetry.
--   pay_date             ISO date the cash hits the foreign-currency
--                        balance — the date whose FX rate is applied.
--   amount_native        Decimal-as-TEXT, strictly positive (coupons
--                        are credits — there is no signed-amount
--                        convention).
--   currency             ISO-4217 of the cash leg.
--   description          Raw IB description verbatim, the audit
--                        anchor that lets a stored row be reconciled
--                        to its source HTML row.
--   statement_row_index  Zero-based position within the Interest
--                        section. Independent of `trades` and
--                        `dividends` row-index spaces.
--   source_statement_hash  Provenance FK with ON DELETE CASCADE so
--                          `ingest --replace` clears these rows
--                          atomically with the trades.

CREATE TABLE bond_coupons (
    bond_coupon_id        INTEGER PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    instrument_id         INTEGER NOT NULL REFERENCES instruments(instrument_id),
    pay_date              TEXT    NOT NULL,
    amount_native         TEXT    NOT NULL,
    currency              TEXT    NOT NULL,
    description           TEXT    NOT NULL,
    statement_row_index   INTEGER NOT NULL CHECK (statement_row_index >= 0),
    source_statement_hash TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    UNIQUE (source_statement_hash, statement_row_index)
) STRICT;

-- Hot path: chronological coupon stream for one bond. Mirrors
-- `ix_dividends_instrument_pay` so a future
-- `show coupon --instrument <id>` audit command gets an index-only
-- scan.
CREATE INDEX ix_bond_coupons_instrument_pay
    ON bond_coupons (instrument_id, pay_date);

-- FX projector: load every coupon that touches a given currency
-- between two dates. Same shape as `ix_dividends_pay_currency` so the
-- repo's `for_currency` filter is index-covered end to end.
CREATE INDEX ix_bond_coupons_pay_currency
    ON bond_coupons (currency, pay_date);

-- Provenance / withdraw-and-reimport: same role as the matching
-- index on `dividends` and `trades`. Lets `ingest --replace` find
-- rows to cascade-delete via `statement_hash` without scanning.
CREATE INDEX ix_bond_coupons_statement
    ON bond_coupons (source_statement_hash);
