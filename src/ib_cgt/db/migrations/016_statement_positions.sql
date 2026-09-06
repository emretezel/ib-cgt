-- 016_statement_positions.sql — store each statement's Open Positions rows.
--
-- Why this migration exists
-- -------------------------
-- An IB statement ends with the positions still open on the last day
-- of its period (`tblOpenPositions_<acct>Body`): stocks, bonds and
-- futures, one row per symbol per currency, signed quantity. That is
-- the broker's independent view of what the trades should net to.
-- The calculator reconciles its trade-derived end-of-history position
-- for every instrument against the *latest* statement's rows: a
-- confirmed open short is an expected residual, a position the
-- statement does not carry is a data gap (pre-history purchase, a
-- missing statement, an over-sold holding), and a statement position
-- with no trades behind it is a holding bought before the history
-- begins. Check C7 runs the same reconciliation standalone.
--
-- Only the quantity is stored. IB also prints cost basis and market
-- value, but neither is a fact this project needs — cost basis is
-- what the matching engines compute — and storing them would invite
-- a second source of truth for the pool cost.
--
-- Schema overview
-- ---------------
--   statement_hash        Provenance FK with ON DELETE CASCADE, so
--                         `ingest --replace` and `db reset` clear the
--                         rows with the statement.
--   statement_row_index   Zero-based position within the Open
--                         Positions section — its own row-index
--                         space, like `trades` / `dividends`.
--   instrument_id         FK to the parent `instruments` row, resolved
--                         at ingest exactly as trades are (ISIN for
--                         bonds, multiplier + expiry for futures).
--   quantity              Signed Decimal-as-TEXT; negative = short.
--                         The domain object rejects zero.
--   UNIQUE (statement_hash, instrument_id)   IB prints one row per
--                         symbol per account, so two rows for one
--                         instrument in one statement is a parse bug
--                         worth failing loudly on.

CREATE TABLE statement_positions (
    statement_hash      TEXT    NOT NULL
        REFERENCES statements(statement_hash) ON DELETE CASCADE,
    statement_row_index INTEGER NOT NULL CHECK (statement_row_index >= 0),
    instrument_id       INTEGER NOT NULL REFERENCES instruments(instrument_id),
    quantity            TEXT    NOT NULL,
    PRIMARY KEY (statement_hash, statement_row_index),
    UNIQUE (statement_hash, instrument_id)
) STRICT;

-- No further index: the only read path is
-- `StatementPositionRepo.for_statement` (`WHERE statement_hash = ?`),
-- which the primary key's leading column already serves.
