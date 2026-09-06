-- 020_tax_run_issues.sql — record what a tax-year computation could not do.
--
-- Why this migration exists
-- -------------------------
-- `ib-cgt compute --year` persists everything that worked and records
-- everything that did not — an engine failure on one instrument, a
-- position the latest statement does not confirm, an FX pool
-- residual, a history that stops short of the year end — as issues
-- on the run ("save what worked"). Storing them beside the figures
-- means a later reader of `tax_runs` can tell a complete run from an
-- incomplete one without re-running anything.
--
-- Why severity is not a column
-- ----------------------------
-- Severity is a function of `kind` (`RunIssueKind.severity` in
-- `ib_cgt.domain.run_issue`). Storing it too would be a transitive
-- dependency (3NF) and would let the two drift; the domain enum is
-- the single source of truth and the kind list here mirrors it.
--
-- Why `instrument_id` is nullable
-- -------------------------------
-- Most issues are about one instrument (the failing contract, the
-- over-sold stock, the synthetic FX pool instrument for a residual).
-- The coverage kinds and `empty_year` describe the run as a whole and
-- carry no instrument — the CHECK ties NULL-ness to exactly those
-- kinds so the column is never NULL by accident.

CREATE TABLE tax_run_issues (
    run_id        INTEGER NOT NULL REFERENCES tax_runs(run_id) ON DELETE CASCADE,
    seq           INTEGER NOT NULL CHECK (seq >= 0),
    kind          TEXT    NOT NULL CHECK (kind IN (
                      'position_mismatch', 'rate_not_found', 'inconsistent_trades',
                      'engine_failure', 'open_short_position', 'fx_residual',
                      'history_incomplete', 'history_no_lookahead', 'empty_year')),
    instrument_id INTEGER REFERENCES instruments(instrument_id),
    message       TEXT    NOT NULL,
    PRIMARY KEY (run_id, seq),
    CHECK ((instrument_id IS NULL)
           = (kind IN ('history_incomplete', 'history_no_lookahead', 'empty_year')))
) STRICT;
