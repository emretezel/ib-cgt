-- 011_reclassify_existing_gilts.sql — flip historical UKT/GBP rows to CGT-exempt.
--
-- Why this migration exists
-- -------------------------
-- Up to and including the previous mapper revision, every bond was
-- ingested with `is_cgt_exempt=0` because the classifier in
-- `ingest/mapper.py:_build_bond` hardcoded the flag. The mapper now
-- auto-detects UK gilts (description prefix `"United Kingdom Gilt"`,
-- with a `UKT `-prefix-on-GBP fallback) and writes
-- `is_cgt_exempt=1` for them at ingest time.
--
-- This migration brings already-ingested rows in line with the new
-- classifier without forcing a re-ingest. It applies the same
-- symbol-prefix fallback that `_classify_bond_exempt` uses when no
-- description is available — matching every UKT-prefixed GBP bond
-- the user has on file (verified against the corpus: all 12
-- existing bond_instruments rows are UK gilts).
--
-- Idempotent: re-running the migration is a no-op (the WHERE clause
-- skips rows already flagged exempt). Non-UKT/GBP bonds are
-- untouched — the user must re-ingest after extending the
-- `IB_CGT_BONDS_EXEMPT` allowlist or, for non-gilt corporate-bond
-- exemptions, can update individual rows by hand.
--
-- Match the classifier's symbol-prefix fallback: `"UKT "` exact
-- (trailing space) so we don't accidentally flag a non-gilt symbol
-- like `"UKTREASURYBILL"` (none observed, but the prefix is the
-- contract).

UPDATE bond_instruments
   SET is_cgt_exempt = 1
 WHERE currency = 'GBP'
   AND symbol LIKE 'UKT %'
   AND is_cgt_exempt = 0;
