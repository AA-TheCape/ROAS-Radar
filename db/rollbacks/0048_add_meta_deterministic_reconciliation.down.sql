BEGIN;

DROP VIEW IF EXISTS meta_ads_deterministic_reconciliation_investigation;
DROP TABLE IF EXISTS meta_ads_deterministic_reconciliation_mismatches;
DROP TABLE IF EXISTS meta_ads_deterministic_reconciliation_runs;

COMMIT;
