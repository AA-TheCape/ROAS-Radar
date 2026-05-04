BEGIN;

DROP TABLE IF EXISTS ga4_fallback_candidates CASCADE;
DROP FUNCTION IF EXISTS ensure_ga4_fallback_candidate_partition(date);

COMMIT;
