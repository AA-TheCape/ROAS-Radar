BEGIN;

DROP INDEX IF EXISTS meta_ads_metadata_cache_lookup_failure_idx;
DROP INDEX IF EXISTS meta_ads_metadata_cache_freshness_idx;
DROP INDEX IF EXISTS meta_ads_metadata_cache_account_type_idx;

DROP TABLE IF EXISTS meta_ads_metadata_cache;

COMMIT;
