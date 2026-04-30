BEGIN;

DROP INDEX IF EXISTS ad_platform_entity_metadata_freshness_idx;
DROP INDEX IF EXISTS ad_platform_entity_metadata_entity_lookup_idx;
DROP INDEX IF EXISTS ad_platform_entity_metadata_lookup_idx;
DROP INDEX IF EXISTS ad_platform_entity_metadata_scope_key_uidx;

DROP TABLE IF EXISTS ad_platform_entity_metadata;

COMMIT;
