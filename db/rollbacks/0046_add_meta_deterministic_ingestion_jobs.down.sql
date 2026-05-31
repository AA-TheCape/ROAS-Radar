BEGIN;

DROP INDEX IF EXISTS meta_ads_connections_deterministic_enabled_idx;
DROP TABLE IF EXISTS meta_ads_deterministic_sync_jobs;
DROP TABLE IF EXISTS meta_ads_deterministic_sync_checkpoints;

ALTER TABLE meta_ads_connections
  DROP COLUMN IF EXISTS deterministic_view_impression_last_planned_for,
  DROP COLUMN IF EXISTS deterministic_view_impression_sync_enabled;

COMMIT;
