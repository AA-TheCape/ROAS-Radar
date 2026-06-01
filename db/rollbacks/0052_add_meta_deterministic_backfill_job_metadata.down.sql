BEGIN;

DROP INDEX IF EXISTS meta_ads_deterministic_sync_jobs_requested_by_idx;

ALTER TABLE meta_ads_deterministic_sync_jobs
  DROP COLUMN IF EXISTS enqueue_worker_id,
  DROP COLUMN IF EXISTS requested_by;

COMMIT;
