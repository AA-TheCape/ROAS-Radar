BEGIN;

ALTER TABLE meta_ads_deterministic_sync_jobs
  ADD COLUMN requested_by text,
  ADD COLUMN enqueue_worker_id text;

CREATE INDEX meta_ads_deterministic_sync_jobs_requested_by_idx
  ON meta_ads_deterministic_sync_jobs (requested_by, created_at DESC)
  WHERE requested_by IS NOT NULL;

COMMIT;
