BEGIN;

WITH referenced_jobs AS (
  SELECT connection_id, report_date AS sync_date
  FROM meta_ads_order_value_raw_records
  WHERE sync_job_id IS NOT NULL
  UNION
  SELECT meta_connection_id AS connection_id, report_date AS sync_date
  FROM meta_ads_order_value_aggregates
  WHERE sync_job_id IS NOT NULL
)
INSERT INTO meta_ads_order_value_sync_jobs (
  connection_id,
  sync_date,
  status,
  attempts,
  available_at,
  locked_at,
  locked_by,
  last_error,
  completed_at,
  created_at,
  updated_at
)
SELECT
  referenced_jobs.connection_id,
  referenced_jobs.sync_date,
  COALESCE(sync_jobs.status, 'completed'),
  COALESCE(sync_jobs.attempts, 0),
  COALESCE(sync_jobs.available_at, now()),
  sync_jobs.locked_at,
  sync_jobs.locked_by,
  sync_jobs.last_error,
  sync_jobs.completed_at,
  COALESCE(sync_jobs.created_at, now()),
  COALESCE(sync_jobs.updated_at, now())
FROM referenced_jobs
LEFT JOIN meta_ads_sync_jobs AS sync_jobs
  ON sync_jobs.connection_id = referenced_jobs.connection_id
 AND sync_jobs.sync_date = referenced_jobs.sync_date
ON CONFLICT (connection_id, sync_date) DO NOTHING;

UPDATE meta_ads_order_value_raw_records AS raw_records
SET sync_job_id = sync_jobs.id
FROM meta_ads_order_value_sync_jobs AS sync_jobs
WHERE raw_records.sync_job_id IS NOT NULL
  AND sync_jobs.connection_id = raw_records.connection_id
  AND sync_jobs.sync_date = raw_records.report_date
  AND raw_records.sync_job_id <> sync_jobs.id;

UPDATE meta_ads_order_value_aggregates AS aggregates
SET sync_job_id = sync_jobs.id
FROM meta_ads_order_value_sync_jobs AS sync_jobs
WHERE aggregates.sync_job_id IS NOT NULL
  AND sync_jobs.connection_id = aggregates.meta_connection_id
  AND sync_jobs.sync_date = aggregates.report_date
  AND aggregates.sync_job_id <> sync_jobs.id;

ALTER TABLE meta_ads_order_value_raw_records
  DROP CONSTRAINT IF EXISTS meta_ads_order_value_raw_records_sync_job_id_fkey;

ALTER TABLE meta_ads_order_value_raw_records
  ADD CONSTRAINT meta_ads_order_value_raw_records_sync_job_id_fkey
  FOREIGN KEY (sync_job_id)
  REFERENCES meta_ads_order_value_sync_jobs(id)
  ON DELETE SET NULL;

ALTER TABLE meta_ads_order_value_aggregates
  DROP CONSTRAINT IF EXISTS meta_ads_order_value_aggregates_sync_job_id_fkey;

ALTER TABLE meta_ads_order_value_aggregates
  ADD CONSTRAINT meta_ads_order_value_aggregates_sync_job_id_fkey
  FOREIGN KEY (sync_job_id)
  REFERENCES meta_ads_order_value_sync_jobs(id)
  ON DELETE CASCADE;

COMMIT;
