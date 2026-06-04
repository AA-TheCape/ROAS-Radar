BEGIN;

DROP TABLE IF EXISTS mmm_model_run_input_snapshots;
DROP TABLE IF EXISTS mmm_weekly_channel_input_mart_v1;

ALTER TABLE mmm_model_runs
  DROP CONSTRAINT IF EXISTS mmm_model_runs_mart_version_check;

ALTER TABLE mmm_model_runs
  ADD CONSTRAINT mmm_model_runs_mart_version_check
  CHECK (mart_version = 'mmm_daily_input_mart_v1');

COMMIT;
