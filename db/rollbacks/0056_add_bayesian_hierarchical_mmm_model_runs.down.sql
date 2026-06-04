BEGIN;

ALTER TABLE mmm_model_runs
  DROP CONSTRAINT IF EXISTS mmm_model_runs_model_type_version_check;

ALTER TABLE mmm_model_runs
  ADD CONSTRAINT mmm_model_runs_model_type_check
  CHECK (model_type = 'baseline_linear_mmm'),
  ADD CONSTRAINT mmm_model_runs_model_version_check
  CHECK (model_version = 'baseline_linear_mmm_v1');

COMMIT;
