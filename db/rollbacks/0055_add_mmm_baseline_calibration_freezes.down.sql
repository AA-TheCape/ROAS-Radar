BEGIN;

DROP TRIGGER IF EXISTS mmm_baseline_freeze_no_delete ON mmm_baseline_calibration_freezes;
DROP TRIGGER IF EXISTS mmm_baseline_freeze_no_update ON mmm_baseline_calibration_freezes;
DROP FUNCTION IF EXISTS prevent_mmm_baseline_freeze_mutation();

ALTER TABLE mmm_model_runs
  DROP COLUMN IF EXISTS approved_freeze_id;

DROP TABLE IF EXISTS mmm_baseline_calibration_freezes;

COMMIT;
