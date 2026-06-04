BEGIN;

CREATE TABLE mmm_baseline_calibration_freezes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  freeze_schema_version text NOT NULL DEFAULT 'mmm_baseline_calibration_freeze_v1',
  mart_version text NOT NULL DEFAULT 'mmm_weekly_channel_input_mart_v1',
  snapshot_version text NOT NULL DEFAULT 'mmm_weekly_channel_snapshot_v1',
  freeze_status text NOT NULL DEFAULT 'pending',
  generation_timestamp timestamptz NOT NULL DEFAULT now(),
  calibration_start_date date NOT NULL,
  calibration_end_date date NOT NULL,
  attribution_model text NOT NULL,
  row_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
  deterministic_attribution_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  freshness_metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  campaign_metadata_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  exposure_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  data_quality_checks jsonb NOT NULL DEFAULT '{}'::jsonb,
  aggregate_metric_totals jsonb NOT NULL DEFAULT '{}'::jsonb,
  evidence_hash text NOT NULL,
  snapshot_rows jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_by text NOT NULL DEFAULT 'system',
  approved_by text,
  approved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (freeze_schema_version = 'mmm_baseline_calibration_freeze_v1'),
  CHECK (mart_version = 'mmm_weekly_channel_input_mart_v1'),
  CHECK (snapshot_version = 'mmm_weekly_channel_snapshot_v1'),
  CHECK (freeze_status IN ('pending', 'approved', 'rejected')),
  CHECK (calibration_end_date >= calibration_start_date),
  CHECK (char_length(evidence_hash) = 64),
  CHECK (jsonb_typeof(snapshot_rows) = 'array'),
  CHECK (
    (freeze_status = 'approved' AND approved_by IS NOT NULL AND approved_at IS NOT NULL)
    OR (freeze_status <> 'approved' AND approved_at IS NULL)
  )
);

CREATE INDEX mmm_baseline_calibration_freezes_window_idx
  ON mmm_baseline_calibration_freezes (
    calibration_start_date DESC,
    calibration_end_date DESC,
    attribution_model,
    generation_timestamp DESC
  );

CREATE INDEX mmm_baseline_calibration_freezes_status_idx
  ON mmm_baseline_calibration_freezes (freeze_status, generation_timestamp DESC);

CREATE UNIQUE INDEX mmm_baseline_calibration_freezes_evidence_uidx
  ON mmm_baseline_calibration_freezes (evidence_hash, freeze_status);

ALTER TABLE mmm_model_runs
  ADD COLUMN approved_freeze_id uuid REFERENCES mmm_baseline_calibration_freezes(id);

CREATE INDEX mmm_model_runs_approved_freeze_idx
  ON mmm_model_runs (approved_freeze_id);

CREATE OR REPLACE FUNCTION prevent_mmm_baseline_freeze_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'mmm_baseline_calibration_freezes rows are immutable';
END;
$$;

CREATE TRIGGER mmm_baseline_freeze_no_update
  BEFORE UPDATE ON mmm_baseline_calibration_freezes
  FOR EACH ROW
  EXECUTE FUNCTION prevent_mmm_baseline_freeze_mutation();

CREATE TRIGGER mmm_baseline_freeze_no_delete
  BEFORE DELETE ON mmm_baseline_calibration_freezes
  FOR EACH ROW
  EXECUTE FUNCTION prevent_mmm_baseline_freeze_mutation();

COMMIT;
