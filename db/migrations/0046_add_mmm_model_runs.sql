BEGIN;

CREATE TABLE mmm_model_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  model_type text NOT NULL,
  model_version text NOT NULL,
  mart_version text NOT NULL,
  attribution_model text NOT NULL,
  run_status text NOT NULL DEFAULT 'completed',
  training_start_date date NOT NULL,
  training_end_date date NOT NULL,
  holdout_start_date date,
  holdout_end_date date,
  run_config jsonb NOT NULL DEFAULT '{}'::jsonb,
  input_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  model_artifact jsonb NOT NULL DEFAULT '{}'::jsonb,
  calibration_report jsonb NOT NULL DEFAULT '{}'::jsonb,
  validation_report jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_code text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  CHECK (model_type = 'baseline_linear_mmm'),
  CHECK (model_version = 'baseline_linear_mmm_v1'),
  CHECK (mart_version = 'mmm_daily_input_mart_v1'),
  CHECK (run_status IN ('completed', 'failed')),
  CHECK (training_end_date >= training_start_date),
  CHECK (
    holdout_start_date IS NULL
    OR holdout_end_date IS NULL
    OR holdout_end_date >= holdout_start_date
  )
);

CREATE INDEX mmm_model_runs_window_idx
  ON mmm_model_runs (training_start_date DESC, training_end_date DESC, created_at DESC);

CREATE INDEX mmm_model_runs_attribution_model_idx
  ON mmm_model_runs (attribution_model, created_at DESC);

COMMIT;
