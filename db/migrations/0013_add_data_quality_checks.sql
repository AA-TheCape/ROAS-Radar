BEGIN;

CREATE TABLE data_quality_check_runs (
  id bigserial PRIMARY KEY,
  run_date date NOT NULL,
  check_key text NOT NULL,
  status text NOT NULL DEFAULT 'healthy',
  severity text NOT NULL DEFAULT 'info',
  discrepancy_count integer NOT NULL DEFAULT 0,
  summary text NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  checked_at timestamptz NOT NULL DEFAULT now(),
  alert_emitted_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_date, check_key),
  CHECK (status IN ('healthy', 'warning', 'failed')),
  CHECK (severity IN ('info', 'warning', 'critical'))
);

CREATE INDEX data_quality_check_runs_run_date_idx
  ON data_quality_check_runs (run_date DESC, checked_at DESC);

CREATE INDEX data_quality_check_runs_status_idx
  ON data_quality_check_runs (status, severity, checked_at DESC);

COMMIT;
