BEGIN;

CREATE TABLE IF NOT EXISTS ga4_bigquery_hourly_jobs (
  pipeline_name text NOT NULL,
  hour_start timestamptz NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  requested_by text,
  locked_at timestamptz,
  locked_by text,
  last_run_started_at timestamptz,
  last_run_completed_at timestamptz,
  last_error text,
  dead_lettered_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (pipeline_name, hour_start),
  CHECK (char_length(pipeline_name) <= 128),
  CHECK (requested_by IS NULL OR char_length(requested_by) <= 255),
  CHECK (locked_by IS NULL OR char_length(locked_by) <= 255),
  CHECK (status IN ('pending', 'processing', 'retry', 'completed', 'dead_lettered')),
  CHECK (attempts >= 0)
);

CREATE INDEX IF NOT EXISTS ga4_bigquery_hourly_jobs_available_idx
  ON ga4_bigquery_hourly_jobs (status, available_at, hour_start);

CREATE INDEX IF NOT EXISTS ga4_bigquery_hourly_jobs_dead_letter_idx
  ON ga4_bigquery_hourly_jobs (status, dead_lettered_at DESC, hour_start DESC);

COMMIT;
