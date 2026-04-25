CREATE TABLE order_attribution_backfill_runs (
  id text PRIMARY KEY,
  status text NOT NULL,
  submitted_at timestamptz NOT NULL DEFAULT now(),
  submitted_by text NOT NULL,
  started_at timestamptz,
  completed_at timestamptz,
  options jsonb NOT NULL DEFAULT '{}'::jsonb,
  report jsonb,
  error_code text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT order_attribution_backfill_runs_status_check
    CHECK (status IN ('queued', 'processing', 'completed', 'failed')),
  CONSTRAINT order_attribution_backfill_runs_error_check
    CHECK (
      (error_code IS NULL AND error_message IS NULL)
      OR (error_code IS NOT NULL AND error_message IS NOT NULL)
    )
);

CREATE INDEX order_attribution_backfill_runs_status_submitted_idx
  ON order_attribution_backfill_runs (status, submitted_at DESC);
