BEGIN;

CREATE TABLE IF NOT EXISTS campaign_metadata_backfill_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  status text NOT NULL,
  requested_by text NOT NULL,
  worker_id text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  window_start date NOT NULL,
  window_end date NOT NULL,
  dry_run boolean NOT NULL DEFAULT false,
  report jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_code text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT campaign_metadata_backfill_runs_status_chk
    CHECK (status IN ('processing', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS campaign_metadata_backfill_runs_started_idx
  ON campaign_metadata_backfill_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS campaign_metadata_backfill_runs_window_idx
  ON campaign_metadata_backfill_runs (window_start, window_end, started_at DESC);

COMMIT;
