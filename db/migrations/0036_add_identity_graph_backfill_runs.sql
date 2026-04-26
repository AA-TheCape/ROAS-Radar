BEGIN;

CREATE TABLE IF NOT EXISTS identity_graph_backfill_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  status text NOT NULL DEFAULT 'processing',
  requested_by text NOT NULL,
  worker_id text,
  options jsonb NOT NULL DEFAULT '{}'::jsonb,
  checkpoints jsonb NOT NULL DEFAULT '{}'::jsonb,
  metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  reconciliation jsonb,
  report jsonb,
  error_code text,
  error_message text,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('processing', 'completed', 'failed')),
  CHECK (char_length(requested_by) <= 255),
  CHECK (worker_id IS NULL OR char_length(worker_id) <= 255),
  CHECK (error_code IS NULL OR char_length(error_code) <= 128),
  CHECK (error_message IS NULL OR char_length(error_message) <= 2048),
  CHECK (completed_at IS NULL OR completed_at >= started_at),
  CHECK (last_heartbeat_at >= started_at)
);

CREATE INDEX IF NOT EXISTS identity_graph_backfill_runs_status_updated_idx
  ON identity_graph_backfill_runs (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS identity_graph_backfill_runs_started_idx
  ON identity_graph_backfill_runs (started_at DESC);

COMMIT;
