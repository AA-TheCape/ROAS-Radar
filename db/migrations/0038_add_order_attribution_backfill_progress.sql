BEGIN;

ALTER TABLE order_attribution_backfill_runs
  ADD COLUMN IF NOT EXISTS progress jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS last_heartbeat_at timestamptz;

UPDATE order_attribution_backfill_runs
SET
  progress = '{}'::jsonb
WHERE progress IS NULL;

UPDATE order_attribution_backfill_runs
SET
  last_heartbeat_at = COALESCE(last_heartbeat_at, started_at, submitted_at, now())
WHERE last_heartbeat_at IS NULL;

CREATE INDEX IF NOT EXISTS order_attribution_backfill_runs_status_heartbeat_idx
  ON order_attribution_backfill_runs (status, last_heartbeat_at, submitted_at);

COMMIT;
