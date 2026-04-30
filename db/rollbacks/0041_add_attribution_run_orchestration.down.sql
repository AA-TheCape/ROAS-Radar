BEGIN;

DROP INDEX IF EXISTS attribution_runs_heartbeat_idx;
DROP INDEX IF EXISTS attribution_runs_scope_window_idx;
DROP INDEX IF EXISTS attribution_runs_active_concurrency_idx;
DROP INDEX IF EXISTS attribution_runs_idempotency_key_idx;

ALTER TABLE attribution_runs
  DROP CONSTRAINT IF EXISTS attribution_runs_window_bounds_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_config_hash_length_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_snapshot_hash_length_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_batch_size_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_submitted_by_length_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_idempotency_key_length_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_concurrency_key_length_check,
  DROP CONSTRAINT IF EXISTS attribution_runs_scope_key_length_check;

ALTER TABLE attribution_runs
  DROP COLUMN IF EXISTS resumed_from_run_id,
  DROP COLUMN IF EXISTS last_heartbeat_at,
  DROP COLUMN IF EXISTS claimed_by,
  DROP COLUMN IF EXISTS error_message,
  DROP COLUMN IF EXISTS error_code,
  DROP COLUMN IF EXISTS report,
  DROP COLUMN IF EXISTS progress,
  DROP COLUMN IF EXISTS run_config_hash,
  DROP COLUMN IF EXISTS input_snapshot_hash,
  DROP COLUMN IF EXISTS input_snapshot,
  DROP COLUMN IF EXISTS batch_size,
  DROP COLUMN IF EXISTS window_end_utc,
  DROP COLUMN IF EXISTS window_start_utc,
  DROP COLUMN IF EXISTS submitted_by,
  DROP COLUMN IF EXISTS idempotency_key,
  DROP COLUMN IF EXISTS concurrency_key,
  DROP COLUMN IF EXISTS scope_key;

COMMIT;
