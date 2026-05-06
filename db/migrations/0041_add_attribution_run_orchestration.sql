BEGIN;

ALTER TABLE attribution_runs
  ADD COLUMN scope_key text NOT NULL DEFAULT 'global',
  ADD COLUMN concurrency_key text NOT NULL DEFAULT 'global',
  ADD COLUMN idempotency_key text,
  ADD COLUMN submitted_by text,
  ADD COLUMN window_start_utc timestamptz,
  ADD COLUMN window_end_utc timestamptz,
  ADD COLUMN batch_size integer NOT NULL DEFAULT 100,
  ADD COLUMN input_snapshot jsonb NOT NULL DEFAULT '{"orderIds":[]}'::jsonb,
  ADD COLUMN input_snapshot_hash text,
  ADD COLUMN run_config_hash text,
  ADD COLUMN progress jsonb NOT NULL DEFAULT '{"processedOrders":0,"succeededOrders":0,"failedOrders":0,"retryOrderIds":[],"lastProcessedOrderId":null,"cursor":{"offset":0,"completed":false,"batchesProcessed":0}}'::jsonb,
  ADD COLUMN report jsonb,
  ADD COLUMN error_code text,
  ADD COLUMN error_message text,
  ADD COLUMN claimed_by text,
  ADD COLUMN last_heartbeat_at timestamptz,
  ADD COLUMN resumed_from_run_id uuid REFERENCES attribution_runs(id) ON DELETE SET NULL;

UPDATE attribution_runs
SET
  idempotency_key = COALESCE(idempotency_key, id::text),
  submitted_by = COALESCE(submitted_by, run_metadata->>'submittedBy', trigger_source),
  input_snapshot_hash = COALESCE(input_snapshot_hash, encode(digest(id::text, 'sha256'), 'hex')),
  run_config_hash = COALESCE(run_config_hash, encode(digest((trigger_source || ':' || scope_key || ':' || concurrency_key), 'sha256'), 'hex'))
WHERE idempotency_key IS NULL
   OR submitted_by IS NULL
   OR input_snapshot_hash IS NULL
   OR run_config_hash IS NULL;

ALTER TABLE attribution_runs
  ALTER COLUMN idempotency_key SET NOT NULL,
  ALTER COLUMN submitted_by SET NOT NULL,
  ALTER COLUMN input_snapshot_hash SET NOT NULL,
  ALTER COLUMN run_config_hash SET NOT NULL;

ALTER TABLE attribution_runs
  ADD CONSTRAINT attribution_runs_scope_key_length_check
    CHECK (char_length(scope_key) BETWEEN 1 AND 255),
  ADD CONSTRAINT attribution_runs_concurrency_key_length_check
    CHECK (char_length(concurrency_key) BETWEEN 1 AND 255),
  ADD CONSTRAINT attribution_runs_idempotency_key_length_check
    CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
  ADD CONSTRAINT attribution_runs_submitted_by_length_check
    CHECK (char_length(submitted_by) BETWEEN 1 AND 255),
  ADD CONSTRAINT attribution_runs_batch_size_check
    CHECK (batch_size BETWEEN 1 AND 5000),
  ADD CONSTRAINT attribution_runs_snapshot_hash_length_check
    CHECK (char_length(input_snapshot_hash) = 64),
  ADD CONSTRAINT attribution_runs_config_hash_length_check
    CHECK (char_length(run_config_hash) = 64),
  ADD CONSTRAINT attribution_runs_window_bounds_check
    CHECK (
      window_start_utc IS NULL
      OR window_end_utc IS NULL
      OR window_end_utc >= window_start_utc
    );

CREATE UNIQUE INDEX attribution_runs_idempotency_key_idx
  ON attribution_runs (idempotency_key);

CREATE UNIQUE INDEX attribution_runs_active_concurrency_idx
  ON attribution_runs (concurrency_key)
  WHERE run_status IN ('pending', 'running');

CREATE INDEX attribution_runs_scope_window_idx
  ON attribution_runs (scope_key, window_start_utc, window_end_utc, created_at_utc DESC);

CREATE INDEX attribution_runs_heartbeat_idx
  ON attribution_runs (run_status, last_heartbeat_at);

COMMIT;
