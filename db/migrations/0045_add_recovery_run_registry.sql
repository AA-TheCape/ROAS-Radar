BEGIN;

CREATE TABLE IF NOT EXISTS recovery_job_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_type text NOT NULL,
  status text NOT NULL DEFAULT 'queued',
  mode text NOT NULL DEFAULT 'manual',
  initiated_by text NOT NULL,
  dry_run boolean NOT NULL DEFAULT false,
  time_range_start timestamptz NOT NULL,
  time_range_end timestamptz NOT NULL,
  idempotency_key text NOT NULL,
  concurrency_key text NOT NULL,
  scope_key text NOT NULL DEFAULT 'global',
  resume_from_run_id uuid REFERENCES recovery_job_runs(id) ON DELETE SET NULL,
  rerun_of_run_id uuid REFERENCES recovery_job_runs(id) ON DELETE SET NULL,
  input_parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  records_discovered integer NOT NULL DEFAULT 0,
  records_claimed integer NOT NULL DEFAULT 0,
  records_processed integer NOT NULL DEFAULT 0,
  records_succeeded integer NOT NULL DEFAULT 0,
  records_failed integer NOT NULL DEFAULT 0,
  records_skipped integer NOT NULL DEFAULT 0,
  records_retried integer NOT NULL DEFAULT 0,
  side_effects_attempted integer NOT NULL DEFAULT 0,
  side_effects_succeeded integer NOT NULL DEFAULT 0,
  side_effects_suppressed integer NOT NULL DEFAULT 0,
  claimed_by text,
  queued_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  completed_at timestamptz,
  last_heartbeat_at timestamptz,
  error_code text,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT recovery_job_runs_status_chk
    CHECK (status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled')),
  CONSTRAINT recovery_job_runs_mode_chk
    CHECK (mode IN ('manual', 'scheduled', 'automatic')),
  CONSTRAINT recovery_job_runs_time_range_chk
    CHECK (time_range_end >= time_range_start),
  CONSTRAINT recovery_job_runs_terminal_time_chk
    CHECK (
      completed_at IS NULL
      OR (
        status IN ('succeeded', 'partial_failure', 'failed', 'cancelled')
        AND started_at IS NOT NULL
        AND completed_at >= started_at
      )
    ),
  CONSTRAINT recovery_job_runs_error_pair_chk
    CHECK (
      (error_code IS NULL AND error_message IS NULL)
      OR (error_code IS NOT NULL AND error_message IS NOT NULL)
    ),
  CONSTRAINT recovery_job_runs_job_type_length_chk
    CHECK (char_length(job_type) BETWEEN 1 AND 128),
  CONSTRAINT recovery_job_runs_initiated_by_length_chk
    CHECK (char_length(initiated_by) BETWEEN 1 AND 255),
  CONSTRAINT recovery_job_runs_idempotency_key_length_chk
    CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
  CONSTRAINT recovery_job_runs_concurrency_key_length_chk
    CHECK (char_length(concurrency_key) BETWEEN 1 AND 255),
  CONSTRAINT recovery_job_runs_scope_key_length_chk
    CHECK (char_length(scope_key) BETWEEN 1 AND 255),
  CONSTRAINT recovery_job_runs_claimed_by_length_chk
    CHECK (claimed_by IS NULL OR char_length(claimed_by) <= 255),
  CONSTRAINT recovery_job_runs_error_code_length_chk
    CHECK (error_code IS NULL OR char_length(error_code) <= 128),
  CONSTRAINT recovery_job_runs_error_message_length_chk
    CHECK (error_message IS NULL OR char_length(error_message) <= 2048),
  CONSTRAINT recovery_job_runs_counters_nonnegative_chk
    CHECK (
      records_discovered >= 0
      AND records_claimed >= 0
      AND records_processed >= 0
      AND records_succeeded >= 0
      AND records_failed >= 0
      AND records_skipped >= 0
      AND records_retried >= 0
      AND side_effects_attempted >= 0
      AND side_effects_succeeded >= 0
      AND side_effects_suppressed >= 0
    )
);

CREATE UNIQUE INDEX recovery_job_runs_idempotency_key_idx
  ON recovery_job_runs (idempotency_key);

CREATE UNIQUE INDEX recovery_job_runs_active_concurrency_idx
  ON recovery_job_runs (job_type, concurrency_key)
  WHERE status IN ('queued', 'running');

CREATE INDEX recovery_job_runs_created_idx
  ON recovery_job_runs (created_at DESC);

CREATE INDEX recovery_job_runs_type_created_idx
  ON recovery_job_runs (job_type, created_at DESC);

CREATE INDEX recovery_job_runs_status_created_idx
  ON recovery_job_runs (status, created_at DESC);

CREATE INDEX recovery_job_runs_initiated_created_idx
  ON recovery_job_runs (initiated_by, created_at DESC);

CREATE INDEX recovery_job_runs_window_idx
  ON recovery_job_runs (job_type, time_range_start, time_range_end, created_at DESC);

CREATE INDEX recovery_job_runs_resume_lookup_idx
  ON recovery_job_runs (job_type, scope_key, status, last_heartbeat_at);

CREATE TABLE IF NOT EXISTS recovery_job_records (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES recovery_job_runs(id) ON DELETE CASCADE,
  job_type text NOT NULL,
  record_type text NOT NULL,
  record_key text NOT NULL,
  source_fingerprint text,
  side_effect_key text,
  processing_status text NOT NULL DEFAULT 'queued',
  attempt_count integer NOT NULL DEFAULT 0,
  last_attempt_at timestamptz,
  next_attempt_at timestamptz,
  locked_by text,
  locked_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  status_updated_at timestamptz NOT NULL DEFAULT now(),
  result jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_error_code text,
  last_error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT recovery_job_records_status_chk
    CHECK (processing_status IN ('queued', 'processing', 'succeeded', 'failed', 'skipped', 'retry_pending')),
  CONSTRAINT recovery_job_records_job_type_length_chk
    CHECK (char_length(job_type) BETWEEN 1 AND 128),
  CONSTRAINT recovery_job_records_record_type_length_chk
    CHECK (char_length(record_type) BETWEEN 1 AND 128),
  CONSTRAINT recovery_job_records_record_key_length_chk
    CHECK (char_length(record_key) BETWEEN 1 AND 512),
  CONSTRAINT recovery_job_records_fingerprint_length_chk
    CHECK (source_fingerprint IS NULL OR char_length(source_fingerprint) <= 128),
  CONSTRAINT recovery_job_records_side_effect_key_length_chk
    CHECK (side_effect_key IS NULL OR char_length(side_effect_key) <= 255),
  CONSTRAINT recovery_job_records_attempt_count_chk
    CHECK (attempt_count >= 0),
  CONSTRAINT recovery_job_records_completion_time_chk
    CHECK (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at),
  CONSTRAINT recovery_job_records_last_error_pair_chk
    CHECK (
      (last_error_code IS NULL AND last_error_message IS NULL)
      OR (last_error_code IS NOT NULL AND last_error_message IS NOT NULL)
    ),
  CONSTRAINT recovery_job_records_last_error_code_length_chk
    CHECK (last_error_code IS NULL OR char_length(last_error_code) <= 128),
  CONSTRAINT recovery_job_records_last_error_message_length_chk
    CHECK (last_error_message IS NULL OR char_length(last_error_message) <= 2048)
);

CREATE UNIQUE INDEX recovery_job_records_run_record_idx
  ON recovery_job_records (run_id, record_type, record_key);

CREATE UNIQUE INDEX recovery_job_records_side_effect_key_idx
  ON recovery_job_records (side_effect_key)
  WHERE side_effect_key IS NOT NULL;

CREATE INDEX recovery_job_records_run_status_idx
  ON recovery_job_records (run_id, processing_status, status_updated_at DESC);

CREATE INDEX recovery_job_records_type_status_idx
  ON recovery_job_records (job_type, record_type, processing_status, status_updated_at DESC);

CREATE INDEX recovery_job_records_created_idx
  ON recovery_job_records (created_at DESC);

CREATE INDEX recovery_job_records_retry_idx
  ON recovery_job_records (processing_status, next_attempt_at)
  WHERE processing_status = 'retry_pending';

CREATE TABLE IF NOT EXISTS recovery_job_checkpoints (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES recovery_job_runs(id) ON DELETE CASCADE,
  checkpoint_name text NOT NULL,
  sequence_number bigint NOT NULL DEFAULT 0,
  cursor_value jsonb NOT NULL DEFAULT '{}'::jsonb,
  high_watermark_at timestamptz,
  processed_through_at timestamptz,
  records_processed integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT recovery_job_checkpoints_name_length_chk
    CHECK (char_length(checkpoint_name) BETWEEN 1 AND 128),
  CONSTRAINT recovery_job_checkpoints_sequence_chk
    CHECK (sequence_number >= 0),
  CONSTRAINT recovery_job_checkpoints_records_processed_chk
    CHECK (records_processed >= 0)
);

CREATE UNIQUE INDEX recovery_job_checkpoints_run_name_idx
  ON recovery_job_checkpoints (run_id, checkpoint_name);

CREATE INDEX recovery_job_checkpoints_updated_idx
  ON recovery_job_checkpoints (updated_at DESC);

CREATE TABLE IF NOT EXISTS recovery_job_errors (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES recovery_job_runs(id) ON DELETE CASCADE,
  record_status_id bigint REFERENCES recovery_job_records(id) ON DELETE SET NULL,
  job_type text NOT NULL,
  record_type text,
  record_key text,
  severity text NOT NULL DEFAULT 'error',
  error_code text NOT NULL,
  error_message text NOT NULL,
  error_details jsonb NOT NULL DEFAULT '{}'::jsonb,
  retryable boolean NOT NULL DEFAULT false,
  occurred_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT recovery_job_errors_severity_chk
    CHECK (severity IN ('warning', 'error', 'fatal')),
  CONSTRAINT recovery_job_errors_job_type_length_chk
    CHECK (char_length(job_type) BETWEEN 1 AND 128),
  CONSTRAINT recovery_job_errors_record_type_length_chk
    CHECK (record_type IS NULL OR char_length(record_type) <= 128),
  CONSTRAINT recovery_job_errors_record_key_length_chk
    CHECK (record_key IS NULL OR char_length(record_key) <= 512),
  CONSTRAINT recovery_job_errors_code_length_chk
    CHECK (char_length(error_code) BETWEEN 1 AND 128),
  CONSTRAINT recovery_job_errors_message_length_chk
    CHECK (char_length(error_message) BETWEEN 1 AND 2048)
);

CREATE INDEX recovery_job_errors_run_occurred_idx
  ON recovery_job_errors (run_id, occurred_at DESC);

CREATE INDEX recovery_job_errors_type_occurred_idx
  ON recovery_job_errors (job_type, occurred_at DESC);

CREATE INDEX recovery_job_errors_record_idx
  ON recovery_job_errors (job_type, record_type, record_key, occurred_at DESC);

CREATE TABLE IF NOT EXISTS recovery_job_status_events (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES recovery_job_runs(id) ON DELETE CASCADE,
  from_status text,
  to_status text NOT NULL,
  changed_by text NOT NULL,
  reason text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  changed_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT recovery_job_status_events_from_status_chk
    CHECK (from_status IS NULL OR from_status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled')),
  CONSTRAINT recovery_job_status_events_to_status_chk
    CHECK (to_status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled')),
  CONSTRAINT recovery_job_status_events_changed_by_length_chk
    CHECK (char_length(changed_by) BETWEEN 1 AND 255),
  CONSTRAINT recovery_job_status_events_reason_length_chk
    CHECK (reason IS NULL OR char_length(reason) <= 1024),
  CONSTRAINT recovery_job_status_events_transition_chk
    CHECK (from_status IS NULL OR from_status <> to_status)
);

CREATE INDEX recovery_job_status_events_run_changed_idx
  ON recovery_job_status_events (run_id, changed_at DESC);

CREATE INDEX recovery_job_status_events_changed_idx
  ON recovery_job_status_events (changed_at DESC);

COMMIT;
