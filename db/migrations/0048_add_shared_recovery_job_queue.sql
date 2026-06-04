BEGIN;

ALTER TABLE recovery_job_runs
  DROP CONSTRAINT IF EXISTS recovery_job_runs_status_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_runs_terminal_time_chk;

ALTER TABLE recovery_job_runs
  ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 100,
  ADD COLUMN IF NOT EXISTS available_at timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS max_attempts integer NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS heartbeat_timeout_seconds integer NOT NULL DEFAULT 300,
  ADD COLUMN IF NOT EXISTS lock_expires_at timestamptz,
  ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz,
  ADD COLUMN IF NOT EXISTS last_error_details jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS completion_report jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE recovery_job_runs
  ADD CONSTRAINT recovery_job_runs_status_chk
    CHECK (status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled', 'dead_lettered')),
  ADD CONSTRAINT recovery_job_runs_terminal_time_chk
    CHECK (
      completed_at IS NULL
      OR (
        status IN ('succeeded', 'partial_failure', 'failed', 'cancelled', 'dead_lettered')
        AND started_at IS NOT NULL
        AND completed_at >= started_at
      )
    ),
  ADD CONSTRAINT recovery_job_runs_queue_priority_chk
    CHECK (priority >= 0),
  ADD CONSTRAINT recovery_job_runs_attempts_chk
    CHECK (attempt_count >= 0 AND max_attempts > 0 AND heartbeat_timeout_seconds > 0),
  ADD CONSTRAINT recovery_job_runs_dead_letter_time_chk
    CHECK (dead_lettered_at IS NULL OR status = 'dead_lettered');

ALTER TABLE recovery_job_status_events
  DROP CONSTRAINT IF EXISTS recovery_job_status_events_from_status_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_status_events_to_status_chk;

ALTER TABLE recovery_job_status_events
  ADD CONSTRAINT recovery_job_status_events_from_status_chk
    CHECK (from_status IS NULL OR from_status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled', 'dead_lettered')),
  ADD CONSTRAINT recovery_job_status_events_to_status_chk
    CHECK (to_status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled', 'dead_lettered'));

CREATE INDEX IF NOT EXISTS recovery_job_runs_queue_claim_idx
  ON recovery_job_runs (priority ASC, available_at ASC, queued_at ASC, id ASC)
  WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS recovery_job_runs_running_heartbeat_idx
  ON recovery_job_runs (lock_expires_at ASC, last_heartbeat_at ASC)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS recovery_job_runs_dead_lettered_idx
  ON recovery_job_runs (dead_lettered_at DESC, job_type)
  WHERE status = 'dead_lettered';

CREATE TABLE IF NOT EXISTS recovery_job_completion_reports (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES recovery_job_runs(id) ON DELETE CASCADE,
  job_type text NOT NULL,
  status text NOT NULL,
  report jsonb NOT NULL DEFAULT '{}'::jsonb,
  counters jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT recovery_job_completion_reports_status_chk
    CHECK (status IN ('succeeded', 'partial_failure', 'failed', 'cancelled', 'dead_lettered')),
  CONSTRAINT recovery_job_completion_reports_job_type_length_chk
    CHECK (char_length(job_type) BETWEEN 1 AND 128)
);

CREATE UNIQUE INDEX IF NOT EXISTS recovery_job_completion_reports_run_idx
  ON recovery_job_completion_reports (run_id);

CREATE INDEX IF NOT EXISTS recovery_job_completion_reports_created_idx
  ON recovery_job_completion_reports (created_at DESC);

COMMIT;
