BEGIN;

DROP TABLE IF EXISTS recovery_job_completion_reports;

DROP INDEX IF EXISTS recovery_job_runs_dead_lettered_idx;
DROP INDEX IF EXISTS recovery_job_runs_running_heartbeat_idx;
DROP INDEX IF EXISTS recovery_job_runs_queue_claim_idx;

ALTER TABLE recovery_job_status_events
  DROP CONSTRAINT IF EXISTS recovery_job_status_events_from_status_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_status_events_to_status_chk;

ALTER TABLE recovery_job_status_events
  ADD CONSTRAINT recovery_job_status_events_from_status_chk
    CHECK (from_status IS NULL OR from_status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled')),
  ADD CONSTRAINT recovery_job_status_events_to_status_chk
    CHECK (to_status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled'));

ALTER TABLE recovery_job_runs
  DROP CONSTRAINT IF EXISTS recovery_job_runs_dead_letter_time_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_runs_attempts_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_runs_queue_priority_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_runs_terminal_time_chk,
  DROP CONSTRAINT IF EXISTS recovery_job_runs_status_chk;

ALTER TABLE recovery_job_runs
  DROP COLUMN IF EXISTS completion_report,
  DROP COLUMN IF EXISTS last_error_details,
  DROP COLUMN IF EXISTS dead_lettered_at,
  DROP COLUMN IF EXISTS lock_expires_at,
  DROP COLUMN IF EXISTS heartbeat_timeout_seconds,
  DROP COLUMN IF EXISTS max_attempts,
  DROP COLUMN IF EXISTS attempt_count,
  DROP COLUMN IF EXISTS available_at,
  DROP COLUMN IF EXISTS priority;

ALTER TABLE recovery_job_runs
  ADD CONSTRAINT recovery_job_runs_status_chk
    CHECK (status IN ('queued', 'running', 'succeeded', 'partial_failure', 'failed', 'cancelled')),
  ADD CONSTRAINT recovery_job_runs_terminal_time_chk
    CHECK (
      completed_at IS NULL
      OR (
        status IN ('succeeded', 'partial_failure', 'failed', 'cancelled')
        AND started_at IS NOT NULL
        AND completed_at >= started_at
      )
    );

COMMIT;
