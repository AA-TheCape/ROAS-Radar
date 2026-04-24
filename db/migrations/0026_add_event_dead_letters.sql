BEGIN;

ALTER TABLE attribution_jobs
  DROP CONSTRAINT IF EXISTS attribution_jobs_status_check;

ALTER TABLE attribution_jobs
  ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz;

ALTER TABLE attribution_jobs
  ADD CONSTRAINT attribution_jobs_status_check
  CHECK (status IN ('pending', 'processing', 'retry', 'completed', 'failed'));

CREATE INDEX IF NOT EXISTS attribution_jobs_dead_letter_idx
  ON attribution_jobs (dead_lettered_at DESC)
  WHERE status = 'failed';

CREATE TABLE IF NOT EXISTS event_dead_letters (
  id bigserial PRIMARY KEY,
  event_type text NOT NULL,
  source_table text NOT NULL,
  source_record_id text NOT NULL,
  source_queue_key text,
  status text NOT NULL DEFAULT 'pending_replay',
  first_failed_at timestamptz NOT NULL DEFAULT now(),
  last_failed_at timestamptz NOT NULL DEFAULT now(),
  last_error_message text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_context jsonb NOT NULL DEFAULT '{}'::jsonb,
  failure_count integer NOT NULL DEFAULT 1,
  replayed_at timestamptz,
  last_replay_run_id bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS event_dead_letters_source_uidx
  ON event_dead_letters (event_type, source_table, source_record_id);

CREATE TABLE IF NOT EXISTS event_replay_runs (...);
CREATE TABLE IF NOT EXISTS event_replay_run_items (...);

COMMIT;
