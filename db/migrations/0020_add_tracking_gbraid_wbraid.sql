BEGIN;

ALTER TABLE tracking_sessions
  ADD COLUMN IF NOT EXISTS initial_gbraid text,
  ADD COLUMN IF NOT EXISTS initial_wbraid text;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS gbraid text,
  ADD COLUMN IF NOT EXISTS wbraid text;

COMMIT;
