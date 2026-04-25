BEGIN;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS ingestion_source text NOT NULL DEFAULT 'browser';

CREATE INDEX IF NOT EXISTS tracking_events_session_ingestion_source_occurred_at_idx
  ON tracking_events (session_id, ingestion_source, occurred_at DESC);

COMMIT;
