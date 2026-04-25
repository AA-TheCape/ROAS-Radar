BEGIN;

ALTER TABLE session_attribution_touch_events
  ADD COLUMN IF NOT EXISTS ingestion_fingerprint text;

CREATE UNIQUE INDEX IF NOT EXISTS session_attribution_touch_events_ingestion_fingerprint_uidx
  ON session_attribution_touch_events (ingestion_fingerprint)
  WHERE ingestion_fingerprint IS NOT NULL;

COMMIT;
