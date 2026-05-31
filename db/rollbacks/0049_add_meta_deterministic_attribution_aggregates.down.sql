BEGIN;

DROP TABLE IF EXISTS meta_ads_deterministic_attribution_aggregates;

ALTER TABLE raw_deterministic_events
  DROP CONSTRAINT IF EXISTS raw_deterministic_events_id_source_uidx;

COMMIT;
