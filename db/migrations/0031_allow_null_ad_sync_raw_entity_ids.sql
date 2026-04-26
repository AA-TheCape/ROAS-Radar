BEGIN;

ALTER TABLE meta_ads_raw_spend_records
  ALTER COLUMN entity_id DROP NOT NULL;

ALTER TABLE google_ads_raw_spend_records
  ALTER COLUMN entity_id DROP NOT NULL;

COMMIT;
