BEGIN;

ALTER TABLE IF EXISTS meta_ads_order_value_aggregates
  DROP COLUMN IF EXISTS raw_revenue_record_ids;

DROP TABLE IF EXISTS meta_ads_order_value_raw_records;
DROP TABLE IF EXISTS meta_ads_order_value_sync_runs;

COMMIT;
