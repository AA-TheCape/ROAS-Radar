BEGIN;

ALTER TABLE attribution_order_credits
  DROP COLUMN IF EXISTS confidence_label,
  DROP COLUMN IF EXISTS match_source;

ALTER TABLE attribution_results
  DROP COLUMN IF EXISTS ga4_session_id,
  DROP COLUMN IF EXISTS ga4_client_id,
  DROP COLUMN IF EXISTS confidence_label,
  DROP COLUMN IF EXISTS match_source;

COMMIT;
