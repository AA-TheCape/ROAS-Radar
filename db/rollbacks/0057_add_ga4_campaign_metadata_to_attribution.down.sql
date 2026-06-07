BEGIN;

DROP INDEX IF EXISTS attribution_order_credits_google_campaign_metadata_idx;
DROP INDEX IF EXISTS attribution_results_google_campaign_metadata_idx;
DROP INDEX IF EXISTS ga4_fallback_candidates_campaign_id_idx;

ALTER TABLE ga4_fallback_candidates
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_channel_metadata_source_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_account_metadata_source_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_campaign_metadata_source_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_channel_subtype_length_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_channel_type_length_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_account_name_length_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_account_id_length_check,
  DROP CONSTRAINT IF EXISTS ga4_fallback_candidates_campaign_id_length_check;

ALTER TABLE attribution_order_credits
  DROP COLUMN IF EXISTS attributed_channel_metadata_source,
  DROP COLUMN IF EXISTS attributed_account_metadata_source,
  DROP COLUMN IF EXISTS attributed_campaign_metadata_source,
  DROP COLUMN IF EXISTS attributed_channel_subtype,
  DROP COLUMN IF EXISTS attributed_channel_type,
  DROP COLUMN IF EXISTS attributed_account_name,
  DROP COLUMN IF EXISTS attributed_account_id,
  DROP COLUMN IF EXISTS attributed_campaign_id;

ALTER TABLE attribution_results
  DROP COLUMN IF EXISTS attributed_channel_metadata_source,
  DROP COLUMN IF EXISTS attributed_account_metadata_source,
  DROP COLUMN IF EXISTS attributed_campaign_metadata_source,
  DROP COLUMN IF EXISTS attributed_channel_subtype,
  DROP COLUMN IF EXISTS attributed_channel_type,
  DROP COLUMN IF EXISTS attributed_account_name,
  DROP COLUMN IF EXISTS attributed_account_id,
  DROP COLUMN IF EXISTS attributed_campaign_id;

ALTER TABLE ga4_fallback_candidates
  DROP COLUMN IF EXISTS channel_metadata_source,
  DROP COLUMN IF EXISTS account_metadata_source,
  DROP COLUMN IF EXISTS campaign_metadata_source,
  DROP COLUMN IF EXISTS channel_subtype,
  DROP COLUMN IF EXISTS channel_type,
  DROP COLUMN IF EXISTS account_name,
  DROP COLUMN IF EXISTS account_id,
  DROP COLUMN IF EXISTS campaign_id;

COMMIT;
