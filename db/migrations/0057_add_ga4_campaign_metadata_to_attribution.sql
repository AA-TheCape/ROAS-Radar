BEGIN;

ALTER TABLE ga4_fallback_candidates
  ADD COLUMN IF NOT EXISTS campaign_id text,
  ADD COLUMN IF NOT EXISTS account_id text,
  ADD COLUMN IF NOT EXISTS account_name text,
  ADD COLUMN IF NOT EXISTS channel_type text,
  ADD COLUMN IF NOT EXISTS channel_subtype text,
  ADD COLUMN IF NOT EXISTS campaign_metadata_source text,
  ADD COLUMN IF NOT EXISTS account_metadata_source text,
  ADD COLUMN IF NOT EXISTS channel_metadata_source text;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS attributed_campaign_id text,
  ADD COLUMN IF NOT EXISTS attributed_account_id text,
  ADD COLUMN IF NOT EXISTS attributed_account_name text,
  ADD COLUMN IF NOT EXISTS attributed_channel_type text,
  ADD COLUMN IF NOT EXISTS attributed_channel_subtype text,
  ADD COLUMN IF NOT EXISTS attributed_campaign_metadata_source text,
  ADD COLUMN IF NOT EXISTS attributed_account_metadata_source text,
  ADD COLUMN IF NOT EXISTS attributed_channel_metadata_source text;

ALTER TABLE attribution_order_credits
  ADD COLUMN IF NOT EXISTS attributed_campaign_id text,
  ADD COLUMN IF NOT EXISTS attributed_account_id text,
  ADD COLUMN IF NOT EXISTS attributed_account_name text,
  ADD COLUMN IF NOT EXISTS attributed_channel_type text,
  ADD COLUMN IF NOT EXISTS attributed_channel_subtype text,
  ADD COLUMN IF NOT EXISTS attributed_campaign_metadata_source text,
  ADD COLUMN IF NOT EXISTS attributed_account_metadata_source text,
  ADD COLUMN IF NOT EXISTS attributed_channel_metadata_source text;

ALTER TABLE ga4_fallback_candidates
  ADD CONSTRAINT ga4_fallback_candidates_campaign_id_length_check
    CHECK (campaign_id IS NULL OR char_length(campaign_id) <= 255) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_account_id_length_check
    CHECK (account_id IS NULL OR char_length(account_id) <= 255) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_account_name_length_check
    CHECK (account_name IS NULL OR char_length(account_name) <= 255) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_channel_type_length_check
    CHECK (channel_type IS NULL OR char_length(channel_type) <= 128) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_channel_subtype_length_check
    CHECK (channel_subtype IS NULL OR char_length(channel_subtype) <= 128) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_campaign_metadata_source_check
    CHECK (campaign_metadata_source IS NULL OR campaign_metadata_source IN ('ga4_raw', 'google_ads_transfer', 'unresolved')) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_account_metadata_source_check
    CHECK (account_metadata_source IS NULL OR account_metadata_source IN ('ga4_raw', 'google_ads_transfer', 'unresolved')) NOT VALID,
  ADD CONSTRAINT ga4_fallback_candidates_channel_metadata_source_check
    CHECK (channel_metadata_source IS NULL OR channel_metadata_source IN ('ga4_raw', 'google_ads_transfer', 'unresolved')) NOT VALID;

CREATE INDEX IF NOT EXISTS ga4_fallback_candidates_campaign_id_idx
  ON ga4_fallback_candidates (campaign_id)
  WHERE campaign_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS attribution_results_google_campaign_metadata_idx
  ON attribution_results (attributed_account_id, attributed_campaign_id)
  WHERE attributed_campaign_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS attribution_order_credits_google_campaign_metadata_idx
  ON attribution_order_credits (attribution_model, attributed_account_id, attributed_campaign_id)
  WHERE attributed_campaign_id IS NOT NULL;

COMMIT;
