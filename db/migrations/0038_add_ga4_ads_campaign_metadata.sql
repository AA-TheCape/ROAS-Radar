BEGIN;

ALTER TABLE ga4_session_attribution
  ADD COLUMN IF NOT EXISTS campaign_id text,
  ADD COLUMN IF NOT EXISTS account_id text,
  ADD COLUMN IF NOT EXISTS account_name text,
  ADD COLUMN IF NOT EXISTS channel_type text,
  ADD COLUMN IF NOT EXISTS channel_subtype text,
  ADD COLUMN IF NOT EXISTS campaign_metadata_source text NOT NULL DEFAULT 'unresolved',
  ADD COLUMN IF NOT EXISTS account_metadata_source text NOT NULL DEFAULT 'unresolved',
  ADD COLUMN IF NOT EXISTS channel_metadata_source text NOT NULL DEFAULT 'unresolved';

ALTER TABLE ga4_session_attribution
  ADD CONSTRAINT ga4_session_attribution_campaign_id_length_check
    CHECK (campaign_id IS NULL OR char_length(campaign_id) <= 255) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_account_id_length_check
    CHECK (account_id IS NULL OR char_length(account_id) <= 255) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_account_name_length_check
    CHECK (account_name IS NULL OR char_length(account_name) <= 255) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_channel_type_length_check
    CHECK (channel_type IS NULL OR char_length(channel_type) <= 128) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_channel_subtype_length_check
    CHECK (channel_subtype IS NULL OR char_length(channel_subtype) <= 128) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_campaign_metadata_source_check
    CHECK (campaign_metadata_source IN ('ga4_raw', 'google_ads_transfer', 'unresolved')) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_account_metadata_source_check
    CHECK (account_metadata_source IN ('ga4_raw', 'google_ads_transfer', 'unresolved')) NOT VALID,
  ADD CONSTRAINT ga4_session_attribution_channel_metadata_source_check
    CHECK (channel_metadata_source IN ('ga4_raw', 'google_ads_transfer', 'unresolved')) NOT VALID;

CREATE INDEX IF NOT EXISTS ga4_session_attribution_campaign_id_idx
  ON ga4_session_attribution (campaign_id)
  WHERE campaign_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ga4_session_attribution_account_id_idx
  ON ga4_session_attribution (account_id)
  WHERE account_id IS NOT NULL;

COMMIT;
