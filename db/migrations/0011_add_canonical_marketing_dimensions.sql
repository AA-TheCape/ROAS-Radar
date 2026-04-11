BEGIN;

ALTER TABLE meta_ads_daily_spend
  ADD COLUMN IF NOT EXISTS canonical_source text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_medium text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_campaign text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_content text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_term text NOT NULL DEFAULT 'unknown';

ALTER TABLE google_ads_daily_spend
  ADD COLUMN IF NOT EXISTS canonical_source text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_medium text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_campaign text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_content text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS canonical_term text NOT NULL DEFAULT 'unknown';

UPDATE meta_ads_daily_spend
SET
  canonical_source = 'meta',
  canonical_medium = 'paid_social',
  canonical_campaign = COALESCE(NULLIF(lower(regexp_replace(trim(campaign_name), '\s+', ' ', 'g')), ''), 'unknown'),
  canonical_content = COALESCE(
    NULLIF(lower(regexp_replace(trim(creative_name), '\s+', ' ', 'g')), ''),
    NULLIF(lower(regexp_replace(trim(ad_name), '\s+', ' ', 'g')), ''),
    'unknown'
  ),
  canonical_term = 'unknown';

UPDATE google_ads_daily_spend
SET
  canonical_source = 'google',
  canonical_medium = 'cpc',
  canonical_campaign = COALESCE(NULLIF(lower(regexp_replace(trim(campaign_name), '\s+', ' ', 'g')), ''), 'unknown'),
  canonical_content = COALESCE(
    NULLIF(lower(regexp_replace(trim(creative_name), '\s+', ' ', 'g')), ''),
    NULLIF(lower(regexp_replace(trim(ad_name), '\s+', ' ', 'g')), ''),
    'unknown'
  ),
  canonical_term = 'unknown';

CREATE INDEX IF NOT EXISTS meta_ads_daily_spend_canonical_dimensions_idx
  ON meta_ads_daily_spend (report_date DESC, canonical_source, canonical_medium, canonical_campaign, granularity);

CREATE INDEX IF NOT EXISTS google_ads_daily_spend_canonical_dimensions_idx
  ON google_ads_daily_spend (report_date DESC, canonical_source, canonical_medium, canonical_campaign, granularity);

COMMIT;
