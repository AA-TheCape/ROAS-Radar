BEGIN;

CREATE TABLE IF NOT EXISTS ad_exposure_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key text NOT NULL,
  tenant_id text,
  workspace_id text,
  source_platform text NOT NULL,
  exposure_type text NOT NULL,
  occurred_at timestamptz NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  received_at timestamptz NOT NULL DEFAULT now(),
  identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL,
  identity_resolution_status text NOT NULL,
  identity_resolution_reason text,
  roas_radar_session_id uuid,
  shopify_customer_id text,
  hashed_email text,
  phone_hash text,
  checkout_token text,
  cart_token text,
  account_id text,
  account_name text,
  campaign_id text,
  campaign_name text,
  adset_id text,
  adset_name text,
  ad_id text,
  ad_name text,
  creative_id text,
  creative_name text,
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  content text NOT NULL DEFAULT 'unknown',
  term text NOT NULL DEFAULT 'unknown',
  validity_status text NOT NULL DEFAULT 'valid',
  invalid_reason text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ad_exposure_events_idempotency_key_chk
    CHECK (NULLIF(btrim(idempotency_key), '') IS NOT NULL),
  CONSTRAINT ad_exposure_events_source_platform_chk
    CHECK (source_platform IN ('meta_ads', 'google_ads', 'tiktok_ads', 'pinterest_ads', 'snapchat_ads', 'unknown')),
  CONSTRAINT ad_exposure_events_exposure_type_chk
    CHECK (exposure_type IN ('impression', 'view')),
  CONSTRAINT ad_exposure_events_identity_resolution_status_chk
    CHECK (identity_resolution_status IN ('resolved', 'unresolved', 'skipped', 'conflict')),
  CONSTRAINT ad_exposure_events_validity_status_chk
    CHECK (validity_status IN ('valid', 'invalid')),
  CONSTRAINT ad_exposure_events_invalid_reason_chk
    CHECK ((validity_status = 'valid' AND invalid_reason IS NULL) OR (validity_status = 'invalid' AND invalid_reason IS NOT NULL)),
  CONSTRAINT ad_exposure_events_hashed_email_chk
    CHECK (hashed_email IS NULL OR char_length(hashed_email) = 64),
  CONSTRAINT ad_exposure_events_phone_hash_chk
    CHECK (phone_hash IS NULL OR char_length(phone_hash) = 64)
);

CREATE UNIQUE INDEX IF NOT EXISTS ad_exposure_events_idempotency_uidx
  ON ad_exposure_events (idempotency_key);

CREATE INDEX IF NOT EXISTS ad_exposure_events_identity_view_lookback_idx
  ON ad_exposure_events (identity_journey_id, occurred_at DESC)
  WHERE identity_journey_id IS NOT NULL AND validity_status = 'valid';

CREATE INDEX IF NOT EXISTS ad_exposure_events_campaign_join_idx
  ON ad_exposure_events (
    source_platform,
    account_id,
    campaign_id,
    adset_id,
    ad_id,
    creative_id,
    occurred_at DESC
  )
  WHERE validity_status = 'valid';

CREATE INDEX IF NOT EXISTS ad_exposure_events_coverage_idx
  ON ad_exposure_events (occurred_at DESC, source_platform, exposure_type, validity_status, identity_resolution_status);

CREATE INDEX IF NOT EXISTS ad_exposure_events_taxonomy_idx
  ON ad_exposure_events (source, medium, campaign, content, term, occurred_at DESC)
  WHERE validity_status = 'valid';

COMMIT;
