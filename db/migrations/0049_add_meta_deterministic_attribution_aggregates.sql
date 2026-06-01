BEGIN;

-- Contract reference: docs/meta-deterministic-view-attribution-contract-v1.md
ALTER TABLE raw_deterministic_events
  ADD CONSTRAINT raw_deterministic_events_id_source_uidx UNIQUE (id, source_id);

CREATE TABLE meta_ads_deterministic_attribution_aggregates (
  id bigserial PRIMARY KEY,
  organization_id bigint NOT NULL,
  meta_connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  source_id bigint NOT NULL REFERENCES deterministic_event_sources(id) ON DELETE RESTRICT,
  raw_event_id bigint NOT NULL REFERENCES raw_deterministic_events(id) ON DELETE CASCADE,
  fact_id bigint REFERENCES deterministic_event_facts(id) ON DELETE SET NULL,
  platform text NOT NULL DEFAULT 'meta_ads',
  ad_account_id text NOT NULL,
  report_date date NOT NULL,
  campaign_id text,
  campaign_name text,
  adset_id text,
  adset_name text,
  ad_id text,
  ad_name text,
  event_type text NOT NULL,
  attribution_family text NOT NULL,
  attribution_window text NOT NULL DEFAULT '7d_view',
  attribution_window_days integer NOT NULL DEFAULT 7,
  aggregate_count bigint NOT NULL DEFAULT 0,
  evidence_origin text NOT NULL DEFAULT 'api',
  platform_verified boolean NOT NULL DEFAULT true,
  verification_status text NOT NULL DEFAULT 'verified',
  verified_by_source_id bigint REFERENCES deterministic_event_sources(id) ON DELETE RESTRICT,
  verified_at_utc timestamptz,
  raw_record_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_platform_chk
    CHECK (platform = 'meta_ads'),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_account_chk
    CHECK (NULLIF(btrim(ad_account_id), '') IS NOT NULL),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_entity_present_chk
    CHECK (campaign_id IS NOT NULL OR ad_id IS NOT NULL),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_event_type_chk
    CHECK (event_type IN ('impression', 'view')),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_family_chk
    CHECK (attribution_family IN ('deterministic_views', 'deterministic_impressions')),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_family_event_chk
    CHECK (
      (attribution_family = 'deterministic_views' AND event_type = 'view')
      OR (attribution_family = 'deterministic_impressions' AND event_type = 'impression')
    ),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_window_chk
    CHECK (attribution_window = '7d_view' AND attribution_window_days = 7),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_count_chk
    CHECK (aggregate_count >= 0),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_evidence_origin_chk
    CHECK (evidence_origin = 'api'),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_verification_status_chk
    CHECK (verification_status IN ('pending', 'verified', 'failed', 'superseded')),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_verified_shape_chk
    CHECK (
      (
        platform_verified = true
        AND verification_status = 'verified'
        AND verified_by_source_id IS NOT NULL
        AND verified_at_utc IS NOT NULL
      )
      OR platform_verified = false
    ),
  CONSTRAINT meta_ads_deterministic_attribution_aggregates_name_lengths_chk
    CHECK (
      (campaign_name IS NULL OR char_length(campaign_name) <= 255)
      AND (adset_name IS NULL OR char_length(adset_name) <= 255)
      AND (ad_name IS NULL OR char_length(ad_name) <= 255)
    )
);

ALTER TABLE meta_ads_deterministic_attribution_aggregates
  ADD CONSTRAINT meta_ads_deterministic_attribution_source_scope_fk
  FOREIGN KEY (source_id, platform, ad_account_id, evidence_origin)
  REFERENCES deterministic_event_sources(id, platform, account_id, evidence_origin)
  ON DELETE RESTRICT;

ALTER TABLE meta_ads_deterministic_attribution_aggregates
  ADD CONSTRAINT meta_ads_deterministic_attribution_raw_source_fk
  FOREIGN KEY (raw_event_id, source_id)
  REFERENCES raw_deterministic_events(id, source_id)
  ON DELETE CASCADE;

ALTER TABLE meta_ads_deterministic_attribution_aggregates
  ADD CONSTRAINT meta_ads_deterministic_attribution_fact_platform_fk
  FOREIGN KEY (fact_id, platform)
  REFERENCES deterministic_event_facts(id, platform)
  ON DELETE RESTRICT;

CREATE UNIQUE INDEX meta_ads_deterministic_attribution_identity_uidx
  ON meta_ads_deterministic_attribution_aggregates (
    organization_id,
    ad_account_id,
    report_date,
    attribution_family,
    attribution_window,
    COALESCE(campaign_id, ''),
    COALESCE(adset_id, ''),
    COALESCE(ad_id, '')
  );

CREATE INDEX meta_ads_deterministic_attribution_reporting_idx
  ON meta_ads_deterministic_attribution_aggregates (
    organization_id,
    report_date DESC,
    attribution_family,
    ad_account_id
  )
  WHERE platform_verified = true AND verification_status = 'verified';

CREATE INDEX meta_ads_deterministic_attribution_campaign_idx
  ON meta_ads_deterministic_attribution_aggregates (
    organization_id,
    campaign_id,
    report_date DESC,
    attribution_family
  )
  WHERE campaign_id IS NOT NULL;

CREATE INDEX meta_ads_deterministic_attribution_adset_idx
  ON meta_ads_deterministic_attribution_aggregates (
    organization_id,
    adset_id,
    report_date DESC,
    attribution_family
  )
  WHERE adset_id IS NOT NULL;

CREATE INDEX meta_ads_deterministic_attribution_ad_idx
  ON meta_ads_deterministic_attribution_aggregates (
    organization_id,
    ad_id,
    report_date DESC,
    attribution_family
  )
  WHERE ad_id IS NOT NULL;

CREATE INDEX meta_ads_deterministic_attribution_connection_idx
  ON meta_ads_deterministic_attribution_aggregates (
    meta_connection_id,
    report_date DESC,
    attribution_family
  );

CREATE INDEX meta_ads_deterministic_attribution_source_idx
  ON meta_ads_deterministic_attribution_aggregates (source_id, report_date DESC);

CREATE INDEX meta_ads_deterministic_attribution_raw_event_idx
  ON meta_ads_deterministic_attribution_aggregates (raw_event_id);

CREATE INDEX meta_ads_deterministic_attribution_fact_idx
  ON meta_ads_deterministic_attribution_aggregates (fact_id)
  WHERE fact_id IS NOT NULL;

COMMIT;
