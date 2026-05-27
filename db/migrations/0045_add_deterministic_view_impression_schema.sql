BEGIN;

CREATE TABLE deterministic_event_sources (
  id bigserial PRIMARY KEY,
  source_key text NOT NULL,
  platform text NOT NULL,
  account_id text NOT NULL,
  evidence_origin text NOT NULL,
  source_type text NOT NULL,
  sync_job_id bigint,
  external_request_id text,
  api_version text,
  requested_range_start date,
  requested_range_end date,
  received_at_utc timestamptz NOT NULL DEFAULT now(),
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_key),
  UNIQUE (id, evidence_origin),
  UNIQUE (id, platform, account_id, evidence_origin),
  CONSTRAINT deterministic_event_sources_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  CONSTRAINT deterministic_event_sources_evidence_origin_chk
    CHECK (evidence_origin IN ('api', 'pixel', 'server', 'manual_import', 'derived')),
  CONSTRAINT deterministic_event_sources_source_type_chk
    CHECK (source_type IN ('ads_insights', 'ads_report', 'platform_export', 'internal_replay')),
  CONSTRAINT deterministic_event_sources_account_id_chk
    CHECK (NULLIF(btrim(account_id), '') IS NOT NULL),
  CONSTRAINT deterministic_event_sources_request_range_chk
    CHECK (
      requested_range_start IS NULL
      OR requested_range_end IS NULL
      OR requested_range_end >= requested_range_start
    ),
  CONSTRAINT deterministic_event_sources_api_metadata_chk
    CHECK (
      evidence_origin <> 'api'
      OR NULLIF(btrim(COALESCE(external_request_id, source_key)), '') IS NOT NULL
    )
);

CREATE INDEX deterministic_event_sources_platform_account_received_idx
  ON deterministic_event_sources (platform, account_id, received_at_utc DESC);

CREATE INDEX deterministic_event_sources_range_idx
  ON deterministic_event_sources (platform, account_id, requested_range_start, requested_range_end)
  WHERE requested_range_start IS NOT NULL OR requested_range_end IS NOT NULL;

CREATE TABLE raw_deterministic_events (
  id bigserial PRIMARY KEY,
  source_id bigint NOT NULL REFERENCES deterministic_event_sources(id) ON DELETE CASCADE,
  platform text NOT NULL,
  account_id text NOT NULL,
  campaign_id text,
  adset_id text,
  ad_id text,
  creative_id text,
  event_type text NOT NULL,
  event_date date NOT NULL,
  event_timestamp_utc timestamptz,
  event_count bigint NOT NULL DEFAULT 1,
  evidence_origin text NOT NULL,
  platform_verified boolean NOT NULL DEFAULT false,
  external_event_id text,
  dedupe_key text NOT NULL,
  raw_payload jsonb NOT NULL,
  ingested_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  UNIQUE (dedupe_key),
  CONSTRAINT raw_deterministic_events_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  CONSTRAINT raw_deterministic_events_event_type_chk
    CHECK (event_type IN ('impression', 'view')),
  CONSTRAINT raw_deterministic_events_evidence_origin_chk
    CHECK (evidence_origin IN ('api', 'pixel', 'server', 'manual_import', 'derived')),
  CONSTRAINT raw_deterministic_events_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api'),
  CONSTRAINT raw_deterministic_events_account_id_chk
    CHECK (NULLIF(btrim(account_id), '') IS NOT NULL),
  CONSTRAINT raw_deterministic_events_entity_present_chk
    CHECK (campaign_id IS NOT NULL OR ad_id IS NOT NULL),
  CONSTRAINT raw_deterministic_events_event_count_chk
    CHECK (event_count > 0),
  CONSTRAINT raw_deterministic_events_dedupe_key_chk
    CHECK (NULLIF(btrim(dedupe_key), '') IS NOT NULL),
  CONSTRAINT raw_deterministic_events_retained_until_chk
    CHECK (retained_until >= ingested_at_utc)
);

ALTER TABLE raw_deterministic_events
  ADD CONSTRAINT raw_deterministic_events_source_scope_fk
  FOREIGN KEY (source_id, platform, account_id, evidence_origin)
  REFERENCES deterministic_event_sources(id, platform, account_id, evidence_origin)
  ON DELETE CASCADE;

CREATE INDEX raw_deterministic_events_account_campaign_date_idx
  ON raw_deterministic_events (platform, account_id, campaign_id, event_date DESC, event_type);

CREATE INDEX raw_deterministic_events_account_ad_date_idx
  ON raw_deterministic_events (platform, account_id, ad_id, event_date DESC, event_type)
  WHERE ad_id IS NOT NULL;

CREATE INDEX raw_deterministic_events_source_date_idx
  ON raw_deterministic_events (source_id, event_date DESC);

CREATE INDEX raw_deterministic_events_retained_until_idx
  ON raw_deterministic_events (retained_until);

CREATE TABLE deterministic_event_facts (
  id bigserial PRIMARY KEY,
  source_id bigint NOT NULL REFERENCES deterministic_event_sources(id) ON DELETE RESTRICT,
  raw_event_id bigint REFERENCES raw_deterministic_events(id) ON DELETE SET NULL,
  platform text NOT NULL,
  account_id text NOT NULL,
  campaign_id text,
  campaign_name text,
  adset_id text,
  adset_name text,
  ad_id text,
  ad_name text,
  creative_id text,
  event_type text NOT NULL,
  fact_date date NOT NULL,
  fact_timestamp_utc timestamptz,
  event_count bigint NOT NULL,
  evidence_origin text NOT NULL,
  platform_verified boolean NOT NULL DEFAULT false,
  normalization_status text NOT NULL DEFAULT 'normalized',
  normalization_notes jsonb NOT NULL DEFAULT '{}'::jsonb,
  normalized_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  CONSTRAINT deterministic_event_facts_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  CONSTRAINT deterministic_event_facts_event_type_chk
    CHECK (event_type IN ('impression', 'view')),
  CONSTRAINT deterministic_event_facts_evidence_origin_chk
    CHECK (evidence_origin IN ('api', 'pixel', 'server', 'manual_import', 'derived')),
  CONSTRAINT deterministic_event_facts_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api'),
  CONSTRAINT deterministic_event_facts_account_id_chk
    CHECK (NULLIF(btrim(account_id), '') IS NOT NULL),
  CONSTRAINT deterministic_event_facts_entity_present_chk
    CHECK (campaign_id IS NOT NULL OR ad_id IS NOT NULL),
  CONSTRAINT deterministic_event_facts_event_count_chk
    CHECK (event_count >= 0),
  CONSTRAINT deterministic_event_facts_normalization_status_chk
    CHECK (normalization_status IN ('normalized', 'partial', 'rejected')),
  CONSTRAINT deterministic_event_facts_name_lengths_chk
    CHECK (
      (campaign_name IS NULL OR char_length(campaign_name) <= 255)
      AND (adset_name IS NULL OR char_length(adset_name) <= 255)
      AND (ad_name IS NULL OR char_length(ad_name) <= 255)
    ),
  CONSTRAINT deterministic_event_facts_retained_until_chk
    CHECK (retained_until >= normalized_at_utc)
);

ALTER TABLE deterministic_event_facts
  ADD CONSTRAINT deterministic_event_facts_source_scope_fk
  FOREIGN KEY (source_id, platform, account_id, evidence_origin)
  REFERENCES deterministic_event_sources(id, platform, account_id, evidence_origin)
  ON DELETE RESTRICT;

CREATE UNIQUE INDEX deterministic_event_facts_scope_uidx
  ON deterministic_event_facts (
    platform,
    account_id,
    event_type,
    fact_date,
    COALESCE(campaign_id, ''),
    COALESCE(adset_id, ''),
    COALESCE(ad_id, ''),
    COALESCE(creative_id, ''),
    evidence_origin
  );

CREATE INDEX deterministic_event_facts_account_campaign_date_idx
  ON deterministic_event_facts (platform, account_id, campaign_id, fact_date DESC, event_type);

CREATE INDEX deterministic_event_facts_account_ad_date_idx
  ON deterministic_event_facts (platform, account_id, ad_id, fact_date DESC, event_type)
  WHERE ad_id IS NOT NULL;

CREATE INDEX deterministic_event_facts_account_date_idx
  ON deterministic_event_facts (platform, account_id, fact_date DESC, event_type);

CREATE INDEX deterministic_event_facts_source_date_idx
  ON deterministic_event_facts (source_id, fact_date DESC);

CREATE INDEX deterministic_event_facts_retained_until_idx
  ON deterministic_event_facts (retained_until);

CREATE TABLE deterministic_event_verification_statuses (
  id bigserial PRIMARY KEY,
  fact_id bigint NOT NULL REFERENCES deterministic_event_facts(id) ON DELETE CASCADE,
  verification_status text NOT NULL,
  evidence_origin text NOT NULL,
  platform_verified boolean NOT NULL DEFAULT false,
  verified_by_source_id bigint,
  verified_at_utc timestamptz,
  failure_reason text,
  verification_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  UNIQUE (fact_id),
  CONSTRAINT deterministic_event_verification_statuses_status_chk
    CHECK (verification_status IN ('unverified', 'pending', 'verified', 'failed', 'superseded')),
  CONSTRAINT deterministic_event_verification_statuses_evidence_origin_chk
    CHECK (evidence_origin IN ('api', 'pixel', 'server', 'manual_import', 'derived')),
  CONSTRAINT deterministic_event_verification_statuses_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api'),
  CONSTRAINT deterministic_event_verification_statuses_verified_shape_chk
    CHECK (
      (
        platform_verified = true
        AND verification_status = 'verified'
        AND verified_at_utc IS NOT NULL
        AND verified_by_source_id IS NOT NULL
      )
      OR platform_verified = false
    ),
  CONSTRAINT deterministic_event_verification_statuses_failure_reason_chk
    CHECK (failure_reason IS NULL OR char_length(failure_reason) <= 255)
);

ALTER TABLE deterministic_event_verification_statuses
  ADD CONSTRAINT deterministic_event_verification_statuses_source_origin_fk
  FOREIGN KEY (verified_by_source_id, evidence_origin)
  REFERENCES deterministic_event_sources(id, evidence_origin)
  ON DELETE RESTRICT;

CREATE INDEX deterministic_event_verification_statuses_status_idx
  ON deterministic_event_verification_statuses (verification_status, created_at_utc DESC);

CREATE INDEX deterministic_event_verification_statuses_source_idx
  ON deterministic_event_verification_statuses (verified_by_source_id, created_at_utc DESC)
  WHERE verified_by_source_id IS NOT NULL;

CREATE TABLE deterministic_model_outputs (
  id bigserial PRIMARY KEY,
  run_id uuid REFERENCES attribution_runs(id) ON DELETE CASCADE,
  order_id text,
  fact_id bigint NOT NULL REFERENCES deterministic_event_facts(id) ON DELETE CASCADE,
  model_key text NOT NULL,
  output_type text NOT NULL,
  platform text NOT NULL,
  account_id text NOT NULL,
  campaign_id text,
  adset_id text,
  ad_id text,
  event_type text NOT NULL,
  fact_date date NOT NULL,
  evidence_origin text NOT NULL,
  platform_verified boolean NOT NULL DEFAULT false,
  contribution_weight numeric(12, 8) NOT NULL DEFAULT 0,
  contributed_event_count numeric(18, 6) NOT NULL DEFAULT 0,
  output_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  generated_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  FOREIGN KEY (run_id, order_id) REFERENCES attribution_order_inputs(run_id, order_id) ON DELETE CASCADE,
  UNIQUE (run_id, order_id, fact_id, model_key, output_type),
  CONSTRAINT deterministic_model_outputs_model_key_chk
    CHECK (model_key IN (
      'first_touch',
      'last_touch',
      'last_non_direct',
      'linear',
      'clicks_only',
      'hinted_fallback_only',
      'deterministic_views',
      'deterministic_impressions'
    )),
  CONSTRAINT deterministic_model_outputs_output_type_chk
    CHECK (output_type IN ('candidate', 'eligible_input', 'credited_input', 'suppressed_input')),
  CONSTRAINT deterministic_model_outputs_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  CONSTRAINT deterministic_model_outputs_event_type_chk
    CHECK (event_type IN ('impression', 'view')),
  CONSTRAINT deterministic_model_outputs_evidence_origin_chk
    CHECK (evidence_origin IN ('api', 'pixel', 'server', 'manual_import', 'derived')),
  CONSTRAINT deterministic_model_outputs_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api'),
  CONSTRAINT deterministic_model_outputs_account_id_chk
    CHECK (NULLIF(btrim(account_id), '') IS NOT NULL),
  CONSTRAINT deterministic_model_outputs_entity_present_chk
    CHECK (campaign_id IS NOT NULL OR ad_id IS NOT NULL),
  CONSTRAINT deterministic_model_outputs_contribution_weight_chk
    CHECK (contribution_weight >= 0 AND contribution_weight <= 1.0),
  CONSTRAINT deterministic_model_outputs_event_count_chk
    CHECK (contributed_event_count >= 0),
  CONSTRAINT deterministic_model_outputs_retained_until_chk
    CHECK (retained_until >= generated_at_utc)
);

CREATE INDEX deterministic_model_outputs_account_campaign_date_idx
  ON deterministic_model_outputs (platform, account_id, campaign_id, fact_date DESC, event_type, model_key);

CREATE INDEX deterministic_model_outputs_account_ad_date_idx
  ON deterministic_model_outputs (platform, account_id, ad_id, fact_date DESC, event_type, model_key)
  WHERE ad_id IS NOT NULL;

CREATE INDEX deterministic_model_outputs_run_order_idx
  ON deterministic_model_outputs (run_id, order_id, model_key)
  WHERE run_id IS NOT NULL AND order_id IS NOT NULL;

CREATE INDEX deterministic_model_outputs_fact_idx
  ON deterministic_model_outputs (fact_id, generated_at_utc DESC);

CREATE INDEX deterministic_model_outputs_retained_until_idx
  ON deterministic_model_outputs (retained_until);

COMMIT;
