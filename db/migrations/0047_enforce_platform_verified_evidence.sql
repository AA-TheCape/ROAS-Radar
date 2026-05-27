BEGIN;

ALTER TABLE deterministic_event_sources
  ADD COLUMN api_endpoint text,
  ADD COLUMN api_request_timestamp_utc timestamptz,
  ADD COLUMN api_account_id text,
  ADD COLUMN api_request_id text;

UPDATE deterministic_event_sources
SET
  api_endpoint = COALESCE(api_endpoint, source_metadata->>'endpoint'),
  api_request_timestamp_utc = COALESCE(api_request_timestamp_utc, received_at_utc),
  api_account_id = COALESCE(api_account_id, account_id),
  api_request_id = COALESCE(api_request_id, external_request_id, source_key)
WHERE platform = 'meta_ads'
  AND evidence_origin = 'api';

UPDATE raw_deterministic_events
SET platform_verified = false
WHERE platform_verified = true
  AND (platform <> 'meta_ads' OR evidence_origin <> 'api');

UPDATE deterministic_event_facts
SET platform_verified = false
WHERE platform_verified = true
  AND (platform <> 'meta_ads' OR evidence_origin <> 'api');

UPDATE deterministic_model_outputs
SET platform_verified = false
WHERE platform_verified = true
  AND (platform <> 'meta_ads' OR evidence_origin <> 'api');

ALTER TABLE deterministic_event_sources
  ADD CONSTRAINT deterministic_event_sources_meta_api_provenance_chk
  CHECK (
    platform <> 'meta_ads'
    OR evidence_origin <> 'api'
    OR (
      NULLIF(btrim(COALESCE(api_endpoint, source_metadata->>'endpoint')), '') IS NOT NULL
      AND api_request_timestamp_utc IS NOT NULL
      AND NULLIF(btrim(COALESCE(api_account_id, account_id)), '') IS NOT NULL
      AND NULLIF(btrim(COALESCE(api_request_id, external_request_id, source_key)), '') IS NOT NULL
    )
  );

ALTER TABLE raw_deterministic_events
  DROP CONSTRAINT raw_deterministic_events_api_verified_chk,
  ADD CONSTRAINT raw_deterministic_events_api_verified_chk
    CHECK (platform_verified = false OR (platform = 'meta_ads' AND evidence_origin = 'api'));

ALTER TABLE deterministic_event_facts
  DROP CONSTRAINT deterministic_event_facts_api_verified_chk,
  ADD CONSTRAINT deterministic_event_facts_api_verified_chk
    CHECK (platform_verified = false OR (platform = 'meta_ads' AND evidence_origin = 'api'));

ALTER TABLE deterministic_event_verification_statuses
  ADD COLUMN platform text;

UPDATE deterministic_event_verification_statuses statuses
SET platform = facts.platform
FROM deterministic_event_facts facts
WHERE facts.id = statuses.fact_id;

UPDATE deterministic_event_verification_statuses
SET
  platform_verified = false,
  verification_status = CASE WHEN verification_status = 'verified' THEN 'failed' ELSE verification_status END,
  failure_reason = COALESCE(failure_reason, 'non_meta_api_evidence')
WHERE platform_verified = true
  AND (platform <> 'meta_ads' OR evidence_origin <> 'api');

ALTER TABLE deterministic_event_facts
  ADD CONSTRAINT deterministic_event_facts_id_platform_uidx UNIQUE (id, platform);

ALTER TABLE deterministic_event_verification_statuses
  ALTER COLUMN platform SET NOT NULL,
  ADD CONSTRAINT deterministic_event_verification_statuses_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  DROP CONSTRAINT deterministic_event_verification_statuses_api_verified_chk,
  ADD CONSTRAINT deterministic_event_verification_statuses_api_verified_chk
    CHECK (platform_verified = false OR (platform = 'meta_ads' AND evidence_origin = 'api'));

ALTER TABLE deterministic_event_verification_statuses
  ADD CONSTRAINT deterministic_event_verification_statuses_fact_platform_fk
  FOREIGN KEY (fact_id, platform)
  REFERENCES deterministic_event_facts(id, platform)
  ON DELETE CASCADE;

ALTER TABLE deterministic_model_outputs
  DROP CONSTRAINT deterministic_model_outputs_api_verified_chk,
  ADD CONSTRAINT deterministic_model_outputs_api_verified_chk
    CHECK (platform_verified = false OR (platform = 'meta_ads' AND evidence_origin = 'api'));

CREATE TABLE deterministic_event_evidence_quarantine (
  id bigserial PRIMARY KEY,
  source_id bigint REFERENCES deterministic_event_sources(id) ON DELETE SET NULL,
  platform text NOT NULL,
  account_id text,
  evidence_origin text,
  event_type text,
  event_date date,
  dedupe_key text,
  reason_code text NOT NULL,
  reason_detail text,
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  quarantined_at_utc timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT deterministic_event_evidence_quarantine_platform_chk
    CHECK (platform IN ('google_ads', 'meta_ads')),
  CONSTRAINT deterministic_event_evidence_quarantine_evidence_origin_chk
    CHECK (evidence_origin IS NULL OR evidence_origin IN ('api', 'pixel', 'server', 'manual_import', 'derived')),
  CONSTRAINT deterministic_event_evidence_quarantine_event_type_chk
    CHECK (event_type IS NULL OR event_type IN ('impression', 'view')),
  CONSTRAINT deterministic_event_evidence_quarantine_reason_code_chk
    CHECK (NULLIF(btrim(reason_code), '') IS NOT NULL)
);

CREATE INDEX deterministic_event_evidence_quarantine_reason_idx
  ON deterministic_event_evidence_quarantine (reason_code, quarantined_at_utc DESC);

CREATE INDEX deterministic_event_evidence_quarantine_source_idx
  ON deterministic_event_evidence_quarantine (source_id, quarantined_at_utc DESC)
  WHERE source_id IS NOT NULL;

COMMIT;
