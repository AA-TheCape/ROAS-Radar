BEGIN;

UPDATE deterministic_event_sources
SET
  api_endpoint = COALESCE(api_endpoint, source_metadata->>'endpoint'),
  api_request_timestamp_utc = COALESCE(api_request_timestamp_utc, received_at_utc),
  api_account_id = COALESCE(api_account_id, account_id),
  api_request_id = COALESCE(api_request_id, external_request_id, source_key)
WHERE platform = 'meta_ads'
  AND evidence_origin = 'api';

UPDATE meta_ads_deterministic_attribution_aggregates aggregates
SET raw_record_metadata = aggregates.raw_record_metadata || jsonb_build_object(
  'sourceId', aggregates.source_id,
  'sourceTable', 'deterministic_event_sources',
  'rawEventId', aggregates.raw_event_id,
  'rawTable', 'raw_deterministic_events',
  'apiVersion', COALESCE(sources.api_version, aggregates.raw_record_metadata->>'apiVersion'),
  'apiEndpoint', sources.api_endpoint,
  'apiAccountId', sources.api_account_id,
  'apiRequestTimestampUtc', sources.api_request_timestamp_utc,
  'requestId', sources.api_request_id
)
FROM deterministic_event_sources sources
WHERE sources.id = aggregates.source_id
  AND aggregates.platform_verified = true;

UPDATE deterministic_event_verification_statuses statuses
SET
  platform_verified = false,
  verification_status = CASE WHEN verification_status = 'verified' THEN 'failed' ELSE verification_status END,
  failure_reason = COALESCE(failure_reason, 'non_meta_api_evidence')
WHERE platform_verified = true
  AND NOT EXISTS (
    SELECT 1
    FROM deterministic_event_sources sources
    WHERE sources.id = statuses.verified_by_source_id
      AND sources.platform = statuses.platform
      AND sources.evidence_origin = statuses.evidence_origin
  );

UPDATE meta_ads_deterministic_attribution_aggregates aggregates
SET
  platform_verified = false,
  verification_status = CASE WHEN verification_status = 'verified' THEN 'failed' ELSE verification_status END
WHERE platform_verified = true
  AND NOT EXISTS (
    SELECT 1
    FROM deterministic_event_sources sources
    WHERE sources.id = aggregates.verified_by_source_id
      AND sources.platform = aggregates.platform
      AND sources.evidence_origin = aggregates.evidence_origin
  );

ALTER TABLE deterministic_event_sources
  DROP CONSTRAINT IF EXISTS deterministic_event_sources_meta_api_provenance_chk,
  ADD CONSTRAINT deterministic_event_sources_meta_api_provenance_chk
  CHECK (
    platform <> 'meta_ads'
    OR evidence_origin <> 'api'
    OR (
      NULLIF(btrim(api_endpoint), '') IS NOT NULL
      AND api_request_timestamp_utc IS NOT NULL
      AND NULLIF(btrim(api_account_id), '') IS NOT NULL
      AND NULLIF(btrim(api_request_id), '') IS NOT NULL
    )
  );

ALTER TABLE deterministic_event_sources
  ADD CONSTRAINT deterministic_event_sources_id_platform_origin_uidx
  UNIQUE (id, platform, evidence_origin);

ALTER TABLE deterministic_event_verification_statuses
  ADD CONSTRAINT deterministic_event_verification_statuses_verified_source_scope_fk
  FOREIGN KEY (verified_by_source_id, platform, evidence_origin)
  REFERENCES deterministic_event_sources(id, platform, evidence_origin)
  ON DELETE RESTRICT;

ALTER TABLE meta_ads_deterministic_attribution_aggregates
  ADD CONSTRAINT meta_ads_deterministic_attribution_verified_source_scope_fk
  FOREIGN KEY (verified_by_source_id, platform, evidence_origin)
  REFERENCES deterministic_event_sources(id, platform, evidence_origin)
  ON DELETE RESTRICT,
  ADD CONSTRAINT meta_ads_deterministic_attribution_raw_traceability_chk
  CHECK (
    platform_verified = false
    OR (
      raw_record_metadata ? 'sourceId'
      AND raw_record_metadata ? 'rawEventId'
      AND raw_record_metadata ? 'rawTable'
      AND raw_record_metadata ? 'apiVersion'
      AND raw_record_metadata ? 'apiEndpoint'
      AND raw_record_metadata ? 'apiAccountId'
      AND raw_record_metadata ? 'apiRequestTimestampUtc'
      AND raw_record_metadata ? 'requestId'
      AND NULLIF(btrim(raw_record_metadata->>'rawTable'), '') IS NOT NULL
      AND NULLIF(btrim(raw_record_metadata->>'apiVersion'), '') IS NOT NULL
      AND NULLIF(btrim(raw_record_metadata->>'apiEndpoint'), '') IS NOT NULL
      AND NULLIF(btrim(raw_record_metadata->>'apiAccountId'), '') IS NOT NULL
      AND NULLIF(btrim(raw_record_metadata->>'apiRequestTimestampUtc'), '') IS NOT NULL
      AND NULLIF(btrim(raw_record_metadata->>'requestId'), '') IS NOT NULL
    )
  );

COMMIT;
