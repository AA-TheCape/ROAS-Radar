BEGIN;

ALTER TABLE meta_ads_deterministic_attribution_aggregates
  DROP CONSTRAINT IF EXISTS meta_ads_deterministic_attribution_raw_traceability_chk,
  DROP CONSTRAINT IF EXISTS meta_ads_deterministic_attribution_verified_source_scope_fk;

ALTER TABLE deterministic_event_verification_statuses
  DROP CONSTRAINT IF EXISTS deterministic_event_verification_statuses_verified_source_scope_fk;

ALTER TABLE deterministic_event_sources
  DROP CONSTRAINT IF EXISTS deterministic_event_sources_id_platform_origin_uidx,
  DROP CONSTRAINT IF EXISTS deterministic_event_sources_meta_api_provenance_chk,
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

COMMIT;
