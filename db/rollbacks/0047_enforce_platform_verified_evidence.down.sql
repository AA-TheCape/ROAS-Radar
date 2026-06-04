BEGIN;

DROP TABLE IF EXISTS deterministic_event_evidence_quarantine;

ALTER TABLE deterministic_model_outputs
  DROP CONSTRAINT IF EXISTS deterministic_model_outputs_api_verified_chk,
  ADD CONSTRAINT deterministic_model_outputs_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api');

ALTER TABLE deterministic_event_verification_statuses
  DROP CONSTRAINT IF EXISTS deterministic_event_verification_statuses_fact_platform_fk,
  DROP CONSTRAINT IF EXISTS deterministic_event_verification_statuses_api_verified_chk,
  ADD CONSTRAINT deterministic_event_verification_statuses_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api'),
  DROP CONSTRAINT IF EXISTS deterministic_event_verification_statuses_platform_chk,
  DROP COLUMN IF EXISTS platform;

ALTER TABLE deterministic_event_facts
  DROP CONSTRAINT IF EXISTS deterministic_event_facts_api_verified_chk,
  ADD CONSTRAINT deterministic_event_facts_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api');

ALTER TABLE deterministic_event_facts
  DROP CONSTRAINT IF EXISTS deterministic_event_facts_id_platform_uidx;

ALTER TABLE raw_deterministic_events
  DROP CONSTRAINT IF EXISTS raw_deterministic_events_api_verified_chk,
  ADD CONSTRAINT raw_deterministic_events_api_verified_chk
    CHECK (platform_verified = false OR evidence_origin = 'api');

ALTER TABLE deterministic_event_sources
  DROP CONSTRAINT IF EXISTS deterministic_event_sources_meta_api_provenance_chk,
  DROP COLUMN IF EXISTS api_request_id,
  DROP COLUMN IF EXISTS api_account_id,
  DROP COLUMN IF EXISTS api_request_timestamp_utc,
  DROP COLUMN IF EXISTS api_endpoint;

COMMIT;
