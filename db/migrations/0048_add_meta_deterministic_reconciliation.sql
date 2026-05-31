BEGIN;

CREATE TABLE meta_ads_deterministic_reconciliation_runs (
  id bigserial PRIMARY KEY,
  run_date date NOT NULL UNIQUE,
  status text NOT NULL,
  checked_at timestamptz NOT NULL DEFAULT now(),
  compared_scope_count integer NOT NULL DEFAULT 0,
  mismatch_count integer NOT NULL DEFAULT 0,
  total_api_expected_count bigint NOT NULL DEFAULT 0,
  total_raw_ingested_count bigint NOT NULL DEFAULT 0,
  total_fact_count bigint NOT NULL DEFAULT 0,
  absolute_tolerance bigint NOT NULL DEFAULT 0,
  relative_tolerance numeric(12, 8) NOT NULL DEFAULT 0,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('passed', 'failed'))
);

CREATE TABLE meta_ads_deterministic_reconciliation_mismatches (
  id bigserial PRIMARY KEY,
  run_id bigint NOT NULL REFERENCES meta_ads_deterministic_reconciliation_runs(id) ON DELETE CASCADE,
  run_date date NOT NULL,
  account_id text NOT NULL,
  campaign_id text,
  adset_id text,
  ad_id text,
  event_type text NOT NULL,
  api_expected_count bigint NOT NULL DEFAULT 0,
  raw_ingested_count bigint NOT NULL DEFAULT 0,
  fact_count bigint NOT NULL DEFAULT 0,
  absolute_delta bigint NOT NULL DEFAULT 0,
  relative_delta numeric(12, 8),
  anomaly_flags text[] NOT NULL DEFAULT ARRAY[]::text[],
  sample_source_ids bigint[] NOT NULL DEFAULT ARRAY[]::bigint[],
  sample_raw_event_ids bigint[] NOT NULL DEFAULT ARRAY[]::bigint[],
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (event_type IN ('impression', 'view'))
);

CREATE INDEX meta_ads_deterministic_reconciliation_runs_checked_idx
  ON meta_ads_deterministic_reconciliation_runs (checked_at DESC, status);

CREATE INDEX meta_ads_deterministic_reconciliation_mismatches_run_idx
  ON meta_ads_deterministic_reconciliation_mismatches (run_date DESC, account_id, event_type);

CREATE VIEW meta_ads_deterministic_reconciliation_investigation AS
SELECT
  runs.run_date,
  runs.status AS run_status,
  runs.checked_at,
  mismatches.account_id,
  mismatches.campaign_id,
  mismatches.adset_id,
  mismatches.ad_id,
  mismatches.event_type,
  mismatches.api_expected_count,
  mismatches.raw_ingested_count,
  mismatches.fact_count,
  mismatches.absolute_delta,
  mismatches.relative_delta,
  mismatches.anomaly_flags,
  mismatches.sample_source_ids,
  mismatches.sample_raw_event_ids
FROM meta_ads_deterministic_reconciliation_runs runs
JOIN meta_ads_deterministic_reconciliation_mismatches mismatches
  ON mismatches.run_id = runs.id;

COMMIT;
