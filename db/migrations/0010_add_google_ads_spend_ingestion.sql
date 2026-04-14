BEGIN;

CREATE TABLE google_ads_connections (
  id bigserial PRIMARY KEY,
  customer_id text NOT NULL UNIQUE,
  login_customer_id text,
  developer_token_encrypted bytea NOT NULL,
  client_id text NOT NULL,
  client_secret_encrypted bytea NOT NULL,
  refresh_token_encrypted bytea NOT NULL,
  token_scopes text[] NOT NULL DEFAULT ARRAY['https://www.googleapis.com/auth/adwords']::text[],
  last_refreshed_at timestamptz,
  last_sync_planned_for date,
  last_sync_started_at timestamptz,
  last_sync_completed_at timestamptz,
  last_sync_status text NOT NULL DEFAULT 'idle',
  last_sync_error text,
  status text NOT NULL DEFAULT 'active',
  customer_descriptive_name text,
  currency_code text,
  raw_customer_data jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('active', 'revoked', 'error')),
  CHECK (last_sync_status IN ('idle', 'running', 'succeeded', 'retry', 'failed'))
);

CREATE INDEX google_ads_connections_status_idx
  ON google_ads_connections (status, updated_at DESC);

CREATE TABLE google_ads_sync_jobs (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES google_ads_connections(id) ON DELETE CASCADE,
  sync_date date NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  locked_by text,
  last_error text,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (connection_id, sync_date),
  CHECK (status IN ('pending', 'processing', 'retry', 'completed', 'failed'))
);

CREATE INDEX google_ads_sync_jobs_status_available_idx
  ON google_ads_sync_jobs (status, available_at, id);

CREATE TABLE google_ads_raw_spend_records (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES google_ads_connections(id) ON DELETE CASCADE,
  sync_job_id bigint NOT NULL REFERENCES google_ads_sync_jobs(id) ON DELETE CASCADE,
  report_date date NOT NULL,
  level text NOT NULL,
  entity_id text NOT NULL,
  currency text,
  spend numeric(12, 2) NOT NULL DEFAULT 0,
  impressions bigint NOT NULL DEFAULT 0,
  clicks bigint NOT NULL DEFAULT 0,
  raw_payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (connection_id, report_date, level, entity_id),
  CHECK (level IN ('campaign', 'ad'))
);

CREATE INDEX google_ads_raw_spend_records_date_level_idx
  ON google_ads_raw_spend_records (report_date DESC, level);

CREATE TABLE google_ads_daily_spend (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES google_ads_connections(id) ON DELETE CASCADE,
  raw_record_id bigint REFERENCES google_ads_raw_spend_records(id) ON DELETE SET NULL,
  sync_job_id bigint NOT NULL REFERENCES google_ads_sync_jobs(id) ON DELETE CASCADE,
  report_date date NOT NULL,
  granularity text NOT NULL,
  entity_key text NOT NULL,
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
  currency text,
  spend numeric(12, 2) NOT NULL DEFAULT 0,
  impressions bigint NOT NULL DEFAULT 0,
  clicks bigint NOT NULL DEFAULT 0,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (connection_id, report_date, granularity, entity_key),
  CHECK (granularity IN ('account', 'campaign', 'adset', 'ad', 'creative'))
);

CREATE INDEX google_ads_daily_spend_date_granularity_idx
  ON google_ads_daily_spend (report_date DESC, granularity);

CREATE TABLE google_ads_reconciliation_runs (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES google_ads_connections(id) ON DELETE CASCADE,
  checked_range_start date NOT NULL,
  checked_range_end date NOT NULL,
  missing_dates jsonb NOT NULL DEFAULT '[]'::jsonb,
  enqueued_jobs integer NOT NULL DEFAULT 0,
  status text NOT NULL DEFAULT 'healthy',
  checked_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('healthy', 'missing_dates'))
);

CREATE INDEX google_ads_reconciliation_runs_connection_checked_idx
  ON google_ads_reconciliation_runs (connection_id, checked_at DESC);

COMMIT;
