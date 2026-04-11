BEGIN;

CREATE TABLE meta_ads_connections (
  id bigserial PRIMARY KEY,
  ad_account_id text NOT NULL UNIQUE,
  access_token_encrypted bytea NOT NULL,
  token_type text NOT NULL DEFAULT 'Bearer',
  granted_scopes text[] NOT NULL DEFAULT '{}'::text[],
  token_expires_at timestamptz,
  last_refreshed_at timestamptz,
  last_sync_planned_for date,
  last_sync_started_at timestamptz,
  last_sync_completed_at timestamptz,
  last_sync_status text NOT NULL DEFAULT 'idle',
  last_sync_error text,
  status text NOT NULL DEFAULT 'active',
  account_name text,
  account_currency text,
  raw_account_data jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('active', 'revoked', 'error')),
  CHECK (last_sync_status IN ('idle', 'running', 'succeeded', 'retry', 'failed'))
);

CREATE INDEX meta_ads_connections_status_idx
  ON meta_ads_connections (status, updated_at DESC);

CREATE TABLE meta_ads_oauth_states (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state_digest text NOT NULL UNIQUE,
  redirect_path text,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz
);

CREATE INDEX meta_ads_oauth_states_expires_idx
  ON meta_ads_oauth_states (expires_at DESC);

CREATE TABLE meta_ads_sync_jobs (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
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

CREATE INDEX meta_ads_sync_jobs_status_available_idx
  ON meta_ads_sync_jobs (status, available_at, id);

CREATE TABLE meta_ads_raw_spend_records (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  sync_job_id bigint NOT NULL REFERENCES meta_ads_sync_jobs(id) ON DELETE CASCADE,
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
  CHECK (level IN ('account', 'campaign', 'adset', 'ad'))
);

CREATE INDEX meta_ads_raw_spend_records_date_level_idx
  ON meta_ads_raw_spend_records (report_date DESC, level);

CREATE TABLE meta_ads_daily_spend (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  raw_record_id bigint REFERENCES meta_ads_raw_spend_records(id) ON DELETE SET NULL,
  sync_job_id bigint NOT NULL REFERENCES meta_ads_sync_jobs(id) ON DELETE CASCADE,
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

CREATE INDEX meta_ads_daily_spend_date_granularity_idx
  ON meta_ads_daily_spend (report_date DESC, granularity);

COMMIT;
