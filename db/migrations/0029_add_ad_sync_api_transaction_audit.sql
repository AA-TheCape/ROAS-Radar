BEGIN;

CREATE TABLE ad_sync_api_transactions (
  id bigserial PRIMARY KEY,
  platform text NOT NULL,
  connection_id bigint NOT NULL,
  sync_job_id bigint NOT NULL,
  transaction_source text NOT NULL,
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  request_method text NOT NULL,
  request_url text NOT NULL,
  request_payload jsonb,
  request_started_at timestamptz NOT NULL,
  response_status integer,
  response_payload jsonb,
  response_received_at timestamptz,
  error_message text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (platform IN ('meta_ads', 'google_ads')),
  CHECK (char_length(transaction_source) <= 128),
  CHECK (char_length(request_method) BETWEEN 1 AND 16)
);

CREATE INDEX ad_sync_api_transactions_platform_connection_started_idx
  ON ad_sync_api_transactions (platform, connection_id, request_started_at DESC);

CREATE INDEX ad_sync_api_transactions_platform_sync_job_started_idx
  ON ad_sync_api_transactions (platform, sync_job_id, request_started_at DESC);

CREATE INDEX ad_sync_api_transactions_source_received_idx
  ON ad_sync_api_transactions (transaction_source, response_received_at DESC);

COMMIT;
