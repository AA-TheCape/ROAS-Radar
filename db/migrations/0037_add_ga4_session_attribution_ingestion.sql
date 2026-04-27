BEGIN;

CREATE TABLE IF NOT EXISTS ga4_session_attribution (
  ga4_session_key text PRIMARY KEY,
  ga4_user_key text NOT NULL,
  ga4_client_id text,
  ga4_session_id text NOT NULL,
  session_started_at timestamptz NOT NULL,
  last_event_at timestamptz NOT NULL,
  source text,
  medium text,
  campaign text,
  content text,
  term text,
  click_id_type text,
  click_id_value text,
  source_export_hour timestamptz NOT NULL,
  source_dataset text NOT NULL,
  source_table_type text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (char_length(ga4_session_key) <= 512),
  CHECK (char_length(ga4_user_key) <= 255),
  CHECK (ga4_client_id IS NULL OR char_length(ga4_client_id) <= 255),
  CHECK (char_length(ga4_session_id) <= 255),
  CHECK (source IS NULL OR char_length(source) <= 255),
  CHECK (medium IS NULL OR char_length(medium) <= 255),
  CHECK (campaign IS NULL OR char_length(campaign) <= 255),
  CHECK (content IS NULL OR char_length(content) <= 255),
  CHECK (term IS NULL OR char_length(term) <= 255),
  CHECK (click_id_type IS NULL OR char_length(click_id_type) <= 64),
  CHECK (click_id_value IS NULL OR char_length(click_id_value) <= 255),
  CHECK (char_length(source_dataset) <= 255),
  CHECK (char_length(source_table_type) <= 32),
  CHECK (session_started_at <= last_event_at)
);

CREATE INDEX IF NOT EXISTS ga4_session_attribution_user_lookup_idx
  ON ga4_session_attribution (ga4_user_key, last_event_at DESC);

CREATE INDEX IF NOT EXISTS ga4_session_attribution_export_hour_idx
  ON ga4_session_attribution (source_export_hour DESC);

CREATE TABLE IF NOT EXISTS ga4_bigquery_ingestion_state (
  pipeline_name text PRIMARY KEY,
  watermark_hour timestamptz,
  last_run_started_at timestamptz,
  last_run_completed_at timestamptz,
  last_run_status text NOT NULL DEFAULT 'idle',
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (char_length(pipeline_name) <= 128),
  CHECK (last_run_status IN ('idle', 'running', 'completed', 'failed'))
);

COMMIT;
