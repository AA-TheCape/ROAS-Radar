BEGIN;

CREATE TABLE meta_ads_order_value_sync_runs (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  trigger_source text NOT NULL DEFAULT 'application_scheduler',
  status text NOT NULL DEFAULT 'running',
  window_start_date date NOT NULL,
  window_end_date date NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  records_received integer NOT NULL DEFAULT 0,
  raw_rows_persisted integer NOT NULL DEFAULT 0,
  aggregate_rows_upserted integer NOT NULL DEFAULT 0,
  error_count integer NOT NULL DEFAULT 0,
  error_details jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('running', 'completed', 'partial_failure', 'failed'))
);

CREATE INDEX meta_ads_order_value_sync_runs_connection_started_idx
  ON meta_ads_order_value_sync_runs (connection_id, started_at DESC);

CREATE INDEX meta_ads_order_value_sync_runs_status_started_idx
  ON meta_ads_order_value_sync_runs (status, started_at DESC);

CREATE TABLE meta_ads_order_value_raw_records (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  sync_run_id bigint NOT NULL REFERENCES meta_ads_order_value_sync_runs(id) ON DELETE CASCADE,
  sync_job_id bigint REFERENCES meta_ads_sync_jobs(id) ON DELETE SET NULL,
  report_date date NOT NULL,
  campaign_id text NOT NULL,
  campaign_name text,
  action_type text,
  raw_payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX meta_ads_order_value_raw_records_run_report_idx
  ON meta_ads_order_value_raw_records (sync_run_id, report_date DESC, campaign_id);

CREATE INDEX meta_ads_order_value_raw_records_connection_report_idx
  ON meta_ads_order_value_raw_records (connection_id, report_date DESC, campaign_id);

ALTER TABLE meta_ads_order_value_aggregates
  ADD COLUMN raw_revenue_record_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMIT;
