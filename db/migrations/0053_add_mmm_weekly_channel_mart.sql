BEGIN;

ALTER TABLE mmm_model_runs
  DROP CONSTRAINT IF EXISTS mmm_model_runs_mart_version_check;

ALTER TABLE mmm_model_runs
  ADD CONSTRAINT mmm_model_runs_mart_version_check
  CHECK (mart_version IN ('mmm_daily_input_mart_v1', 'mmm_weekly_channel_input_mart_v1'));

CREATE TABLE mmm_weekly_channel_input_mart_v1 (
  week_start_date date NOT NULL,
  week_end_date date NOT NULL,
  mart_version text NOT NULL DEFAULT 'mmm_weekly_channel_input_mart_v1',
  source_mart_version text NOT NULL DEFAULT 'mmm_daily_input_mart_v1',
  attribution_model text NOT NULL,
  channel_key text NOT NULL,
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  channel text NOT NULL DEFAULT 'unknown',
  channel_group text NOT NULL DEFAULT 'unknown',
  spend numeric(14, 2) NOT NULL DEFAULT 0,
  impressions bigint NOT NULL DEFAULT 0,
  clicks bigint NOT NULL DEFAULT 0,
  shopify_orders bigint NOT NULL DEFAULT 0,
  shopify_revenue numeric(14, 2) NOT NULL DEFAULT 0,
  attribution_credit_orders numeric(14, 8) NOT NULL DEFAULT 0,
  attribution_credit_revenue numeric(14, 2) NOT NULL DEFAULT 0,
  new_customer_credit_orders numeric(14, 8) NOT NULL DEFAULT 0,
  returning_customer_credit_orders numeric(14, 8) NOT NULL DEFAULT 0,
  new_customer_credit_revenue numeric(14, 2) NOT NULL DEFAULT 0,
  returning_customer_credit_revenue numeric(14, 2) NOT NULL DEFAULT 0,
  match_source_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  confidence_label_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  controls jsonb NOT NULL DEFAULT '{}'::jsonb,
  deterministic_anchors jsonb NOT NULL DEFAULT '{}'::jsonb,
  missingness_report jsonb NOT NULL DEFAULT '{}'::jsonb,
  leakage_report jsonb NOT NULL DEFAULT '{}'::jsonb,
  dq_status text NOT NULL DEFAULT 'pass',
  source_row_count bigint NOT NULL DEFAULT 0,
  generated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (week_start_date, mart_version, attribution_model, channel_key),
  CHECK (week_end_date >= week_start_date),
  CHECK (mart_version = 'mmm_weekly_channel_input_mart_v1'),
  CHECK (source_mart_version = 'mmm_daily_input_mart_v1'),
  CHECK (dq_status IN ('pass', 'warn', 'fail')),
  CHECK (spend >= 0),
  CHECK (impressions >= 0),
  CHECK (clicks >= 0),
  CHECK (shopify_orders >= 0),
  CHECK (shopify_revenue >= 0),
  CHECK (attribution_credit_orders >= 0),
  CHECK (attribution_credit_revenue >= 0)
);

CREATE INDEX mmm_weekly_channel_input_mart_v1_window_idx
  ON mmm_weekly_channel_input_mart_v1 (week_start_date DESC, week_end_date DESC, attribution_model);

CREATE INDEX mmm_weekly_channel_input_mart_v1_channel_idx
  ON mmm_weekly_channel_input_mart_v1 (source, medium, campaign, channel, week_start_date DESC);

CREATE TABLE mmm_model_run_input_snapshots (
  model_run_id uuid NOT NULL REFERENCES mmm_model_runs(id) ON DELETE CASCADE,
  snapshot_version text NOT NULL DEFAULT 'mmm_weekly_channel_snapshot_v1',
  mart_version text NOT NULL,
  row_number integer NOT NULL,
  row_hash text NOT NULL,
  input_row jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (model_run_id, snapshot_version, row_number),
  UNIQUE (model_run_id, snapshot_version, row_hash),
  CHECK (snapshot_version = 'mmm_weekly_channel_snapshot_v1'),
  CHECK (mart_version = 'mmm_weekly_channel_input_mart_v1'),
  CHECK (row_number > 0)
);

CREATE INDEX mmm_model_run_input_snapshots_run_idx
  ON mmm_model_run_input_snapshots (model_run_id, created_at DESC);

COMMIT;
