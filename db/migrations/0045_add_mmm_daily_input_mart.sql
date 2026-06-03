BEGIN;

CREATE TABLE mmm_daily_input_mart_v1 (
  metric_date date NOT NULL,
  mart_version text NOT NULL DEFAULT 'v1',
  mart_row_type text NOT NULL,
  attribution_model text NOT NULL DEFAULT 'none',
  platform text NOT NULL,
  platform_connection_id bigint,
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
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  content text NOT NULL DEFAULT 'unknown',
  term text NOT NULL DEFAULT 'unknown',
  currency text,
  spend numeric(12, 2) NOT NULL DEFAULT 0,
  impressions bigint NOT NULL DEFAULT 0,
  clicks bigint NOT NULL DEFAULT 0,
  shopify_orders bigint NOT NULL DEFAULT 0,
  shopify_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  attribution_credit_orders numeric(12, 8) NOT NULL DEFAULT 0,
  attribution_credit_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  new_customer_credit_orders numeric(12, 8) NOT NULL DEFAULT 0,
  returning_customer_credit_orders numeric(12, 8) NOT NULL DEFAULT 0,
  new_customer_credit_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  returning_customer_credit_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  match_source_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  confidence_label_coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
  spend_last_synced_at timestamptz,
  shopify_last_ingested_at timestamptz,
  attribution_last_computed_at timestamptz,
  last_computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (
    metric_date,
    mart_version,
    mart_row_type,
    attribution_model,
    platform,
    granularity,
    entity_key,
    source,
    medium,
    campaign,
    content,
    term
  ),
  CHECK (mart_version = 'v1'),
  CHECK (mart_row_type IN ('paid_media', 'attribution')),
  CHECK (platform IN ('meta', 'google', 'taxonomy')),
  CHECK (spend >= 0),
  CHECK (impressions >= 0),
  CHECK (clicks >= 0),
  CHECK (shopify_orders >= 0),
  CHECK (shopify_revenue >= 0),
  CHECK (attribution_credit_orders >= 0),
  CHECK (attribution_credit_revenue >= 0),
  CHECK (new_customer_credit_orders >= 0),
  CHECK (returning_customer_credit_orders >= 0),
  CHECK (new_customer_credit_revenue >= 0),
  CHECK (returning_customer_credit_revenue >= 0)
);

CREATE INDEX mmm_daily_input_mart_v1_date_model_idx
  ON mmm_daily_input_mart_v1 (metric_date DESC, attribution_model, mart_row_type);

CREATE INDEX mmm_daily_input_mart_v1_taxonomy_idx
  ON mmm_daily_input_mart_v1 (source, medium, campaign, content, term, metric_date DESC);

CREATE INDEX mmm_daily_input_mart_v1_native_ids_idx
  ON mmm_daily_input_mart_v1 (platform, account_id, campaign_id, adset_id, ad_id, creative_id, metric_date DESC);

COMMIT;
