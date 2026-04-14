BEGIN;

CREATE TABLE attribution_order_credits (
  id bigserial PRIMARY KEY,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  attribution_model text NOT NULL,
  touchpoint_position integer NOT NULL,
  session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  touchpoint_occurred_at timestamptz,
  attributed_source text,
  attributed_medium text,
  attributed_campaign text,
  attributed_content text,
  attributed_term text,
  attributed_click_id_type text,
  attributed_click_id_value text,
  credit_weight numeric(12, 8) NOT NULL,
  revenue_credit numeric(12, 2) NOT NULL,
  is_primary boolean NOT NULL DEFAULT false,
  attribution_reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (shopify_order_id, attribution_model, touchpoint_position)
);

CREATE INDEX attribution_order_credits_order_model_idx
  ON attribution_order_credits (shopify_order_id, attribution_model);

CREATE INDEX attribution_order_credits_model_source_campaign_idx
  ON attribution_order_credits (attribution_model, attributed_source, attributed_campaign);

CREATE TABLE daily_attribution_campaign_metrics (
  metric_date date NOT NULL,
  attribution_model text NOT NULL,
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  content text NOT NULL DEFAULT '',
  visits integer NOT NULL DEFAULT 0,
  orders numeric(12, 8) NOT NULL DEFAULT 0,
  revenue numeric(12, 2) NOT NULL DEFAULT 0,
  last_computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (metric_date, attribution_model, source, medium, campaign, content)
);

CREATE INDEX daily_attribution_campaign_metrics_model_date_idx
  ON daily_attribution_campaign_metrics (attribution_model, metric_date DESC);

CREATE INDEX daily_attribution_campaign_metrics_model_source_campaign_idx
  ON daily_attribution_campaign_metrics (attribution_model, source, campaign);

COMMIT;
