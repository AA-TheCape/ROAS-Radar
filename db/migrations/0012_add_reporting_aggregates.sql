BEGIN;

CREATE TABLE daily_reporting_metrics (
  metric_date date NOT NULL,
  attribution_model text NOT NULL,
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  content text NOT NULL DEFAULT 'unknown',
  term text NOT NULL DEFAULT 'unknown',
  visits integer NOT NULL DEFAULT 0,
  attributed_orders numeric(12, 8) NOT NULL DEFAULT 0,
  attributed_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  spend numeric(12, 2) NOT NULL DEFAULT 0,
  impressions bigint NOT NULL DEFAULT 0,
  clicks bigint NOT NULL DEFAULT 0,
  new_customer_orders numeric(12, 8) NOT NULL DEFAULT 0,
  returning_customer_orders numeric(12, 8) NOT NULL DEFAULT 0,
  new_customer_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  returning_customer_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  last_computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (metric_date, attribution_model, source, medium, campaign, content, term)
);

CREATE INDEX daily_reporting_metrics_model_date_idx
  ON daily_reporting_metrics (attribution_model, metric_date DESC);

CREATE INDEX daily_reporting_metrics_model_source_campaign_idx
  ON daily_reporting_metrics (attribution_model, source, campaign);

COMMIT;
