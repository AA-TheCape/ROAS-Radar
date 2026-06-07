BEGIN;

DROP INDEX IF EXISTS daily_reporting_metrics_dashboard_filters_idx;
DROP INDEX IF EXISTS daily_reporting_metrics_model_source_campaign_idx;
DROP INDEX IF EXISTS daily_reporting_metrics_model_date_idx;

ALTER TABLE daily_reporting_metrics
  DROP CONSTRAINT IF EXISTS daily_reporting_metrics_pkey;

ALTER TABLE daily_reporting_metrics
  ADD COLUMN IF NOT EXISTS attribution_tier text NOT NULL DEFAULT 'all';

ALTER TABLE daily_reporting_metrics
  DROP CONSTRAINT IF EXISTS daily_reporting_metrics_attribution_tier_chk;

ALTER TABLE daily_reporting_metrics
  ADD CONSTRAINT daily_reporting_metrics_attribution_tier_chk
  CHECK (
    attribution_tier IN (
      'all',
      'deterministic_first_party',
      'deterministic_shopify_hint',
      'platform_reported_meta',
      'ga4_fallback',
      'unattributed'
    )
  );

ALTER TABLE daily_reporting_metrics
  ADD PRIMARY KEY (metric_date, attribution_model, attribution_tier, source, medium, campaign, content, term);

CREATE INDEX daily_reporting_metrics_model_date_idx
  ON daily_reporting_metrics (attribution_model, attribution_tier, metric_date DESC);

CREATE INDEX daily_reporting_metrics_model_source_campaign_idx
  ON daily_reporting_metrics (attribution_model, attribution_tier, source, campaign);

CREATE INDEX daily_reporting_metrics_dashboard_filters_idx
  ON daily_reporting_metrics (attribution_model, attribution_tier, metric_date DESC, source, campaign)
  INCLUDE (medium, visits, attributed_orders, attributed_revenue, spend);

COMMIT;
