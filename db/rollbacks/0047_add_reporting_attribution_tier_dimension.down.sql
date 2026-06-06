BEGIN;

DELETE FROM daily_reporting_metrics
WHERE attribution_tier <> 'all';

DROP INDEX IF EXISTS daily_reporting_metrics_dashboard_filters_idx;
DROP INDEX IF EXISTS daily_reporting_metrics_model_source_campaign_idx;
DROP INDEX IF EXISTS daily_reporting_metrics_model_date_idx;

ALTER TABLE daily_reporting_metrics
  DROP CONSTRAINT IF EXISTS daily_reporting_metrics_pkey;

ALTER TABLE daily_reporting_metrics
  DROP CONSTRAINT IF EXISTS daily_reporting_metrics_attribution_tier_chk;

ALTER TABLE daily_reporting_metrics
  DROP COLUMN IF EXISTS attribution_tier;

ALTER TABLE daily_reporting_metrics
  ADD PRIMARY KEY (metric_date, attribution_model, source, medium, campaign, content, term);

CREATE INDEX daily_reporting_metrics_model_date_idx
  ON daily_reporting_metrics (attribution_model, metric_date DESC);

CREATE INDEX daily_reporting_metrics_model_source_campaign_idx
  ON daily_reporting_metrics (attribution_model, source, campaign);

CREATE INDEX daily_reporting_metrics_dashboard_filters_idx
  ON daily_reporting_metrics (attribution_model, metric_date DESC, source, campaign)
  INCLUDE (medium, visits, attributed_orders, attributed_revenue, spend);

COMMIT;
