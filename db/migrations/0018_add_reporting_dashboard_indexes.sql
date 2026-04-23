BEGIN;

CREATE INDEX daily_reporting_metrics_dashboard_filters_idx
  ON daily_reporting_metrics (attribution_model, metric_date DESC, source, campaign)
  INCLUDE (medium, visits, attributed_orders, attributed_revenue, spend);

COMMIT;
