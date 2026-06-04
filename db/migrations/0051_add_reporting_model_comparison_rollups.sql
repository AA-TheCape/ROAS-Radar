BEGIN;

CREATE TABLE reporting_model_comparison_daily (
  metric_date date NOT NULL,
  attribution_model text NOT NULL,
  reporting_view text NOT NULL,
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  visits integer NOT NULL DEFAULT 0,
  attributed_orders numeric(12, 8) NOT NULL DEFAULT 0,
  attributed_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  spend numeric(12, 2) NOT NULL DEFAULT 0,
  strict_deterministic_orders numeric(12, 8) NOT NULL DEFAULT 0,
  fallback_included_orders numeric(12, 8) NOT NULL DEFAULT 0,
  blended_deterministic_orders numeric(12, 8) NOT NULL DEFAULT 0,
  last_computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (metric_date, attribution_model, reporting_view, source, medium, campaign),
  CHECK (reporting_view IN ('strict_deterministic', 'fallback_included', 'blended_deterministic')),
  CHECK (visits >= 0),
  CHECK (attributed_orders >= 0),
  CHECK (attributed_revenue >= 0),
  CHECK (spend >= 0),
  CHECK (strict_deterministic_orders >= 0),
  CHECK (fallback_included_orders >= 0),
  CHECK (blended_deterministic_orders >= 0)
);

CREATE INDEX reporting_model_comparison_daily_model_date_idx
  ON reporting_model_comparison_daily (attribution_model, metric_date DESC, reporting_view);

CREATE INDEX reporting_model_comparison_daily_channel_idx
  ON reporting_model_comparison_daily (source, medium, campaign, metric_date DESC);

COMMIT;
