BEGIN;

CREATE TABLE IF NOT EXISTS ga4_fallback_shadow_comparisons (
  shopify_order_id text PRIMARY KEY REFERENCES shopify_orders (shopify_order_id) ON DELETE CASCADE,
  order_occurred_at timestamptz NOT NULL,
  order_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  rollout_mode text NOT NULL CHECK (rollout_mode IN ('shadow')),
  current_match_source text NOT NULL,
  current_confidence_score numeric(4, 2) NOT NULL,
  current_confidence_label text NOT NULL,
  current_attribution_reason text NOT NULL,
  current_source text,
  current_medium text,
  current_campaign text,
  shadow_match_source text NOT NULL,
  shadow_confidence_score numeric(4, 2) NOT NULL,
  shadow_confidence_label text NOT NULL,
  shadow_attribution_reason text NOT NULL,
  shadow_source text,
  shadow_medium text,
  shadow_campaign text,
  shadow_ga4_client_id text,
  shadow_ga4_session_id text,
  shadow_would_change_winner boolean NOT NULL DEFAULT false,
  evaluated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ga4_fallback_shadow_comparisons_order_occurred_at_idx
  ON ga4_fallback_shadow_comparisons (order_occurred_at DESC);

CREATE INDEX IF NOT EXISTS ga4_fallback_shadow_comparisons_shadow_match_source_idx
  ON ga4_fallback_shadow_comparisons (shadow_match_source, order_occurred_at DESC);

COMMIT;
