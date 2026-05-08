BEGIN;

CREATE TABLE attribution_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  attribution_spec_version text NOT NULL DEFAULT 'v1',
  run_status text NOT NULL DEFAULT 'pending',
  trigger_source text NOT NULL DEFAULT 'manual',
  started_at_utc timestamptz,
  completed_at_utc timestamptz,
  failed_at_utc timestamptz,
  lookback_click_window_days integer NOT NULL DEFAULT 28,
  lookback_view_window_days integer NOT NULL DEFAULT 7,
  run_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  CHECK (attribution_spec_version = 'v1'),
  CHECK (run_status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
  CHECK (char_length(trigger_source) <= 64),
  CHECK (lookback_click_window_days = 28),
  CHECK (lookback_view_window_days = 7),
  CHECK (retained_until >= created_at_utc),
  CHECK (completed_at_utc IS NULL OR started_at_utc IS NULL OR completed_at_utc >= started_at_utc),
  CHECK (failed_at_utc IS NULL OR started_at_utc IS NULL OR failed_at_utc >= started_at_utc)
);

CREATE INDEX attribution_runs_status_created_idx
  ON attribution_runs (run_status, created_at_utc DESC);

CREATE INDEX attribution_runs_retained_until_idx
  ON attribution_runs (retained_until);

CREATE TABLE attribution_order_inputs (
  run_id uuid NOT NULL REFERENCES attribution_runs(id) ON DELETE CASCADE,
  schema_version integer NOT NULL DEFAULT 1,
  order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  order_platform text NOT NULL DEFAULT 'shopify',
  order_occurred_at_utc timestamptz NOT NULL,
  order_timestamp_source text NOT NULL,
  currency_code text NOT NULL,
  subtotal_amount numeric(12, 2) NOT NULL,
  total_amount numeric(12, 2) NOT NULL,
  landing_session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  checkout_token text,
  cart_token text,
  shopify_customer_id text,
  email_hash text,
  source_name text,
  identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL,
  raw_order_ref jsonb NOT NULL DEFAULT '{}'::jsonb,
  normalized_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  PRIMARY KEY (run_id, order_id),
  CHECK (schema_version = 1),
  CHECK (order_platform = 'shopify'),
  CHECK (order_timestamp_source IN ('processed_at', 'created_at_shopify', 'ingested_at')),
  CHECK (char_length(currency_code) BETWEEN 3 AND 16),
  CHECK (subtotal_amount >= 0),
  CHECK (total_amount >= 0),
  CHECK (checkout_token IS NULL OR char_length(checkout_token) <= 255),
  CHECK (cart_token IS NULL OR char_length(cart_token) <= 255),
  CHECK (shopify_customer_id IS NULL OR char_length(shopify_customer_id) <= 255),
  CHECK (email_hash IS NULL OR char_length(email_hash) = 64),
  CHECK (source_name IS NULL OR char_length(source_name) <= 255),
  CHECK (retained_until >= normalized_at_utc)
);

CREATE INDEX attribution_order_inputs_run_order_time_idx
  ON attribution_order_inputs (run_id, order_occurred_at_utc DESC, order_id);

CREATE INDEX attribution_order_inputs_landing_session_idx
  ON attribution_order_inputs (landing_session_id, order_occurred_at_utc DESC)
  WHERE landing_session_id IS NOT NULL;

CREATE INDEX attribution_order_inputs_identity_journey_idx
  ON attribution_order_inputs (identity_journey_id, order_occurred_at_utc DESC)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX attribution_order_inputs_checkout_token_idx
  ON attribution_order_inputs (checkout_token)
  WHERE checkout_token IS NOT NULL;

CREATE INDEX attribution_order_inputs_cart_token_idx
  ON attribution_order_inputs (cart_token)
  WHERE cart_token IS NOT NULL;

CREATE TABLE attribution_touchpoint_inputs (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL,
  order_id text NOT NULL,
  schema_version integer NOT NULL DEFAULT 1,
  touchpoint_id text NOT NULL,
  session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  identity_journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL,
  touchpoint_occurred_at_utc timestamptz NOT NULL,
  touchpoint_captured_at_utc timestamptz NOT NULL,
  touchpoint_source_kind text NOT NULL,
  ingestion_source text NOT NULL,
  source text,
  medium text,
  campaign text,
  content text,
  term text,
  click_id_type text,
  click_id_value text,
  evidence_source text NOT NULL,
  is_direct boolean NOT NULL,
  engagement_type text NOT NULL,
  is_synthetic boolean NOT NULL DEFAULT false,
  is_eligible boolean NOT NULL DEFAULT true,
  ineligibility_reason text,
  attribution_reason text,
  attribution_hint jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '180 days'),
  UNIQUE (run_id, order_id, touchpoint_id),
  FOREIGN KEY (run_id, order_id) REFERENCES attribution_order_inputs(run_id, order_id) ON DELETE CASCADE,
  CHECK (schema_version = 1),
  CHECK (char_length(touchpoint_id) <= 255),
  CHECK (touchpoint_source_kind IN ('session_first_touch', 'session_event', 'shopify_hint')),
  CHECK (ingestion_source IN ('browser', 'server', 'request_query', 'shopify_marketing_hint')),
  CHECK (source IS NULL OR char_length(source) <= 255),
  CHECK (medium IS NULL OR char_length(medium) <= 255),
  CHECK (campaign IS NULL OR char_length(campaign) <= 255),
  CHECK (content IS NULL OR char_length(content) <= 255),
  CHECK (term IS NULL OR char_length(term) <= 255),
  CHECK (click_id_type IS NULL OR click_id_type IN ('gclid', 'gbraid', 'wbraid', 'fbclid', 'ttclid', 'msclkid')),
  CHECK (click_id_value IS NULL OR char_length(click_id_value) <= 255),
  CHECK (evidence_source IN (
    'landing_session_id',
    'checkout_token',
    'cart_token',
    'customer_identity',
    'shopify_marketing_hint',
    'ga4_fallback'
  )),
  CHECK (engagement_type IN ('click', 'view', 'unknown')),
  CHECK (is_eligible OR ineligibility_reason IS NOT NULL),
  CHECK (ineligibility_reason IS NULL OR char_length(ineligibility_reason) <= 255),
  CHECK (attribution_reason IS NULL OR char_length(attribution_reason) <= 255),
  CHECK (retained_until >= created_at_utc)
);

CREATE INDEX attribution_touchpoint_inputs_run_order_eligibility_idx
  ON attribution_touchpoint_inputs (run_id, order_id, is_eligible, engagement_type, touchpoint_occurred_at_utc DESC);

CREATE INDEX attribution_touchpoint_inputs_session_occurred_at_idx
  ON attribution_touchpoint_inputs (session_id, touchpoint_occurred_at_utc DESC)
  WHERE session_id IS NOT NULL;

CREATE INDEX attribution_touchpoint_inputs_identity_journey_idx
  ON attribution_touchpoint_inputs (identity_journey_id, touchpoint_occurred_at_utc DESC)
  WHERE identity_journey_id IS NOT NULL;

CREATE INDEX attribution_touchpoint_inputs_source_campaign_idx
  ON attribution_touchpoint_inputs (source, medium, campaign, touchpoint_occurred_at_utc DESC)
  WHERE is_eligible;

CREATE INDEX attribution_touchpoint_inputs_retained_until_idx
  ON attribution_touchpoint_inputs (retained_until);

CREATE TABLE attribution_model_summaries (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES attribution_runs(id) ON DELETE CASCADE,
  attribution_spec_version text NOT NULL DEFAULT 'v1',
  order_id text NOT NULL,
  model_key text NOT NULL,
  allocation_status text NOT NULL,
  winner_touchpoint_id text,
  winner_session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  winner_evidence_source text,
  winner_attribution_reason text,
  total_credit_weight numeric(12, 8) NOT NULL DEFAULT 0,
  total_revenue_credited numeric(12, 2) NOT NULL DEFAULT 0,
  touchpoint_count_considered integer NOT NULL DEFAULT 0,
  eligible_click_count integer NOT NULL DEFAULT 0,
  eligible_view_count integer NOT NULL DEFAULT 0,
  lookback_rule_applied text NOT NULL,
  winner_selection_rule text NOT NULL,
  direct_suppression_applied boolean NOT NULL DEFAULT false,
  deterministic_block_applied boolean NOT NULL DEFAULT false,
  normalization_failures_count integer NOT NULL DEFAULT 0,
  order_occurred_at_utc timestamptz NOT NULL,
  generated_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  UNIQUE (run_id, order_id, model_key),
  FOREIGN KEY (run_id, order_id) REFERENCES attribution_order_inputs(run_id, order_id) ON DELETE CASCADE,
  CHECK (attribution_spec_version = 'v1'),
  CHECK (model_key IN (
    'first_touch',
    'last_touch',
    'last_non_direct',
    'linear',
    'clicks_only',
    'hinted_fallback_only'
  )),
  CHECK (allocation_status IN ('attributed', 'no_eligible_touches', 'blocked_by_deterministic', 'unattributed')),
  CHECK (winner_touchpoint_id IS NULL OR char_length(winner_touchpoint_id) <= 255),
  CHECK (winner_evidence_source IS NULL OR winner_evidence_source IN (
    'landing_session_id',
    'checkout_token',
    'cart_token',
    'customer_identity',
    'shopify_marketing_hint',
    'ga4_fallback'
  )),
  CHECK (winner_attribution_reason IS NULL OR char_length(winner_attribution_reason) <= 255),
  CHECK (total_credit_weight >= 0 AND total_credit_weight <= 1.0),
  CHECK (total_revenue_credited >= 0),
  CHECK (touchpoint_count_considered >= 0),
  CHECK (eligible_click_count >= 0),
  CHECK (eligible_view_count >= 0),
  CHECK (normalization_failures_count >= 0),
  CHECK (lookback_rule_applied IN ('28d_click', '7d_view', 'mixed')),
  CHECK (winner_selection_rule IN (
    'first_touch',
    'last_touch',
    'last_non_direct',
    'clicks_only',
    'hinted_fallback_only',
    'linear'
  )),
  CHECK (retained_until >= generated_at_utc)
);

CREATE INDEX attribution_model_summaries_model_order_time_idx
  ON attribution_model_summaries (model_key, order_occurred_at_utc DESC);

CREATE INDEX attribution_model_summaries_run_model_status_idx
  ON attribution_model_summaries (run_id, model_key, allocation_status);

CREATE INDEX attribution_model_summaries_winner_session_idx
  ON attribution_model_summaries (winner_session_id, generated_at_utc DESC)
  WHERE winner_session_id IS NOT NULL;

CREATE INDEX attribution_model_summaries_retained_until_idx
  ON attribution_model_summaries (retained_until);

CREATE TABLE attribution_model_credits (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL,
  order_id text NOT NULL,
  model_key text NOT NULL,
  attribution_spec_version text NOT NULL DEFAULT 'v1',
  touchpoint_id text NOT NULL,
  session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  touchpoint_position integer NOT NULL,
  occurred_at_utc timestamptz NOT NULL,
  source text,
  medium text,
  campaign text,
  content text,
  term text,
  click_id_type text,
  click_id_value text,
  touch_type text NOT NULL,
  is_direct boolean NOT NULL,
  evidence_source text NOT NULL,
  is_synthetic boolean NOT NULL DEFAULT false,
  attribution_reason text NOT NULL,
  credit_weight numeric(12, 8) NOT NULL,
  revenue_credit numeric(12, 2) NOT NULL,
  is_primary boolean NOT NULL DEFAULT false,
  match_source text,
  confidence_label text NOT NULL DEFAULT 'none',
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '400 days'),
  UNIQUE (run_id, order_id, model_key, touchpoint_id),
  FOREIGN KEY (run_id, order_id, model_key)
    REFERENCES attribution_model_summaries(run_id, order_id, model_key)
    ON DELETE CASCADE,
  FOREIGN KEY (run_id, order_id, touchpoint_id)
    REFERENCES attribution_touchpoint_inputs(run_id, order_id, touchpoint_id)
    ON DELETE CASCADE,
  CHECK (attribution_spec_version = 'v1'),
  CHECK (model_key IN (
    'first_touch',
    'last_touch',
    'last_non_direct',
    'linear',
    'clicks_only',
    'hinted_fallback_only'
  )),
  CHECK (char_length(touchpoint_id) <= 255),
  CHECK (touchpoint_position >= 1),
  CHECK (source IS NULL OR char_length(source) <= 255),
  CHECK (medium IS NULL OR char_length(medium) <= 255),
  CHECK (campaign IS NULL OR char_length(campaign) <= 255),
  CHECK (content IS NULL OR char_length(content) <= 255),
  CHECK (term IS NULL OR char_length(term) <= 255),
  CHECK (click_id_type IS NULL OR click_id_type IN ('gclid', 'gbraid', 'wbraid', 'fbclid', 'ttclid', 'msclkid')),
  CHECK (click_id_value IS NULL OR char_length(click_id_value) <= 255),
  CHECK (touch_type IN ('click', 'view')),
  CHECK (evidence_source IN (
    'landing_session_id',
    'checkout_token',
    'cart_token',
    'customer_identity',
    'shopify_marketing_hint',
    'ga4_fallback'
  )),
  CHECK (char_length(attribution_reason) <= 255),
  CHECK (credit_weight > 0 AND credit_weight <= 1.0),
  CHECK (revenue_credit >= 0),
  CHECK (match_source IS NULL OR char_length(match_source) <= 64),
  CHECK (confidence_label IN ('none', 'low', 'medium', 'high')),
  CHECK (retained_until >= created_at_utc)
);

CREATE INDEX attribution_model_credits_order_model_idx
  ON attribution_model_credits (order_id, model_key, is_primary DESC, occurred_at_utc DESC);

CREATE INDEX attribution_model_credits_session_occurred_at_idx
  ON attribution_model_credits (session_id, occurred_at_utc DESC)
  WHERE session_id IS NOT NULL;

CREATE INDEX attribution_model_credits_reporting_idx
  ON attribution_model_credits (model_key, source, medium, campaign, occurred_at_utc DESC);

CREATE INDEX attribution_model_credits_run_model_idx
  ON attribution_model_credits (run_id, model_key, occurred_at_utc DESC);

CREATE INDEX attribution_model_credits_retained_until_idx
  ON attribution_model_credits (retained_until);

CREATE TABLE attribution_explain_records (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES attribution_runs(id) ON DELETE CASCADE,
  order_id text NOT NULL,
  touchpoint_id text,
  model_key text,
  explain_stage text NOT NULL,
  decision text NOT NULL,
  decision_reason text NOT NULL,
  details_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  order_occurred_at_utc timestamptz,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '180 days'),
  FOREIGN KEY (run_id, order_id) REFERENCES attribution_order_inputs(run_id, order_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id, order_id, touchpoint_id)
    REFERENCES attribution_touchpoint_inputs(run_id, order_id, touchpoint_id)
    ON DELETE CASCADE,
  CHECK (touchpoint_id IS NULL OR char_length(touchpoint_id) <= 255),
  CHECK (model_key IS NULL OR model_key IN (
    'first_touch',
    'last_touch',
    'last_non_direct',
    'linear',
    'clicks_only',
    'hinted_fallback_only'
  )),
  CHECK (explain_stage IN ('candidate_extraction', 'eligibility_filter', 'model_scoring', 'fallback')),
  CHECK (decision IN ('included', 'excluded', 'winner', 'fallback_used', 'no_credit')),
  CHECK (char_length(decision_reason) <= 255),
  CHECK (retained_until >= created_at_utc)
);

CREATE INDEX attribution_explain_records_run_order_stage_idx
  ON attribution_explain_records (run_id, order_id, explain_stage, created_at_utc DESC);

CREATE INDEX attribution_explain_records_touchpoint_idx
  ON attribution_explain_records (touchpoint_id, created_at_utc DESC)
  WHERE touchpoint_id IS NOT NULL;

CREATE INDEX attribution_explain_records_stage_decision_idx
  ON attribution_explain_records (explain_stage, decision, created_at_utc DESC);

CREATE INDEX attribution_explain_records_retained_until_idx
  ON attribution_explain_records (retained_until);

COMMIT;
