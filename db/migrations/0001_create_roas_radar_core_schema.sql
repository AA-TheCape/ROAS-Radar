BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tracking_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  anonymous_user_id text,
  landing_page text,
  referrer_url text,
  initial_utm_source text,
  initial_utm_medium text,
  initial_utm_campaign text,
  initial_utm_content text,
  initial_utm_term text,
  initial_gclid text,
  initial_fbclid text,
  initial_ttclid text,
  initial_msclkid text,
  user_agent text,
  ip_hash text
);

CREATE INDEX tracking_sessions_last_seen_at_idx
  ON tracking_sessions (last_seen_at DESC);
CREATE INDEX tracking_sessions_source_campaign_idx
  ON tracking_sessions (initial_utm_source, initial_utm_campaign);

CREATE TABLE tracking_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id uuid NOT NULL REFERENCES tracking_sessions(id) ON DELETE CASCADE,
  event_type text NOT NULL,
  occurred_at timestamptz NOT NULL,
  page_url text,
  referrer_url text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text,
  utm_term text,
  gclid text,
  fbclid text,
  ttclid text,
  msclkid text,
  shopify_cart_token text,
  shopify_checkout_token text,
  client_event_id text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX tracking_events_client_event_id_uidx
  ON tracking_events (client_event_id)
  WHERE client_event_id IS NOT NULL;
CREATE INDEX tracking_events_session_occurred_at_idx
  ON tracking_events (session_id, occurred_at DESC);
CREATE INDEX tracking_events_type_occurred_at_idx
  ON tracking_events (event_type, occurred_at DESC);
CREATE INDEX tracking_events_checkout_token_idx
  ON tracking_events (shopify_checkout_token)
  WHERE shopify_checkout_token IS NOT NULL;

CREATE TABLE shopify_customers (
  id bigserial PRIMARY KEY,
  shopify_customer_id text NOT NULL UNIQUE,
  email text,
  phone text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shopify_orders (
  id bigserial PRIMARY KEY,
  shopify_order_id text NOT NULL UNIQUE,
  shopify_order_number text,
  shopify_customer_id text,
  email text,
  currency_code text NOT NULL,
  subtotal_price numeric(12, 2) NOT NULL DEFAULT 0,
  total_price numeric(12, 2) NOT NULL DEFAULT 0,
  financial_status text,
  fulfillment_status text,
  processed_at timestamptz,
  created_at_shopify timestamptz,
  updated_at_shopify timestamptz,
  landing_session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  checkout_token text,
  cart_token text,
  source_name text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX shopify_orders_created_at_shopify_idx
  ON shopify_orders (created_at_shopify DESC);
CREATE INDEX shopify_orders_landing_session_id_idx
  ON shopify_orders (landing_session_id)
  WHERE landing_session_id IS NOT NULL;
CREATE INDEX shopify_orders_checkout_token_idx
  ON shopify_orders (checkout_token)
  WHERE checkout_token IS NOT NULL;
CREATE INDEX shopify_orders_email_idx
  ON shopify_orders (email)
  WHERE email IS NOT NULL;

CREATE TABLE attribution_results (
  id bigserial PRIMARY KEY,
  shopify_order_id text NOT NULL UNIQUE REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  session_id uuid REFERENCES tracking_sessions(id) ON DELETE SET NULL,
  attribution_model text NOT NULL,
  attributed_source text,
  attributed_medium text,
  attributed_campaign text,
  attributed_content text,
  attributed_term text,
  attributed_click_id_type text,
  attributed_click_id_value text,
  confidence_score numeric(5, 2) NOT NULL DEFAULT 0,
  attribution_reason text NOT NULL,
  attributed_at timestamptz NOT NULL DEFAULT now(),
  reprocess_version integer NOT NULL DEFAULT 1
);

CREATE INDEX attribution_results_attributed_at_idx
  ON attribution_results (attributed_at DESC);
CREATE INDEX attribution_results_source_campaign_idx
  ON attribution_results (attributed_source, attributed_campaign);

CREATE TABLE shopify_webhook_receipts (
  id bigserial PRIMARY KEY,
  topic text NOT NULL,
  shop_domain text NOT NULL,
  webhook_id text,
  payload_hash text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz,
  status text NOT NULL,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX shopify_webhook_receipts_webhook_id_uidx
  ON shopify_webhook_receipts (webhook_id)
  WHERE webhook_id IS NOT NULL;
CREATE INDEX shopify_webhook_receipts_topic_received_at_idx
  ON shopify_webhook_receipts (topic, received_at DESC);

CREATE TABLE daily_campaign_metrics (
  metric_date date NOT NULL,
  source text NOT NULL,
  medium text NOT NULL,
  campaign text NOT NULL,
  content text NOT NULL DEFAULT '',
  visits integer NOT NULL DEFAULT 0,
  orders integer NOT NULL DEFAULT 0,
  revenue numeric(12, 2) NOT NULL DEFAULT 0,
  last_computed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (metric_date, source, medium, campaign, content)
);

COMMIT;
