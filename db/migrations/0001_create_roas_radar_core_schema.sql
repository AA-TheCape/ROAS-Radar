BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE visitors (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  anonymous_user_id text,
  external_customer_id text,
  email text,
  email_hash text,
  phone text,
  phone_hash text,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT visitors_anonymous_user_id_key UNIQUE (anonymous_user_id),
  CONSTRAINT visitors_external_customer_id_key UNIQUE (external_customer_id)
);

CREATE INDEX visitors_first_seen_at_idx ON visitors (first_seen_at DESC);
CREATE INDEX visitors_last_seen_at_idx ON visitors (last_seen_at DESC);
CREATE INDEX visitors_email_hash_idx ON visitors (email_hash) WHERE email_hash IS NOT NULL;
CREATE INDEX visitors_phone_hash_idx ON visitors (phone_hash) WHERE phone_hash IS NOT NULL;

CREATE TABLE ad_platforms (
  id bigserial PRIMARY KEY,
  platform_key text NOT NULL,
  display_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ad_platforms_platform_key_key UNIQUE (platform_key)
);

CREATE TABLE traffic_channels (
  id bigserial PRIMARY KEY,
  channel_key text NOT NULL,
  display_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT traffic_channels_channel_key_key UNIQUE (channel_key)
);

CREATE TABLE campaigns (
  id bigserial PRIMARY KEY,
  ad_platform_id bigint NOT NULL REFERENCES ad_platforms(id),
  external_campaign_id text,
  campaign_name text NOT NULL,
  campaign_status text,
  objective text,
  source text,
  medium text,
  currency_code char(3),
  starts_at timestamptz,
  ends_at timestamptz,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT campaigns_platform_external_campaign_key UNIQUE (ad_platform_id, external_campaign_id)
);

CREATE INDEX campaigns_platform_name_idx ON campaigns (ad_platform_id, campaign_name);
CREATE INDEX campaigns_source_medium_idx ON campaigns (source, medium);

CREATE TABLE ad_groups (
  id bigserial PRIMARY KEY,
  campaign_id bigint NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  external_ad_group_id text,
  ad_group_name text NOT NULL,
  ad_group_status text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ad_groups_campaign_external_ad_group_key UNIQUE (campaign_id, external_ad_group_id)
);

CREATE INDEX ad_groups_campaign_name_idx ON ad_groups (campaign_id, ad_group_name);

CREATE TABLE creatives (
  id bigserial PRIMARY KEY,
  ad_group_id bigint REFERENCES ad_groups(id) ON DELETE SET NULL,
  campaign_id bigint NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
  external_creative_id text,
  creative_name text NOT NULL,
  creative_type text,
  destination_url text,
  preview_url text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT creatives_campaign_external_creative_key UNIQUE (campaign_id, external_creative_id)
);

CREATE INDEX creatives_campaign_name_idx ON creatives (campaign_id, creative_name);
CREATE INDEX creatives_ad_group_idx ON creatives (ad_group_id) WHERE ad_group_id IS NOT NULL;

CREATE TABLE sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visitor_id uuid NOT NULL REFERENCES visitors(id) ON DELETE CASCADE,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  landing_page_url text,
  referrer_url text,
  user_agent text,
  ip_hash text,
  country_code char(2),
  region_code text,
  city text,
  device_type text,
  browser text,
  operating_system text,
  initial_channel_id bigint REFERENCES traffic_channels(id),
  initial_campaign_id bigint REFERENCES campaigns(id),
  initial_ad_group_id bigint REFERENCES ad_groups(id),
  initial_creative_id bigint REFERENCES creatives(id),
  initial_utm_source text,
  initial_utm_medium text,
  initial_utm_campaign text,
  initial_utm_content text,
  initial_utm_term text,
  initial_gclid text,
  initial_fbclid text,
  initial_ttclid text,
  initial_msclkid text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX sessions_id_visitor_id_key ON sessions (id, visitor_id);
CREATE INDEX sessions_visitor_started_at_idx ON sessions (visitor_id, started_at DESC);
CREATE INDEX sessions_started_at_brin_idx ON sessions USING BRIN (started_at);
CREATE INDEX sessions_initial_campaign_idx ON sessions (initial_campaign_id, started_at DESC) WHERE initial_campaign_id IS NOT NULL;
CREATE INDEX sessions_initial_click_ids_idx ON sessions (initial_gclid, initial_fbclid, initial_ttclid, initial_msclkid);

CREATE TABLE touchpoints (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visitor_id uuid NOT NULL REFERENCES visitors(id) ON DELETE CASCADE,
  session_id uuid NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
  ad_platform_id bigint REFERENCES ad_platforms(id),
  channel_id bigint REFERENCES traffic_channels(id),
  campaign_id bigint REFERENCES campaigns(id),
  ad_group_id bigint REFERENCES ad_groups(id),
  creative_id bigint REFERENCES creatives(id),
  touchpoint_type text NOT NULL,
  occurred_at timestamptz NOT NULL,
  page_url text,
  referrer_url text,
  event_name text,
  is_paid boolean NOT NULL DEFAULT false,
  is_direct boolean NOT NULL DEFAULT false,
  position_in_session integer,
  source text,
  medium text,
  campaign_name text,
  content text,
  term text,
  click_id_type text,
  click_id_value text,
  gclid text,
  fbclid text,
  ttclid text,
  msclkid text,
  cart_token text,
  checkout_token text,
  client_event_id text,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT touchpoints_client_event_id_key UNIQUE (client_event_id)
);

CREATE UNIQUE INDEX touchpoints_id_visitor_id_key ON touchpoints (id, visitor_id);
CREATE INDEX touchpoints_visitor_occurred_at_idx ON touchpoints (visitor_id, occurred_at DESC);
CREATE INDEX touchpoints_session_occurred_at_idx ON touchpoints (session_id, occurred_at DESC);
CREATE INDEX touchpoints_campaign_occurred_at_idx ON touchpoints (campaign_id, occurred_at DESC) WHERE campaign_id IS NOT NULL;
CREATE INDEX touchpoints_creative_occurred_at_idx ON touchpoints (creative_id, occurred_at DESC) WHERE creative_id IS NOT NULL;
CREATE INDEX touchpoints_checkout_token_idx ON touchpoints (checkout_token) WHERE checkout_token IS NOT NULL;
CREATE INDEX touchpoints_cart_token_idx ON touchpoints (cart_token) WHERE cart_token IS NOT NULL;
CREATE INDEX touchpoints_click_id_idx ON touchpoints (click_id_type, click_id_value) WHERE click_id_value IS NOT NULL;
CREATE INDEX touchpoints_occurred_at_brin_idx ON touchpoints USING BRIN (occurred_at);

CREATE TABLE orders (
  id bigserial PRIMARY KEY,
  shopify_order_id text,
  order_number text NOT NULL,
  visitor_id uuid REFERENCES visitors(id),
  source_session_id uuid,
  source_touchpoint_id uuid,
  external_customer_id text,
  email text,
  email_hash text,
  currency_code char(3) NOT NULL,
  subtotal_amount numeric(12, 2) NOT NULL DEFAULT 0,
  discount_amount numeric(12, 2) NOT NULL DEFAULT 0,
  shipping_amount numeric(12, 2) NOT NULL DEFAULT 0,
  tax_amount numeric(12, 2) NOT NULL DEFAULT 0,
  total_amount numeric(12, 2) NOT NULL DEFAULT 0,
  financial_status text,
  fulfillment_status text,
  source_name text,
  cart_token text,
  checkout_token text,
  ordered_at timestamptz NOT NULL,
  paid_at timestamptz,
  processed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT orders_shopify_order_id_key UNIQUE (shopify_order_id),
  CONSTRAINT orders_order_number_key UNIQUE (order_number),
  CONSTRAINT orders_source_session_visitor_fkey FOREIGN KEY (source_session_id, visitor_id) REFERENCES sessions(id, visitor_id),
  CONSTRAINT orders_source_touchpoint_visitor_fkey FOREIGN KEY (source_touchpoint_id, visitor_id) REFERENCES touchpoints(id, visitor_id)
);

CREATE UNIQUE INDEX orders_id_visitor_id_key ON orders (id, visitor_id) WHERE visitor_id IS NOT NULL;
CREATE INDEX orders_visitor_ordered_at_idx ON orders (visitor_id, ordered_at DESC) WHERE visitor_id IS NOT NULL;
CREATE INDEX orders_source_session_idx ON orders (source_session_id) WHERE source_session_id IS NOT NULL;
CREATE INDEX orders_source_touchpoint_idx ON orders (source_touchpoint_id) WHERE source_touchpoint_id IS NOT NULL;
CREATE INDEX orders_email_hash_ordered_at_idx ON orders (email_hash, ordered_at DESC) WHERE email_hash IS NOT NULL;
CREATE INDEX orders_checkout_token_idx ON orders (checkout_token) WHERE checkout_token IS NOT NULL;
CREATE INDEX orders_ordered_at_brin_idx ON orders USING BRIN (ordered_at);

CREATE TABLE order_line_items (
  id bigserial PRIMARY KEY,
  order_id bigint NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  line_number integer NOT NULL,
  external_line_item_id text,
  sku text,
  product_id text,
  variant_id text,
  product_name text NOT NULL,
  variant_name text,
  quantity integer NOT NULL CHECK (quantity > 0),
  unit_price numeric(12, 2) NOT NULL DEFAULT 0,
  discount_amount numeric(12, 2) NOT NULL DEFAULT 0,
  total_amount numeric(12, 2) NOT NULL DEFAULT 0,
  currency_code char(3) NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT order_line_items_order_line_number_key UNIQUE (order_id, line_number),
  CONSTRAINT order_line_items_external_line_item_key UNIQUE NULLS NOT DISTINCT (order_id, external_line_item_id)
);

CREATE INDEX order_line_items_order_idx ON order_line_items (order_id);
CREATE INDEX order_line_items_sku_idx ON order_line_items (sku) WHERE sku IS NOT NULL;
CREATE INDEX order_line_items_product_idx ON order_line_items (product_id) WHERE product_id IS NOT NULL;

CREATE TABLE attribution_models (
  id bigserial PRIMARY KEY,
  model_key text NOT NULL,
  display_name text NOT NULL,
  description text,
  default_config jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT attribution_models_model_key_key UNIQUE (model_key)
);

INSERT INTO attribution_models (model_key, display_name, description, default_config)
VALUES
  ('first_touch', 'First Touch', 'Assigns full credit to the first eligible touchpoint.', '{}'::jsonb),
  ('last_touch', 'Last Touch', 'Assigns full credit to the last eligible touchpoint.', '{}'::jsonb),
  ('linear', 'Linear', 'Splits credit evenly across all eligible touchpoints.', '{}'::jsonb),
  ('time_decay', 'Time Decay', 'Weights touchpoints by recency using a configurable half-life.', '{"half_life_hours":168}'::jsonb),
  ('position_based', 'Position Based', 'Weights first and last touch more heavily than middle touches.', '{"first_touch_weight":0.4,"last_touch_weight":0.4,"middle_weight":0.2}'::jsonb),
  ('rule_based_weighted', 'Rule Based Weighted', 'Uses configurable rules to weight click ids, channels, and events.', '{"weights":[]}'::jsonb);

CREATE TABLE attribution_results (
  id bigserial PRIMARY KEY,
  order_id bigint NOT NULL,
  visitor_id uuid NOT NULL REFERENCES visitors(id) ON DELETE CASCADE,
  model_id bigint NOT NULL REFERENCES attribution_models(id),
  touchpoint_id uuid REFERENCES touchpoints(id) ON DELETE SET NULL,
  session_id uuid REFERENCES sessions(id) ON DELETE SET NULL,
  conversion_type text NOT NULL DEFAULT 'order',
  conversion_at timestamptz NOT NULL,
  lookback_window_days integer NOT NULL DEFAULT 7,
  touchpoint_position integer,
  touchpoint_count integer NOT NULL DEFAULT 1,
  weight numeric(8, 6) NOT NULL CHECK (weight >= 0 AND weight <= 1),
  revenue_credit numeric(12, 2) NOT NULL DEFAULT 0,
  order_credit numeric(12, 6) NOT NULL DEFAULT 0,
  is_primary boolean NOT NULL DEFAULT false,
  attributed_source text,
  attributed_medium text,
  attributed_campaign text,
  attributed_content text,
  attributed_term text,
  attributed_click_id_type text,
  attributed_click_id_value text,
  decision_reason text NOT NULL,
  rule_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT attribution_results_order_visitor_fkey FOREIGN KEY (order_id, visitor_id) REFERENCES orders(id, visitor_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX attribution_results_order_model_touchpoint_key
  ON attribution_results (order_id, model_id, COALESCE(touchpoint_id, '00000000-0000-0000-0000-000000000000'::uuid), conversion_type);
CREATE INDEX attribution_results_model_conversion_idx ON attribution_results (model_id, conversion_at DESC);
CREATE INDEX attribution_results_order_model_idx ON attribution_results (order_id, model_id);
CREATE INDEX attribution_results_touchpoint_idx ON attribution_results (touchpoint_id) WHERE touchpoint_id IS NOT NULL;
CREATE INDEX attribution_results_session_idx ON attribution_results (session_id) WHERE session_id IS NOT NULL;
CREATE INDEX attribution_results_campaign_model_idx ON attribution_results (attributed_campaign, model_id, conversion_at DESC) WHERE attributed_campaign IS NOT NULL;
CREATE INDEX attribution_results_conversion_at_brin_idx ON attribution_results USING BRIN (conversion_at);

CREATE TABLE ad_spend_daily (
  id bigserial PRIMARY KEY,
  spend_date date NOT NULL,
  ad_platform_id bigint NOT NULL REFERENCES ad_platforms(id),
  campaign_id bigint NOT NULL REFERENCES campaigns(id),
  ad_group_id bigint REFERENCES ad_groups(id),
  creative_id bigint REFERENCES creatives(id),
  currency_code char(3) NOT NULL,
  impressions bigint NOT NULL DEFAULT 0,
  clicks bigint NOT NULL DEFAULT 0,
  spend_amount numeric(12, 2) NOT NULL DEFAULT 0,
  conversions bigint,
  conversion_value numeric(12, 2),
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX ad_spend_daily_grain_key
  ON ad_spend_daily (
    spend_date,
    ad_platform_id,
    campaign_id,
    COALESCE(ad_group_id, -1),
    COALESCE(creative_id, -1)
  );
CREATE INDEX ad_spend_daily_date_campaign_idx ON ad_spend_daily (spend_date DESC, campaign_id);
CREATE INDEX ad_spend_daily_date_platform_idx ON ad_spend_daily (spend_date DESC, ad_platform_id);
CREATE INDEX ad_spend_daily_creative_idx ON ad_spend_daily (creative_id, spend_date DESC) WHERE creative_id IS NOT NULL;
CREATE INDEX ad_spend_daily_spend_date_brin_idx ON ad_spend_daily USING BRIN (spend_date);

COMMIT;
