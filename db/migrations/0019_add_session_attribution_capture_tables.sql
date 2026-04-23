BEGIN;

CREATE TABLE session_attribution_identities (
  roas_radar_session_id uuid PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  first_captured_at timestamptz NOT NULL DEFAULT now(),
  last_captured_at timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  customer_identity_id uuid REFERENCES customer_identities(id) ON DELETE SET NULL,
  landing_url text,
  referrer_url text,
  initial_utm_source text,
  initial_utm_medium text,
  initial_utm_campaign text,
  initial_utm_content text,
  initial_utm_term text,
  initial_gclid text,
  initial_gbraid text,
  initial_wbraid text,
  initial_fbclid text,
  initial_ttclid text,
  initial_msclkid text,
  CHECK (retained_until >= first_captured_at),
  CHECK (landing_url IS NULL OR char_length(landing_url) <= 2048),
  CHECK (referrer_url IS NULL OR char_length(referrer_url) <= 2048),
  CHECK (initial_utm_source IS NULL OR char_length(initial_utm_source) <= 255),
  CHECK (initial_utm_medium IS NULL OR char_length(initial_utm_medium) <= 255),
  CHECK (initial_utm_campaign IS NULL OR char_length(initial_utm_campaign) <= 255),
  CHECK (initial_utm_content IS NULL OR char_length(initial_utm_content) <= 255),
  CHECK (initial_utm_term IS NULL OR char_length(initial_utm_term) <= 255),
  CHECK (initial_gclid IS NULL OR char_length(initial_gclid) <= 255),
  CHECK (initial_gbraid IS NULL OR char_length(initial_gbraid) <= 255),
  CHECK (initial_wbraid IS NULL OR char_length(initial_wbraid) <= 255),
  CHECK (initial_fbclid IS NULL OR char_length(initial_fbclid) <= 255),
  CHECK (initial_ttclid IS NULL OR char_length(initial_ttclid) <= 255),
  CHECK (initial_msclkid IS NULL OR char_length(initial_msclkid) <= 255)
);

CREATE INDEX session_attribution_identities_retained_until_idx
  ON session_attribution_identities (retained_until);

CREATE INDEX session_attribution_identities_customer_identity_idx
  ON session_attribution_identities (customer_identity_id)
  WHERE customer_identity_id IS NOT NULL;

CREATE TABLE session_attribution_touch_events (
  id bigserial PRIMARY KEY,
  roas_radar_session_id uuid NOT NULL REFERENCES session_attribution_identities(roas_radar_session_id) ON DELETE CASCADE,
  event_type text NOT NULL,
  occurred_at timestamptz NOT NULL,
  captured_at timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  page_url text,
  referrer_url text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text,
  utm_term text,
  gclid text,
  gbraid text,
  wbraid text,
  fbclid text,
  ttclid text,
  msclkid text,
  shopify_cart_token text,
  shopify_checkout_token text,
  ingestion_source text NOT NULL DEFAULT 'browser',
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  CHECK (retained_until >= captured_at),
  CHECK (page_url IS NULL OR char_length(page_url) <= 2048),
  CHECK (referrer_url IS NULL OR char_length(referrer_url) <= 2048),
  CHECK (utm_source IS NULL OR char_length(utm_source) <= 255),
  CHECK (utm_medium IS NULL OR char_length(utm_medium) <= 255),
  CHECK (utm_campaign IS NULL OR char_length(utm_campaign) <= 255),
  CHECK (utm_content IS NULL OR char_length(utm_content) <= 255),
  CHECK (utm_term IS NULL OR char_length(utm_term) <= 255),
  CHECK (gclid IS NULL OR char_length(gclid) <= 255),
  CHECK (gbraid IS NULL OR char_length(gbraid) <= 255),
  CHECK (wbraid IS NULL OR char_length(wbraid) <= 255),
  CHECK (fbclid IS NULL OR char_length(fbclid) <= 255),
  CHECK (ttclid IS NULL OR char_length(ttclid) <= 255),
  CHECK (msclkid IS NULL OR char_length(msclkid) <= 255),
  CHECK (shopify_cart_token IS NULL OR char_length(shopify_cart_token) <= 255),
  CHECK (shopify_checkout_token IS NULL OR char_length(shopify_checkout_token) <= 255),
  CHECK (char_length(ingestion_source) <= 64)
);

CREATE INDEX session_attribution_touch_events_session_occurred_at_idx
  ON session_attribution_touch_events (roas_radar_session_id, occurred_at DESC);

CREATE INDEX session_attribution_touch_events_occurred_at_idx
  ON session_attribution_touch_events (occurred_at DESC);

CREATE INDEX session_attribution_touch_events_retained_until_idx
  ON session_attribution_touch_events (retained_until);

CREATE INDEX session_attribution_touch_events_checkout_token_idx
  ON session_attribution_touch_events (shopify_checkout_token)
  WHERE shopify_checkout_token IS NOT NULL;

CREATE INDEX session_attribution_touch_events_cart_token_idx
  ON session_attribution_touch_events (shopify_cart_token)
  WHERE shopify_cart_token IS NOT NULL;

CREATE TABLE order_attribution_links (
  id bigserial PRIMARY KEY,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  roas_radar_session_id uuid NOT NULL REFERENCES session_attribution_identities(roas_radar_session_id) ON DELETE CASCADE,
  attribution_model text NOT NULL,
  link_type text NOT NULL,
  attribution_reason text NOT NULL,
  linked_at timestamptz NOT NULL DEFAULT now(),
  order_occurred_at timestamptz,
  is_primary boolean NOT NULL DEFAULT false,
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (retained_until >= created_at),
  CHECK (char_length(shopify_order_id) <= 255),
  CHECK (char_length(attribution_model) <= 64),
  CHECK (char_length(link_type) <= 64),
  CHECK (char_length(attribution_reason) <= 255)
);

CREATE UNIQUE INDEX order_attribution_links_order_session_model_type_uidx
  ON order_attribution_links (shopify_order_id, roas_radar_session_id, attribution_model, link_type);

CREATE UNIQUE INDEX order_attribution_links_primary_order_model_uidx
  ON order_attribution_links (shopify_order_id, attribution_model)
  WHERE is_primary;

CREATE INDEX order_attribution_links_order_lookup_idx
  ON order_attribution_links (shopify_order_id, is_primary DESC, linked_at DESC);

CREATE INDEX order_attribution_links_session_lookup_idx
  ON order_attribution_links (roas_radar_session_id, linked_at DESC);

CREATE INDEX order_attribution_links_retained_until_idx
  ON order_attribution_links (retained_until);

COMMIT;
