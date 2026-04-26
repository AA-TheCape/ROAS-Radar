BEGIN;

CREATE TABLE IF NOT EXISTS customer_journey (
  session_id uuid PRIMARY KEY REFERENCES tracking_sessions(id) ON DELETE CASCADE,
  identity_journey_id uuid NOT NULL REFERENCES identity_journeys(id) ON DELETE CASCADE,
  authoritative_shopify_customer_id text,
  primary_email_hash text,
  primary_phone_hash text,
  journey_status text NOT NULL,
  journey_merge_version integer NOT NULL,
  journey_created_at timestamptz NOT NULL,
  journey_last_resolved_at timestamptz NOT NULL,
  journey_lookback_window_started_at timestamptz NOT NULL,
  journey_lookback_window_expires_at timestamptz NOT NULL,
  journey_last_touch_eligible_at timestamptz NOT NULL,
  journey_started_at timestamptz NOT NULL,
  journey_ended_at timestamptz NOT NULL,
  journey_session_number integer NOT NULL,
  reverse_journey_session_number integer NOT NULL,
  journey_session_count integer NOT NULL,
  journey_event_start_number integer NOT NULL,
  journey_event_end_number integer NOT NULL,
  journey_event_count integer NOT NULL,
  journey_order_count integer NOT NULL,
  journey_order_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  journey_first_order_at timestamptz,
  journey_last_order_at timestamptz,
  session_started_at timestamptz NOT NULL,
  session_ended_at timestamptz NOT NULL,
  first_event_at timestamptz,
  last_event_at timestamptz,
  session_event_count integer NOT NULL DEFAULT 0,
  page_view_count integer NOT NULL DEFAULT 0,
  product_view_count integer NOT NULL DEFAULT 0,
  add_to_cart_count integer NOT NULL DEFAULT 0,
  checkout_started_count integer NOT NULL DEFAULT 0,
  session_order_count integer NOT NULL DEFAULT 0,
  session_order_revenue numeric(12, 2) NOT NULL DEFAULT 0,
  session_first_order_at timestamptz,
  session_last_order_at timestamptz,
  is_first_session boolean NOT NULL DEFAULT false,
  is_last_session boolean NOT NULL DEFAULT false,
  is_converting_session boolean NOT NULL DEFAULT false,
  anonymous_user_id text,
  landing_page text,
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
  source_max_updated_at timestamptz NOT NULL,
  refreshed_at timestamptz NOT NULL DEFAULT now(),
  CHECK (journey_status IN ('active', 'quarantined', 'merged', 'conflicted')),
  CHECK (journey_merge_version >= 1),
  CHECK (journey_session_number >= 1),
  CHECK (reverse_journey_session_number >= 1),
  CHECK (journey_session_count >= 1),
  CHECK (journey_event_start_number >= 0),
  CHECK (journey_event_end_number >= 0),
  CHECK (journey_event_count >= 0),
  CHECK (journey_order_count >= 0),
  CHECK (session_event_count >= 0),
  CHECK (page_view_count >= 0),
  CHECK (product_view_count >= 0),
  CHECK (add_to_cart_count >= 0),
  CHECK (checkout_started_count >= 0),
  CHECK (session_order_count >= 0),
  CHECK (journey_order_revenue >= 0),
  CHECK (session_order_revenue >= 0),
  CHECK (journey_ended_at >= journey_started_at),
  CHECK (session_ended_at >= session_started_at),
  CHECK (journey_last_touch_eligible_at >= journey_lookback_window_started_at),
  CHECK (journey_lookback_window_expires_at >= journey_lookback_window_started_at)
);

CREATE INDEX IF NOT EXISTS customer_journey_identity_journey_idx
  ON customer_journey (identity_journey_id, journey_session_number ASC);

CREATE INDEX IF NOT EXISTS customer_journey_source_updated_idx
  ON customer_journey (source_max_updated_at DESC);

CREATE INDEX IF NOT EXISTS customer_journey_conversion_idx
  ON customer_journey (is_converting_session, session_last_order_at DESC);

CREATE INDEX IF NOT EXISTS customer_journey_channel_idx
  ON customer_journey (utm_source, utm_medium, utm_campaign, session_started_at DESC);

COMMIT;
