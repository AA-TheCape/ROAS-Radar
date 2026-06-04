BEGIN;

CREATE TABLE IF NOT EXISTS attribution_sources (
  id smallint PRIMARY KEY,
  code text NOT NULL,
  display_label text NOT NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  UNIQUE (code),
  CHECK (id > 0),
  CHECK (code = lower(code)),
  CHECK (code ~ '^[a-z0-9_]+$'),
  CHECK (char_length(code) <= 64),
  CHECK (char_length(display_label) BETWEEN 1 AND 120),
  CHECK (updated_at_utc >= created_at_utc)
);

CREATE INDEX IF NOT EXISTS attribution_sources_active_code_idx
  ON attribution_sources (is_active, code);

CREATE TABLE IF NOT EXISTS matching_methods (
  id smallint PRIMARY KEY,
  attribution_source_id smallint NOT NULL REFERENCES attribution_sources(id) ON DELETE RESTRICT,
  code text NOT NULL,
  display_label text NOT NULL,
  is_active boolean NOT NULL DEFAULT true,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  updated_at_utc timestamptz NOT NULL DEFAULT now(),
  UNIQUE (code),
  UNIQUE (attribution_source_id, code),
  CHECK (id > 0),
  CHECK (code = lower(code)),
  CHECK (code ~ '^[a-z0-9_]+$'),
  CHECK (char_length(code) <= 64),
  CHECK (char_length(display_label) BETWEEN 1 AND 120),
  CHECK (updated_at_utc >= created_at_utc)
);

CREATE INDEX IF NOT EXISTS matching_methods_source_active_idx
  ON matching_methods (attribution_source_id, is_active, code);

INSERT INTO attribution_sources (
  id,
  code,
  display_label,
  is_active
)
VALUES
  (1, 'landing_session_id', 'Landing session ID', true),
  (2, 'checkout_token', 'Checkout token', true),
  (3, 'cart_token', 'Cart token', true),
  (4, 'customer_identity', 'Customer identity', true),
  (5, 'stitched_identity_journey', 'Stitched identity journey', true),
  (6, 'shopify_marketing_hint', 'Shopify marketing hint', true),
  (7, 'shopify_hint_fallback', 'Shopify hint fallback', true),
  (8, 'ga4_fallback', 'GA4 fallback', true),
  (9, 'unattributed', 'Unattributed', true)
ON CONFLICT (id) DO UPDATE
SET
  code = EXCLUDED.code,
  display_label = EXCLUDED.display_label,
  is_active = EXCLUDED.is_active,
  updated_at_utc = now();

INSERT INTO matching_methods (
  id,
  attribution_source_id,
  code,
  display_label,
  is_active
)
VALUES
  (1, 1, 'matched_by_landing_session', 'Matched by landing session', true),
  (2, 2, 'matched_by_checkout_token', 'Matched by checkout token', true),
  (3, 3, 'matched_by_cart_token', 'Matched by cart token', true),
  (4, 4, 'matched_by_customer_identity', 'Matched by customer identity', true),
  (5, 5, 'matched_by_identity_journey', 'Matched by identity journey', true),
  (6, 7, 'shopify_hint_derived', 'Shopify hint derived', true),
  (7, 8, 'ga4_fallback_derived', 'GA4 fallback derived', true),
  (8, 8, 'ga4_fallback_match', 'GA4 fallback match', true),
  (9, 9, 'unattributed', 'Unattributed', true),
  (10, 7, 'synthetic_hint', 'Synthetic hint', true),
  (11, 9, 'unknown', 'Unknown', true)
ON CONFLICT (id) DO UPDATE
SET
  attribution_source_id = EXCLUDED.attribution_source_id,
  code = EXCLUDED.code,
  display_label = EXCLUDED.display_label,
  is_active = EXCLUDED.is_active,
  updated_at_utc = now();

COMMIT;
