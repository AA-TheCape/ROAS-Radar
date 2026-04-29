BEGIN;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS attribution_tier text,
  ADD COLUMN IF NOT EXISTS attribution_source text,
  ADD COLUMN IF NOT EXISTS attribution_matched_at timestamptz,
  ADD COLUMN IF NOT EXISTS attribution_reason text;

WITH derived_attribution AS (
  SELECT
    results.shopify_order_id,
    CASE
      WHEN results.attribution_reason = 'shopify_hint_derived' THEN 'deterministic_shopify_hint'
      WHEN results.attribution_reason IN (
        'matched_by_landing_session',
        'matched_by_checkout_token',
        'matched_by_cart_token',
        'matched_by_customer_identity'
      ) THEN 'deterministic_first_party'
      WHEN results.attribution_reason = 'unattributed' THEN 'unattributed'
      ELSE 'ga4_fallback'
    END AS attribution_tier,
    CASE
      WHEN results.attribution_reason = 'shopify_hint_derived' THEN 'shopify_marketing_hint'
      WHEN results.attribution_reason = 'matched_by_landing_session' THEN 'landing_session_id'
      WHEN results.attribution_reason = 'matched_by_checkout_token' THEN 'checkout_token'
      WHEN results.attribution_reason = 'matched_by_cart_token' THEN 'cart_token'
      WHEN results.attribution_reason = 'matched_by_customer_identity' THEN 'stitched_identity_journey'
      WHEN results.attribution_reason = 'unattributed' THEN 'unattributed'
      ELSE 'ga4_fallback'
    END AS attribution_source,
    results.attributed_at AS attribution_matched_at,
    results.attribution_reason
  FROM attribution_results results
)
UPDATE shopify_orders orders
SET
  attribution_tier = COALESCE(derived.attribution_tier, 'unattributed'),
  attribution_source = COALESCE(derived.attribution_source, 'unattributed'),
  attribution_matched_at = derived.attribution_matched_at,
  attribution_reason = COALESCE(derived.attribution_reason, orders.attribution_reason)
FROM derived_attribution derived
WHERE orders.shopify_order_id = derived.shopify_order_id;

UPDATE shopify_orders
SET
  attribution_tier = COALESCE(attribution_tier, 'unattributed'),
  attribution_source = COALESCE(attribution_source, 'unattributed'),
  attribution_reason = COALESCE(attribution_reason, 'unattributed')
WHERE attribution_tier IS NULL
   OR attribution_source IS NULL
   OR attribution_reason IS NULL;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_tier_chk;

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_attribution_tier_chk
  CHECK (
    attribution_tier IN (
      'deterministic_first_party',
      'deterministic_shopify_hint',
      'ga4_fallback',
      'unattributed'
    )
  ) NOT VALID;

ALTER TABLE shopify_orders
  VALIDATE CONSTRAINT shopify_orders_attribution_tier_chk;

COMMIT;
