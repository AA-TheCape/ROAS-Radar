BEGIN;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS match_source text,
  ADD COLUMN IF NOT EXISTS confidence_label text,
  ADD COLUMN IF NOT EXISTS ga4_client_id text,
  ADD COLUMN IF NOT EXISTS ga4_session_id text;

ALTER TABLE attribution_order_credits
  ADD COLUMN IF NOT EXISTS match_source text,
  ADD COLUMN IF NOT EXISTS confidence_label text;

UPDATE attribution_results
SET
  match_source = CASE
    WHEN attribution_reason = 'matched_by_landing_session' THEN 'landing_session_id'
    WHEN attribution_reason = 'matched_by_checkout_token' THEN 'checkout_token'
    WHEN attribution_reason = 'matched_by_cart_token' THEN 'cart_token'
    WHEN attribution_reason = 'matched_by_customer_identity' THEN 'customer_identity'
    WHEN attribution_reason = 'shopify_hint_derived' THEN 'shopify_hint_fallback'
    ELSE 'unattributed'
  END,
  confidence_label = CASE
    WHEN confidence_score >= 0.90 THEN 'high'
    WHEN confidence_score >= 0.60 THEN 'medium'
    WHEN confidence_score > 0 THEN 'low'
    ELSE 'none'
  END
WHERE match_source IS NULL
   OR confidence_label IS NULL;

UPDATE attribution_order_credits
SET
  match_source = CASE
    WHEN attribution_reason = 'matched_by_landing_session' THEN 'landing_session_id'
    WHEN attribution_reason = 'matched_by_checkout_token' THEN 'checkout_token'
    WHEN attribution_reason = 'matched_by_cart_token' THEN 'cart_token'
    WHEN attribution_reason = 'matched_by_customer_identity' THEN 'customer_identity'
    WHEN attribution_reason = 'shopify_hint_derived' THEN 'shopify_hint_fallback'
    ELSE 'unattributed'
  END,
  confidence_label = CASE
    WHEN attribution_reason IN ('matched_by_landing_session', 'matched_by_checkout_token', 'matched_by_cart_token') THEN 'high'
    WHEN attribution_reason = 'matched_by_customer_identity' THEN 'medium'
    WHEN attribution_reason = 'shopify_hint_derived' THEN 'low'
    ELSE 'none'
  END
WHERE match_source IS NULL
   OR confidence_label IS NULL;

ALTER TABLE attribution_results
  ALTER COLUMN match_source SET NOT NULL,
  ALTER COLUMN confidence_label SET NOT NULL;

ALTER TABLE attribution_order_credits
  ALTER COLUMN match_source SET NOT NULL,
  ALTER COLUMN confidence_label SET NOT NULL;

COMMIT;
