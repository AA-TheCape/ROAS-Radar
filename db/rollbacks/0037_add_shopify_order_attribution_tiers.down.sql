BEGIN;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_tier_chk;

ALTER TABLE shopify_orders
  DROP COLUMN IF EXISTS attribution_reason,
  DROP COLUMN IF EXISTS attribution_matched_at,
  DROP COLUMN IF EXISTS attribution_source,
  DROP COLUMN IF EXISTS attribution_tier;

COMMIT;
