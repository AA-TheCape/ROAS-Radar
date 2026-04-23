BEGIN;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS attribution_snapshot jsonb,
  ADD COLUMN IF NOT EXISTS attribution_snapshot_updated_at timestamptz;

COMMIT;
