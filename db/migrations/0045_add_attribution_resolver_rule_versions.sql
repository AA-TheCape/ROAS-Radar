BEGIN;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS resolver_rule_version text;

UPDATE attribution_results
SET resolver_rule_version = 'attribution_resolver_v1'
WHERE resolver_rule_version IS NULL;

ALTER TABLE attribution_results
  ALTER COLUMN resolver_rule_version SET NOT NULL;

ALTER TABLE attribution_results
  ALTER COLUMN resolver_rule_version SET DEFAULT 'attribution_resolver_v1';

ALTER TABLE attribution_results
  DROP CONSTRAINT IF EXISTS attribution_results_resolver_rule_version_chk;

ALTER TABLE attribution_results
  ADD CONSTRAINT attribution_results_resolver_rule_version_chk
  CHECK (
    resolver_rule_version IN (
      'attribution_resolver_v1',
      'attribution_resolver_v2'
    )
  );

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS attribution_resolver_rule_version text;

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_resolver_rule_version SET DEFAULT 'attribution_resolver_v1';

UPDATE shopify_orders
SET attribution_resolver_rule_version = 'attribution_resolver_v1'
WHERE attribution_tier IS NOT NULL
  AND attribution_resolver_rule_version IS NULL;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_resolver_rule_version_chk;

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_attribution_resolver_rule_version_chk
  CHECK (
    (
      attribution_tier IS NULL
      AND attribution_resolver_rule_version IS NULL
    )
    OR attribution_resolver_rule_version IN (
      'attribution_resolver_v1',
      'attribution_resolver_v2'
    )
  );

COMMIT;
