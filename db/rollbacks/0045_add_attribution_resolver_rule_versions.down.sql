BEGIN;

ALTER TABLE attribution_results
  DROP CONSTRAINT IF EXISTS attribution_results_resolver_rule_version_chk,
  DROP COLUMN IF EXISTS resolver_rule_version;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_resolver_rule_version_chk,
  DROP COLUMN IF EXISTS attribution_resolver_rule_version;

COMMIT;
