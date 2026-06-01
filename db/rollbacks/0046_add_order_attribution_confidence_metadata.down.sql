BEGIN;

DROP INDEX IF EXISTS attribution_results_last_run_idx;
DROP INDEX IF EXISTS attribution_results_method_attributed_idx;
DROP INDEX IF EXISTS attribution_results_source_attributed_idx;
DROP INDEX IF EXISTS shopify_orders_attribution_confidence_idx;
DROP INDEX IF EXISTS shopify_orders_matching_method_run_idx;
DROP INDEX IF EXISTS shopify_orders_attribution_source_run_idx;

ALTER TABLE attribution_results
  DROP CONSTRAINT IF EXISTS attribution_results_matching_method_id_fkey,
  DROP CONSTRAINT IF EXISTS attribution_results_attribution_source_id_fkey,
  DROP CONSTRAINT IF EXISTS attribution_results_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS attribution_results_confidence_contract_version_chk,
  DROP COLUMN IF EXISTS confidence_contract_version,
  DROP COLUMN IF EXISTS last_attribution_run_at,
  DROP COLUMN IF EXISTS matching_method_id,
  DROP COLUMN IF EXISTS attribution_source_id;

ALTER TABLE attribution_order_credits
  DROP CONSTRAINT IF EXISTS attribution_order_credits_confidence_contract_version_chk,
  DROP COLUMN IF EXISTS confidence_contract_version;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_matching_method_id_fkey,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_source_id_fkey,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_confidence_contract_version_chk,
  DROP COLUMN IF EXISTS attribution_confidence_contract_version,
  DROP COLUMN IF EXISTS last_attribution_run_at,
  DROP COLUMN IF EXISTS attribution_confidence_score,
  DROP COLUMN IF EXISTS matching_method_id,
  DROP COLUMN IF EXISTS attribution_source_id;

COMMIT;
