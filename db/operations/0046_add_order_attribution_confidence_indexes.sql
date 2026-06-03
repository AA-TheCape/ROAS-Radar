-- Run with psql after migration 0046 is applied. Do not run through src/db/migrate.ts:
-- the migration runner wraps every file in one transaction, and PostgreSQL rejects
-- CREATE INDEX CONCURRENTLY inside a transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS shopify_orders_attribution_source_run_idx
  ON shopify_orders (attribution_source_id, last_attribution_run_at DESC, created_at_shopify DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS shopify_orders_matching_method_run_idx
  ON shopify_orders (matching_method_id, last_attribution_run_at DESC, created_at_shopify DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS shopify_orders_attribution_confidence_idx
  ON shopify_orders (attribution_confidence_score, created_at_shopify DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS attribution_results_source_attributed_idx
  ON attribution_results (attribution_source_id, attributed_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS attribution_results_method_attributed_idx
  ON attribution_results (matching_method_id, attributed_at DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS attribution_results_last_run_idx
  ON attribution_results (last_attribution_run_at DESC);
