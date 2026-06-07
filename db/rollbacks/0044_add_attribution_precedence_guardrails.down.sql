BEGIN;

DROP TRIGGER IF EXISTS attribution_decision_artifacts_linked_order_precedence_guard
  ON attribution_decision_artifacts;

DROP TRIGGER IF EXISTS attribution_results_canonical_precedence_guard
  ON attribution_results;

DROP TRIGGER IF EXISTS shopify_orders_canonical_precedence_guard
  ON shopify_orders;

DROP TRIGGER IF EXISTS attribution_decision_artifacts_precedence_guard
  ON attribution_decision_artifacts;

DROP FUNCTION IF EXISTS roas_enforce_decision_artifact_linked_order_precedence();
DROP FUNCTION IF EXISTS roas_enforce_attribution_result_canonical_precedence();
DROP FUNCTION IF EXISTS roas_enforce_shopify_order_canonical_precedence();
DROP FUNCTION IF EXISTS roas_validate_shopify_order_canonical_precedence(text);
DROP FUNCTION IF EXISTS roas_validate_attribution_decision_artifact_precedence();
DROP FUNCTION IF EXISTS roas_canonical_tier_from_reason(text);
DROP FUNCTION IF EXISTS roas_attribution_tier_rank(text);

COMMIT;
