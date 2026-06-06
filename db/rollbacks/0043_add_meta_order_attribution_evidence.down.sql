BEGIN;

ALTER TABLE attribution_results
  DROP CONSTRAINT IF EXISTS attribution_results_decision_artifact_fk,
  DROP CONSTRAINT IF EXISTS attribution_results_meta_attribution_evidence_fk,
  DROP CONSTRAINT IF EXISTS attribution_results_meta_attribution_summary_link_chk,
  DROP CONSTRAINT IF EXISTS attribution_results_meta_attribution_evaluation_outcome_chk;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_latest_attribution_decision_artifact_fk,
  DROP CONSTRAINT IF EXISTS shopify_orders_meta_attribution_evidence_fk,
  DROP CONSTRAINT IF EXISTS shopify_orders_meta_attribution_summary_link_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_meta_attribution_confidence_label_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_meta_attribution_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_meta_attribution_evaluation_outcome_chk;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_tier_chk;

UPDATE shopify_orders
SET attribution_tier = 'ga4_fallback'
WHERE attribution_tier = 'platform_reported_meta';

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

ALTER TABLE attribution_results
  DROP COLUMN IF EXISTS attribution_decision_artifact_id,
  DROP COLUMN IF EXISTS meta_attribution_affected_canonical,
  DROP COLUMN IF EXISTS meta_attribution_evaluation_outcome,
  DROP COLUMN IF EXISTS meta_attribution_evidence_id;

ALTER TABLE shopify_orders
  DROP COLUMN IF EXISTS latest_attribution_decision_artifact_id,
  DROP COLUMN IF EXISTS meta_attribution_affected_canonical,
  DROP COLUMN IF EXISTS meta_attribution_present,
  DROP COLUMN IF EXISTS meta_attribution_confidence_label,
  DROP COLUMN IF EXISTS meta_attribution_confidence_score,
  DROP COLUMN IF EXISTS meta_attribution_evaluation_outcome,
  DROP COLUMN IF EXISTS meta_attribution_evidence_id;

DROP TABLE IF EXISTS attribution_decision_artifacts;
DROP TABLE IF EXISTS meta_order_attribution_evidence;

COMMIT;
