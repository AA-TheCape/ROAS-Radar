BEGIN;

DO $$
DECLARE
  incomplete_order_count bigint;
  incomplete_result_count bigint;
  incomplete_credit_count bigint;
BEGIN
  SELECT COUNT(*) INTO incomplete_order_count
  FROM shopify_orders
  WHERE attribution_source_id IS NULL
    OR matching_method_id IS NULL
    OR attribution_confidence_score IS NULL
    OR attribution_confidence_contract_version IS DISTINCT FROM 'v1';

  SELECT COUNT(*) INTO incomplete_result_count
  FROM attribution_results
  WHERE attribution_source_id IS NULL
    OR matching_method_id IS NULL
    OR confidence_contract_version IS DISTINCT FROM 'v1'
    OR last_attribution_run_at IS NULL;

  SELECT COUNT(*) INTO incomplete_credit_count
  FROM attribution_order_credits
  WHERE confidence_contract_version IS DISTINCT FROM 'v1';

  IF incomplete_order_count > 0 OR incomplete_result_count > 0 OR incomplete_credit_count > 0 THEN
    RAISE EXCEPTION
      'confidence metadata contract blocked: incomplete_orders=%, incomplete_results=%, incomplete_credits=%',
      incomplete_order_count,
      incomplete_result_count,
      incomplete_credit_count;
  END IF;
END $$;

ALTER TABLE shopify_orders
  VALIDATE CONSTRAINT shopify_orders_attribution_confidence_score_chk,
  VALIDATE CONSTRAINT shopify_orders_attribution_confidence_contract_version_chk,
  VALIDATE CONSTRAINT shopify_orders_attribution_source_id_fkey,
  VALIDATE CONSTRAINT shopify_orders_matching_method_id_fkey,
  VALIDATE CONSTRAINT shopify_orders_attribution_source_method_pair_fkey;

ALTER TABLE attribution_results
  VALIDATE CONSTRAINT attribution_results_confidence_score_chk,
  VALIDATE CONSTRAINT attribution_results_confidence_contract_version_chk,
  VALIDATE CONSTRAINT attribution_results_attribution_source_id_fkey,
  VALIDATE CONSTRAINT attribution_results_matching_method_id_fkey,
  VALIDATE CONSTRAINT attribution_results_attribution_source_method_pair_fkey;

ALTER TABLE attribution_order_credits
  VALIDATE CONSTRAINT attribution_order_credits_confidence_contract_version_chk;

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_source_id SET NOT NULL,
  ALTER COLUMN matching_method_id SET NOT NULL,
  ALTER COLUMN attribution_confidence_score SET NOT NULL,
  ALTER COLUMN attribution_confidence_contract_version SET NOT NULL;

ALTER TABLE attribution_results
  ALTER COLUMN attribution_source_id SET NOT NULL,
  ALTER COLUMN matching_method_id SET NOT NULL,
  ALTER COLUMN confidence_contract_version SET NOT NULL,
  ALTER COLUMN last_attribution_run_at SET NOT NULL;

ALTER TABLE attribution_order_credits
  ALTER COLUMN confidence_contract_version SET NOT NULL;

COMMIT;
