BEGIN;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS attribution_source_id smallint,
  ADD COLUMN IF NOT EXISTS matching_method_id smallint,
  ADD COLUMN IF NOT EXISTS attribution_confidence_score numeric(5, 2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS attribution_confidence_contract_version text NOT NULL DEFAULT 'v1',
  ADD COLUMN IF NOT EXISTS last_attribution_run_at timestamptz;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS attribution_source_id smallint,
  ADD COLUMN IF NOT EXISTS matching_method_id smallint,
  ADD COLUMN IF NOT EXISTS confidence_contract_version text NOT NULL DEFAULT 'v1',
  ADD COLUMN IF NOT EXISTS last_attribution_run_at timestamptz;

ALTER TABLE attribution_order_credits
  ADD COLUMN IF NOT EXISTS confidence_contract_version text NOT NULL DEFAULT 'v1';

ALTER TABLE attribution_results
  ALTER COLUMN attribution_source_id SET DEFAULT 9,
  ALTER COLUMN matching_method_id SET DEFAULT 11,
  ALTER COLUMN confidence_contract_version SET DEFAULT 'v1',
  ALTER COLUMN last_attribution_run_at SET DEFAULT now();

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_confidence_contract_version SET DEFAULT 'v1';

ALTER TABLE attribution_order_credits
  ALTER COLUMN confidence_contract_version SET DEFAULT 'v1';

WITH order_metadata AS (
  SELECT
    orders.shopify_order_id,
    COALESCE(order_sources.id, fallback_source.id) AS attribution_source_id,
    COALESCE(methods.id, fallback_method.id) AS matching_method_id,
    LEAST(GREATEST(COALESCE(results.confidence_score, 0), 0), 1)::numeric(5, 2) AS attribution_confidence_score,
    COALESCE(orders.attribution_matched_at, results.attributed_at) AS last_attribution_run_at
  FROM shopify_orders orders
  LEFT JOIN attribution_results results
    ON results.shopify_order_id = orders.shopify_order_id
  LEFT JOIN attribution_sources order_sources
    ON order_sources.code = CASE COALESCE(orders.attribution_source, results.match_source)
      WHEN 'shopify_marketing_hint' THEN 'shopify_hint_fallback'
      WHEN 'deterministic_shopify_hint' THEN 'shopify_hint_fallback'
      WHEN 'stitched_identity_journey' THEN 'customer_identity'
      ELSE COALESCE(orders.attribution_source, results.match_source)
    END
  LEFT JOIN attribution_sources fallback_source
    ON fallback_source.code = 'unattributed'
  LEFT JOIN matching_methods methods
    ON methods.code = CASE COALESCE(orders.attribution_reason, results.attribution_reason, results.match_source)
      WHEN 'matched_by_identity_journey' THEN 'matched_by_customer_identity'
      WHEN 'synthetic_hint' THEN 'shopify_hint_derived'
      ELSE COALESCE(orders.attribution_reason, results.attribution_reason, results.match_source)
    END
  LEFT JOIN matching_methods fallback_method
    ON fallback_method.code = 'unknown'
)
UPDATE shopify_orders orders
SET
  attribution_source_id = metadata.attribution_source_id,
  matching_method_id = metadata.matching_method_id,
  attribution_confidence_score = metadata.attribution_confidence_score,
  last_attribution_run_at = metadata.last_attribution_run_at
FROM order_metadata metadata
WHERE orders.shopify_order_id = metadata.shopify_order_id
  AND (
    orders.attribution_source_id IS NULL
    OR orders.matching_method_id IS NULL
    OR orders.attribution_confidence_score IS DISTINCT FROM metadata.attribution_confidence_score
    OR orders.last_attribution_run_at IS DISTINCT FROM metadata.last_attribution_run_at
  );

WITH result_metadata AS (
  SELECT
    results.id,
    COALESCE(order_sources.id, fallback_source.id) AS attribution_source_id,
    COALESCE(methods.id, fallback_method.id) AS matching_method_id,
    results.attributed_at AS last_attribution_run_at
  FROM attribution_results results
  LEFT JOIN shopify_orders orders
    ON orders.shopify_order_id = results.shopify_order_id
  LEFT JOIN attribution_sources order_sources
    ON order_sources.code = CASE COALESCE(orders.attribution_source, results.match_source)
      WHEN 'shopify_marketing_hint' THEN 'shopify_hint_fallback'
      WHEN 'deterministic_shopify_hint' THEN 'shopify_hint_fallback'
      WHEN 'stitched_identity_journey' THEN 'customer_identity'
      ELSE COALESCE(orders.attribution_source, results.match_source)
    END
  LEFT JOIN attribution_sources fallback_source
    ON fallback_source.code = 'unattributed'
  LEFT JOIN matching_methods methods
    ON methods.code = CASE COALESCE(results.attribution_reason, results.match_source)
      WHEN 'matched_by_identity_journey' THEN 'matched_by_customer_identity'
      WHEN 'synthetic_hint' THEN 'shopify_hint_derived'
      ELSE COALESCE(results.attribution_reason, results.match_source)
    END
  LEFT JOIN matching_methods fallback_method
    ON fallback_method.code = 'unknown'
)
UPDATE attribution_results results
SET
  attribution_source_id = metadata.attribution_source_id,
  matching_method_id = metadata.matching_method_id,
  last_attribution_run_at = metadata.last_attribution_run_at
FROM result_metadata metadata
WHERE results.id = metadata.id
  AND (
    results.attribution_source_id IS NULL
    OR results.matching_method_id IS NULL
    OR results.last_attribution_run_at IS DISTINCT FROM metadata.last_attribution_run_at
  );

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_source_id SET DEFAULT 9,
  ALTER COLUMN matching_method_id SET DEFAULT 11;

UPDATE shopify_orders
SET
  attribution_source_id = 9,
  matching_method_id = 11,
  attribution_confidence_contract_version = 'v1'
WHERE attribution_source_id IS NULL
   OR matching_method_id IS NULL
   OR attribution_confidence_contract_version IS NULL;

UPDATE attribution_results
SET
  attribution_source_id = 9,
  matching_method_id = 11,
  confidence_contract_version = 'v1',
  last_attribution_run_at = COALESCE(last_attribution_run_at, attributed_at)
WHERE attribution_source_id IS NULL
   OR matching_method_id IS NULL
   OR confidence_contract_version IS NULL
   OR last_attribution_run_at IS NULL;

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_source_id SET NOT NULL,
  ALTER COLUMN matching_method_id SET NOT NULL,
  ALTER COLUMN attribution_confidence_contract_version SET NOT NULL,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_confidence_contract_version_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_source_id_fkey,
  DROP CONSTRAINT IF EXISTS shopify_orders_matching_method_id_fkey;

ALTER TABLE attribution_results
  ALTER COLUMN attribution_source_id SET NOT NULL,
  ALTER COLUMN matching_method_id SET NOT NULL,
  ALTER COLUMN confidence_contract_version SET NOT NULL,
  ALTER COLUMN last_attribution_run_at SET NOT NULL,
  DROP CONSTRAINT IF EXISTS attribution_results_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS attribution_results_confidence_contract_version_chk,
  DROP CONSTRAINT IF EXISTS attribution_results_attribution_source_id_fkey,
  DROP CONSTRAINT IF EXISTS attribution_results_matching_method_id_fkey;

UPDATE attribution_order_credits
SET confidence_contract_version = 'v1'
WHERE confidence_contract_version IS NULL;

ALTER TABLE attribution_order_credits
  ALTER COLUMN confidence_contract_version SET NOT NULL,
  DROP CONSTRAINT IF EXISTS attribution_order_credits_confidence_contract_version_chk;

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_confidence_score TYPE numeric(5, 2)
  USING LEAST(GREATEST(COALESCE(attribution_confidence_score, 0), 0), 1)::numeric(5, 2);

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_attribution_confidence_score_chk
  CHECK (
    attribution_confidence_score >= 0
    AND attribution_confidence_score <= 1
  ) NOT VALID,
  ADD CONSTRAINT shopify_orders_attribution_confidence_contract_version_chk
  CHECK (attribution_confidence_contract_version = 'v1') NOT VALID,
  ADD CONSTRAINT shopify_orders_attribution_source_id_fkey
  FOREIGN KEY (attribution_source_id) REFERENCES attribution_sources(id) ON DELETE RESTRICT NOT VALID,
  ADD CONSTRAINT shopify_orders_matching_method_id_fkey
  FOREIGN KEY (matching_method_id) REFERENCES matching_methods(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE attribution_results
  ADD CONSTRAINT attribution_results_confidence_score_chk
  CHECK (
    confidence_score >= 0
    AND confidence_score <= 1
  ) NOT VALID,
  ADD CONSTRAINT attribution_results_confidence_contract_version_chk
  CHECK (confidence_contract_version = 'v1') NOT VALID,
  ADD CONSTRAINT attribution_results_attribution_source_id_fkey
  FOREIGN KEY (attribution_source_id) REFERENCES attribution_sources(id) ON DELETE RESTRICT NOT VALID,
  ADD CONSTRAINT attribution_results_matching_method_id_fkey
  FOREIGN KEY (matching_method_id) REFERENCES matching_methods(id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE attribution_order_credits
  ADD CONSTRAINT attribution_order_credits_confidence_contract_version_chk
  CHECK (confidence_contract_version = 'v1') NOT VALID;

ALTER TABLE shopify_orders
  VALIDATE CONSTRAINT shopify_orders_attribution_confidence_score_chk,
  VALIDATE CONSTRAINT shopify_orders_attribution_confidence_contract_version_chk,
  VALIDATE CONSTRAINT shopify_orders_attribution_source_id_fkey,
  VALIDATE CONSTRAINT shopify_orders_matching_method_id_fkey;

ALTER TABLE attribution_results
  VALIDATE CONSTRAINT attribution_results_confidence_score_chk,
  VALIDATE CONSTRAINT attribution_results_confidence_contract_version_chk,
  VALIDATE CONSTRAINT attribution_results_attribution_source_id_fkey,
  VALIDATE CONSTRAINT attribution_results_matching_method_id_fkey;

ALTER TABLE attribution_order_credits
  VALIDATE CONSTRAINT attribution_order_credits_confidence_contract_version_chk;

CREATE INDEX IF NOT EXISTS shopify_orders_attribution_source_run_idx
  ON shopify_orders (attribution_source_id, last_attribution_run_at DESC, created_at_shopify DESC);

CREATE INDEX IF NOT EXISTS shopify_orders_matching_method_run_idx
  ON shopify_orders (matching_method_id, last_attribution_run_at DESC, created_at_shopify DESC);

CREATE INDEX IF NOT EXISTS shopify_orders_attribution_confidence_idx
  ON shopify_orders (attribution_confidence_score, created_at_shopify DESC);

CREATE INDEX IF NOT EXISTS attribution_results_source_attributed_idx
  ON attribution_results (attribution_source_id, attributed_at DESC);

CREATE INDEX IF NOT EXISTS attribution_results_method_attributed_idx
  ON attribution_results (matching_method_id, attributed_at DESC);

CREATE INDEX IF NOT EXISTS attribution_results_last_run_idx
  ON attribution_results (last_attribution_run_at DESC);

COMMIT;
