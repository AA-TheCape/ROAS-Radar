BEGIN;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS attribution_source_id smallint,
  ADD COLUMN IF NOT EXISTS matching_method_id smallint,
  ADD COLUMN IF NOT EXISTS attribution_confidence_score numeric(5, 2),
  ADD COLUMN IF NOT EXISTS attribution_confidence_contract_version text DEFAULT 'v1',
  ADD COLUMN IF NOT EXISTS last_attribution_run_at timestamptz;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS attribution_source_id smallint,
  ADD COLUMN IF NOT EXISTS matching_method_id smallint,
  ADD COLUMN IF NOT EXISTS confidence_contract_version text DEFAULT 'v1',
  ADD COLUMN IF NOT EXISTS last_attribution_run_at timestamptz;

ALTER TABLE attribution_order_credits
  ADD COLUMN IF NOT EXISTS confidence_contract_version text DEFAULT 'v1';

ALTER TABLE attribution_results
  ALTER COLUMN attribution_source_id SET DEFAULT 9,
  ALTER COLUMN matching_method_id SET DEFAULT 11,
  ALTER COLUMN confidence_contract_version SET DEFAULT 'v1',
  ALTER COLUMN last_attribution_run_at SET DEFAULT now();

ALTER TABLE shopify_orders
  ALTER COLUMN attribution_source_id SET DEFAULT 9,
  ALTER COLUMN matching_method_id SET DEFAULT 11,
  ALTER COLUMN attribution_confidence_score SET DEFAULT 0,
  ALTER COLUMN attribution_confidence_contract_version SET DEFAULT 'v1';

ALTER TABLE attribution_order_credits
  ALTER COLUMN confidence_contract_version SET DEFAULT 'v1';

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_confidence_contract_version_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_source_method_pair_fkey,
  DROP CONSTRAINT IF EXISTS shopify_orders_attribution_source_id_fkey,
  DROP CONSTRAINT IF EXISTS shopify_orders_matching_method_id_fkey;

ALTER TABLE attribution_results
  DROP CONSTRAINT IF EXISTS attribution_results_confidence_score_chk,
  DROP CONSTRAINT IF EXISTS attribution_results_confidence_contract_version_chk,
  DROP CONSTRAINT IF EXISTS attribution_results_attribution_source_method_pair_fkey,
  DROP CONSTRAINT IF EXISTS attribution_results_attribution_source_id_fkey,
  DROP CONSTRAINT IF EXISTS attribution_results_matching_method_id_fkey;

ALTER TABLE attribution_order_credits
  DROP CONSTRAINT IF EXISTS attribution_order_credits_confidence_contract_version_chk;

ALTER TABLE matching_methods
  DROP CONSTRAINT IF EXISTS matching_methods_attribution_source_id_id_key;

ALTER TABLE matching_methods
  ADD CONSTRAINT matching_methods_attribution_source_id_id_key
  UNIQUE (attribution_source_id, id);

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_attribution_confidence_score_chk
  CHECK (
    attribution_confidence_score IS NULL
    OR (
      attribution_confidence_score >= 0
      AND attribution_confidence_score <= 1
    )
  ) NOT VALID,
  ADD CONSTRAINT shopify_orders_attribution_confidence_contract_version_chk
  CHECK (attribution_confidence_contract_version = 'v1') NOT VALID,
  ADD CONSTRAINT shopify_orders_attribution_source_id_fkey
  FOREIGN KEY (attribution_source_id) REFERENCES attribution_sources(id) ON DELETE RESTRICT NOT VALID,
  ADD CONSTRAINT shopify_orders_matching_method_id_fkey
  FOREIGN KEY (matching_method_id) REFERENCES matching_methods(id) ON DELETE RESTRICT NOT VALID,
  ADD CONSTRAINT shopify_orders_attribution_source_method_pair_fkey
  FOREIGN KEY (attribution_source_id, matching_method_id)
  REFERENCES matching_methods(attribution_source_id, id) ON DELETE RESTRICT NOT VALID;

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
  FOREIGN KEY (matching_method_id) REFERENCES matching_methods(id) ON DELETE RESTRICT NOT VALID,
  ADD CONSTRAINT attribution_results_attribution_source_method_pair_fkey
  FOREIGN KEY (attribution_source_id, matching_method_id)
  REFERENCES matching_methods(attribution_source_id, id) ON DELETE RESTRICT NOT VALID;

ALTER TABLE attribution_order_credits
  ADD CONSTRAINT attribution_order_credits_confidence_contract_version_chk
  CHECK (confidence_contract_version = 'v1') NOT VALID;

COMMIT;
