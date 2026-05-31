BEGIN;

CREATE TABLE IF NOT EXISTS attribution_recovery_audit_logs (
  id bigserial PRIMARY KEY,
  recovery_run_id uuid REFERENCES recovery_job_runs(id) ON DELETE SET NULL,
  job_type text NOT NULL,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  changed_by text NOT NULL,
  change_reason text NOT NULL,
  before_attribution jsonb NOT NULL,
  after_attribution jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT attribution_recovery_audit_job_type_length_chk
    CHECK (char_length(job_type) BETWEEN 1 AND 128),
  CONSTRAINT attribution_recovery_audit_changed_by_length_chk
    CHECK (char_length(changed_by) BETWEEN 1 AND 255),
  CONSTRAINT attribution_recovery_audit_reason_length_chk
    CHECK (char_length(change_reason) BETWEEN 1 AND 255)
);

CREATE INDEX attribution_recovery_audit_order_created_idx
  ON attribution_recovery_audit_logs (shopify_order_id, created_at DESC);

CREATE INDEX attribution_recovery_audit_run_created_idx
  ON attribution_recovery_audit_logs (recovery_run_id, created_at DESC)
  WHERE recovery_run_id IS NOT NULL;

COMMIT;
