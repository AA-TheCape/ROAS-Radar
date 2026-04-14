BEGIN;

ALTER TABLE attribution_results
  ADD COLUMN IF NOT EXISTS model_version integer NOT NULL DEFAULT 1;

ALTER TABLE attribution_order_credits
  ADD COLUMN IF NOT EXISTS model_version integer NOT NULL DEFAULT 1;

CREATE TABLE attribution_jobs (
  id bigserial PRIMARY KEY,
  queue_key text NOT NULL UNIQUE,
  job_type text NOT NULL DEFAULT 'order',
  shopify_order_id text REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  requested_reason text NOT NULL,
  requested_model_version integer NOT NULL DEFAULT 1,
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  locked_by text,
  last_error text,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (job_type = 'order'),
  CHECK (status IN ('pending', 'processing', 'retry', 'completed')),
  CHECK (shopify_order_id IS NOT NULL)
);

CREATE INDEX attribution_jobs_status_available_idx
  ON attribution_jobs (status, available_at, id);

CREATE INDEX attribution_jobs_order_idx
  ON attribution_jobs (shopify_order_id);

CREATE INDEX attribution_jobs_locked_idx
  ON attribution_jobs (locked_at)
  WHERE status = 'processing';

COMMIT;
