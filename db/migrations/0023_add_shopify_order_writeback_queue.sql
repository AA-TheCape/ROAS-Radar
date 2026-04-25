BEGIN;

CREATE TABLE shopify_order_writeback_jobs (
  id bigserial PRIMARY KEY,
  queue_key text NOT NULL UNIQUE,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  requested_reason text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  locked_by text,
  last_error text,
  completed_at timestamptz,
  dead_lettered_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('pending', 'processing', 'retry', 'completed', 'failed'))
);

CREATE INDEX shopify_order_writeback_jobs_status_available_idx
  ON shopify_order_writeback_jobs (status, available_at, id);

CREATE INDEX shopify_order_writeback_jobs_order_idx
  ON shopify_order_writeback_jobs (shopify_order_id);

CREATE INDEX shopify_order_writeback_jobs_locked_idx
  ON shopify_order_writeback_jobs (locked_at)
  WHERE status = 'processing';

CREATE INDEX shopify_order_writeback_jobs_dead_letter_idx
  ON shopify_order_writeback_jobs (dead_lettered_at DESC)
  WHERE status = 'failed';

COMMIT;
