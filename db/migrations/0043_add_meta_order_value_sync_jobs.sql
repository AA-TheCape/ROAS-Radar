BEGIN;

CREATE TABLE meta_ads_order_value_sync_jobs (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  sync_date date NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  locked_by text,
  last_error text,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (connection_id, sync_date),
  CHECK (status IN ('pending', 'processing', 'retry', 'completed', 'failed'))
);

CREATE INDEX meta_ads_order_value_sync_jobs_status_available_idx
  ON meta_ads_order_value_sync_jobs (status, available_at, id);

COMMIT;
