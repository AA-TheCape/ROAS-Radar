BEGIN;

ALTER TABLE meta_ads_connections
  ADD COLUMN deterministic_view_impression_sync_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN deterministic_view_impression_last_planned_for date;

CREATE TABLE meta_ads_deterministic_sync_checkpoints (
  connection_id bigint PRIMARY KEY REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  last_completed_date date,
  last_cursor text,
  last_synced_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE meta_ads_deterministic_sync_jobs (
  id bigserial PRIMARY KEY,
  connection_id bigint NOT NULL REFERENCES meta_ads_connections(id) ON DELETE CASCADE,
  sync_date date NOT NULL,
  cursor text,
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

CREATE INDEX meta_ads_deterministic_sync_jobs_status_available_idx
  ON meta_ads_deterministic_sync_jobs (status, available_at, id);

CREATE INDEX meta_ads_deterministic_sync_jobs_connection_date_idx
  ON meta_ads_deterministic_sync_jobs (connection_id, sync_date DESC);

CREATE INDEX meta_ads_connections_deterministic_enabled_idx
  ON meta_ads_connections (deterministic_view_impression_sync_enabled, status, id)
  WHERE deterministic_view_impression_sync_enabled = true;

COMMIT;
