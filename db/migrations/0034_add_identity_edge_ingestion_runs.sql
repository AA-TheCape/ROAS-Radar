BEGIN;

CREATE TABLE IF NOT EXISTS identity_edge_ingestion_runs (
  id bigserial PRIMARY KEY,
  idempotency_key text NOT NULL,
  evidence_source text NOT NULL,
  source_table text,
  source_record_id text,
  source_timestamp timestamptz NOT NULL,
  status text NOT NULL DEFAULT 'started',
  journey_id uuid REFERENCES identity_journeys(id) ON DELETE SET NULL,
  outcome_reason text,
  processed_nodes integer NOT NULL DEFAULT 0,
  attached_nodes integer NOT NULL DEFAULT 0,
  rehomed_nodes integer NOT NULL DEFAULT 0,
  quarantined_nodes integer NOT NULL DEFAULT 0,
  processed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (status IN ('started', 'completed', 'conflicted')),
  CHECK (char_length(idempotency_key) <= 512),
  CHECK (char_length(evidence_source) <= 64),
  CHECK (source_table IS NULL OR char_length(source_table) <= 128),
  CHECK (source_record_id IS NULL OR char_length(source_record_id) <= 255),
  CHECK (outcome_reason IS NULL OR char_length(outcome_reason) <= 128),
  CHECK (processed_nodes >= 0),
  CHECK (attached_nodes >= 0),
  CHECK (rehomed_nodes >= 0),
  CHECK (quarantined_nodes >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_edge_ingestion_runs_idempotency_key_uidx
  ON identity_edge_ingestion_runs (idempotency_key);

CREATE INDEX IF NOT EXISTS identity_edge_ingestion_runs_source_lookup_idx
  ON identity_edge_ingestion_runs (evidence_source, source_table, source_timestamp DESC);

CREATE INDEX IF NOT EXISTS identity_edge_ingestion_runs_journey_lookup_idx
  ON identity_edge_ingestion_runs (journey_id, source_timestamp DESC)
  WHERE journey_id IS NOT NULL;

COMMIT;
