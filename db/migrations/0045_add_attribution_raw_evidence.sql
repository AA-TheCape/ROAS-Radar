BEGIN;

CREATE TABLE attribution_raw_evidence (
  id bigserial PRIMARY KEY,
  run_id uuid NOT NULL,
  order_id text NOT NULL,
  evidence_type text NOT NULL,
  source_table text NOT NULL,
  source_record_id text NOT NULL,
  touchpoint_id text,
  session_id uuid,
  ingestion_source text,
  event_type text,
  occurred_at_utc timestamptz,
  captured_at_utc timestamptz,
  evidence_status text NOT NULL,
  error_code text,
  error_message text,
  normalized_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  raw_payload jsonb NOT NULL,
  payload_size_bytes integer NOT NULL,
  payload_hash text NOT NULL,
  created_at_utc timestamptz NOT NULL DEFAULT now(),
  retained_until timestamptz NOT NULL DEFAULT (now() + interval '180 days'),
  FOREIGN KEY (run_id, order_id) REFERENCES attribution_order_inputs(run_id, order_id) ON DELETE CASCADE,
  CHECK (evidence_type IN ('shopify_hint', 'tracking_touchpoint')),
  CHECK (source_table IN ('shopify_orders', 'session_attribution_touch_events')),
  CHECK (char_length(source_record_id) BETWEEN 1 AND 255),
  CHECK (touchpoint_id IS NULL OR char_length(touchpoint_id) <= 255),
  CHECK (ingestion_source IS NULL OR char_length(ingestion_source) <= 64),
  CHECK (event_type IS NULL OR char_length(event_type) <= 64),
  CHECK (evidence_status IN ('valid', 'malformed')),
  CHECK ((evidence_status = 'valid' AND error_code IS NULL) OR (evidence_status = 'malformed' AND error_code IS NOT NULL)),
  CHECK (error_code IS NULL OR char_length(error_code) <= 128),
  CHECK (error_message IS NULL OR char_length(error_message) <= 512),
  CHECK (payload_size_bytes >= 0),
  CHECK (char_length(payload_hash) = 64),
  CHECK (retained_until >= created_at_utc)
);

CREATE INDEX attribution_raw_evidence_order_run_idx
  ON attribution_raw_evidence (order_id, run_id, evidence_type);

CREATE INDEX attribution_raw_evidence_run_status_idx
  ON attribution_raw_evidence (run_id, evidence_status, evidence_type);

CREATE INDEX attribution_raw_evidence_touchpoint_idx
  ON attribution_raw_evidence (touchpoint_id)
  WHERE touchpoint_id IS NOT NULL;

CREATE INDEX attribution_raw_evidence_session_idx
  ON attribution_raw_evidence (session_id, occurred_at_utc DESC)
  WHERE session_id IS NOT NULL;

CREATE INDEX attribution_raw_evidence_payload_hash_idx
  ON attribution_raw_evidence (payload_hash);

CREATE INDEX attribution_raw_evidence_retained_until_idx
  ON attribution_raw_evidence (retained_until);

COMMIT;
