BEGIN;

CREATE TABLE IF NOT EXISTS identity_journey_merge_audits (
  id bigserial PRIMARY KEY,
  winner_journey_id uuid NOT NULL REFERENCES identity_journeys(id) ON DELETE CASCADE,
  loser_journey_id uuid NOT NULL REFERENCES identity_journeys(id) ON DELETE CASCADE,
  merge_reason_code text NOT NULL,
  evidence_source text NOT NULL,
  source_table text,
  source_record_id text,
  source_timestamp timestamptz NOT NULL,
  winner_score jsonb NOT NULL,
  loser_score jsonb NOT NULL,
  candidate_scores jsonb NOT NULL DEFAULT '[]'::jsonb,
  rehomed_nodes integer NOT NULL DEFAULT 0,
  quarantined_nodes integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (winner_journey_id <> loser_journey_id),
  CHECK (char_length(merge_reason_code) <= 128),
  CHECK (char_length(evidence_source) <= 64),
  CHECK (source_table IS NULL OR char_length(source_table) <= 128),
  CHECK (source_record_id IS NULL OR char_length(source_record_id) <= 255),
  CHECK (jsonb_typeof(candidate_scores) = 'array'),
  CHECK (rehomed_nodes >= 0),
  CHECK (quarantined_nodes >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS identity_journey_merge_audits_loser_uidx
  ON identity_journey_merge_audits (loser_journey_id);

CREATE INDEX IF NOT EXISTS identity_journey_merge_audits_winner_created_idx
  ON identity_journey_merge_audits (winner_journey_id, created_at DESC);

CREATE INDEX IF NOT EXISTS identity_journey_merge_audits_source_idx
  ON identity_journey_merge_audits (evidence_source, source_timestamp DESC);

COMMIT;
