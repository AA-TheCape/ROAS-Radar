BEGIN;

CREATE TABLE mmm_readiness_gates (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  gate_version text NOT NULL DEFAULT 'mmm_readiness_gate_v1',
  start_date date NOT NULL,
  end_date date NOT NULL,
  mart_row_type text,
  attribution_model text,
  platform text,
  source text,
  campaign text,
  evidence_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  checklist_statuses jsonb NOT NULL DEFAULT '[]'::jsonb,
  owner_approvals jsonb NOT NULL DEFAULT '[]'::jsonb,
  waivers jsonb NOT NULL DEFAULT '[]'::jsonb,
  unresolved_critical_issue_count integer NOT NULL DEFAULT 0,
  evidence_hash text NOT NULL,
  gate_status text NOT NULL DEFAULT 'pending',
  final_state text NOT NULL DEFAULT 'blocked',
  decision_reason text,
  decided_by text,
  decided_at timestamptz,
  created_by text NOT NULL DEFAULT 'system',
  updated_by text NOT NULL DEFAULT 'system',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (gate_version = 'mmm_readiness_gate_v1'),
  CHECK (start_date <= end_date),
  CHECK (mart_row_type IS NULL OR mart_row_type IN ('paid_media', 'attribution')),
  CHECK (platform IS NULL OR platform IN ('meta', 'google', 'taxonomy')),
  CHECK (char_length(evidence_hash) = 64),
  CHECK (gate_status IN ('pending', 'approved', 'blocked')),
  CHECK (final_state IN ('approved', 'blocked'))
);

CREATE UNIQUE INDEX mmm_readiness_gates_scope_uidx
  ON mmm_readiness_gates (
    start_date,
    end_date,
    COALESCE(mart_row_type, ''),
    COALESCE(attribution_model, ''),
    COALESCE(platform, ''),
    COALESCE(source, ''),
    COALESCE(campaign, '')
  );

CREATE INDEX mmm_readiness_gates_window_idx
  ON mmm_readiness_gates (start_date DESC, end_date DESC, updated_at DESC);

CREATE INDEX mmm_readiness_gates_status_idx
  ON mmm_readiness_gates (gate_status, final_state, updated_at DESC);

COMMIT;
