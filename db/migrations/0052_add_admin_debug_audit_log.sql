BEGIN;

CREATE TABLE IF NOT EXISTS admin_debug_audit_log (
  id bigserial PRIMARY KEY,
  actor_kind text NOT NULL,
  actor_user_id bigint,
  actor_email text NOT NULL,
  action text NOT NULL,
  target_type text NOT NULL,
  target_id text,
  request_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  result_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (actor_kind IN ('internal', 'user')),
  CHECK (char_length(actor_email) <= 255),
  CHECK (char_length(action) <= 128),
  CHECK (char_length(target_type) <= 128),
  CHECK (target_id IS NULL OR char_length(target_id) <= 255)
);

CREATE INDEX IF NOT EXISTS admin_debug_audit_log_created_idx
  ON admin_debug_audit_log (created_at DESC);

CREATE INDEX IF NOT EXISTS admin_debug_audit_log_action_created_idx
  ON admin_debug_audit_log (action, created_at DESC);

CREATE INDEX IF NOT EXISTS admin_debug_audit_log_target_idx
  ON admin_debug_audit_log (target_type, target_id, created_at DESC)
  WHERE target_id IS NOT NULL;

COMMIT;
