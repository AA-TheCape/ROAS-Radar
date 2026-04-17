CREATE TABLE app_users (
  id bigserial PRIMARY KEY,
  email text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  display_name text NOT NULL,
  is_admin boolean NOT NULL DEFAULT false,
  status text NOT NULL DEFAULT 'active',
  last_login_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT app_users_email_lowercase CHECK (email = lower(email)),
  CONSTRAINT app_users_status_check CHECK (status IN ('active', 'disabled'))
);

CREATE INDEX app_users_status_idx
  ON app_users (status, email);

CREATE TABLE app_sessions (
  id bigserial PRIMARY KEY,
  user_id bigint NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
  token_digest text NOT NULL UNIQUE,
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX app_sessions_user_id_idx
  ON app_sessions (user_id, expires_at DESC);

CREATE INDEX app_sessions_active_idx
  ON app_sessions (expires_at DESC)
  WHERE revoked_at IS NULL;

INSERT INTO app_users (
  email,
  password_hash,
  display_name,
  is_admin,
  status
)
VALUES (
  'aa@thecapemarine.com',
  'scrypt$FptbZExcFM33gSxaN78Jng$yYp1tFCMzYQs51dmEGQ22lrBznLhTJqB8sVw4k8eApYZWiNGOVWVR33BbyxB3901qz0XrpCrrFTYKzeyutWeyA',
  'Andrew Anderson',
  true,
  'active'
)
ON CONFLICT (email) DO NOTHING;
