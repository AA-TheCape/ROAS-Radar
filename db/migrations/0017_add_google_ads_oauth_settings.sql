BEGIN;

CREATE TABLE google_ads_settings (
  id bigserial PRIMARY KEY,
  client_id text NOT NULL,
  client_secret_encrypted bytea,
  developer_token_encrypted bytea,
  app_base_url text NOT NULL,
  app_scopes text[] NOT NULL DEFAULT ARRAY['https://www.googleapis.com/auth/adwords']::text[],
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX google_ads_settings_updated_at_idx
  ON google_ads_settings (updated_at DESC);

CREATE TABLE google_ads_oauth_states (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  state_digest text NOT NULL UNIQUE,
  redirect_path text,
  customer_id text NOT NULL,
  login_customer_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz
);

CREATE INDEX google_ads_oauth_states_expires_idx
  ON google_ads_oauth_states (expires_at DESC);

COMMIT;
