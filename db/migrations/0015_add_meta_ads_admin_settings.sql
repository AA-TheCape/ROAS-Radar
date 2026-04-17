BEGIN;

CREATE TABLE meta_ads_settings (
  id bigserial PRIMARY KEY,
  app_id text NOT NULL,
  app_secret_encrypted bytea,
  app_base_url text NOT NULL,
  app_scopes text[] NOT NULL DEFAULT '{}'::text[],
  ad_account_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX meta_ads_settings_updated_at_idx
  ON meta_ads_settings (updated_at DESC);

COMMIT;
