BEGIN;

CREATE TABLE shopify_app_installations (
  id bigserial PRIMARY KEY,
  shop_domain text NOT NULL UNIQUE,
  access_token_encrypted bytea NOT NULL,
  scopes text[] NOT NULL DEFAULT '{}'::text[],
  status text NOT NULL DEFAULT 'active',
  installed_at timestamptz NOT NULL DEFAULT now(),
  reconnected_at timestamptz,
  uninstalled_at timestamptz,
  webhook_base_url text NOT NULL,
  webhook_subscriptions jsonb NOT NULL DEFAULT '[]'::jsonb,
  shop_name text,
  shop_email text,
  shop_currency text,
  raw_shop_data jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX shopify_app_installations_status_idx
  ON shopify_app_installations (status, updated_at DESC);

CREATE TABLE shopify_oauth_states (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  shop_domain text NOT NULL,
  state_digest text NOT NULL UNIQUE,
  return_to text,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz
);

CREATE INDEX shopify_oauth_states_shop_domain_idx
  ON shopify_oauth_states (shop_domain, expires_at DESC);

COMMIT;
