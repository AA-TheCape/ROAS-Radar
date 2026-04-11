BEGIN;

CREATE TABLE customer_identities (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  hashed_email text UNIQUE,
  shopify_customer_id text UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  last_stitched_at timestamptz NOT NULL DEFAULT now(),
  CHECK (hashed_email IS NOT NULL OR shopify_customer_id IS NOT NULL)
);

ALTER TABLE tracking_sessions
  ADD COLUMN IF NOT EXISTS customer_identity_id uuid REFERENCES customer_identities(id) ON DELETE SET NULL;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS customer_identity_id uuid REFERENCES customer_identities(id) ON DELETE SET NULL;

ALTER TABLE shopify_customers
  ADD COLUMN IF NOT EXISTS email_hash text,
  ADD COLUMN IF NOT EXISTS customer_identity_id uuid REFERENCES customer_identities(id) ON DELETE SET NULL;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS email_hash text,
  ADD COLUMN IF NOT EXISTS customer_identity_id uuid REFERENCES customer_identities(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS tracking_sessions_customer_identity_id_idx
  ON tracking_sessions (customer_identity_id)
  WHERE customer_identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS tracking_events_customer_identity_id_idx
  ON tracking_events (customer_identity_id)
  WHERE customer_identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_customers_email_hash_idx
  ON shopify_customers (email_hash)
  WHERE email_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_customers_customer_identity_id_idx
  ON shopify_customers (customer_identity_id)
  WHERE customer_identity_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_orders_email_hash_idx
  ON shopify_orders (email_hash)
  WHERE email_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_orders_customer_identity_id_idx
  ON shopify_orders (customer_identity_id)
  WHERE customer_identity_id IS NOT NULL;

COMMIT;
