BEGIN;

ALTER TABLE shopify_customers
  ADD COLUMN IF NOT EXISTS phone_hash text;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS phone_hash text;

UPDATE shopify_customers
SET email_hash = COALESCE(
  email_hash,
  encode(digest(lower(btrim(email)), 'sha256'), 'hex')
)
WHERE email_hash IS NULL
  AND email IS NOT NULL
  AND btrim(email) <> '';

UPDATE shopify_orders
SET email_hash = COALESCE(
  email_hash,
  encode(digest(lower(btrim(email)), 'sha256'), 'hex')
)
WHERE email_hash IS NULL
  AND email IS NOT NULL
  AND btrim(email) <> '';

UPDATE shopify_customers
SET
  email = NULL,
  phone = NULL
WHERE email IS NOT NULL
   OR phone IS NOT NULL;

UPDATE shopify_orders
SET email = NULL
WHERE email IS NOT NULL;

ALTER TABLE shopify_customers
  DROP CONSTRAINT IF EXISTS shopify_customers_email_plaintext_blocked,
  DROP CONSTRAINT IF EXISTS shopify_customers_phone_plaintext_blocked,
  DROP CONSTRAINT IF EXISTS shopify_customers_email_hash_format_chk,
  DROP CONSTRAINT IF EXISTS shopify_customers_phone_hash_format_chk;

ALTER TABLE shopify_orders
  DROP CONSTRAINT IF EXISTS shopify_orders_email_plaintext_blocked,
  DROP CONSTRAINT IF EXISTS shopify_orders_email_hash_format_chk,
  DROP CONSTRAINT IF EXISTS shopify_orders_phone_hash_format_chk;

ALTER TABLE shopify_customers
  ADD CONSTRAINT shopify_customers_email_plaintext_blocked CHECK (email IS NULL),
  ADD CONSTRAINT shopify_customers_phone_plaintext_blocked CHECK (phone IS NULL),
  ADD CONSTRAINT shopify_customers_email_hash_format_chk CHECK (email_hash IS NULL OR email_hash ~ '^[0-9a-f]{64}$'),
  ADD CONSTRAINT shopify_customers_phone_hash_format_chk CHECK (phone_hash IS NULL OR phone_hash ~ '^[0-9a-f]{64}$');

ALTER TABLE shopify_orders
  ADD CONSTRAINT shopify_orders_email_plaintext_blocked CHECK (email IS NULL),
  ADD CONSTRAINT shopify_orders_email_hash_format_chk CHECK (email_hash IS NULL OR email_hash ~ '^[0-9a-f]{64}$'),
  ADD CONSTRAINT shopify_orders_phone_hash_format_chk CHECK (phone_hash IS NULL OR phone_hash ~ '^[0-9a-f]{64}$');

CREATE INDEX IF NOT EXISTS shopify_customers_phone_hash_idx
  ON shopify_customers (phone_hash)
  WHERE phone_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_orders_phone_hash_idx
  ON shopify_orders (phone_hash)
  WHERE phone_hash IS NOT NULL;

COMMIT;
