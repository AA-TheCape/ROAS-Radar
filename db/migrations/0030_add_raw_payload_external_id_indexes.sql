BEGIN;

ALTER TABLE shopify_webhook_receipts
  ADD COLUMN IF NOT EXISTS payload_external_id text;

UPDATE shopify_webhook_receipts
SET payload_external_id = NULLIF(COALESCE(raw_payload ->> 'id', webhook_id), '')
WHERE payload_external_id IS NULL;

CREATE INDEX IF NOT EXISTS shopify_webhook_receipts_payload_lookup_idx
  ON shopify_webhook_receipts (payload_source, payload_external_id, received_at DESC)
  WHERE payload_external_id IS NOT NULL;

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS payload_external_id text;

UPDATE shopify_orders
SET payload_external_id = shopify_order_id
WHERE payload_external_id IS NULL;

CREATE INDEX IF NOT EXISTS shopify_orders_payload_lookup_idx
  ON shopify_orders (payload_source, payload_external_id, payload_received_at DESC)
  WHERE payload_external_id IS NOT NULL;

ALTER TABLE meta_ads_connections
  ADD COLUMN IF NOT EXISTS raw_account_external_id text;

UPDATE meta_ads_connections
SET raw_account_external_id = ad_account_id
WHERE raw_account_external_id IS NULL;

CREATE INDEX IF NOT EXISTS meta_ads_connections_raw_account_lookup_idx
  ON meta_ads_connections (raw_account_source, raw_account_external_id, raw_account_received_at DESC)
  WHERE raw_account_external_id IS NOT NULL;

ALTER TABLE meta_ads_raw_spend_records
  ADD COLUMN IF NOT EXISTS payload_external_id text;

UPDATE meta_ads_raw_spend_records
SET payload_external_id = entity_id
WHERE payload_external_id IS NULL;

CREATE INDEX IF NOT EXISTS meta_ads_raw_spend_records_payload_lookup_idx
  ON meta_ads_raw_spend_records (payload_source, payload_external_id, payload_received_at DESC)
  WHERE payload_external_id IS NOT NULL;

ALTER TABLE google_ads_connections
  ADD COLUMN IF NOT EXISTS raw_customer_external_id text;

UPDATE google_ads_connections
SET raw_customer_external_id = customer_id
WHERE raw_customer_external_id IS NULL;

CREATE INDEX IF NOT EXISTS google_ads_connections_raw_customer_lookup_idx
  ON google_ads_connections (raw_customer_source, raw_customer_external_id, raw_customer_received_at DESC)
  WHERE raw_customer_external_id IS NOT NULL;

ALTER TABLE google_ads_raw_spend_records
  ADD COLUMN IF NOT EXISTS payload_external_id text;

UPDATE google_ads_raw_spend_records
SET payload_external_id = entity_id
WHERE payload_external_id IS NULL;

CREATE INDEX IF NOT EXISTS google_ads_raw_spend_records_payload_lookup_idx
  ON google_ads_raw_spend_records (payload_source, payload_external_id, payload_received_at DESC)
  WHERE payload_external_id IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'shopify_webhook_receipts_payload_external_id_len_chk'
  ) THEN
    ALTER TABLE shopify_webhook_receipts
      ADD CONSTRAINT shopify_webhook_receipts_payload_external_id_len_chk
      CHECK (payload_external_id IS NULL OR char_length(payload_external_id) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'shopify_orders_payload_external_id_len_chk'
  ) THEN
    ALTER TABLE shopify_orders
      ADD CONSTRAINT shopify_orders_payload_external_id_len_chk
      CHECK (char_length(payload_external_id) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'meta_ads_connections_raw_account_external_id_len_chk'
  ) THEN
    ALTER TABLE meta_ads_connections
      ADD CONSTRAINT meta_ads_connections_raw_account_external_id_len_chk
      CHECK (char_length(raw_account_external_id) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'meta_ads_raw_spend_records_payload_external_id_len_chk'
  ) THEN
    ALTER TABLE meta_ads_raw_spend_records
      ADD CONSTRAINT meta_ads_raw_spend_records_payload_external_id_len_chk
      CHECK (char_length(payload_external_id) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'google_ads_connections_raw_customer_external_id_len_chk'
  ) THEN
    ALTER TABLE google_ads_connections
      ADD CONSTRAINT google_ads_connections_raw_customer_external_id_len_chk
      CHECK (char_length(raw_customer_external_id) <= 255);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'google_ads_raw_spend_records_payload_external_id_len_chk'
  ) THEN
    ALTER TABLE google_ads_raw_spend_records
      ADD CONSTRAINT google_ads_raw_spend_records_payload_external_id_len_chk
      CHECK (char_length(payload_external_id) <= 255);
  END IF;
END
$$;

COMMIT;
