BEGIN;

ALTER TABLE shopify_webhook_receipts
  ADD COLUMN IF NOT EXISTS payload_source text,
  ADD COLUMN IF NOT EXISTS payload_size_bytes integer;

UPDATE shopify_webhook_receipts
SET payload_source = 'shopify_webhook'
WHERE payload_source IS NULL;

UPDATE shopify_webhook_receipts
SET payload_size_bytes = octet_length(convert_to(raw_payload::text, 'utf8'))
WHERE payload_size_bytes IS NULL;

ALTER TABLE shopify_webhook_receipts
  ALTER COLUMN payload_source SET DEFAULT 'shopify_webhook',
  ALTER COLUMN payload_source SET NOT NULL,
  ALTER COLUMN payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_webhook_receipts_payload_source_received_at_idx
  ON shopify_webhook_receipts (payload_source, received_at DESC);

ALTER TABLE shopify_orders
  ADD COLUMN IF NOT EXISTS payload_source text,
  ADD COLUMN IF NOT EXISTS payload_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS payload_hash text;

UPDATE shopify_orders
SET payload_source = 'shopify_order'
WHERE payload_source IS NULL;

UPDATE shopify_orders
SET payload_received_at = COALESCE(ingested_at, now())
WHERE payload_received_at IS NULL;

UPDATE shopify_orders
SET payload_size_bytes = octet_length(convert_to(raw_payload::text, 'utf8'))
WHERE payload_size_bytes IS NULL;

UPDATE shopify_orders
SET payload_hash = encode(digest(raw_payload::text, 'sha256'), 'hex')
WHERE payload_hash IS NULL;

ALTER TABLE shopify_orders
  ALTER COLUMN payload_source SET DEFAULT 'shopify_order',
  ALTER COLUMN payload_source SET NOT NULL,
  ALTER COLUMN payload_received_at SET DEFAULT now(),
  ALTER COLUMN payload_received_at SET NOT NULL,
  ALTER COLUMN payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS shopify_orders_payload_source_received_at_idx
  ON shopify_orders (payload_source, payload_received_at DESC);

CREATE INDEX IF NOT EXISTS shopify_orders_payload_hash_idx
  ON shopify_orders (payload_hash)
  WHERE payload_hash IS NOT NULL;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS payload_source text,
  ADD COLUMN IF NOT EXISTS payload_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS payload_hash text;

UPDATE tracking_events
SET payload_source = COALESCE(ingestion_source, 'browser')
WHERE payload_source IS NULL;

UPDATE tracking_events
SET payload_received_at = COALESCE(ingested_at, occurred_at, now())
WHERE payload_received_at IS NULL;

UPDATE tracking_events
SET payload_size_bytes = octet_length(convert_to(raw_payload::text, 'utf8'))
WHERE payload_size_bytes IS NULL;

UPDATE tracking_events
SET payload_hash = encode(digest(raw_payload::text, 'sha256'), 'hex')
WHERE payload_hash IS NULL;

ALTER TABLE tracking_events
  ALTER COLUMN payload_source SET DEFAULT 'browser',
  ALTER COLUMN payload_source SET NOT NULL,
  ALTER COLUMN payload_received_at SET DEFAULT now(),
  ALTER COLUMN payload_received_at SET NOT NULL,
  ALTER COLUMN payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS tracking_events_payload_source_received_at_idx
  ON tracking_events (payload_source, payload_received_at DESC);

CREATE INDEX IF NOT EXISTS tracking_events_payload_hash_idx
  ON tracking_events (payload_hash)
  WHERE payload_hash IS NOT NULL;

ALTER TABLE session_attribution_touch_events
  ADD COLUMN IF NOT EXISTS payload_source text,
  ADD COLUMN IF NOT EXISTS payload_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS payload_hash text;

UPDATE session_attribution_touch_events
SET payload_source = COALESCE(ingestion_source, 'browser')
WHERE payload_source IS NULL;

UPDATE session_attribution_touch_events
SET payload_received_at = COALESCE(captured_at, occurred_at, now())
WHERE payload_received_at IS NULL;

UPDATE session_attribution_touch_events
SET payload_size_bytes = octet_length(convert_to(raw_payload::text, 'utf8'))
WHERE payload_size_bytes IS NULL;

UPDATE session_attribution_touch_events
SET payload_hash = encode(digest(raw_payload::text, 'sha256'), 'hex')
WHERE payload_hash IS NULL;

ALTER TABLE session_attribution_touch_events
  ALTER COLUMN payload_source SET DEFAULT 'browser',
  ALTER COLUMN payload_source SET NOT NULL,
  ALTER COLUMN payload_received_at SET DEFAULT now(),
  ALTER COLUMN payload_received_at SET NOT NULL,
  ALTER COLUMN payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS session_attribution_touch_events_payload_source_received_at_idx
  ON session_attribution_touch_events (payload_source, payload_received_at DESC);

CREATE INDEX IF NOT EXISTS session_attribution_touch_events_payload_hash_idx
  ON session_attribution_touch_events (payload_hash)
  WHERE payload_hash IS NOT NULL;

ALTER TABLE meta_ads_connections
  ADD COLUMN IF NOT EXISTS raw_account_source text,
  ADD COLUMN IF NOT EXISTS raw_account_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS raw_account_payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS raw_account_payload_hash text;

UPDATE meta_ads_connections
SET raw_account_source = 'meta_ads_account'
WHERE raw_account_source IS NULL;

UPDATE meta_ads_connections
SET raw_account_received_at = COALESCE(updated_at, created_at, now())
WHERE raw_account_received_at IS NULL;

UPDATE meta_ads_connections
SET raw_account_payload_size_bytes = octet_length(convert_to(raw_account_data::text, 'utf8'))
WHERE raw_account_payload_size_bytes IS NULL;

UPDATE meta_ads_connections
SET raw_account_payload_hash = encode(digest(raw_account_data::text, 'sha256'), 'hex')
WHERE raw_account_payload_hash IS NULL;

ALTER TABLE meta_ads_connections
  ALTER COLUMN raw_account_source SET DEFAULT 'meta_ads_account',
  ALTER COLUMN raw_account_source SET NOT NULL,
  ALTER COLUMN raw_account_received_at SET DEFAULT now(),
  ALTER COLUMN raw_account_received_at SET NOT NULL,
  ALTER COLUMN raw_account_payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS meta_ads_connections_raw_account_received_at_idx
  ON meta_ads_connections (raw_account_received_at DESC);

ALTER TABLE meta_ads_raw_spend_records
  ADD COLUMN IF NOT EXISTS payload_source text,
  ADD COLUMN IF NOT EXISTS payload_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS payload_hash text;

UPDATE meta_ads_raw_spend_records
SET payload_source = 'meta_ads_insights'
WHERE payload_source IS NULL;

UPDATE meta_ads_raw_spend_records
SET payload_received_at = COALESCE(updated_at, created_at, now())
WHERE payload_received_at IS NULL;

UPDATE meta_ads_raw_spend_records
SET payload_size_bytes = octet_length(convert_to(raw_payload::text, 'utf8'))
WHERE payload_size_bytes IS NULL;

UPDATE meta_ads_raw_spend_records
SET payload_hash = encode(digest(raw_payload::text, 'sha256'), 'hex')
WHERE payload_hash IS NULL;

ALTER TABLE meta_ads_raw_spend_records
  ALTER COLUMN payload_source SET DEFAULT 'meta_ads_insights',
  ALTER COLUMN payload_source SET NOT NULL,
  ALTER COLUMN payload_received_at SET DEFAULT now(),
  ALTER COLUMN payload_received_at SET NOT NULL,
  ALTER COLUMN payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS meta_ads_raw_spend_records_source_received_at_idx
  ON meta_ads_raw_spend_records (payload_source, payload_received_at DESC);

CREATE INDEX IF NOT EXISTS meta_ads_raw_spend_records_payload_hash_idx
  ON meta_ads_raw_spend_records (payload_hash)
  WHERE payload_hash IS NOT NULL;

ALTER TABLE google_ads_connections
  ADD COLUMN IF NOT EXISTS raw_customer_source text,
  ADD COLUMN IF NOT EXISTS raw_customer_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS raw_customer_payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS raw_customer_payload_hash text;

UPDATE google_ads_connections
SET raw_customer_source = 'google_ads_customer'
WHERE raw_customer_source IS NULL;

UPDATE google_ads_connections
SET raw_customer_received_at = COALESCE(updated_at, created_at, now())
WHERE raw_customer_received_at IS NULL;

UPDATE google_ads_connections
SET raw_customer_payload_size_bytes = octet_length(convert_to(raw_customer_data::text, 'utf8'))
WHERE raw_customer_payload_size_bytes IS NULL;

UPDATE google_ads_connections
SET raw_customer_payload_hash = encode(digest(raw_customer_data::text, 'sha256'), 'hex')
WHERE raw_customer_payload_hash IS NULL;

ALTER TABLE google_ads_connections
  ALTER COLUMN raw_customer_source SET DEFAULT 'google_ads_customer',
  ALTER COLUMN raw_customer_source SET NOT NULL,
  ALTER COLUMN raw_customer_received_at SET DEFAULT now(),
  ALTER COLUMN raw_customer_received_at SET NOT NULL,
  ALTER COLUMN raw_customer_payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS google_ads_connections_raw_customer_received_at_idx
  ON google_ads_connections (raw_customer_received_at DESC);

ALTER TABLE google_ads_raw_spend_records
  ADD COLUMN IF NOT EXISTS payload_source text,
  ADD COLUMN IF NOT EXISTS payload_received_at timestamptz,
  ADD COLUMN IF NOT EXISTS payload_size_bytes integer,
  ADD COLUMN IF NOT EXISTS payload_hash text;

UPDATE google_ads_raw_spend_records
SET payload_source = 'google_ads_api'
WHERE payload_source IS NULL;

UPDATE google_ads_raw_spend_records
SET payload_received_at = COALESCE(updated_at, created_at, now())
WHERE payload_received_at IS NULL;

UPDATE google_ads_raw_spend_records
SET payload_size_bytes = octet_length(convert_to(raw_payload::text, 'utf8'))
WHERE payload_size_bytes IS NULL;

UPDATE google_ads_raw_spend_records
SET payload_hash = encode(digest(raw_payload::text, 'sha256'), 'hex')
WHERE payload_hash IS NULL;

ALTER TABLE google_ads_raw_spend_records
  ALTER COLUMN payload_source SET DEFAULT 'google_ads_api',
  ALTER COLUMN payload_source SET NOT NULL,
  ALTER COLUMN payload_received_at SET DEFAULT now(),
  ALTER COLUMN payload_received_at SET NOT NULL,
  ALTER COLUMN payload_size_bytes SET NOT NULL;

CREATE INDEX IF NOT EXISTS google_ads_raw_spend_records_source_received_at_idx
  ON google_ads_raw_spend_records (payload_source, payload_received_at DESC);

CREATE INDEX IF NOT EXISTS google_ads_raw_spend_records_payload_hash_idx
  ON google_ads_raw_spend_records (payload_hash)
  WHERE payload_hash IS NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'shopify_webhook_receipts_payload_source_len_chk'
  ) THEN
    ALTER TABLE shopify_webhook_receipts
      ADD CONSTRAINT shopify_webhook_receipts_payload_source_len_chk
      CHECK (char_length(payload_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'shopify_webhook_receipts_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE shopify_webhook_receipts
      ADD CONSTRAINT shopify_webhook_receipts_payload_size_bytes_chk
      CHECK (payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'shopify_orders_payload_source_len_chk'
  ) THEN
    ALTER TABLE shopify_orders
      ADD CONSTRAINT shopify_orders_payload_source_len_chk
      CHECK (char_length(payload_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'shopify_orders_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE shopify_orders
      ADD CONSTRAINT shopify_orders_payload_size_bytes_chk
      CHECK (payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_payload_source_len_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_payload_source_len_chk
      CHECK (char_length(payload_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'tracking_events_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE tracking_events
      ADD CONSTRAINT tracking_events_payload_size_bytes_chk
      CHECK (payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'session_attribution_touch_events_payload_source_len_chk'
  ) THEN
    ALTER TABLE session_attribution_touch_events
      ADD CONSTRAINT session_attribution_touch_events_payload_source_len_chk
      CHECK (char_length(payload_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'session_attribution_touch_events_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE session_attribution_touch_events
      ADD CONSTRAINT session_attribution_touch_events_payload_size_bytes_chk
      CHECK (payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'meta_ads_connections_raw_account_source_len_chk'
  ) THEN
    ALTER TABLE meta_ads_connections
      ADD CONSTRAINT meta_ads_connections_raw_account_source_len_chk
      CHECK (char_length(raw_account_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'meta_ads_connections_raw_account_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE meta_ads_connections
      ADD CONSTRAINT meta_ads_connections_raw_account_payload_size_bytes_chk
      CHECK (raw_account_payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'meta_ads_raw_spend_records_payload_source_len_chk'
  ) THEN
    ALTER TABLE meta_ads_raw_spend_records
      ADD CONSTRAINT meta_ads_raw_spend_records_payload_source_len_chk
      CHECK (char_length(payload_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'meta_ads_raw_spend_records_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE meta_ads_raw_spend_records
      ADD CONSTRAINT meta_ads_raw_spend_records_payload_size_bytes_chk
      CHECK (payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'google_ads_connections_raw_customer_source_len_chk'
  ) THEN
    ALTER TABLE google_ads_connections
      ADD CONSTRAINT google_ads_connections_raw_customer_source_len_chk
      CHECK (char_length(raw_customer_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'google_ads_connections_raw_customer_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE google_ads_connections
      ADD CONSTRAINT google_ads_connections_raw_customer_payload_size_bytes_chk
      CHECK (raw_customer_payload_size_bytes >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'google_ads_raw_spend_records_payload_source_len_chk'
  ) THEN
    ALTER TABLE google_ads_raw_spend_records
      ADD CONSTRAINT google_ads_raw_spend_records_payload_source_len_chk
      CHECK (char_length(payload_source) <= 128);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'google_ads_raw_spend_records_payload_size_bytes_chk'
  ) THEN
    ALTER TABLE google_ads_raw_spend_records
      ADD CONSTRAINT google_ads_raw_spend_records_payload_size_bytes_chk
      CHECK (payload_size_bytes >= 0);
  END IF;
END $$;

COMMIT;
