BEGIN;

CREATE TABLE IF NOT EXISTS shopify_order_raw_payload_versions (
  id bigserial PRIMARY KEY,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  payload_version integer NOT NULL,
  payload_hash text NOT NULL,
  payload_size_bytes integer NOT NULL,
  payload_source text NOT NULL DEFAULT 'shopify_order',
  payload_external_id text,
  source_topic text,
  source_receipt_id bigint REFERENCES shopify_webhook_receipts(id) ON DELETE SET NULL,
  raw_payload jsonb NOT NULL,
  refreshed_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT shopify_order_raw_payload_versions_version_chk
    CHECK (payload_version > 0),
  CONSTRAINT shopify_order_raw_payload_versions_size_chk
    CHECK (payload_size_bytes >= 0),
  CONSTRAINT shopify_order_raw_payload_versions_hash_chk
    CHECK (char_length(payload_hash) = 64),
  CONSTRAINT shopify_order_raw_payload_versions_source_chk
    CHECK (char_length(payload_source) BETWEEN 1 AND 128),
  CONSTRAINT shopify_order_raw_payload_versions_external_id_chk
    CHECK (payload_external_id IS NULL OR char_length(payload_external_id) <= 255),
  CONSTRAINT shopify_order_raw_payload_versions_topic_chk
    CHECK (source_topic IS NULL OR char_length(source_topic) <= 128)
);

CREATE UNIQUE INDEX IF NOT EXISTS shopify_order_raw_payload_versions_order_hash_uidx
  ON shopify_order_raw_payload_versions (shopify_order_id, payload_hash);

CREATE UNIQUE INDEX IF NOT EXISTS shopify_order_raw_payload_versions_order_version_uidx
  ON shopify_order_raw_payload_versions (shopify_order_id, payload_version);

CREATE INDEX IF NOT EXISTS shopify_order_raw_payload_versions_refreshed_idx
  ON shopify_order_raw_payload_versions (refreshed_at DESC);

CREATE INDEX IF NOT EXISTS shopify_order_raw_payload_versions_order_refreshed_idx
  ON shopify_order_raw_payload_versions (shopify_order_id, refreshed_at DESC);

INSERT INTO shopify_order_raw_payload_versions (
  shopify_order_id,
  payload_version,
  payload_hash,
  payload_size_bytes,
  payload_source,
  payload_external_id,
  raw_payload,
  refreshed_at,
  created_at,
  updated_at
)
SELECT
  shopify_order_id,
  1,
  payload_hash,
  payload_size_bytes,
  payload_source,
  payload_external_id,
  raw_payload,
  payload_received_at,
  payload_received_at,
  payload_received_at
FROM shopify_orders
WHERE payload_hash IS NOT NULL
ON CONFLICT (shopify_order_id, payload_hash) DO NOTHING;

COMMIT;
