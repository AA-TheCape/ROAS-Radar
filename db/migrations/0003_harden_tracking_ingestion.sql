BEGIN;

ALTER TABLE tracking_events
  ADD COLUMN IF NOT EXISTS ingestion_fingerprint text,
  ADD COLUMN IF NOT EXISTS ingested_at timestamptz NOT NULL DEFAULT now();

WITH normalized_events AS (
  SELECT
    id,
    encode(
      digest(
        jsonb_build_object(
          'session_id', session_id::text,
          'event_type', event_type,
          'occurred_at', occurred_at,
          'page_url', COALESCE(page_url, ''),
          'referrer_url', COALESCE(referrer_url, ''),
          'shopify_cart_token', COALESCE(shopify_cart_token, ''),
          'shopify_checkout_token', COALESCE(shopify_checkout_token, '')
        )::text,
        'sha256'
      ),
      'hex'
    ) AS fingerprint
  FROM tracking_events
)
UPDATE tracking_events AS tracking_events
SET ingestion_fingerprint = normalized_events.fingerprint
FROM normalized_events
WHERE tracking_events.id = normalized_events.id
  AND tracking_events.ingestion_fingerprint IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS tracking_events_ingestion_fingerprint_uidx
  ON tracking_events (ingestion_fingerprint)
  WHERE ingestion_fingerprint IS NOT NULL;

COMMIT;
