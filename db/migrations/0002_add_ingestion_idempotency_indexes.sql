BEGIN;

CREATE INDEX IF NOT EXISTS tracking_events_cart_token_idx
  ON tracking_events (shopify_cart_token)
  WHERE shopify_cart_token IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS shopify_webhook_receipts_topic_shop_domain_payload_hash_uidx
  ON shopify_webhook_receipts (topic, shop_domain, payload_hash);

COMMIT;
