BEGIN;

CREATE TABLE shopify_order_line_items (
  id bigserial PRIMARY KEY,
  shopify_order_id text NOT NULL REFERENCES shopify_orders(shopify_order_id) ON DELETE CASCADE,
  shopify_line_item_id text NOT NULL,
  shopify_product_id text,
  shopify_variant_id text,
  sku text,
  title text,
  variant_title text,
  vendor text,
  quantity integer NOT NULL DEFAULT 0,
  price numeric(12, 2) NOT NULL DEFAULT 0,
  total_discount numeric(12, 2) NOT NULL DEFAULT 0,
  fulfillment_status text,
  requires_shipping boolean,
  taxable boolean,
  raw_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX shopify_order_line_items_order_line_uidx
  ON shopify_order_line_items (shopify_order_id, shopify_line_item_id);

CREATE INDEX shopify_order_line_items_order_idx
  ON shopify_order_line_items (shopify_order_id);

CREATE INDEX shopify_order_line_items_product_idx
  ON shopify_order_line_items (shopify_product_id)
  WHERE shopify_product_id IS NOT NULL;

COMMIT;
