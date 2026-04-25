import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const { __rawPayloadStorageTestUtils } = await import('../src/shared/raw-payload-storage.js');

let cachedModules:
  | {
      pool: typeof import('../src/db/pool.js').pool;
      resetE2EDatabase: typeof import('./e2e-harness.js').resetE2EDatabase;
      shopifyTestUtils: typeof import('../src/modules/shopify/index.js').__shopifyTestUtils;
      shopifyWritebackTestUtils: typeof import('../src/modules/shopify/writeback.js').__shopifyWritebackTestUtils;
    }
  | null = null;

async function getModules() {
  if (cachedModules) {
    return cachedModules;
  }

  const [poolModule, harnessModule, shopifyModule, writebackModule] = await Promise.all([
    import('../src/db/pool.js'),
    import('./e2e-harness.js'),
    import('../src/modules/shopify/index.js'),
    import('../src/modules/shopify/writeback.js')
  ]);

  cachedModules = {
    pool: poolModule.pool,
    resetE2EDatabase: harnessModule.resetE2EDatabase,
    shopifyTestUtils: shopifyModule.__shopifyTestUtils,
    shopifyWritebackTestUtils: writebackModule.__shopifyWritebackTestUtils
  };

  return cachedModules;
}

async function persistOrderViaIngress(topic: 'orders/create' | 'orders/backfill', rawPayload: Record<string, unknown>) {
  const { shopifyTestUtils } = await getModules();
  const rawBody = Buffer.from(JSON.stringify(rawPayload), 'utf8');

  await shopifyTestUtils.persistWebhook({
    payload: {
      id: rawPayload.id as string | number,
      order_number: rawPayload.order_number as string | number,
      customer: (rawPayload.customer as {
        id?: string | number;
        email?: string | null;
        phone?: string | null;
      } | null | undefined) ?? null,
      email: (rawPayload.email as string | null | undefined) ?? null,
      currency: (rawPayload.currency as string | undefined) ?? undefined,
      subtotal_price: rawPayload.subtotal_price as string | number | undefined,
      total_price: rawPayload.total_price as string | number | undefined,
      financial_status: (rawPayload.financial_status as string | null | undefined) ?? null,
      fulfillment_status: (rawPayload.fulfillment_status as string | null | undefined) ?? null,
      processed_at: (rawPayload.processed_at as string | null | undefined) ?? null,
      created_at: (rawPayload.created_at as string | null | undefined) ?? null,
      updated_at: (rawPayload.updated_at as string | null | undefined) ?? null,
      checkout_token: (rawPayload.checkout_token as string | null | undefined) ?? null,
      cart_token: (rawPayload.cart_token as string | null | undefined) ?? null,
      landing_site: (rawPayload.landing_site as string | null | undefined) ?? null,
      referring_site: (rawPayload.referring_site as string | null | undefined) ?? null,
      source_name: (rawPayload.source_name as string | null | undefined) ?? null,
      line_items: (rawPayload.line_items as Array<{
        id?: string | number;
        product_id?: string | number | null;
        variant_id?: string | number | null;
        sku?: string | null;
        title?: string | null;
        name?: string | null;
        vendor?: string | null;
        quantity?: number;
        price?: string | number;
        total_discount?: string | number;
        fulfillment_status?: string | null;
        requires_shipping?: boolean | null;
        taxable?: boolean | null;
      }>) ?? [],
      note_attributes: (rawPayload.note_attributes as Array<{
        name?: string;
        value?: string | number | boolean | null;
      }> | undefined) ?? undefined,
      attributes: (rawPayload.attributes as Array<{
        name?: string;
        value?: string | number | boolean | null;
      }> | undefined) ?? undefined
    },
    rawPayload,
    rawBody,
    shopDomain: 'example-shop.myshopify.com',
    topic,
    webhookId: topic === 'orders/create' ? `webhook-${String(rawPayload.id)}` : null
  });
}

async function fetchPersistedRawPayloads(shopifyOrderId: string) {
  const { pool } = await getModules();

  const [orderResult, receiptResult, lineItemResult] = await Promise.all([
    pool.query<{
      raw_payload: Record<string, unknown>;
      payload_source: string;
      payload_external_id: string | null;
      payload_size_bytes: number;
      payload_hash: string;
    }>(
      `
        SELECT raw_payload, payload_source, payload_external_id, payload_size_bytes, payload_hash
        FROM shopify_orders
        WHERE shopify_order_id = $1
      `,
      [shopifyOrderId]
    ),
    pool.query<{
      raw_payload: Record<string, unknown>;
      payload_source: string;
      payload_external_id: string | null;
      payload_size_bytes: number;
      payload_hash: string;
    }>(
      `
        SELECT raw_payload, payload_source, payload_external_id, payload_size_bytes, payload_hash
        FROM shopify_webhook_receipts
        ORDER BY id DESC
        LIMIT 1
      `
    ),
    pool.query<{ raw_payload: Record<string, unknown> }>(
      `
        SELECT raw_payload
        FROM shopify_order_line_items
        WHERE shopify_order_id = $1
        ORDER BY shopify_line_item_id
      `,
      [shopifyOrderId]
    )
  ]);

  assert.equal(orderResult.rowCount, 1);
  assert.equal(receiptResult.rowCount, 1);
  assert.equal(lineItemResult.rowCount, 1);

  return {
    order: orderResult.rows[0],
    receipt: receiptResult.rows[0],
    lineItem: lineItemResult.rows[0].raw_payload
  };
}

function buildLargeText(prefix: string): string {
  return `${prefix}-${'x'.repeat(12_000)}`;
}

function buildRawOrder(shopifyOrderId: string) {
  return {
    id: shopifyOrderId,
    order_number: 1001,
    email: 'Buyer@example.com',
    currency: 'USD',
    subtotal_price: '19.99',
    total_price: '21.49',
    financial_status: 'paid',
    fulfillment_status: null,
    processed_at: '2026-04-24T10:00:00.000Z',
    created_at: '2026-04-24T09:55:00.000Z',
    updated_at: '2026-04-24T10:01:00.000Z',
    checkout_token: `checkout-${shopifyOrderId}`,
    cart_token: `cart-${shopifyOrderId}`,
    source_name: 'web',
    customer: {
      id: `customer-${shopifyOrderId}`,
      email: 'Buyer@example.com',
      phone: null
    },
    note_attributes: [
      { name: 'channel', value: 'email' },
      { name: 'nullable-debug', value: null },
      { name: 'oversized-debug', value: buildLargeText(`note-${shopifyOrderId}`) }
    ],
    attributes: [
      { name: 'raw-array', value: 'present' },
      { name: 'feature-flag', value: true }
    ],
    landing_site: `https://example.com/products/widget?utm_campaign=Spring Launch ${shopifyOrderId}`,
    referring_site: null,
    unknown_top_level: {
      nested: {
        keep: true,
        nullValue: null,
        tags: ['alpha', 'beta', { variant: 'gamma' }]
      },
      notes: [buildLargeText(`top-${shopifyOrderId}`)]
    },
    line_items: [
      {
        id: `line-${shopifyOrderId}`,
        product_id: `product-${shopifyOrderId}`,
        variant_id: `variant-${shopifyOrderId}`,
        sku: 'SKU-123',
        title: 'Starter Widget',
        name: 'Blue / Large',
        vendor: 'Acme',
        quantity: 2,
        price: '9.995',
        total_discount: '0.00',
        fulfillment_status: null,
        requires_shipping: true,
        taxable: true,
        admin_graphql_api_id: `gid://shopify/LineItem/${shopifyOrderId}`,
        grams: 450,
        properties: [
          { name: '_bundle', value: 'starter-kit' },
          { name: 'gift_wrap', value: 'yes' }
        ],
        extra_discounts: [null, { code: 'SPRING-LAUNCH', amount: '2.50' }],
        tax_lines: [
          {
            price: '1.50',
            rate: 0.075,
            title: 'State Tax'
          }
        ],
        custom_metadata: {
          warehouse: {
            bin: 'A-12',
            auditTrail: [1, 2, 3]
          },
          largeText: buildLargeText(`line-${shopifyOrderId}`)
        }
      }
    ]
  };
}

test.beforeEach(async () => {
  const { resetE2EDatabase, shopifyWritebackTestUtils } = await getModules();
  shopifyWritebackTestUtils.reset();
  await resetE2EDatabase();
});

test.after(async () => {
  const { pool, resetE2EDatabase } = await getModules();
  await resetE2EDatabase();
  await pool.end();
});

test('Shopify webhook and backfill ingestion preserve raw orders without trimming', async () => {
  const webhookOrder = buildRawOrder('raw-webhook-order-1');

  await persistOrderViaIngress('orders/create', webhookOrder);

  const persistedWebhook = await fetchPersistedRawPayloads('raw-webhook-order-1');
  const webhookMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(webhookOrder);
  const webhookReceiptJson = JSON.stringify(webhookOrder);

  assert.deepEqual(persistedWebhook.order.raw_payload, webhookOrder);
  assert.equal(persistedWebhook.order.payload_source, 'shopify_order');
  assert.equal(persistedWebhook.order.payload_external_id, 'raw-webhook-order-1');
  assert.equal(persistedWebhook.order.payload_size_bytes, webhookMetadata.payloadSizeBytes);
  assert.equal(persistedWebhook.order.payload_hash, webhookMetadata.payloadHash);
  assert.deepEqual(persistedWebhook.receipt.raw_payload, webhookOrder);
  assert.equal(persistedWebhook.receipt.payload_source, 'shopify_webhook');
  assert.equal(persistedWebhook.receipt.payload_external_id, 'raw-webhook-order-1');
  assert.equal(persistedWebhook.receipt.payload_size_bytes, Buffer.byteLength(webhookReceiptJson, 'utf8'));
  assert.equal(persistedWebhook.receipt.payload_hash, createHash('sha256').update(webhookReceiptJson).digest('hex'));
  assert.deepEqual(persistedWebhook.lineItem, webhookOrder.line_items[0]);
  assert.equal(persistedWebhook.lineItem.admin_graphql_api_id, 'gid://shopify/LineItem/raw-webhook-order-1');
  assert.deepEqual(persistedWebhook.lineItem.properties, [
    { name: '_bundle', value: 'starter-kit' },
    { name: 'gift_wrap', value: 'yes' }
  ]);
  assert.equal(
    (persistedWebhook.order.raw_payload as { unknown_top_level: { nested: { nullValue: null } } }).unknown_top_level.nested.nullValue,
    null
  );
  assert.equal(
    (
      persistedWebhook.order.raw_payload as {
        note_attributes: Array<{ name: string; value: string | null }>;
      }
    ).note_attributes[2]?.value,
    webhookOrder.note_attributes[2]?.value
  );
  assert.deepEqual(
    (
      persistedWebhook.lineItem as {
        extra_discounts: Array<unknown>;
      }
    ).extra_discounts,
    webhookOrder.line_items[0].extra_discounts
  );

  const { resetE2EDatabase } = await getModules();
  await resetE2EDatabase();

  const backfillOrder = buildRawOrder('raw-backfill-order-1');

  await persistOrderViaIngress('orders/backfill', backfillOrder);

  const persistedBackfill = await fetchPersistedRawPayloads('raw-backfill-order-1');
  const backfillMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(backfillOrder);
  const backfillReceiptJson = JSON.stringify(backfillOrder);

  assert.deepEqual(persistedBackfill.order.raw_payload, backfillOrder);
  assert.equal(persistedBackfill.order.payload_source, 'shopify_order');
  assert.equal(persistedBackfill.order.payload_external_id, 'raw-backfill-order-1');
  assert.equal(persistedBackfill.order.payload_size_bytes, backfillMetadata.payloadSizeBytes);
  assert.equal(persistedBackfill.order.payload_hash, backfillMetadata.payloadHash);
  assert.deepEqual(persistedBackfill.receipt.raw_payload, backfillOrder);
  assert.equal(persistedBackfill.receipt.payload_source, 'shopify_webhook');
  assert.equal(persistedBackfill.receipt.payload_external_id, 'raw-backfill-order-1');
  assert.equal(persistedBackfill.receipt.payload_size_bytes, Buffer.byteLength(backfillReceiptJson, 'utf8'));
  assert.equal(persistedBackfill.receipt.payload_hash, createHash('sha256').update(backfillReceiptJson).digest('hex'));
  assert.deepEqual(persistedBackfill.lineItem, backfillOrder.line_items[0]);
  assert.equal(persistedBackfill.lineItem.grams, 450);
  assert.deepEqual(persistedBackfill.lineItem.tax_lines, [
    {
      price: '1.50',
      rate: 0.075,
      title: 'State Tax'
    }
  ]);
  assert.deepEqual(
    (
      persistedBackfill.lineItem as {
        custom_metadata: { warehouse: { auditTrail: number[] } };
      }
    ).custom_metadata.warehouse.auditTrail,
    [1, 2, 3]
  );
  assert.equal(
    (
      persistedBackfill.order.raw_payload as {
        unknown_top_level: { notes: string[] };
      }
    ).unknown_top_level.notes[0],
    backfillOrder.unknown_top_level.notes[0]
  );
});
