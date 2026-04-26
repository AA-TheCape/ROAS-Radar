import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.SHOPIFY_APP_API_SECRET = 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET = 'test-webhook-secret';

const { __shopifyTestUtils } = await import('../src/modules/shopify/index.js');

test('verifyWebhookSignature validates Shopify webhook signatures using the raw request body', () => {
  const rawBody = Buffer.from(JSON.stringify({ id: 123, source_name: 'web' }), 'utf8');
  const signature = createHmac('sha256', 'test-webhook-secret').update(rawBody).digest('base64');

  assert.equal(__shopifyTestUtils.verifyWebhookSignature(rawBody, signature), true);
  assert.equal(__shopifyTestUtils.verifyWebhookSignature(rawBody, `${signature}tampered`), false);
});

test('buildLineItemExternalId reuses Shopify ids and falls back to a stable per-order key', () => {
  assert.equal(
    __shopifyTestUtils.buildLineItemExternalId('order-1', { id: 456 }, 0),
    '456'
  );
  assert.equal(
    __shopifyTestUtils.buildLineItemExternalId('order-1', { title: 'Widget' }, 1),
    'order-1:line:2'
  );
});

test('extractRawShopifyLineItems returns untouched raw line item nodes from the source payload', () => {
  const rawLineItems = __shopifyTestUtils.extractRawShopifyLineItems({
    id: 123,
    line_items: [
      {
        id: 456,
        title: 'Widget',
        admin_graphql_api_id: 'gid://shopify/LineItem/456',
        properties: [{ name: '_bundle', value: 'starter-kit' }]
      }
    ]
  });

  assert.deepEqual(rawLineItems, [
    {
      id: 456,
      title: 'Widget',
      admin_graphql_api_id: 'gid://shopify/LineItem/456',
      properties: [{ name: '_bundle', value: 'starter-kit' }]
    }
  ]);
});
