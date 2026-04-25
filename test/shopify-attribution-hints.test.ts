import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.SHOPIFY_APP_API_SECRET = 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET = 'test-webhook-secret';

async function getShopifyTestUtils() {
  const shopifyModule = await import('../src/modules/shopify/index.js');
  return shopifyModule.__shopifyTestUtils;
}

test('extractShopifyHintAttribution returns null when Shopify hints contain no attribution dimensions', async () => {
  const shopifyTestUtils = await getShopifyTestUtils();
  const attribution = shopifyTestUtils.extractShopifyHintAttribution({
    id: 'order-no-hints',
    customer: null,
    email: null,
    source_name: 'web',
    landing_site: 'https://store.example/products/widget',
    referring_site: 'https://www.google.com/',
    line_items: []
  });

  assert.equal(attribution, null);
});

test('extractShopifyHintAttribution treats click-id-only hints as non-direct synthetic attribution with click-id confidence', async () => {
  const shopifyTestUtils = await getShopifyTestUtils();
  const attribution = shopifyTestUtils.extractShopifyHintAttribution({
    id: 'order-click-id-hint',
    customer: null,
    email: null,
    source_name: 'web',
    landing_site: 'https://store.example/products/widget?fbclid=FB-CLICK-123',
    line_items: []
  });

  assert.deepEqual(attribution, {
    source: 'meta',
    medium: 'paid_social',
    campaign: null,
    content: null,
    term: null,
    clickIdType: 'fbclid',
    clickIdValue: 'FB-CLICK-123',
    confidenceScore: 0.55
  });
});

test('extractShopifyHintAttribution normalizes UTM hints and uses non-click synthetic confidence when no click id is present', async () => {
  const shopifyTestUtils = await getShopifyTestUtils();
  const attribution = shopifyTestUtils.extractShopifyHintAttribution({
    id: 'order-utm-hint',
    customer: null,
    email: null,
    source_name: 'web',
    landing_site:
      'https://store.example/products/widget?utm_source=Google&utm_medium=Paid_Social&utm_campaign=Spring-Launch&utm_content=Hero&utm_term=Widgets',
    line_items: []
  });

  assert.deepEqual(attribution, {
    source: 'google',
    medium: 'paid_social',
    campaign: 'spring-launch',
    content: 'hero',
    term: 'widgets',
    clickIdType: null,
    clickIdValue: null,
    confidenceScore: 0.4
  });
});

test('extractShopifyHintAttribution reads canonical landing/page keys, strips fragments, and ignores referrer-only keys', async () => {
  const shopifyTestUtils = await getShopifyTestUtils();
  const attribution = shopifyTestUtils.extractShopifyHintAttribution({
    id: 'order-canonical-url-hint',
    customer: null,
    email: null,
    source_name: 'web',
    landing_site: 'https://store.example/products/widget',
    note_attributes: [
      {
        name: 'page_url',
        value: 'https://store.example/products/widget?utm_source=Google&utm_medium=CPC&gbraid=GBRAID-123#section'
      },
      {
        name: 'referrer_url',
        value: 'https://www.google.com/search?q=widget&utm_source=should-not-win'
      }
    ],
    line_items: []
  });

  assert.deepEqual(attribution, {
    source: 'google',
    medium: 'cpc',
    campaign: null,
    content: null,
    term: null,
    clickIdType: 'gbraid',
    clickIdValue: 'GBRAID-123',
    confidenceScore: 0.55
  });
});
