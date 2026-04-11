import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.SHOPIFY_APP_API_KEY = 'test-key';
process.env.SHOPIFY_APP_API_SECRET = 'test-secret';
process.env.SHOPIFY_APP_API_VERSION = '2025-01';
process.env.SHOPIFY_APP_BASE_URL = 'https://api.example.com';
process.env.SHOPIFY_APP_ENCRYPTION_KEY = 'test-encryption-key';
process.env.SHOPIFY_APP_SCOPES = 'read_orders,write_products';

const { __shopifyTestUtils } = await import('../src/modules/shopify/index.js');

test('normalizeShopDomain accepts valid myshopify domains', () => {
  assert.equal(__shopifyTestUtils.normalizeShopDomain('Example-Shop.myshopify.com'), 'example-shop.myshopify.com');
});

test('normalizeShopDomain rejects invalid domains', () => {
  assert.throws(
    () => __shopifyTestUtils.normalizeShopDomain('store.example.com'),
    /valid \*\.myshopify\.com/
  );
});

test('verifyShopifyOAuthHmac validates Shopify callback signatures', () => {
  const originalUrl =
    '/shopify/oauth/callback?code=test-code&shop=example-shop.myshopify.com&state=test-state&timestamp=1712794114';
  const message = __shopifyTestUtils.createOAuthHmacMessage(originalUrl);
  const validHmac = createHmac('sha256', 'test-secret').update(message).digest('hex');

  assert.equal(__shopifyTestUtils.verifyShopifyOAuthHmac(`${originalUrl}&hmac=${validHmac}`, validHmac), true);
  assert.equal(__shopifyTestUtils.verifyShopifyOAuthHmac(`${originalUrl}&hmac=invalid`, 'invalid'), false);
});

test('buildShopifyInstallUrl includes expected OAuth parameters', () => {
  const installUrl = new URL(
    __shopifyTestUtils.buildShopifyInstallUrl('example-shop.myshopify.com', 'state-123', '/dashboard')
  );

  assert.equal(installUrl.origin, 'https://example-shop.myshopify.com');
  assert.equal(installUrl.pathname, '/admin/oauth/authorize');
  assert.equal(installUrl.searchParams.get('client_id'), 'test-key');
  assert.equal(installUrl.searchParams.get('scope'), 'read_orders,write_products');
  assert.equal(installUrl.searchParams.get('redirect_uri'), 'https://api.example.com/shopify/oauth/callback');
  assert.equal(installUrl.searchParams.get('state'), 'state-123');
  assert.equal(installUrl.searchParams.get('return_to'), '/dashboard');
});
