async function getModules() {
  const poolModule = await import('../src/db/pool.js');
  const writebackModule = await import('../src/modules/shopify/writeback.js');

  return {
    pool: poolModule.pool,
    enqueueShopifyOrderWriteback: writebackModule.enqueueShopifyOrderWriteback,
    processShopifyOrderWritebackQueue: writebackModule.processShopifyOrderWritebackQueue,
    reconcileRecentShopifyOrderAttributes: writebackModule.reconcileRecentShopifyOrderAttributes,
    testUtils: writebackModule.__shopifyWritebackTestUtils
  };
}

function buildCanonicalShopifyAttributes(sessionId: string): Array<{ key: string; value: string }> {
  return [
    { key: 'schema_version', value: '1' },
    { key: 'roas_radar_session_id', value: sessionId },
    { key: 'landing_url', value: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale' },
    { key: 'referrer_url', value: 'https://www.google.com/search?q=widget' },
    {
      key: 'page_url',
      value: 'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123'
    },
    { key: 'utm_source', value: 'google' },
    { key: 'utm_medium', value: 'cpc' },
    { key: 'utm_campaign', value: 'spring-sale' },
    { key: 'utm_content', value: 'hero' },
    { key: 'utm_term', value: 'widgets' },
    { key: 'gclid', value: 'GCLID-123' },
    { key: 'gbraid', value: 'GBRAID-123' },
    { key: 'wbraid', value: 'WBRAID-123' }
  ];
}

test('reconcileRecentShopifyOrderAttributes requeues recent orders with missing canonical Shopify attributes idempotently', async () => {
  ...
  const firstReport = await reconcileRecentShopifyOrderAttributes({
    workerId: 'test-shopify-reconciliation-1',
    limit: 10,
    lookbackDays: 30,
    now: new Date('2026-04-23T00:00:00.000Z')
  });

  assert.equal(firstReport.scannedOrders, 1);
  assert.equal(firstReport.ordersNeedingWriteback, 1);
  assert.equal(firstReport.requeuedOrders, 1);
  ...

  const secondReport = await reconcileRecentShopifyOrderAttributes({
    workerId: 'test-shopify-reconciliation-2',
    limit: 10,
    lookbackDays: 30,
    now: new Date('2026-04-23T00:00:00.000Z')
  });

  assert.equal(secondReport.requeuedOrders, 1);

  const secondQueueState = await pool.query<{ total: string }>(`
    SELECT COUNT(*)::text AS total
    FROM shopify_order_writeback_jobs
    WHERE shopify_order_id = '1001'
  `);
  assert.equal(secondQueueState.rows[0].total, '1');
});

test('reconcileRecentShopifyOrderAttributes reports up-to-date, skipped, and failed orders separately', async () => {
  ...
  const report = await reconcileRecentShopifyOrderAttributes({
    workerId: 'test-shopify-reconciliation-report',
    limit: 10,
    lookbackDays: 30,
    now: new Date('2026-04-24T00:00:00.000Z')
  });

  assert.equal(report.scannedOrders, 4);
  assert.equal(report.ordersNeedingWriteback, 1);
  assert.equal(report.requeuedOrders, 1);
  assert.equal(report.upToDateOrders, 1);
  assert.equal(report.skippedOrders, 1);
  assert.equal(report.failedOrders, 1);
});
