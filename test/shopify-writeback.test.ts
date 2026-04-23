import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.SHOPIFY_APP_API_SECRET ??= 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET ??= 'test-webhook-secret';
process.env.SHOPIFY_APP_API_VERSION ??= '2026-01';
process.env.SHOPIFY_APP_ENCRYPTION_KEY ??= 'test-encryption-key';
process.env.SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES ??= '2';

async function getModules() {
  const poolModule = await import('../src/db/pool.js');
  const writebackModule = await import('../src/modules/shopify/writeback.js');

  return {
    pool: poolModule.pool,
    enqueueShopifyOrderWriteback: writebackModule.enqueueShopifyOrderWriteback,
    processShopifyOrderWritebackQueue: writebackModule.processShopifyOrderWritebackQueue,
    testUtils: writebackModule.__shopifyWritebackTestUtils
  };
}

async function resetIntegrationDatabase() {
  const { pool } = await getModules();

  await pool.query(`
    TRUNCATE TABLE
      shopify_order_writeback_jobs,
      attribution_jobs,
      attribution_order_credits,
      attribution_results,
      daily_reporting_metrics,
      order_attribution_links,
      session_attribution_touch_events,
      session_attribution_identities,
      shopify_order_line_items,
      shopify_orders,
      shopify_webhook_receipts,
      tracking_events,
      tracking_sessions,
      shopify_customers,
      shopify_app_installations,
      customer_identities
    RESTART IDENTITY CASCADE
  `);
}

async function seedActiveInstallation() {
  const { pool } = await getModules();

  await pool.query(
    `
      INSERT INTO shopify_app_installations (
        shop_domain,
        access_token_encrypted,
        scopes,
        status,
        installed_at,
        webhook_base_url,
        webhook_subscriptions,
        shop_name,
        shop_email,
        shop_currency,
        raw_shop_data,
        created_at,
        updated_at
      )
      VALUES (
        'example-shop.myshopify.com',
        pgp_sym_encrypt('test-access-token', $1, 'cipher-algo=aes256, compress-algo=0'),
        ARRAY['read_orders', 'write_orders']::text[],
        'active',
        now(),
        'https://api.example.com/webhooks/shopify',
        '[]'::jsonb,
        'Example Shop',
        'owner@example.com',
        'USD',
        '{}'::jsonb,
        now(),
        now()
      )
    `,
    [process.env.SHOPIFY_APP_ENCRYPTION_KEY]
  );
}

async function seedAttributedOrder() {
  const { pool } = await getModules();

  await pool.query(
    `
      INSERT INTO tracking_sessions (
        id,
        first_seen_at,
        last_seen_at,
        landing_page,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid,
        initial_fbclid,
        initial_ttclid,
        initial_msclkid
      )
      VALUES (
        $1::uuid,
        $2::timestamptz,
        $2::timestamptz,
        $3,
        $4,
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123',
        NULL,
        NULL,
        NULL
      )
    `,
    [
      '11111111-1111-4111-8111-111111111111',
      '2026-04-21T12:00:00.000Z',
      'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale#g',
      'https://www.google.com/search?q=widget'
    ]
  );

  await pool.query(
    `
      INSERT INTO tracking_events (
        session_id,
        event_type,
        occurred_at,
        page_url,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        gclid,
        gbraid,
        raw_payload
      )
      VALUES (
        $1::uuid,
        'page_view',
        '2026-04-21T12:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'GCLID-123',
        'GBRAID-123',
        '{}'::jsonb
      )
    `,
    ['11111111-1111-4111-8111-111111111111']
  );

  await pool.query(
    `
      INSERT INTO shopify_orders (
        shopify_order_id,
        shopify_order_number,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        landing_session_id,
        source_name,
        raw_payload,
        ingested_at
      )
      VALUES (
        '1001',
        '1001',
        'USD',
        '125.00',
        '125.00',
        '2026-04-21T12:10:00.000Z',
        $1::uuid,
        'web',
        '{}'::jsonb,
        now()
      )
    `,
    ['11111111-1111-4111-8111-111111111111']
  );

  await pool.query(
    `
      INSERT INTO attribution_results (
        shopify_order_id,
        session_id,
        attribution_model,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        confidence_score,
        attribution_reason,
        attributed_at,
        reprocess_version
      )
      VALUES (
        '1001',
        $1::uuid,
        'last_touch',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'gbraid',
        'GBRAID-123',
        1,
        'matched_by_landing_session',
        '2026-04-21T12:10:30.000Z',
        1
      )
    `,
    ['11111111-1111-4111-8111-111111111111']
  );
}

test('shopify order writeback queue keys are namespaced by order id', async () => {
  const { testUtils } = await getModules();
  assert.equal(testUtils.buildShopifyOrderWritebackQueueKey('1001'), 'shopify-order-writeback:1001');
  assert.equal(testUtils.computeShopifyOrderWritebackRetryDelaySeconds(1), 30);
  assert.equal(testUtils.computeShopifyOrderWritebackRetryDelaySeconds(10), 1800);
});

test('processShopifyOrderWritebackQueue writes canonical attributes and metafields to Shopify orders', async () => {
  await resetIntegrationDatabase();
  await seedActiveInstallation();
  await seedAttributedOrder();

  const { enqueueShopifyOrderWriteback, processShopifyOrderWritebackQueue } = await getModules();
  const originalFetch = globalThis.fetch;
  const calls: Array<{ query: string; variables: Record<string, unknown> }> = [];

  globalThis.fetch = async (_url, init) => {
    const body = JSON.parse(String(init?.body)) as { query: string; variables: Record<string, unknown> };
    calls.push(body);

    if (body.query.includes('query OrderWritebackOrder')) {
      return new Response(
        JSON.stringify({
          data: {
            order: {
              id: 'gid://shopify/Order/1001',
              customAttributes: [{ key: 'gift_note', value: 'Leave at door' }]
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    if (body.query.includes('mutation UpdateOrderWritebackAttributes')) {
      return new Response(
        JSON.stringify({
          data: {
            orderUpdate: {
              userErrors: []
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    if (body.query.includes('mutation SetOrderWritebackMetafields')) {
      return new Response(
        JSON.stringify({
          data: {
            metafieldsSet: {
              userErrors: []
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    throw new Error(`Unexpected Shopify request: ${body.query}`);
  };

  try {
    await enqueueShopifyOrderWriteback('1001', 'test_shopify_writeback');
    const result = await processShopifyOrderWritebackQueue({
      workerId: 'test-shopify-writeback-success',
      limit: 10,
      staleScanLimit: 0
    });

    assert.equal(result.succeededJobs, 1);
    assert.equal(result.failedJobs, 0);
    assert.equal(result.deadLetteredJobs, 0);

    const attributeUpdateCall = calls.find((call) => call.query.includes('mutation UpdateOrderWritebackAttributes'));
    assert.ok(attributeUpdateCall);

    const customAttributes = (attributeUpdateCall?.variables.input as { customAttributes: Array<{ key: string; value: string }> })
      .customAttributes;
    assert.deepEqual(
      customAttributes.find((attribute) => attribute.key === 'gift_note'),
      { key: 'gift_note', value: 'Leave at door' }
    );
    assert.deepEqual(
      customAttributes.find((attribute) => attribute.key === 'roas_radar_session_id'),
      { key: 'roas_radar_session_id', value: '11111111-1111-4111-8111-111111111111' }
    );
    assert.deepEqual(
      customAttributes.find((attribute) => attribute.key === 'utm_source'),
      { key: 'utm_source', value: 'google' }
    );

    const metafieldsCall = calls.find((call) => call.query.includes('mutation SetOrderWritebackMetafields'));
    assert.ok(metafieldsCall);

    const metafields = metafieldsCall?.variables.metafields as Array<{
      key: string;
      namespace: string;
      type: string;
      value: string;
    }>;
    assert.equal(metafields.length, 2);
    assert.deepEqual(
      metafields.map((metafield) => metafield.key).sort(),
      ['attribution_capture_v1', 'attribution_result_v1']
    );

    const attributionResultPayload = JSON.parse(
      metafields.find((metafield) => metafield.key === 'attribution_result_v1')?.value ?? '{}'
    ) as Record<string, unknown>;
    assert.equal(attributionResultPayload.roas_radar_session_id, '11111111-1111-4111-8111-111111111111');
    assert.equal(attributionResultPayload.attributed_source, 'google');
    assert.equal(attributionResultPayload.attribution_reason, 'matched_by_landing_session');
  } finally {
    globalThis.fetch = originalFetch;
    await resetIntegrationDatabase();
  }
});

test('processShopifyOrderWritebackQueue retries after a partial Shopify write failure and completes on the next attempt', async () => {
  await resetIntegrationDatabase();
  await seedActiveInstallation();
  await seedAttributedOrder();

  const { enqueueShopifyOrderWriteback, processShopifyOrderWritebackQueue, pool } = await getModules();
  const originalFetch = globalThis.fetch;
  let metafieldsAttempts = 0;
  let orderUpdateAttempts = 0;

  globalThis.fetch = async (_url, init) => {
    const body = JSON.parse(String(init?.body)) as { query: string };

    if (body.query.includes('query OrderWritebackOrder')) {
      return new Response(
        JSON.stringify({
          data: {
            order: {
              id: 'gid://shopify/Order/1001',
              customAttributes: []
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    if (body.query.includes('mutation UpdateOrderWritebackAttributes')) {
      orderUpdateAttempts += 1;

      return new Response(
        JSON.stringify({
          data: {
            orderUpdate: {
              userErrors: []
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    if (body.query.includes('mutation SetOrderWritebackMetafields')) {
      metafieldsAttempts += 1;

      if (metafieldsAttempts === 1) {
        return new Response(JSON.stringify({ errors: [{ message: 'metafields failed' }] }), {
          status: 500,
          headers: { 'content-type': 'application/json' }
        });
      }

      return new Response(
        JSON.stringify({
          data: {
            metafieldsSet: {
              userErrors: []
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    throw new Error(`Unexpected Shopify request: ${body.query}`);
  };

  try {
    await enqueueShopifyOrderWriteback('1001', 'test_retry');
    const firstAttempt = await processShopifyOrderWritebackQueue({
      workerId: 'test-shopify-writeback-retry-1',
      limit: 10,
      staleScanLimit: 0
    });

    assert.equal(firstAttempt.succeededJobs, 0);
    assert.equal(firstAttempt.failedJobs, 1);
    assert.equal(firstAttempt.deadLetteredJobs, 0);

    const retryState = await pool.query<{ status: string; attempts: number; last_error: string | null }>(
      `
        SELECT status, attempts, last_error
        FROM shopify_order_writeback_jobs
        WHERE shopify_order_id = '1001'
      `
    );

    assert.equal(retryState.rows[0].status, 'retry');
    assert.equal(retryState.rows[0].attempts, 1);
    assert.match(retryState.rows[0].last_error ?? '', /shopify_admin_api_failed/);

    await pool.query(`
      UPDATE shopify_order_writeback_jobs
      SET available_at = now()
      WHERE shopify_order_id = '1001'
    `);

    const secondAttempt = await processShopifyOrderWritebackQueue({
      workerId: 'test-shopify-writeback-retry-2',
      limit: 10,
      staleScanLimit: 0
    });

    assert.equal(secondAttempt.succeededJobs, 1);
    assert.equal(secondAttempt.failedJobs, 0);
    assert.equal(orderUpdateAttempts, 2);
    assert.equal(metafieldsAttempts, 2);
  } finally {
    globalThis.fetch = originalFetch;
    await resetIntegrationDatabase();
  }
});

test('processShopifyOrderWritebackQueue dead-letters jobs after the retry budget is exhausted', async () => {
  await resetIntegrationDatabase();
  await seedActiveInstallation();
  await seedAttributedOrder();

  const { enqueueShopifyOrderWriteback, processShopifyOrderWritebackQueue, pool } = await getModules();
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (_url, init) => {
    const body = JSON.parse(String(init?.body)) as { query: string };

    if (body.query.includes('query OrderWritebackOrder')) {
      return new Response(
        JSON.stringify({
          data: {
            order: {
              id: 'gid://shopify/Order/1001',
              customAttributes: []
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    if (body.query.includes('mutation UpdateOrderWritebackAttributes')) {
      return new Response(
        JSON.stringify({
          data: {
            orderUpdate: {
              userErrors: [{ field: ['input', 'customAttributes'], message: 'write denied' }]
            }
          }
        }),
        { status: 200, headers: { 'content-type': 'application/json' } }
      );
    }

    throw new Error(`Unexpected Shopify request: ${body.query}`);
  };

  try {
    await enqueueShopifyOrderWriteback('1001', 'test_dead_letter');

    const firstAttempt = await processShopifyOrderWritebackQueue({
      workerId: 'test-shopify-writeback-dead-letter-1',
      limit: 10,
      staleScanLimit: 0
    });
    assert.equal(firstAttempt.failedJobs, 1);
    assert.equal(firstAttempt.deadLetteredJobs, 0);

    await pool.query(`
      UPDATE shopify_order_writeback_jobs
      SET available_at = now()
      WHERE shopify_order_id = '1001'
    `);

    const secondAttempt = await processShopifyOrderWritebackQueue({
      workerId: 'test-shopify-writeback-dead-letter-2',
      limit: 10,
      staleScanLimit: 0
    });
    assert.equal(secondAttempt.failedJobs, 1);
    assert.equal(secondAttempt.deadLetteredJobs, 1);

    const finalState = await pool.query<{ status: string; dead_lettered_at: Date | null }>(
      `
        SELECT status, dead_lettered_at
        FROM shopify_order_writeback_jobs
        WHERE shopify_order_id = '1001'
      `
    );

    assert.equal(finalState.rows[0].status, 'failed');
    assert.ok(finalState.rows[0].dead_lettered_at);
  } finally {
    globalThis.fetch = originalFetch;
    await resetIntegrationDatabase();
  }
});

test.after(async () => {
  const { pool } = await getModules();
  await pool.end();
});
