// Existing file retained; changed regions shown below.

async function fetchGa4ShadowComparison(shopifyOrderId: string) {
  const { pool } = await getModules();
  const result = await pool.query<{
    rollout_mode: string;
    current_match_source: string;
    shadow_match_source: string;
    shadow_would_change_winner: boolean;
    shadow_ga4_client_id: string | null;
    shadow_ga4_session_id: string | null;
  }>(
    `
      SELECT
        rollout_mode,
        current_match_source,
        shadow_match_source,
        shadow_would_change_winner,
        shadow_ga4_client_id,
        shadow_ga4_session_id
      FROM ga4_fallback_shadow_comparisons
      WHERE shopify_order_id = $1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

async function resetIntegrationDatabase() {
  const { pool } = await getModules();

  await pool.query(`
    TRUNCATE TABLE
      attribution_jobs,
      shopify_order_writeback_jobs,
      ga4_fallback_shadow_comparisons,
      ga4_fallback_candidates,
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
      customer_identities
    RESTART IDENTITY CASCADE
  `);
}

test('shadow rollout records the GA4 comparison without applying the fallback result', async () => {
  await resetIntegrationDatabase();
  process.env.GA4_FALLBACK_ROLLOUT_MODE = 'shadow';
  const { pool } = await getModules();

  try {
    await insertGa4FallbackCandidate({
      occurredAt: '2026-04-07T08:55:00.000Z',
      transactionId: 'order-ga4-shadow-1',
      source: 'google',
      medium: 'cpc',
      campaign: 'shadow-campaign',
      clickIdType: 'gclid',
      clickIdValue: 'gclid-shadow',
      ga4ClientId: 'ga4-shadow-client',
      ga4SessionId: 'ga4-shadow-session'
    });

    await insertShopifyOrder(pool, {
      shopifyOrderId: 'order-ga4-shadow-1',
      processedAt: '2026-04-07T09:05:00.000Z',
      rawPayload: JSON.stringify({
        id: 'order-ga4-shadow-1',
        source_name: 'web',
        landing_site: 'https://store.example/products/widget'
      })
    });

    await processOrder('order-ga4-shadow-1');

    const attributionResult = await fetchAttributionResult('order-ga4-shadow-1');
    assert.equal(attributionResult.match_source, 'unattributed');
    assert.equal(attributionResult.confidence_score, '0.00');

    const shadowComparison = await fetchGa4ShadowComparison('order-ga4-shadow-1');
    assert.deepEqual(shadowComparison, {
      rollout_mode: 'shadow',
      current_match_source: 'unattributed',
      shadow_match_source: 'ga4_fallback',
      shadow_would_change_winner: true,
      shadow_ga4_client_id: 'ga4-shadow-client',
      shadow_ga4_session_id: 'ga4-shadow-session'
    });
  } finally {
    delete process.env.GA4_FALLBACK_ROLLOUT_MODE;
    await resetIntegrationDatabase();
  }
});
