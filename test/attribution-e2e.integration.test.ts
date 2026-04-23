import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.SHOPIFY_APP_API_SECRET = 'test-app-secret';
process.env.SHOPIFY_WEBHOOK_SECRET = 'test-webhook-secret';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');
const harnessModule = await import('./e2e-harness.js');

const { pool } = poolModule;
const { createServer, closeServer } = serverModule;
const {
  ATTRIBUTION_MODELS,
  buildReportingQuery,
  fetchPersistedCredits,
  fetchReportingCampaigns,
  fetchReportingOrders,
  fetchReportingSummary,
  fetchReportingTimeseries,
  resetE2EDatabase,
  seedSyntheticJourney
} = harnessModule;

async function withSeededHarness(
  run: (context: { server: ReturnType<typeof createServer>; seeded: Awaited<ReturnType<typeof seedSyntheticJourney>> }) => Promise<void>
) {
  await resetE2EDatabase();
  const server = createServer();

  try {
    const seeded = await seedSyntheticJourney(server);
    await run({ server, seeded });
  } finally {
    await closeServer(server);
    await resetE2EDatabase();
  }
}

test('synthetic journeys persist deterministic attribution credits across models', async () => {
  await withSeededHarness(async ({ seeded }) => {
    assert.equal(seeded.processedJobs, 2);

    for (const attributionModel of ATTRIBUTION_MODELS) {
      const persistedCredits = await fetchPersistedCredits(seeded.multiTouchOrderId, attributionModel);
      const expectedCredits = seeded.expectedOutputs[attributionModel];

      assert.deepEqual(
        persistedCredits.map((credit) => ({
          source: credit.source,
          medium: credit.medium,
          campaign: credit.campaign,
          revenueCredit: credit.revenueCredit,
          isPrimary: credit.isPrimary,
          attributionReason: credit.attributionReason
        })),
        expectedCredits.map((credit) => ({
          source: credit.source,
          medium: credit.medium,
          campaign: credit.campaign,
          revenueCredit: credit.revenueCredit,
          isPrimary: credit.isPrimary,
          attributionReason: credit.attributionReason
        }))
      );
    }

    const firstTouchCredits = await fetchPersistedCredits(seeded.multiTouchOrderId, 'first_touch');
    const lastTouchCredits = await fetchPersistedCredits(seeded.multiTouchOrderId, 'last_touch');
    const ruleBasedCredits = await fetchPersistedCredits(seeded.multiTouchOrderId, 'rule_based_weighted');

    assert.deepEqual(
      firstTouchCredits.map((credit) => credit.revenueCredit),
      ['120.00', '0.00', '0.00']
    );
    assert.deepEqual(
      lastTouchCredits.map((credit) => credit.revenueCredit),
      ['0.00', '0.00', '120.00']
    );
    assert.deepEqual(
      ruleBasedCredits.map((credit) => ({
        campaign: credit.campaign ?? 'unknown',
        revenueCredit: credit.revenueCredit,
        isPrimary: credit.isPrimary
      })),
      [
        {
          campaign: 'spring-search',
          revenueCredit: '40.91',
          isPrimary: false
        },
        {
          campaign: 'unknown',
          revenueCredit: '10.91',
          isPrimary: false
        },
        {
          campaign: 'retargeting',
          revenueCredit: '68.18',
          isPrimary: true
        }
      ]
    );
  });
});

test('reporting APIs expose model-specific dashboard metrics for seeded journeys', async () => {
  await withSeededHarness(async ({ server, seeded }) => {
    const firstTouchSummary = await fetchReportingSummary(
      server,
      buildReportingQuery(seeded, 'first_touch', {
        source: 'google',
        campaign: 'spring-search'
      })
    );
    const lastTouchSummary = await fetchReportingSummary(
      server,
      buildReportingQuery(seeded, 'last_touch', {
        source: 'google',
        campaign: 'spring-search'
      })
    );

    assert.deepEqual(firstTouchSummary.totals, {
      visits: 1,
      orders: 1,
      revenue: 120,
      spend: 0,
      conversionRate: 1,
      roas: null
    });
    assert.deepEqual(lastTouchSummary.totals, {
      visits: 1,
      orders: 0,
      revenue: 0,
      spend: 0,
      conversionRate: 0,
      roas: null
    });

    const weightedCampaigns = await fetchReportingCampaigns(
      server,
      buildReportingQuery(seeded, 'rule_based_weighted', {
        limit: '10'
      })
    );

    assert.deepEqual(
      weightedCampaigns.rows.map((row) => ({
        campaign: row.campaign,
        revenue: row.revenue,
        orders: row.orders
      })),
      [
        {
          campaign: 'brand-defense',
          revenue: 80,
          orders: 1
        },
        {
          campaign: 'retargeting',
          revenue: 68.18,
          orders: 0.56818182
        },
        {
          campaign: 'spring-search',
          revenue: 40.91,
          orders: 0.34090909
        },
        {
          campaign: 'unknown',
          revenue: 10.91,
          orders: 0.09090909
        }
      ]
    );

    const weightedTimeseries = await fetchReportingTimeseries(
      server,
      buildReportingQuery(seeded, 'rule_based_weighted', {
        groupBy: 'campaign'
      })
    );

    assert.deepEqual(weightedTimeseries.points, [
      {
        date: 'brand-defense',
        visits: 1,
        orders: 1,
        revenue: 80
      },
      {
        date: 'retargeting',
        visits: 1,
        orders: 0.56818182,
        revenue: 68.18
      },
      {
        date: 'spring-search',
        visits: 1,
        orders: 0.34090909,
        revenue: 40.91
      },
      {
        date: 'unknown',
        visits: 1,
        orders: 0.09090909,
        revenue: 10.91
      }
    ]);

    const firstTouchOrders = await fetchReportingOrders(
      server,
      buildReportingQuery(seeded, 'first_touch', {
        limit: '10'
      })
    );
    const lastTouchOrders = await fetchReportingOrders(
      server,
      buildReportingQuery(seeded, 'last_touch', {
        limit: '10'
      })
    );
    const weightedOrders = await fetchReportingOrders(
      server,
      buildReportingQuery(seeded, 'rule_based_weighted', {
        limit: '10'
      })
    );

    const firstTouchMultiTouchOrder = firstTouchOrders.rows.find((row) => row.shopifyOrderId === seeded.multiTouchOrderId);
    const lastTouchMultiTouchOrder = lastTouchOrders.rows.find((row) => row.shopifyOrderId === seeded.multiTouchOrderId);
    const weightedMultiTouchOrder = weightedOrders.rows.find((row) => row.shopifyOrderId === seeded.multiTouchOrderId);

    assert.deepEqual(firstTouchMultiTouchOrder, {
      shopifyOrderId: seeded.multiTouchOrderId,
      processedAt: firstTouchMultiTouchOrder?.processedAt ?? null,
      totalPrice: 120,
      source: 'google',
      medium: 'cpc',
      campaign: 'spring-search',
      attributionReason: 'matched_by_customer_identity'
    });
    assert.deepEqual(lastTouchMultiTouchOrder, {
      shopifyOrderId: seeded.multiTouchOrderId,
      processedAt: lastTouchMultiTouchOrder?.processedAt ?? null,
      totalPrice: 120,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'retargeting',
      attributionReason: 'matched_by_customer_identity'
    });
    assert.deepEqual(weightedMultiTouchOrder, {
      shopifyOrderId: seeded.multiTouchOrderId,
      processedAt: weightedMultiTouchOrder?.processedAt ?? null,
      totalPrice: 120,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'retargeting',
      attributionReason: 'matched_by_customer_identity'
    });
  });
});

test.after(async () => {
  await pool.end();
});
