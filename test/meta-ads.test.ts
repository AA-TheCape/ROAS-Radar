import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const { __metaAdsTestUtils } = await import('../src/modules/meta-ads/index.js');

test('summarizeOrderValueRecords counts null canonical fields and selection modes', () => {
  const summary = __metaAdsTestUtils.summarizeOrderValueRecords([
    {
      attributedRevenue: 100,
      purchaseCount: 2,
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority'
    },
    {
      attributedRevenue: null,
      purchaseCount: 1,
      actionTypeUsed: null,
      canonicalSelectionMode: 'none'
    },
    {
      attributedRevenue: null,
      purchaseCount: null,
      actionTypeUsed: 'omni_purchase',
      canonicalSelectionMode: 'fallback'
    }
  ]);

  assert.deepEqual(summary, {
    totalRows: 3,
    nullAttributedRevenueCount: 2,
    nullPurchaseCountCount: 1,
    nullActionTypeCount: 1,
    fallbackSelectionCount: 1,
    prioritySelectionCount: 1,
    noSelectionCount: 1
  });
});

test('buildOrderValueSyncAnomalies detects zero-row pulls and sudden null-rate spikes against the baseline', () => {
  const records = [
    {
      attributedRevenue: null,
      purchaseCount: null,
      actionTypeUsed: null,
      canonicalSelectionMode: 'none'
    },
    {
      attributedRevenue: null,
      purchaseCount: null,
      actionTypeUsed: null,
      canonicalSelectionMode: 'none'
    },
    {
      attributedRevenue: null,
      purchaseCount: null,
      actionTypeUsed: null,
      canonicalSelectionMode: 'fallback'
    },
    {
      attributedRevenue: 120,
      purchaseCount: 2,
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority'
    },
    {
      attributedRevenue: 140,
      purchaseCount: 3,
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority'
    }
  ];
  const summary = __metaAdsTestUtils.summarizeOrderValueRecords(records);
  const anomalies = __metaAdsTestUtils.buildOrderValueSyncAnomalies({
    rawRowsFetched: 0,
    records,
    summary,
    baseline: {
      totalRows: 5,
      nullAttributedRevenueCount: 0,
      nullPurchaseCountCount: 0,
      nullActionTypeCount: 0
    }
  });

  assert.deepEqual(
    anomalies.map((anomaly: { type: string }) => anomaly.type),
    [
      'zero_rows_pulled',
      'null_attributed_revenue_spike',
      'null_purchase_count_spike',
      'null_action_type_spike'
    ]
  );
});
