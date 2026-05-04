import assert from "node:assert/strict";
import test from "node:test";

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS = '30';
process.env.META_ADS_SYNC_LOOKBACK_DAYS = '7';

const { __metaAdsTestUtils } = await import("../src/modules/meta-ads/index.js");

test('buildPlanningDates uses the initial lookback before the first successful spend sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    null
  );

  assert.equal(dates[0], '2026-03-13');
  assert.equal(dates.at(-1), '2026-04-11');
  assert.equal(dates.length, 30);
});

test('buildPlanningDates narrows to the incremental spend lookback after a successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, [
    '2026-04-05',
    '2026-04-06',
    '2026-04-07',
    '2026-04-08',
    '2026-04-09',
    '2026-04-10',
    '2026-04-11'
  ]);
});

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

test('selectCanonicalActionType prefers purchase, then omni_purchase, then pixel purchase before fallback types', () => {
  assert.deepEqual(
    __metaAdsTestUtils.selectCanonicalActionType([
      {
        action_type: 'onsite_conversion.messaging_purchase',
        action_values: [{ action_type: 'onsite_conversion.messaging_purchase', value: '12.00' }]
      },
      {
        action_type: 'purchase',
        action_values: [{ action_type: 'purchase', value: '15.00' }]
      }
    ]),
    {
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority'
    }
  );

  assert.deepEqual(
    __metaAdsTestUtils.selectCanonicalActionType([
      {
        action_type: 'onsite_conversion.messaging_purchase',
        action_values: [{ action_type: 'onsite_conversion.messaging_purchase', value: '12.00' }]
      },
      {
        action_type: 'omni_purchase',
        action_values: [{ action_type: 'omni_purchase', value: '9.00' }]
      }
    ]),
    {
      actionTypeUsed: 'omni_purchase',
      canonicalSelectionMode: 'priority'
    }
  );
});

test('normalizeOrderValueRows keeps the selected action type stable when one metric array is missing the selected type', () => {
  const normalized = __metaAdsTestUtils.normalizeOrderValueRows({
    currency: 'USD',
    persistedRows: [
      {
        id: 11,
        payload: {
          campaign_id: 'cmp_1',
          campaign_name: 'Campaign One',
          date_start: '2026-04-29',
          date_stop: '2026-04-29',
          spend: '10.00',
          action_type: 'onsite_conversion.messaging_purchase',
          actions: [],
          action_values: [{ action_type: 'onsite_conversion.messaging_purchase', value: '12.00' }],
          purchase_roas: [{ action_type: 'onsite_conversion.messaging_purchase', value: '1.200000' }]
        }
      }
    ]
  });

  assert.equal(normalized.length, 1);
  assert.equal(normalized[0]?.actionTypeUsed, 'onsite_conversion.messaging_purchase');
  assert.equal(normalized[0]?.canonicalSelectionMode, 'fallback');
  assert.equal(normalized[0]?.attributedRevenue, 12);
  assert.equal(normalized[0]?.purchaseCount, null);
});

test('buildMetaAdsMetadataRecords normalizes ids and collapses duplicate names to the latest non-blank value', () => {
  const observedAt = new Date('2026-04-11T12:00:00.000Z');

  const records = __metaAdsTestUtils.buildMetaAdsMetadataRecords({
    accountId: ' 123456789 ',
    observedAt,
    campaignRows: [
      { id: ' cmp_1 ', name: '  Brand   Search ' },
      { id: 'cmp_1', name: ' ' },
      { id: 'cmp_2', name: ' Prospecting ' }
    ],
    adsetRows: [{ id: ' adset_1 ', name: ' US   Ad Set ' }],
    adRows: [{ id: ' ad_1 ', name: ' Headline   A ' }]
  });

  assert.deepEqual(records, [
    {
      platform: 'meta_ads',
      accountId: '123456789',
      entityType: 'campaign',
      entityId: 'cmp_1',
      latestName: 'Brand Search',
      lastSeenAt: observedAt
    },
    {
      platform: 'meta_ads',
      accountId: '123456789',
      entityType: 'campaign',
      entityId: 'cmp_2',
      latestName: 'Prospecting',
      lastSeenAt: observedAt
    },
    {
      platform: 'meta_ads',
      accountId: '123456789',
      entityType: 'adset',
      entityId: 'adset_1',
      latestName: 'US Ad Set',
      lastSeenAt: observedAt
    },
    {
      platform: 'meta_ads',
      accountId: '123456789',
      entityType: 'ad',
      entityId: 'ad_1',
      latestName: 'Headline A',
      lastSeenAt: observedAt
    }
  ]);
});

test('isRetryableMetaAdsApiError treats Meta rate-limit codes as retryable even on HTTP 400 responses', () => {
  const rateLimited = __metaAdsTestUtils.createMetaAdsApiErrorForTest(400, 'Application request limit reached', {
    error: {
      code: 4,
      error_subcode: 123,
      message: 'Application request limit reached'
    }
  });
  const invalid = __metaAdsTestUtils.createMetaAdsApiErrorForTest(400, 'Invalid parameter', {
    error: {
      code: 100,
      message: 'Invalid parameter'
    }
  });

  assert.equal(__metaAdsTestUtils.isRetryableMetaAdsApiError(rateLimited), true);
  assert.equal(__metaAdsTestUtils.isRetryableMetaAdsApiError(invalid), false);
  assert.equal(
    __metaAdsTestUtils.formatMetaAdsError(rateLimited),
    'Application request limit reached (status=400, code=4, subcode=123)'
  );
});
