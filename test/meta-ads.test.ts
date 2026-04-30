import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.META_ADS_APP_ID = 'meta-app-id';
process.env.META_ADS_APP_SECRET = 'meta-app-secret';
process.env.META_ADS_APP_BASE_URL = 'https://api.example.com';
process.env.META_ADS_APP_SCOPES = 'ads_read,business_management';
process.env.META_ADS_ENCRYPTION_KEY = 'meta-encryption-key';
process.env.META_ADS_AD_ACCOUNT_ID = 'act_123456789';
process.env.META_ADS_SYNC_LOOKBACK_DAYS = '3';
process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS = '5';

const { __metaAdsTestUtils } = await import('../src/modules/meta-ads/index.js');
const fixturesDirectory = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures', 'meta-ads');

async function loadJsonFixture<T>(filename: string): Promise<T> {
  const fixture = await readFile(path.join(fixturesDirectory, filename), 'utf8');
  return JSON.parse(fixture) as T;
}

test('buildPlanningDates uses the initial lookback before the first successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(new Date('2026-04-11T12:00:00.000Z'), null);

  assert.deepEqual(dates, ['2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10', '2026-04-11']);
});

test('buildPlanningDates starts from the last successful reporting-timezone business date after at least one successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-09', '2026-04-10', '2026-04-11']);
});

test('buildIncrementalPlanningDates re-enqueues only today after the daily plan has already been created', () => {
  const dates = __metaAdsTestUtils.buildIncrementalPlanningDates(
    new Date('2026-04-25T19:30:00.000Z'),
    new Date('2026-04-25T07:17:28.000Z'),
    '2026-04-25'
  );

  assert.deepEqual(dates, ['2026-04-25']);
});

test('buildPlanningDates only keeps the current business date in window after an earlier successful run on the same day', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T23:00:00.000Z'),
    new Date('2026-04-11T16:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-11']);
});

test('buildOrderValueWindow limits hourly revenue sync to the active operational window with no historical backfill', () => {
  const window = __metaAdsTestUtils.buildOrderValueWindow(new Date('2026-04-30T15:10:00.000Z'));

  assert.deepEqual(window, {
    startDate: '2026-04-29',
    endDate: '2026-04-30',
    dates: ['2026-04-29', '2026-04-30']
  });
});

test('startMetaAdsOrderValueScheduler schedules hourly ticks and avoids overlapping runs', async () => {
  const triggerSources: string[] = [];
  const intervalCalls: number[] = [];
  const pendingResolvers: Array<() => void> = [];
  let intervalHandler: (() => void) | undefined;
  let clearedTimer: unknown = null;

  const stop = __metaAdsTestUtils.startMetaAdsOrderValueScheduler({
    intervalMs: 3_600_000,
    triggerImmediately: false,
    setIntervalFn: ((handler: () => void, intervalMs: number) => {
      intervalHandler = handler;
      intervalCalls.push(intervalMs);
      return 77 as ReturnType<typeof setInterval>;
    }) as typeof setInterval,
    clearIntervalFn: ((timer: ReturnType<typeof setInterval>) => {
      clearedTimer = timer;
    }) as typeof clearInterval,
    runner: ({ triggerSource } = {}) =>
      new Promise((resolve) => {
        triggerSources.push(triggerSource ?? 'missing');
        pendingResolvers.push(resolve);
      })
  });

  assert.deepEqual(intervalCalls, [3_600_000]);

  intervalHandler?.();
  intervalHandler?.();

  assert.deepEqual(triggerSources, ['application_scheduler']);

  pendingResolvers.shift()?.();
  await new Promise((resolve) => setImmediate(resolve));

  intervalHandler?.();

  assert.deepEqual(triggerSources, ['application_scheduler', 'application_scheduler']);

  pendingResolvers.shift()?.();
  await new Promise((resolve) => setImmediate(resolve));

  stop();

  assert.equal(clearedTimer, 77);
});

test('rollupPersistableSpendRows collapses duplicate campaign-level entities before persistence', () => {
  const rolled = __metaAdsTestUtils.rollupPersistableSpendRows([
    {
      rawRecordId: 11,
      normalizedRow: {
        granularity: 'campaign',
        entityKey: 'campaign-1',
        accountId: 'act_1',
        accountName: 'Account',
        campaignId: 'campaign-1',
        campaignName: 'Campaign One',
        adsetId: null,
        adsetName: null,
        adId: null,
        adName: null,
        creativeId: null,
        creativeName: null,
        canonicalSource: 'meta',
        canonicalMedium: 'paid_social',
        canonicalCampaign: 'campaign one',
        canonicalContent: 'unknown',
        canonicalTerm: 'unknown',
        currency: 'USD',
        spend: '12.34',
        impressions: 100,
        clicks: 5,
        rawPayload: { row: 1 }
      }
    },
    {
      rawRecordId: 12,
      normalizedRow: {
        granularity: 'campaign',
        entityKey: 'campaign-1',
        accountId: 'act_1',
        accountName: 'Account',
        campaignId: 'campaign-1',
        campaignName: 'Campaign One',
        adsetId: null,
        adsetName: null,
        adId: null,
        adName: null,
        creativeId: null,
        creativeName: null,
        canonicalSource: 'meta',
        canonicalMedium: 'paid_social',
        canonicalCampaign: 'campaign one',
        canonicalContent: 'unknown',
        canonicalTerm: 'unknown',
        currency: 'USD',
        spend: '7.66',
        impressions: 40,
        clicks: 3,
        rawPayload: { row: 2 }
      }
    },
    {
      rawRecordId: 13,
      normalizedRow: {
        granularity: 'ad',
        entityKey: 'ad-1',
        accountId: 'act_1',
        accountName: 'Account',
        campaignId: 'campaign-1',
        campaignName: 'Campaign One',
        adsetId: 'adset-1',
        adsetName: 'Adset One',
        adId: 'ad-1',
        adName: 'Ad One',
        creativeId: null,
        creativeName: null,
        canonicalSource: 'meta',
        canonicalMedium: 'paid_social',
        canonicalCampaign: 'campaign one',
        canonicalContent: 'ad one',
        canonicalTerm: 'unknown',
        currency: 'USD',
        spend: '3.00',
        impressions: 20,
        clicks: 2,
        rawPayload: { row: 3 }
      }
    }
  ]);

  assert.deepEqual(rolled, [
    {
      rawRecordId: 11,
      normalizedRow: {
        granularity: 'campaign',
        entityKey: 'campaign-1',
        accountId: 'act_1',
        accountName: 'Account',
        campaignId: 'campaign-1',
        campaignName: 'Campaign One',
        adsetId: null,
        adsetName: null,
        adId: null,
        adName: null,
        creativeId: null,
        creativeName: null,
        canonicalSource: 'meta',
        canonicalMedium: 'paid_social',
        canonicalCampaign: 'campaign one',
        canonicalContent: 'unknown',
        canonicalTerm: 'unknown',
        currency: 'USD',
        spend: '20.00',
        impressions: 140,
        clicks: 8,
        rawPayload: { row: 1 }
      }
    },
    {
      rawRecordId: 13,
      normalizedRow: {
        granularity: 'ad',
        entityKey: 'ad-1',
        accountId: 'act_1',
        accountName: 'Account',
        campaignId: 'campaign-1',
        campaignName: 'Campaign One',
        adsetId: 'adset-1',
        adsetName: 'Adset One',
        adId: 'ad-1',
        adName: 'Ad One',
        creativeId: null,
        creativeName: null,
        canonicalSource: 'meta',
        canonicalMedium: 'paid_social',
        canonicalCampaign: 'campaign one',
        canonicalContent: 'ad one',
        canonicalTerm: 'unknown',
        currency: 'USD',
        spend: '3.00',
        impressions: 20,
        clicks: 2,
        rawPayload: { row: 3 }
      }
    }
  ]);
});

test('fetchCampaignDailyRevenueInsights requests the contract query params, handles pagination, and normalizes live fixture rows', async () => {
  const previousFetch = globalThis.fetch;
  const requestUrls: URL[] = [];
  const page1 = await loadJsonFixture<Record<string, unknown>>('campaign-daily-revenue-live-page-1.json');
  const page2 = await loadJsonFixture<Record<string, unknown>>('campaign-daily-revenue-live-page-2.json');

  try {
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
      requestUrls.push(url);

      if (url.searchParams.get('after') === 'page-2') {
        return new Response(JSON.stringify(page2), { status: 200 });
      }

      return new Response(JSON.stringify(page1), { status: 200 });
    }) as typeof globalThis.fetch;

    const records = await __metaAdsTestUtils.fetchCampaignDailyRevenueInsights({
      accessToken: 'meta-token',
      adAccountId: '123456789',
      startDate: '2026-04-28',
      endDate: '2026-04-28',
      currency: 'USD'
    });

    assert.equal(requestUrls.length, 2);
    assert.match(requestUrls[0]?.pathname ?? '', /^\/v\d+\.\d+\/act_123456789\/insights$/);
    assert.equal(
      requestUrls[0]?.searchParams.get('fields'),
      'campaign_id,campaign_name,date_start,date_stop,spend,actions,action_values,purchase_roas'
    );
    assert.equal(requestUrls[0]?.searchParams.get('level'), 'campaign');
    assert.equal(requestUrls[0]?.searchParams.get('time_increment'), '1');
    assert.equal(requestUrls[0]?.searchParams.get('action_breakdowns'), 'action_type');
    assert.equal(requestUrls[0]?.searchParams.get('use_account_attribution_setting'), 'true');
    assert.equal(requestUrls[0]?.searchParams.get('action_report_time'), 'conversion');
    assert.equal(requestUrls[0]?.searchParams.get('time_range'), '{"since":"2026-04-28","until":"2026-04-28"}');

    assert.deepEqual(records, [
      {
        adAccountId: '123456789',
        reportDate: '2026-04-28',
        rawDateStart: '2026-04-28',
        rawDateStop: '2026-04-28',
        campaignId: 'cmp_live_1',
        campaignName: 'Live Campaign One',
        currency: 'USD',
        spend: '12.34',
        attributedRevenue: '45.67',
        purchaseCount: 3,
        purchaseRoas: '3.701135',
        actionTypeUsed: 'purchase',
        canonicalSelectionMode: 'priority',
        actionReportTime: 'conversion',
        useAccountAttributionSetting: true,
        rawActionValues: [
          {
            action_type: 'omni_purchase',
            value: '39.50'
          },
          {
            action_type: 'purchase',
            value: '45.67'
          }
        ],
        rawActions: [
          {
            action_type: 'omni_purchase',
            value: '2'
          },
          {
            action_type: 'purchase',
            value: '3'
          }
        ],
        rawRows: [
          page1.data?.[0],
          page1.data?.[1]
        ]
      },
      {
        adAccountId: '123456789',
        reportDate: '2026-04-28',
        rawDateStart: '2026-04-28',
        rawDateStop: '2026-04-28',
        campaignId: 'cmp_live_2',
        campaignName: 'Live Campaign Two',
        currency: 'USD',
        spend: '22.00',
        attributedRevenue: '88.80',
        purchaseCount: 4,
        purchaseRoas: '4.036364',
        actionTypeUsed: 'offsite_conversion.fb_pixel_purchase',
        canonicalSelectionMode: 'priority',
        actionReportTime: 'conversion',
        useAccountAttributionSetting: true,
        rawActionValues: [
          {
            action_type: 'offsite_conversion.fb_pixel_purchase',
            value: '88.80'
          }
        ],
        rawActions: [
          {
            action_type: 'offsite_conversion.fb_pixel_purchase',
            value: '4'
          }
        ],
        rawRows: [page2.data?.[0]]
      },
      {
        adAccountId: '123456789',
        reportDate: '2026-04-28',
        rawDateStart: '2026-04-28',
        rawDateStop: '2026-04-28',
        campaignId: 'cmp_live_3',
        campaignName: 'Live Campaign Three',
        currency: 'USD',
        spend: '19.25',
        attributedRevenue: null,
        purchaseCount: null,
        purchaseRoas: null,
        actionTypeUsed: null,
        canonicalSelectionMode: 'none',
        actionReportTime: 'conversion',
        useAccountAttributionSetting: true,
        rawActionValues: [],
        rawActions: [
          {
            action_type: 'link_click',
            value: '10'
          }
        ],
        rawRows: [page2.data?.[1]]
      }
    ]);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test('fetchCampaignDailyRevenueInsights applies configured fallback action types and keeps the selected type when one metric array is missing it', async () => {
  const previousFetch = globalThis.fetch;
  const sandboxPage = await loadJsonFixture<Record<string, unknown>>('campaign-daily-revenue-sandbox.json');

  try {
    globalThis.fetch = (async () => new Response(JSON.stringify(sandboxPage), { status: 200 })) as typeof globalThis.fetch;

    const records = await __metaAdsTestUtils.fetchCampaignDailyRevenueInsights({
      accessToken: 'meta-token',
      adAccountId: '123456789',
      startDate: '2026-04-29',
      endDate: '2026-04-29',
      currency: 'USD',
      allowedPurchaseActionTypes: ['purchase', 'omni_purchase', 'onsite_conversion.messaging_purchase']
    });

    assert.deepEqual(records, [
      {
        adAccountId: '123456789',
        reportDate: '2026-04-29',
        rawDateStart: '2026-04-29',
        rawDateStop: '2026-04-29',
        campaignId: 'cmp_sandbox_1',
        campaignName: 'Sandbox Campaign One',
        currency: 'USD',
        spend: '5.00',
        attributedRevenue: '12.00',
        purchaseCount: null,
        purchaseRoas: '2.400000',
        actionTypeUsed: 'onsite_conversion.messaging_purchase',
        canonicalSelectionMode: 'fallback',
        actionReportTime: 'conversion',
        useAccountAttributionSetting: true,
        rawActionValues: [
          {
            action_type: 'onsite_conversion.messaging_purchase',
            value: '12.00'
          }
        ],
        rawActions: [],
        rawRows: [sandboxPage.data?.[0]]
      }
    ]);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test('normalizeCampaignDailyRevenueRecords prefers purchase over lower-priority types and sums duplicate matches', () => {
  const records = __metaAdsTestUtils.normalizeCampaignDailyRevenueRecords(
    '123456789',
    [
      {
        campaign_id: 'cmp_priority',
        campaign_name: 'Priority Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '12.34',
        action_values: [
          { action_type: ' omni_purchase ', value: '5.10' },
          { action_type: ' purchase ', value: '10.50' }
        ],
        actions: [
          { action_type: 'omni_purchase', value: '1' },
          { action_type: 'purchase', value: '2' }
        ],
        purchase_roas: [{ action_type: 'purchase', value: '1.250000' }]
      },
      {
        campaign_id: 'cmp_priority',
        campaign_name: 'Priority Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '0.00',
        action_values: [{ action_type: 'purchase', value: 4.25 }],
        actions: [{ action_type: 'purchase', value: '3' }],
        purchase_roas: [{ action_type: 'purchase', value: '1.500000' }]
      }
    ],
    { currency: 'USD' }
  );

  assert.deepEqual(records, [
    {
      adAccountId: '123456789',
      reportDate: '2026-04-30',
      rawDateStart: '2026-04-30',
      rawDateStop: '2026-04-30',
      campaignId: 'cmp_priority',
      campaignName: 'Priority Campaign',
      currency: 'USD',
      spend: '12.34',
      attributedRevenue: '14.75',
      purchaseCount: 5,
      purchaseRoas: '1.250000',
      actionTypeUsed: 'purchase',
      canonicalSelectionMode: 'priority',
      actionReportTime: 'conversion',
      useAccountAttributionSetting: true,
      rawActionValues: [
        { action_type: ' omni_purchase ', value: '5.10' },
        { action_type: ' purchase ', value: '10.50' },
        { action_type: 'purchase', value: 4.25 }
      ],
      rawActions: [
        { action_type: 'omni_purchase', value: '1' },
        { action_type: 'purchase', value: '2' },
        { action_type: 'purchase', value: '3' }
      ],
      rawRows: [
        {
          campaign_id: 'cmp_priority',
          campaign_name: 'Priority Campaign',
          date_start: '2026-04-30',
          date_stop: '2026-04-30',
          spend: '12.34',
          action_values: [
            { action_type: ' omni_purchase ', value: '5.10' },
            { action_type: ' purchase ', value: '10.50' }
          ],
          actions: [
            { action_type: 'omni_purchase', value: '1' },
            { action_type: 'purchase', value: '2' }
          ],
          purchase_roas: [{ action_type: 'purchase', value: '1.250000' }]
        },
        {
          campaign_id: 'cmp_priority',
          campaign_name: 'Priority Campaign',
          date_start: '2026-04-30',
          date_stop: '2026-04-30',
          spend: '0.00',
          action_values: [{ action_type: 'purchase', value: 4.25 }],
          actions: [{ action_type: 'purchase', value: '3' }],
          purchase_roas: [{ action_type: 'purchase', value: '1.500000' }]
        }
      ]
    }
  ]);
});

test('normalizeCampaignDailyRevenueRecords falls back through omni, offsite, and configured purchase-like action types', () => {
  const records = __metaAdsTestUtils.normalizeCampaignDailyRevenueRecords(
    '123456789',
    [
      {
        campaign_id: 'cmp_omni',
        campaign_name: 'Omni Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '5.00',
        action_values: [{ action_type: 'omni_purchase', value: '8.50' }],
        actions: [{ action_type: 'omni_purchase', value: '2' }]
      },
      {
        campaign_id: 'cmp_offsite',
        campaign_name: 'Offsite Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '6.00',
        action_values: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '9.75' }],
        actions: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '4' }]
      },
      {
        campaign_id: 'cmp_fallback',
        campaign_name: 'Fallback Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '7.00',
        action_values: [{ action_type: 'onsite_conversion.messaging_purchase', value: '11.25' }],
        actions: [{ action_type: 'onsite_conversion.messaging_purchase', value: '5' }]
      }
    ],
    {
      currency: 'USD',
      allowedPurchaseActionTypes: ['purchase', 'omni_purchase', 'onsite_conversion.messaging_purchase']
    }
  );

  assert.deepEqual(
    records.map((record) => ({
      campaignId: record.campaignId,
      actionTypeUsed: record.actionTypeUsed,
      canonicalSelectionMode: record.canonicalSelectionMode,
      attributedRevenue: record.attributedRevenue,
      purchaseCount: record.purchaseCount
    })),
    [
      {
        campaignId: 'cmp_fallback',
        actionTypeUsed: 'onsite_conversion.messaging_purchase',
        canonicalSelectionMode: 'fallback',
        attributedRevenue: '11.25',
        purchaseCount: 5
      },
      {
        campaignId: 'cmp_offsite',
        actionTypeUsed: 'offsite_conversion.fb_pixel_purchase',
        canonicalSelectionMode: 'priority',
        attributedRevenue: '9.75',
        purchaseCount: 4
      },
      {
        campaignId: 'cmp_omni',
        actionTypeUsed: 'omni_purchase',
        canonicalSelectionMode: 'priority',
        attributedRevenue: '8.50',
        purchaseCount: 2
      }
    ]
  );
});

test('normalizeCampaignDailyRevenueRecords preserves selected action type with null metrics and ignores purchase_roas-only matches', () => {
  const records = __metaAdsTestUtils.normalizeCampaignDailyRevenueRecords(
    '123456789',
    [
      {
        campaign_id: 'cmp_null_metrics',
        campaign_name: 'Null Metrics Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '8.00',
        action_values: [
          { action_type: 'purchase', value: null as never },
          { action_type: 'purchase', value: '0' }
        ],
        actions: [
          { action_type: 'purchase', value: null as never },
          { action_type: 'purchase', value: '0' }
        ],
        purchase_roas: [{ action_type: 'purchase', value: '0' }]
      },
      {
        campaign_id: 'cmp_missing_actions',
        campaign_name: 'Missing Actions Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '9.00',
        action_values: [{ action_type: 'omni_purchase', value: '15.00' }],
        purchase_roas: [{ action_type: 'omni_purchase', value: '1.666667' }]
      },
      {
        campaign_id: 'cmp_roas_only',
        campaign_name: 'ROAS Only Campaign',
        date_start: '2026-04-30',
        date_stop: '2026-04-30',
        spend: '10.00',
        action_type: 'purchase',
        purchase_roas: [{ action_type: 'purchase', value: '2.000000' }]
      }
    ],
    { currency: 'USD' }
  );

  assert.deepEqual(
    records.map((record) => ({
      campaignId: record.campaignId,
      attributedRevenue: record.attributedRevenue,
      purchaseCount: record.purchaseCount,
      purchaseRoas: record.purchaseRoas,
      actionTypeUsed: record.actionTypeUsed,
      canonicalSelectionMode: record.canonicalSelectionMode,
      rawActionValues: record.rawActionValues,
      rawActions: record.rawActions
    })),
    [
      {
        campaignId: 'cmp_missing_actions',
        attributedRevenue: '15.00',
        purchaseCount: null,
        purchaseRoas: '1.666667',
        actionTypeUsed: 'omni_purchase',
        canonicalSelectionMode: 'priority',
        rawActionValues: [{ action_type: 'omni_purchase', value: '15.00' }],
        rawActions: []
      },
      {
        campaignId: 'cmp_null_metrics',
        attributedRevenue: '0.00',
        purchaseCount: 0,
        purchaseRoas: '0.000000',
        actionTypeUsed: 'purchase',
        canonicalSelectionMode: 'priority',
        rawActionValues: [
          { action_type: 'purchase', value: null },
          { action_type: 'purchase', value: '0' }
        ],
        rawActions: [
          { action_type: 'purchase', value: null },
          { action_type: 'purchase', value: '0' }
        ]
      },
      {
        campaignId: 'cmp_roas_only',
        attributedRevenue: null,
        purchaseCount: null,
        purchaseRoas: null,
        actionTypeUsed: null,
        canonicalSelectionMode: 'none',
        rawActionValues: [],
        rawActions: []
      }
    ]
  );
});

test('fetchCampaignDailyRevenueInsights retries transient errors and succeeds deterministically on the next page fetch', async () => {
  const previousFetch = globalThis.fetch;
  const page1 = await loadJsonFixture<Record<string, unknown>>('campaign-daily-revenue-live-page-1.json');
  let attempts = 0;

  try {
    globalThis.fetch = (async () => {
      attempts += 1;

      if (attempts === 1) {
        return new Response(
          JSON.stringify({
            error: {
              message: 'Too many calls',
              code: 429
            }
          }),
          { status: 429 }
        );
      }

      return new Response(JSON.stringify({ ...page1, paging: {} }), { status: 200 });
    }) as typeof globalThis.fetch;

    const records = await __metaAdsTestUtils.fetchCampaignDailyRevenueInsights({
      accessToken: 'meta-token',
      adAccountId: '123456789',
      startDate: '2026-04-28',
      endDate: '2026-04-28',
      currency: 'USD'
    });

    assert.equal(attempts, 2);
    assert.equal(records.length, 1);
    assert.equal(records[0]?.campaignId, 'cmp_live_1');
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test('fetchCampaignDailyRevenueInsights does not retry non-transient API errors', async () => {
  const previousFetch = globalThis.fetch;
  let attempts = 0;

  try {
    globalThis.fetch = (async () => {
      attempts += 1;

      return new Response(
        JSON.stringify({
          error: {
            message: 'Bad request',
            code: 100
          }
        }),
        { status: 400 }
      );
    }) as typeof globalThis.fetch;

    await assert.rejects(
      __metaAdsTestUtils.fetchCampaignDailyRevenueInsights({
        accessToken: 'meta-token',
        adAccountId: '123456789',
        startDate: '2026-04-28',
        endDate: '2026-04-28',
        currency: 'USD'
      }),
      (error: unknown) => {
        assert.equal(error instanceof Error, true);
        assert.equal((error as Error).message, 'Bad request');
        return true;
      }
    );

    assert.equal(attempts, 1);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
