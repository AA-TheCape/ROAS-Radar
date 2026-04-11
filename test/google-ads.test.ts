import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.GOOGLE_ADS_ENCRYPTION_KEY = 'google-ads-encryption-key';
process.env.GOOGLE_ADS_SYNC_LOOKBACK_DAYS = '3';
process.env.GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS = '5';

const { __googleAdsTestUtils } = await import('../src/modules/google-ads/index.js');

test('normalizeGoogleAdsCustomerId strips hyphens', () => {
  assert.equal(__googleAdsTestUtils.normalizeGoogleAdsCustomerId('123-456-7890'), '1234567890');
  assert.equal(__googleAdsTestUtils.normalizeGoogleAdsCustomerId('9876543210'), '9876543210');
});

test('computeRetryDelaySeconds backs off exponentially and caps at one hour', () => {
  assert.equal(__googleAdsTestUtils.computeRetryDelaySeconds(1), 60);
  assert.equal(__googleAdsTestUtils.computeRetryDelaySeconds(2), 120);
  assert.equal(__googleAdsTestUtils.computeRetryDelaySeconds(10), 3600);
});

test('buildPlanningDates uses the initial lookback before the first successful sync', () => {
  const dates = __googleAdsTestUtils.buildPlanningDates(new Date('2026-04-11T12:00:00.000Z'), null);

  assert.deepEqual(dates, ['2026-04-06', '2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10']);
});

test('buildPlanningDates switches to the rolling lookback after at least one successful sync', () => {
  const dates = __googleAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-08', '2026-04-09', '2026-04-10']);
});

test('buildReconciliationWindow mirrors the rolling planning window', () => {
  const window = __googleAdsTestUtils.buildReconciliationWindow(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(window, {
    startDate: '2026-04-08',
    endDate: '2026-04-10',
    dates: ['2026-04-08', '2026-04-09', '2026-04-10']
  });
});

test('normalizeSpendSnapshot maps ad groups into Meta-aligned adset fields and emits creative rows', () => {
  const rows = __googleAdsTestUtils.normalizeSpendSnapshot({
    customer: {
      customerId: '1234567890',
      descriptiveName: 'Main Account',
      currencyCode: 'USD',
      rawPayload: {}
    },
    campaignRows: [
      {
        customer: {
          id: '1234567890',
          descriptiveName: 'Main Account',
          currencyCode: 'USD'
        },
        campaign: {
          id: 'cmp_1',
          name: 'Brand'
        },
        metrics: {
          costMicros: '12340000',
          impressions: '100',
          clicks: '5'
        }
      }
    ],
    adRows: [
      {
        customer: {
          id: '1234567890',
          descriptiveName: 'Main Account',
          currencyCode: 'USD'
        },
        campaign: {
          id: 'cmp_1',
          name: 'Brand'
        },
        adGroup: {
          id: 'adgroup_1',
          name: 'Search US'
        },
        adGroupAd: {
          ad: {
            id: 'ad_1',
            name: 'Headline A'
          }
        },
        metrics: {
          costMicros: '2340000',
          impressions: '40',
          clicks: '2'
        }
      }
    ]
  });

  assert.equal(rows.length, 5);
  assert.equal(rows[0].granularity, 'account');
  assert.equal(rows[1].granularity, 'campaign');
  assert.equal(rows[2].granularity, 'adset');
  assert.equal(rows[2].adsetId, 'adgroup_1');
  assert.equal(rows[3].granularity, 'ad');
  assert.equal(rows[3].adId, 'ad_1');
  assert.equal(rows[4].granularity, 'creative');
  assert.equal(rows[4].creativeId, 'ad_1');
  assert.equal(rows[4].spend, '2.34');
  assert.equal(rows[1].canonicalSource, 'google');
  assert.equal(rows[1].canonicalMedium, 'cpc');
  assert.equal(rows[1].canonicalCampaign, 'brand');
  assert.equal(rows[1].canonicalContent, 'unknown');
  assert.equal(rows[4].canonicalContent, 'headline a');
});
