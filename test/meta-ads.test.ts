import assert from 'node:assert/strict';
import test from 'node:test';

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

test('buildPlanningDates uses the initial lookback before the first successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(new Date('2026-04-11T12:00:00.000Z'), null);

  assert.deepEqual(dates, ['2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10', '2026-04-11']);
});

test('buildPlanningDates starts from the last successful business date after at least one successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-10', '2026-04-11']);
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
