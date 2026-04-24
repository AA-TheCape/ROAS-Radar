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

test('buildPlanningDates switches to the rolling lookback after at least one successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-09', '2026-04-10', '2026-04-11']);
});
