import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.META_ADS_APP_ID = 'meta-app-id';
process.env.META_ADS_APP_SECRET = 'meta-app-secret';
process.env.META_ADS_APP_BASE_URL = 'https://api.example.com';
process.env.META_ADS_ENCRYPTION_KEY = 'meta-encryption-key';
process.env.META_ADS_AD_ACCOUNT_ID = 'act_123456789';
process.env.META_ADS_SYNC_LOOKBACK_DAYS = '3';
process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS = '5';

const { __metaAdsTestUtils } = await import('../src/modules/meta-ads/index.js');

test('normalizeMetaAdAccountId strips the act_ prefix', () => {
  assert.equal(__metaAdsTestUtils.normalizeMetaAdAccountId('act_123456789'), '123456789');
  assert.equal(__metaAdsTestUtils.normalizeMetaAdAccountId('987654321'), '987654321');
});

test('buildMetaAdsAuthorizationUrl includes expected OAuth parameters', () => {
  const url = new URL(__metaAdsTestUtils.buildMetaAdsAuthorizationUrl('state-123'));

  assert.equal(url.origin, 'https://www.facebook.com');
  assert.equal(url.pathname, '/dialog/oauth');
  assert.equal(url.searchParams.get('client_id'), 'meta-app-id');
  assert.equal(url.searchParams.get('redirect_uri'), 'https://api.example.com/meta-ads/oauth/callback');
  assert.equal(url.searchParams.get('state'), 'state-123');
  assert.equal(url.searchParams.get('scope'), 'ads_read,business_management');
});

test('computeRetryDelaySeconds backs off exponentially and caps at one hour', () => {
  assert.equal(__metaAdsTestUtils.computeRetryDelaySeconds(1), 60);
  assert.equal(__metaAdsTestUtils.computeRetryDelaySeconds(2), 120);
  assert.equal(__metaAdsTestUtils.computeRetryDelaySeconds(10), 3600);
});

test('buildPlanningDates uses the initial lookback before the first successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(new Date('2026-04-11T12:00:00.000Z'), null);

  assert.deepEqual(dates, ['2026-04-06', '2026-04-07', '2026-04-08', '2026-04-09', '2026-04-10']);
});

test('buildPlanningDates switches to the rolling lookback after at least one successful sync', () => {
  const dates = __metaAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-08', '2026-04-09', '2026-04-10']);
});

test('normalizeInsightRows expands ad rows to creative granularity when creative metadata exists', () => {
  const rows = __metaAdsTestUtils.normalizeInsightRows(
    {
      account_id: 'act_1',
      account_name: 'Main Account',
      campaign_id: 'cmp_1',
      campaign_name: 'Prospecting',
      adset_id: 'adset_1',
      adset_name: 'US',
      ad_id: 'ad_1',
      ad_name: 'Hero',
      spend: '12.34',
      impressions: '100',
      clicks: '4'
    },
    {
      ad_1: {
        creativeId: 'creative_1',
        creativeName: 'Creative A'
      }
    },
    'USD'
  );

  assert.equal(rows.length, 5);
  assert.equal(rows[0].granularity, 'account');
  assert.equal(rows[1].granularity, 'campaign');
  assert.equal(rows[2].granularity, 'adset');
  assert.equal(rows[3].granularity, 'ad');
  assert.equal(rows[4].granularity, 'creative');
  assert.equal(rows[4].creativeId, 'creative_1');
  assert.equal(rows[4].currency, 'USD');
  assert.equal(rows[4].spend, '12.34');
});
