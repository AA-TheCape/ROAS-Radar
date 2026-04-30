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

  assert.deepEqual(dates, ['2026-04-11']);
});

test('buildPlanningDates ignores the last successful sync date for automatic current-day syncs', () => {
  const dates = __googleAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-11']);
});

test('buildReconciliationWindow only covers the current business date for automatic syncs', () => {
  const window = __googleAdsTestUtils.buildReconciliationWindow(
    new Date('2026-04-11T12:00:00.000Z'),
    new Date('2026-04-10T06:00:00.000Z')
  );

  assert.deepEqual(window, {
    startDate: '2026-04-11',
    endDate: '2026-04-11',
    dates: ['2026-04-11']
  });
});

test('buildPlanningDates only keeps the current business date in window after an earlier successful run on the same day', () => {
  const dates = __googleAdsTestUtils.buildPlanningDates(
    new Date('2026-04-11T23:00:00.000Z'),
    new Date('2026-04-11T16:00:00.000Z')
  );

  assert.deepEqual(dates, ['2026-04-11']);
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

test('formatGoogleAdsError includes API status and details payload', () => {
  const error = __googleAdsTestUtils.createGoogleAdsApiErrorForTest(403, 'Google Ads API request failed', {
    error: {
      code: 403,
      message: 'The caller does not have permission',
      status: 'PERMISSION_DENIED'
    }
  });

  const message = __googleAdsTestUtils.formatGoogleAdsError(error);

  assert.equal(
    message,
    'Google Ads API request failed (status 403; details={"error":{"code":403,"message":"The caller does not have permission","status":"PERMISSION_DENIED"}})'
  );
});

test('extractGoogleAdsProviderRetryDelaySeconds reads quota retry delay from Google API details', () => {
  const retryDelaySeconds = __googleAdsTestUtils.extractGoogleAdsProviderRetryDelaySeconds([
    {
      error: {
        code: 429,
        message: 'Resource has been exhausted (e.g. check quota).',
        status: 'RESOURCE_EXHAUSTED',
        details: [
          {
            '@type': 'type.googleapis.com/google.ads.googleads.v22.errors.GoogleAdsFailure',
            errors: [
              {
                errorCode: {
                  quotaError: 'RESOURCE_EXHAUSTED'
                },
                message: 'Too many requests. Retry in 17941 seconds.',
                details: {
                  quotaErrorDetails: {
                    rateScope: 'DEVELOPER',
                    rateName: 'Number of operations for basic access',
                    retryDelay: '17941s'
                  }
                }
              }
            ]
          }
        ]
      }
    }
  ]);

  assert.equal(retryDelaySeconds, 17941);
});
