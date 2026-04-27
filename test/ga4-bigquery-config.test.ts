import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const { resolveGa4BigQueryIngestionConfig } = await import('../src/modules/attribution/ga4-bigquery-config.js');

test('resolveGa4BigQueryIngestionConfig returns a disabled config by default', () => {
  const config = resolveGa4BigQueryIngestionConfig({
    DATABASE_URL: process.env.DATABASE_URL
  });

  assert.deepEqual(config, { enabled: false });
});

test('resolveGa4BigQueryIngestionConfig normalizes enabled ingestion settings', () => {
  const config = resolveGa4BigQueryIngestionConfig({
    DATABASE_URL: process.env.DATABASE_URL,
    GA4_BIGQUERY_ENABLED: 'true',
    GA4_BIGQUERY_PROJECT_ID: 'analytics-prod1',
    GA4_BIGQUERY_LOCATION: 'US',
    GA4_BIGQUERY_DATASET: 'ga4_export',
    GA4_BIGQUERY_EVENTS_TABLE_PATTERN: 'events_*',
    GA4_BIGQUERY_INTRADAY_TABLE_PATTERN: 'events_intraday_*',
    GA4_BIGQUERY_LOOKBACK_HOURS: '36',
    GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID: 'analytics-prod1',
    GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION: 'US',
    GOOGLE_ADS_TRANSFER_DATASET: 'google_ads_transfer',
    GOOGLE_ADS_TRANSFER_TABLE_PATTERN: 'p_ads_*',
    GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS: '14',
    GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS: '123-456-7890, 9876543210'
  });

  assert.equal(config.enabled, true);

  if (!config.enabled) {
    assert.fail('Expected GA4 BigQuery ingestion config to be enabled');
  }

  assert.deepEqual(config, {
    enabled: true,
    ga4: {
      projectId: 'analytics-prod1',
      location: 'US',
      dataset: 'ga4_export',
      eventsTablePattern: 'events_*',
      intradayTablePattern: 'events_intraday_*',
      lookbackHours: 36,
      eventsTableExpression: '`analytics-prod1.ga4_export.events_*`',
      intradayTableExpression: '`analytics-prod1.ga4_export.events_intraday_*`'
    },
    googleAdsTransfer: {
      projectId: 'analytics-prod1',
      location: 'US',
      dataset: 'google_ads_transfer',
      tablePattern: 'p_ads_*',
      lookbackDays: 14,
      tableExpression: '`analytics-prod1.google_ads_transfer.p_ads_*`',
      customerIds: ['1234567890', '9876543210']
    }
  });
});

test('resolveGa4BigQueryIngestionConfig rejects incomplete enabled settings', () => {
  assert.throws(
    () =>
      resolveGa4BigQueryIngestionConfig({
        DATABASE_URL: process.env.DATABASE_URL,
        GA4_BIGQUERY_ENABLED: 'true',
        GA4_BIGQUERY_PROJECT_ID: 'analytics-prod1'
      }),
    /GA4_BIGQUERY_LOCATION is required/
  );
});
