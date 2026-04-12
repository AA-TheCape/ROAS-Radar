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

/* existing tests unchanged below */
