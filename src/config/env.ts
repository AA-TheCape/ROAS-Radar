import 'dotenv/config';

import { z } from 'zod';

const csvStringSchema = z
  .string()
  .trim()
  .transform((value) =>
    value
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  );

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().min(1, 'DATABASE_URL is required'),
  REPORTING_API_TOKEN: z.string().min(1).default('dev-reporting-token'),
  META_ADS_APP_ID: z.string().default(''),
  META_ADS_APP_SECRET: z.string().default(''),
  META_ADS_APP_BASE_URL: z.string().default(''),
  META_ADS_APP_SCOPES: z
    .union([z.string().trim().min(1), z.undefined()])
    .transform((value) => (value ? csvStringSchema.parse(value) : ['ads_read', 'business_management'])),
  META_ADS_API_VERSION: z.string().default('v22.0'),
  META_ADS_ENCRYPTION_KEY: z.string().default(''),
  META_ADS_AD_ACCOUNT_ID: z.string().default(''),
  META_ADS_TOKEN_REFRESH_LEEWAY_HOURS: z.coerce.number().int().positive().default(72),
  META_ADS_SYNC_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  META_ADS_SYNC_INITIAL_LOOKBACK_DAYS: z.coerce.number().int().positive().default(30),
  META_ADS_SYNC_BATCH_SIZE: z.coerce.number().int().positive().default(10),
  META_ADS_SYNC_MAX_RETRIES: z.coerce.number().int().positive().default(8),
  META_ADS_WORKER_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  META_ADS_WORKER_LOOP: z
    .union([z.string(), z.boolean(), z.undefined()])
    .transform((value) => {
      if (typeof value === 'boolean') {
        return value;
      }

      if (typeof value !== 'string') {
        return false;
      }

      return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
    }),
  GOOGLE_ADS_API_VERSION: z.string().default('v19'),
  GOOGLE_ADS_ENCRYPTION_KEY: z.string().default(''),
  GOOGLE_ADS_SYNC_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS: z.coerce.number().int().positive().default(30),
  GOOGLE_ADS_SYNC_BATCH_SIZE: z.coerce.number().int().positive().default(10),
  GOOGLE_ADS_SYNC_MAX_RETRIES: z.coerce.number().int().positive().default(8),
  GOOGLE_ADS_WORKER_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  GOOGLE_ADS_WORKER_LOOP: z
    .union([z.string(), z.boolean(), z.undefined()])
    .transform((value) => {
      if (typeof value === 'boolean') {
        return value;
      }

      if (typeof value !== 'string') {
        return false;
      }

      return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
    }),
  SHOPIFY_APP_API_KEY: z.string().default(''),
  SHOPIFY_APP_API_SECRET: z.string().default(''),
  SHOPIFY_APP_API_VERSION: z.string().default(''),
  SHOPIFY_APP_BASE_URL: z.string().default(''),
  SHOPIFY_APP_ENCRYPTION_KEY: z.string().default(''),
  SHOPIFY_APP_POST_INSTALL_REDIRECT_URL: z.string().default(''),
  SHOPIFY_APP_SCOPES: z
    .union([z.string().trim().min(1), z.undefined()])
    .transform((value) => (value ? csvStringSchema.parse(value) : ['read_orders'])),
  SHOPIFY_WEBHOOK_SECRET: z.string().default(''),
  ATTRIBUTION_WINDOW_DAYS: z.coerce.number().int().positive().default(7),
  ATTRIBUTION_MODEL_VERSION: z.coerce.number().int().positive().default(1),
  ATTRIBUTION_JOB_BATCH_SIZE: z.coerce.number().int().positive().default(50),
  ATTRIBUTION_STALE_SCAN_BATCH_SIZE: z.coerce.number().int().positive().default(200),
  ATTRIBUTION_JOB_LEASE_SECONDS: z.coerce.number().int().positive().default(120),
  ATTRIBUTION_JOB_MAX_RETRIES: z.coerce.number().int().positive().default(10),
  ATTRIBUTION_WORKER_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(5000),
  ATTRIBUTION_WORKER_LOOP: z
    .union([z.string(), z.boolean(), z.undefined()])
    .transform((value) => {
      if (typeof value === 'boolean') {
        return value;
      }

      if (typeof value !== 'string') {
        return false;
      }

      return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
    }),
  TRACKING_ALLOWED_ORIGINS: z
    .union([z.string().trim().min(1), z.undefined()])
    .transform((value) => (value ? csvStringSchema.parse(value) : [])),
  TRACKING_RATE_LIMIT_WINDOW_MS: z.coerce.number().int().positive().default(60_000),
  TRACKING_RATE_LIMIT_MAX: z.coerce.number().int().positive().default(120),
  TRACKING_MAX_EVENT_AGE_HOURS: z.coerce.number().int().positive().default(24 * 7),
  TRACKING_MAX_FUTURE_SKEW_SECONDS: z.coerce.number().int().nonnegative().default(300)
});

export const env = envSchema.parse(process.env);
