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
  TRACKING_ALLOWED_ORIGINS: z
    .union([z.string().trim().min(1), z.undefined()])
    .transform((value) => (value ? csvStringSchema.parse(value) : [])),
  TRACKING_RATE_LIMIT_WINDOW_MS: z.coerce.number().int().positive().default(60_000),
  TRACKING_RATE_LIMIT_MAX: z.coerce.number().int().positive().default(120),
  TRACKING_MAX_EVENT_AGE_HOURS: z.coerce.number().int().positive().default(24 * 7),
  TRACKING_MAX_FUTURE_SKEW_SECONDS: z.coerce.number().int().nonnegative().default(300)
});

export const env = envSchema.parse(process.env);
