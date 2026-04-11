import 'dotenv/config';

import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().min(1, 'DATABASE_URL is required'),
  REPORTING_API_TOKEN: z.string().min(1).default('dev-reporting-token'),
  SHOPIFY_WEBHOOK_SECRET: z.string().default(''),
  ATTRIBUTION_WINDOW_DAYS: z.coerce.number().int().positive().default(7)
});

export const env = envSchema.parse(process.env);

