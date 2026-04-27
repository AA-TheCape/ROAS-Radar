import { z } from 'zod';
const booleanString = z
    .union([z.boolean(), z.string()])
    .optional()
    .transform((value) => {
    if (typeof value === 'boolean') {
        return value;
    }
    if (typeof value !== 'string') {
        return undefined;
    }
    return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
});
const integerString = z
    .union([z.number(), z.string()])
    .optional()
    .transform((value) => {
    if (typeof value === 'number' && Number.isFinite(value)) {
        return Math.trunc(value);
    }
    if (typeof value !== 'string') {
        return undefined;
    }
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : undefined;
});
const stringList = z
    .union([z.string(), z.array(z.string())])
    .optional()
    .transform((value) => {
    if (Array.isArray(value)) {
        return value.map((entry) => entry.trim()).filter(Boolean);
    }
    if (typeof value !== 'string') {
        return [];
    }
    return value
        .split(',')
        .map((entry) => entry.trim())
        .filter(Boolean);
});
const envSchema = z.object({
    NODE_ENV: z.string().optional().default('development'),
    PORT: integerString.default(8080),
    DATABASE_URL: z.string().min(1),
    DATABASE_POOL_MAX: integerString.default(10),
    DATABASE_POOL_MIN: integerString.default(0),
    DATABASE_IDLE_TIMEOUT_MS: integerString.default(30_000),
    DATABASE_CONNECTION_TIMEOUT_MS: integerString.default(10_000),
    DATABASE_STATEMENT_TIMEOUT_MS: integerString.optional(),
    DATABASE_QUERY_TIMEOUT_MS: integerString.optional(),
    DATABASE_MAX_USES: integerString.default(7_500),
    DATABASE_SSL: booleanString.default(false),
    REPORTING_API_TOKEN: z.string().optional().default(''),
    APP_SESSION_TTL_HOURS: integerString.default(24 * 7),
    API_ALLOWED_ORIGINS: stringList.default([]),
    TRACKING_ALLOWED_ORIGINS: stringList.default([]),
    API_JSON_BODY_LIMIT: z.string().optional().default('20mb'),
    TRACKING_BODY_LIMIT: z.string().optional().default('20mb'),
    SHOPIFY_WEBHOOK_BODY_LIMIT: z.string().optional().default('20mb'),
    TRACKING_MAX_EVENT_AGE_HOURS: integerString.default(24 * 14),
    TRACKING_MAX_FUTURE_SKEW_SECONDS: integerString.default(300),
    TRACKING_RATE_LIMIT_MAX: integerString.default(120),
    TRACKING_RATE_LIMIT_WINDOW_MS: integerString.default(60_000),
    ATTRIBUTION_JOB_BATCH_SIZE: integerString.default(25),
    ATTRIBUTION_STALE_SCAN_BATCH_SIZE: integerString.default(100),
    ATTRIBUTION_WORKER_LOOP: booleanString.default(true),
    ATTRIBUTION_WORKER_POLL_INTERVAL_MS: integerString.default(10_000),
    DEAD_LETTER_REPLAY_MAX_BATCH_SIZE: integerString.default(100),
    SHOPIFY_WEBHOOK_SECRET: z.string().optional().default(''),
    SHOPIFY_APP_API_KEY: z.string().optional().default(''),
    SHOPIFY_APP_API_SECRET: z.string().optional().default(''),
    SHOPIFY_APP_API_VERSION: z.string().optional().default('2026-01'),
    SHOPIFY_APP_BASE_URL: z.string().optional().default('http://localhost:8080'),
    SHOPIFY_APP_ENCRYPTION_KEY: z.string().optional().default(''),
    SHOPIFY_APP_POST_INSTALL_REDIRECT_URL: z.string().optional().default(''),
    SHOPIFY_APP_SCOPES: stringList.default([]),
    SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES: integerString.default(5),
    SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE: integerString.default(25),
    SHOPIFY_RECONCILIATION_BATCH_SIZE: integerString.default(100),
    SHOPIFY_RECONCILIATION_LOOKBACK_DAYS: integerString.default(30),
    META_ADS_APP_ID: z.string().optional().default(''),
    META_ADS_APP_SECRET: z.string().optional().default(''),
    META_ADS_APP_BASE_URL: z.string().optional().default('http://localhost:8080'),
    META_ADS_APP_SCOPES: stringList.default([]),
    META_ADS_AD_ACCOUNT_ID: z.string().optional().default(''),
    META_ADS_API_VERSION: z.string().optional().default('v23.0'),
    META_ADS_ENCRYPTION_KEY: z.string().optional().default(''),
    META_ADS_SYNC_BATCH_SIZE: integerString.default(10),
    META_ADS_SYNC_INITIAL_LOOKBACK_DAYS: integerString.default(30),
    META_ADS_SYNC_LOOKBACK_DAYS: integerString.default(7),
    META_ADS_SYNC_MAX_RETRIES: integerString.default(5),
    META_ADS_TOKEN_REFRESH_LEEWAY_HOURS: integerString.default(24),
    META_ADS_WORKER_LOOP: booleanString.default(true),
    META_ADS_WORKER_POLL_INTERVAL_MS: integerString.default(60_000),
    GOOGLE_ADS_APP_BASE_URL: z.string().optional().default('http://localhost:8080'),
    GOOGLE_ADS_CLIENT_ID: z.string().optional().default(''),
    GOOGLE_ADS_CLIENT_SECRET: z.string().optional().default(''),
    GOOGLE_ADS_DEVELOPER_TOKEN: z.string().optional().default(''),
    GOOGLE_ADS_APP_SCOPES: stringList.default(['https://www.googleapis.com/auth/adwords']),
    GOOGLE_ADS_API_VERSION: z.string().optional().default('v19'),
    GOOGLE_ADS_ENCRYPTION_KEY: z.string().optional().default(''),
    GOOGLE_ADS_SYNC_BATCH_SIZE: integerString.default(10),
    GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS: integerString.default(30),
    GOOGLE_ADS_SYNC_LOOKBACK_DAYS: integerString.default(7),
    GOOGLE_ADS_SYNC_MAX_RETRIES: integerString.default(5),
    GOOGLE_ADS_WORKER_LOOP: booleanString.default(true),
    GOOGLE_ADS_WORKER_POLL_INTERVAL_MS: integerString.default(60_000),
    GA4_BIGQUERY_ENABLED: booleanString.default(false),
    GA4_BIGQUERY_PROJECT_ID: z.string().optional().default(''),
    GA4_BIGQUERY_LOCATION: z.string().optional().default(''),
    GA4_BIGQUERY_DATASET: z.string().optional().default(''),
    GA4_BIGQUERY_EVENTS_TABLE_PATTERN: z.string().optional().default(''),
    GA4_BIGQUERY_INTRADAY_TABLE_PATTERN: z.string().optional().default(''),
    GA4_BIGQUERY_LOOKBACK_HOURS: integerString.default(72),
    GA4_BIGQUERY_BACKFILL_HOURS: integerString.default(24),
    GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID: z.string().optional().default(''),
    GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION: z.string().optional().default(''),
    GOOGLE_ADS_TRANSFER_DATASET: z.string().optional().default(''),
    GOOGLE_ADS_TRANSFER_TABLE_PATTERN: z.string().optional().default(''),
    GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS: integerString.default(30),
    GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS: z.string().optional().default(''),
    DATA_QUALITY_TARGET_LAG_DAYS: integerString.default(1),
    DATA_QUALITY_ANOMALY_LOOKBACK_DAYS: integerString.default(7),
    DATA_QUALITY_ANOMALY_THRESHOLD_RATIO: z.coerce.number().default(0.35),
    DATA_QUALITY_ANOMALY_MIN_BASELINE: z.coerce.number().default(5),
    DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD: integerString.default(0),
    DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD: integerString.default(0),
    DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD: integerString.default(0),
    DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD: integerString.default(0),
    DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD: integerString.default(0),
    DATA_QUALITY_SAMPLE_LIMIT: integerString.default(10),
    DATA_QUALITY_CHECK_LOOP: booleanString.default(true),
    DATA_QUALITY_CHECK_INTERVAL_MS: integerString.default(24 * 60 * 60 * 1000),
    SESSION_ATTRIBUTION_RETENTION_DAYS: integerString.default(30),
    GA4_FALLBACK_RETENTION_DAYS: integerString.default(35)
});
export const env = envSchema.parse(process.env);
export function getConfiguredReportingApiToken() {
    const configuredToken = process.env.REPORTING_API_TOKEN;
    if (typeof configuredToken === 'string') {
        return configuredToken.trim();
    }
    return env.REPORTING_API_TOKEN.trim();
}
export function getApiAllowedOrigins() {
    const configuredOrigins = process.env.API_ALLOWED_ORIGINS;
    if (typeof configuredOrigins === 'string') {
        return configuredOrigins
            .split(',')
            .map((entry) => entry.trim())
            .filter(Boolean);
    }
    return env.API_ALLOWED_ORIGINS;
}
