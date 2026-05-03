function parseNumber(value, fallback) {
    if (!value) {
        return fallback;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}
function parseBoolean(value, fallback = false) {
    if (!value) {
        return fallback;
    }
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) {
        return true;
    }
    if (['0', 'false', 'no', 'off'].includes(normalized)) {
        return false;
    }
    return fallback;
}
function parseList(value) {
    if (!value) {
        return [];
    }
    return value
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
}
function parseRequiredString(name, fallback = '') {
    return process.env[name]?.trim() || fallback;
}
export const env = {
    PORT: parseNumber(process.env.PORT, 8080),
    K_SERVICE: parseRequiredString('K_SERVICE', 'roas-radar-api'),
    K_JOB: parseRequiredString('K_JOB', ''),
    K_JOB_EXECUTION: parseRequiredString('K_JOB_EXECUTION', ''),
    GCLOUD_PROJECT: parseRequiredString('GCLOUD_PROJECT', ''),
    GOOGLE_CLOUD_PROJECT: parseRequiredString('GOOGLE_CLOUD_PROJECT', ''),
    UPDATE_SNAPSHOTS: parseBoolean(process.env.UPDATE_SNAPSHOTS, false),
    DATABASE_URL: parseRequiredString('DATABASE_URL', 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar'),
    DATABASE_POOL_MAX: parseNumber(process.env.DATABASE_POOL_MAX, 20),
    DATABASE_POOL_MIN: parseNumber(process.env.DATABASE_POOL_MIN, 0),
    DATABASE_IDLE_TIMEOUT_MS: parseNumber(process.env.DATABASE_IDLE_TIMEOUT_MS, 10_000),
    DATABASE_CONNECTION_TIMEOUT_MS: parseNumber(process.env.DATABASE_CONNECTION_TIMEOUT_MS, 10_000),
    DATABASE_STATEMENT_TIMEOUT_MS: parseNumber(process.env.DATABASE_STATEMENT_TIMEOUT_MS, 30_000),
    DATABASE_QUERY_TIMEOUT_MS: parseNumber(process.env.DATABASE_QUERY_TIMEOUT_MS, 30_000),
    DATABASE_MAX_USES: parseNumber(process.env.DATABASE_MAX_USES, 7_500),
    DATABASE_SSL: parseBoolean(process.env.DATABASE_SSL, false),
    REPORTING_API_TOKEN: parseRequiredString('REPORTING_API_TOKEN', 'dev-reporting-token'),
    API_JSON_BODY_LIMIT: parseRequiredString('API_JSON_BODY_LIMIT', '1mb'),
    API_ALLOWED_ORIGINS: parseList(process.env.API_ALLOWED_ORIGINS),
    TRACKING_ALLOWED_ORIGINS: parseList(process.env.TRACKING_ALLOWED_ORIGINS),
    TRACKING_BODY_LIMIT: parseRequiredString('TRACKING_BODY_LIMIT', '256kb'),
    TRACKING_RATE_LIMIT_WINDOW_MS: parseNumber(process.env.TRACKING_RATE_LIMIT_WINDOW_MS, 60_000),
    TRACKING_RATE_LIMIT_MAX: parseNumber(process.env.TRACKING_RATE_LIMIT_MAX, 300),
    TRACKING_MAX_EVENT_AGE_HOURS: parseNumber(process.env.TRACKING_MAX_EVENT_AGE_HOURS, 168),
    TRACKING_MAX_FUTURE_SKEW_SECONDS: parseNumber(process.env.TRACKING_MAX_FUTURE_SKEW_SECONDS, 300),
    SHOPIFY_WEBHOOK_BODY_LIMIT: parseRequiredString('SHOPIFY_WEBHOOK_BODY_LIMIT', '2mb'),
    SHOPIFY_APP_API_KEY: parseRequiredString('SHOPIFY_APP_API_KEY', ''),
    SHOPIFY_APP_API_SECRET: parseRequiredString('SHOPIFY_APP_API_SECRET', ''),
    SHOPIFY_APP_API_VERSION: parseRequiredString('SHOPIFY_APP_API_VERSION', '2025-01'),
    SHOPIFY_APP_BASE_URL: parseRequiredString('SHOPIFY_APP_BASE_URL', 'http://localhost:8080'),
    SHOPIFY_APP_ENCRYPTION_KEY: parseRequiredString('SHOPIFY_APP_ENCRYPTION_KEY', 'dev-encryption-key-32-characters-min'),
    SHOPIFY_APP_SCOPES: parseRequiredString('SHOPIFY_APP_SCOPES', 'read_orders,write_orders'),
    SHOPIFY_APP_POST_INSTALL_REDIRECT_URL: parseRequiredString('SHOPIFY_APP_POST_INSTALL_REDIRECT_URL', 'http://localhost:5173'),
    SHOPIFY_WEBHOOK_SECRET: parseRequiredString('SHOPIFY_WEBHOOK_SECRET', ''),
    SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES: parseNumber(process.env.SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES, 5),
    SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE: parseNumber(process.env.SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE, 50),
    SHOPIFY_RECONCILIATION_BATCH_SIZE: parseNumber(process.env.SHOPIFY_RECONCILIATION_BATCH_SIZE, 100),
    SHOPIFY_RECONCILIATION_LOOKBACK_DAYS: parseNumber(process.env.SHOPIFY_RECONCILIATION_LOOKBACK_DAYS, 7),
    APP_SESSION_TTL_HOURS: parseNumber(process.env.APP_SESSION_TTL_HOURS, 24),
    SESSION_ATTRIBUTION_RETENTION_DAYS: parseNumber(process.env.SESSION_ATTRIBUTION_RETENTION_DAYS, 30),
    ATTRIBUTION_WORKER_LOOP: parseBoolean(process.env.ATTRIBUTION_WORKER_LOOP, false),
    ATTRIBUTION_WORKER_POLL_INTERVAL_MS: parseNumber(process.env.ATTRIBUTION_WORKER_POLL_INTERVAL_MS, 5_000),
    ATTRIBUTION_JOB_BATCH_SIZE: parseNumber(process.env.ATTRIBUTION_JOB_BATCH_SIZE, 50),
    ATTRIBUTION_STALE_SCAN_BATCH_SIZE: parseNumber(process.env.ATTRIBUTION_STALE_SCAN_BATCH_SIZE, 500),
    ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY: parseRequiredString('ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY', 'system'),
    ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID: parseRequiredString('ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID', 'materializer'),
    IDENTITY_GRAPH_BACKFILL_REQUESTED_BY: parseRequiredString('IDENTITY_GRAPH_BACKFILL_REQUESTED_BY', 'system'),
    IDENTITY_GRAPH_BACKFILL_WORKER_ID: parseRequiredString('IDENTITY_GRAPH_BACKFILL_WORKER_ID', 'identity-backfill'),
    IDENTITY_GRAPH_BACKFILL_SOURCES: parseList(process.env.IDENTITY_GRAPH_BACKFILL_SOURCES),
    DATA_QUALITY_CHECK_LOOP: parseBoolean(process.env.DATA_QUALITY_CHECK_LOOP, false),
    DATA_QUALITY_CHECK_INTERVAL_MS: parseNumber(process.env.DATA_QUALITY_CHECK_INTERVAL_MS, 300_000),
    DATA_QUALITY_TARGET_LAG_DAYS: parseNumber(process.env.DATA_QUALITY_TARGET_LAG_DAYS, 1),
    DATA_QUALITY_ANOMALY_LOOKBACK_DAYS: parseNumber(process.env.DATA_QUALITY_ANOMALY_LOOKBACK_DAYS, 14),
    DATA_QUALITY_ANOMALY_THRESHOLD_RATIO: parseNumber(process.env.DATA_QUALITY_ANOMALY_THRESHOLD_RATIO, 0.4),
    DATA_QUALITY_ANOMALY_MIN_BASELINE: parseNumber(process.env.DATA_QUALITY_ANOMALY_MIN_BASELINE, 25),
    DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD: parseNumber(process.env.DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD, 0.4),
    DATA_QUALITY_SAMPLE_LIMIT: parseNumber(process.env.DATA_QUALITY_SAMPLE_LIMIT, 25),
    DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD: parseNumber(process.env.DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD, 50),
    DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD: parseNumber(process.env.DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD, 5),
    DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD: parseNumber(process.env.DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD, 5),
    DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD: parseNumber(process.env.DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD, 5),
    DEAD_LETTER_REPLAY_MAX_BATCH_SIZE: parseNumber(process.env.DEAD_LETTER_REPLAY_MAX_BATCH_SIZE, 100),
    META_ADS_APP_ID: parseRequiredString('META_ADS_APP_ID', ''),
    META_ADS_APP_SECRET: parseRequiredString('META_ADS_APP_SECRET', ''),
    META_ADS_APP_BASE_URL: parseRequiredString('META_ADS_APP_BASE_URL', 'http://localhost:8080'),
    META_ADS_APP_SCOPES: parseRequiredString('META_ADS_APP_SCOPES', 'ads_read'),
    META_ADS_ENCRYPTION_KEY: parseRequiredString('META_ADS_ENCRYPTION_KEY', 'dev-meta-encryption-key'),
    META_ADS_AD_ACCOUNT_ID: parseRequiredString('META_ADS_AD_ACCOUNT_ID', ''),
    META_ADS_SYNC_LOOKBACK_DAYS: parseNumber(process.env.META_ADS_SYNC_LOOKBACK_DAYS, 30),
    META_ADS_SYNC_INITIAL_LOOKBACK_DAYS: parseNumber(process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS, 90),
    META_ADS_SYNC_BATCH_SIZE: parseNumber(process.env.META_ADS_SYNC_BATCH_SIZE, 25),
    META_ADS_WORKER_LOOP: parseBoolean(process.env.META_ADS_WORKER_LOOP, false),
    META_ADS_WORKER_POLL_INTERVAL_MS: parseNumber(process.env.META_ADS_WORKER_POLL_INTERVAL_MS, 60_000),
    GOOGLE_ADS_APP_BASE_URL: parseRequiredString('GOOGLE_ADS_APP_BASE_URL', 'http://localhost:8080'),
    GOOGLE_ADS_APP_SCOPES: parseRequiredString('GOOGLE_ADS_APP_SCOPES', 'https://www.googleapis.com/auth/adwords'),
    GOOGLE_ADS_CLIENT_ID: parseRequiredString('GOOGLE_ADS_CLIENT_ID', ''),
    GOOGLE_ADS_CLIENT_SECRET: parseRequiredString('GOOGLE_ADS_CLIENT_SECRET', ''),
    GOOGLE_ADS_DEVELOPER_TOKEN: parseRequiredString('GOOGLE_ADS_DEVELOPER_TOKEN', ''),
    GOOGLE_ADS_ENCRYPTION_KEY: parseRequiredString('GOOGLE_ADS_ENCRYPTION_KEY', 'dev-google-encryption-key'),
    GOOGLE_ADS_SYNC_LOOKBACK_DAYS: parseNumber(process.env.GOOGLE_ADS_SYNC_LOOKBACK_DAYS, 30),
    GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS: parseNumber(process.env.GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS, 90),
    GOOGLE_ADS_SYNC_BATCH_SIZE: parseNumber(process.env.GOOGLE_ADS_SYNC_BATCH_SIZE, 25),
    GOOGLE_ADS_WORKER_LOOP: parseBoolean(process.env.GOOGLE_ADS_WORKER_LOOP, false),
    GOOGLE_ADS_WORKER_POLL_INTERVAL_MS: parseNumber(process.env.GOOGLE_ADS_WORKER_POLL_INTERVAL_MS, 60_000)
};
export function getApiAllowedOrigins() {
    return parseList(process.env.API_ALLOWED_ORIGINS);
}
export function getConfiguredReportingApiToken() {
    return process.env.REPORTING_API_TOKEN?.trim() || env.REPORTING_API_TOKEN;
}
