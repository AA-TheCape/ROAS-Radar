import 'dotenv/config';

type EnvValue = string | number | boolean | string[];

type Env = {
  API_JSON_BODY_LIMIT: string;
  API_ALLOWED_ORIGINS: string[];
  APP_SESSION_TTL_HOURS: number;
  ATTRIBUTION_JOB_BATCH_SIZE: number;
  ATTRIBUTION_STALE_SCAN_BATCH_SIZE: number;
  ATTRIBUTION_WORKER_LOOP: boolean;
  ATTRIBUTION_WORKER_POLL_INTERVAL_MS: number;
  DATA_QUALITY_ANOMALY_LOOKBACK_DAYS: number;
  DATA_QUALITY_ANOMALY_MIN_BASELINE: number;
  DATA_QUALITY_ANOMALY_THRESHOLD_RATIO: number;
  DATA_QUALITY_CHECK_INTERVAL_MS: number;
  DATA_QUALITY_CHECK_LOOP: boolean;
  DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD: number;
  DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD: number;
  DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD: number;
  DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD: number;
  DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD: number;
  DATA_QUALITY_SAMPLE_LIMIT: number;
  DATA_QUALITY_TARGET_LAG_DAYS: number;
  DATABASE_CONNECTION_TIMEOUT_MS: number;
  DATABASE_IDLE_TIMEOUT_MS: number;
  DATABASE_MAX_USES: number;
  DATABASE_POOL_MAX: number;
  DATABASE_POOL_MIN: number;
  DATABASE_QUERY_TIMEOUT_MS: number;
  DATABASE_SSL: boolean;
  DATABASE_STATEMENT_TIMEOUT_MS: number;
  DATABASE_URL: string;
  DEAD_LETTER_REPLAY_MAX_BATCH_SIZE: number;
  DEFAULT_ORGANIZATION_ID: number;
  GCLOUD_PROJECT: string;
  GA4_BIGQUERY_BACKFILL_HOURS: number;
  GA4_BIGQUERY_ENABLED: boolean;
  GA4_BIGQUERY_LOOKBACK_HOURS: number;
  GA4_FALLBACK_RETENTION_DAYS: number;
  GOOGLE_ADS_API_VERSION: string;
  GOOGLE_ADS_APP_BASE_URL: string;
  GOOGLE_ADS_APP_SCOPES: string[];
  GOOGLE_ADS_CLIENT_ID: string;
  GOOGLE_ADS_CLIENT_SECRET: string;
  GOOGLE_ADS_DEVELOPER_TOKEN: string;
  GOOGLE_ADS_ENCRYPTION_KEY: string;
  GOOGLE_ADS_SYNC_BATCH_SIZE: number;
  GOOGLE_ADS_SYNC_MAX_RETRIES: number;
  GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS: number;
  GOOGLE_ADS_WORKER_LOOP: boolean;
  GOOGLE_ADS_WORKER_POLL_INTERVAL_MS: number;
  GOOGLE_CLOUD_PROJECT: string;
  IDENTITY_GRAPH_BACKFILL_REQUESTED_BY: string;
  IDENTITY_GRAPH_BACKFILL_SOURCES: string;
  IDENTITY_GRAPH_BACKFILL_WORKER_ID: string;
  K_JOB: string;
  K_JOB_EXECUTION: string;
  K_SERVICE: string;
  META_ADS_AD_ACCOUNT_ID: string;
  META_ADS_API_VERSION: string;
  META_ADS_APP_BASE_URL: string;
  META_ADS_APP_ID: string;
  META_ADS_APP_SCOPES: string[];
  META_ADS_APP_SECRET: string;
  META_ADS_ENCRYPTION_KEY: string;
  META_ADS_ORDER_VALUE_ANOMALY_MIN_ROWS: number;
  META_ADS_ORDER_VALUE_NULL_SPIKE_MIN_RATIO: number;
  META_ADS_ORDER_VALUE_NULL_SPIKE_RATIO_DELTA: number;
  META_ADS_ORDER_VALUE_SYNC_ENABLED: boolean;
  META_ADS_ORDER_VALUE_SYNC_INTERVAL_MS: number;
  META_ADS_ORDER_VALUE_WINDOW_DAYS: number;
  META_ADS_SYNC_BATCH_SIZE: number;
  META_ADS_SYNC_INITIAL_LOOKBACK_DAYS: number;
  META_ADS_SYNC_LOOKBACK_DAYS: number;
  META_ADS_SYNC_MAX_RETRIES: number;
  META_ADS_TOKEN_REFRESH_LEEWAY_HOURS: number;
  META_ADS_WORKER_LOOP: boolean;
  META_ADS_WORKER_POLL_INTERVAL_MS: number;
  ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY: string;
  ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID: string;
  PORT: number;
  REPORTING_API_TOKEN: string;
  SESSION_ATTRIBUTION_RETENTION_DAYS: number;
  SHOPIFY_APP_API_KEY: string;
  SHOPIFY_APP_API_SECRET: string;
  SHOPIFY_APP_API_VERSION: string;
  SHOPIFY_APP_BASE_URL: string;
  SHOPIFY_APP_ENCRYPTION_KEY: string;
  SHOPIFY_APP_POST_INSTALL_REDIRECT_URL: string;
  SHOPIFY_APP_SCOPES: string[];
  SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE: number;
  SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES: number;
  SHOPIFY_RECONCILIATION_BATCH_SIZE: number;
  SHOPIFY_RECONCILIATION_LOOKBACK_DAYS: number;
  SHOPIFY_WEBHOOK_BODY_LIMIT: string;
  SHOPIFY_WEBHOOK_SECRET: string;
  TRACKING_ALLOWED_ORIGINS: string[];
  TRACKING_BODY_LIMIT: string;
  TRACKING_MAX_EVENT_AGE_HOURS: number;
  TRACKING_MAX_FUTURE_SKEW_SECONDS: number;
  TRACKING_RATE_LIMIT_MAX: number;
  TRACKING_RATE_LIMIT_WINDOW_MS: number;
};

type EnvParser<TValue extends EnvValue> = (name: string) => TValue;

const truthyValues = new Set(['1', 'true', 'yes', 'on']);

function readRaw(name: string): string | undefined {
  const value = process.env[name];
  return typeof value === 'string' ? value.trim() : undefined;
}

function parseString(name: string, fallback = ''): string {
  return readRaw(name) ?? fallback;
}

function parseInteger(name: string, fallback: number): number {
  const raw = readRaw(name);

  if (!raw) {
    return fallback;
  }

  const parsed = Number.parseInt(raw, 10);

  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid integer environment variable ${name}: ${raw}`);
  }

  return parsed;
}

function parseNumber(name: string, fallback: number): number {
  const raw = readRaw(name);

  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);

  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric environment variable ${name}: ${raw}`);
  }

  return parsed;
}

function parseBoolean(name: string, fallback: boolean): boolean {
  const raw = readRaw(name);

  if (!raw) {
    return fallback;
  }

  return truthyValues.has(raw.toLowerCase());
}

function parseStringList(name: string, fallback: string[] = []): string[] {
  const raw = readRaw(name);

  if (!raw) {
    return [...fallback];
  }

  return raw
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
}

const parsers: { [TKey in keyof Env]: EnvParser<Env[TKey]> } = {
  API_JSON_BODY_LIMIT: (name) => parseString(name, '1mb'),
  API_ALLOWED_ORIGINS: (name) => parseStringList(name, []),
  APP_SESSION_TTL_HOURS: (name) => parseInteger(name, 168),
  ATTRIBUTION_JOB_BATCH_SIZE: (name) => parseInteger(name, 50),
  ATTRIBUTION_STALE_SCAN_BATCH_SIZE: (name) => parseInteger(name, 100),
  ATTRIBUTION_WORKER_LOOP: (name) => parseBoolean(name, true),
  ATTRIBUTION_WORKER_POLL_INTERVAL_MS: (name) => parseInteger(name, 30000),
  DATA_QUALITY_ANOMALY_LOOKBACK_DAYS: (name) => parseInteger(name, 7),
  DATA_QUALITY_ANOMALY_MIN_BASELINE: (name) => parseInteger(name, 5),
  DATA_QUALITY_ANOMALY_THRESHOLD_RATIO: (name) => parseNumber(name, 0.35),
  DATA_QUALITY_CHECK_INTERVAL_MS: (name) => parseInteger(name, 60 * 60 * 1000),
  DATA_QUALITY_CHECK_LOOP: (name) => parseBoolean(name, true),
  DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD: (name) => parseInteger(name, 0),
  DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD: (name) => parseInteger(name, 0),
  DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD: (name) => parseInteger(name, 0),
  DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD: (name) => parseInteger(name, 0),
  DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD: (name) => parseInteger(name, 0),
  DATA_QUALITY_SAMPLE_LIMIT: (name) => parseInteger(name, 25),
  DATA_QUALITY_TARGET_LAG_DAYS: (name) => parseInteger(name, 1),
  DATABASE_CONNECTION_TIMEOUT_MS: (name) => parseInteger(name, 10000),
  DATABASE_IDLE_TIMEOUT_MS: (name) => parseInteger(name, 30000),
  DATABASE_MAX_USES: (name) => parseInteger(name, 7500),
  DATABASE_POOL_MAX: (name) => parseInteger(name, 10),
  DATABASE_POOL_MIN: (name) => parseInteger(name, 0),
  DATABASE_QUERY_TIMEOUT_MS: (name) => parseInteger(name, 15000),
  DATABASE_SSL: (name) => parseBoolean(name, false),
  DATABASE_STATEMENT_TIMEOUT_MS: (name) => parseInteger(name, 15000),
  DATABASE_URL: (name) => parseString(name, 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test'),
  DEAD_LETTER_REPLAY_MAX_BATCH_SIZE: (name) => parseInteger(name, 100),
  DEFAULT_ORGANIZATION_ID: (name) => parseInteger(name, 1),
  GCLOUD_PROJECT: (name) => parseString(name, ''),
  GA4_BIGQUERY_BACKFILL_HOURS: (name) => parseInteger(name, 168),
  GA4_BIGQUERY_ENABLED: (name) => parseBoolean(name, false),
  GA4_BIGQUERY_LOOKBACK_HOURS: (name) => parseInteger(name, 24),
  GA4_FALLBACK_RETENTION_DAYS: (name) => parseInteger(name, 30),
  GOOGLE_ADS_API_VERSION: (name) => parseString(name, 'v22'),
  GOOGLE_ADS_APP_BASE_URL: (name) => parseString(name, ''),
  GOOGLE_ADS_APP_SCOPES: (name) => parseStringList(name, []),
  GOOGLE_ADS_CLIENT_ID: (name) => parseString(name, ''),
  GOOGLE_ADS_CLIENT_SECRET: (name) => parseString(name, ''),
  GOOGLE_ADS_DEVELOPER_TOKEN: (name) => parseString(name, ''),
  GOOGLE_ADS_ENCRYPTION_KEY: (name) => parseString(name, ''),
  GOOGLE_ADS_SYNC_BATCH_SIZE: (name) => parseInteger(name, 5),
  GOOGLE_ADS_SYNC_MAX_RETRIES: (name) => parseInteger(name, 3),
  GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS: (name) => parseInteger(name, 30),
  GOOGLE_ADS_WORKER_LOOP: (name) => parseBoolean(name, true),
  GOOGLE_ADS_WORKER_POLL_INTERVAL_MS: (name) => parseInteger(name, 60 * 1000),
  GOOGLE_CLOUD_PROJECT: (name) => parseString(name, ''),
  IDENTITY_GRAPH_BACKFILL_REQUESTED_BY: (name) => parseString(name, ''),
  IDENTITY_GRAPH_BACKFILL_SOURCES: (name) => parseString(name, ''),
  IDENTITY_GRAPH_BACKFILL_WORKER_ID: (name) => parseString(name, ''),
  K_JOB: (name) => parseString(name, ''),
  K_JOB_EXECUTION: (name) => parseString(name, ''),
  K_SERVICE: (name) => parseString(name, ''),
  META_ADS_AD_ACCOUNT_ID: (name) => parseString(name, ''),
  META_ADS_API_VERSION: (name) => parseString(name, 'v25.0'),
  META_ADS_APP_BASE_URL: (name) => parseString(name, ''),
  META_ADS_APP_ID: (name) => parseString(name, ''),
  META_ADS_APP_SCOPES: (name) => parseStringList(name, []),
  META_ADS_APP_SECRET: (name) => parseString(name, ''),
  META_ADS_ENCRYPTION_KEY: (name) => parseString(name, ''),
  META_ADS_ORDER_VALUE_ANOMALY_MIN_ROWS: (name) => parseInteger(name, 5),
  META_ADS_ORDER_VALUE_NULL_SPIKE_MIN_RATIO: (name) => parseNumber(name, 0.5),
  META_ADS_ORDER_VALUE_NULL_SPIKE_RATIO_DELTA: (name) => parseNumber(name, 0.3),
  META_ADS_ORDER_VALUE_SYNC_ENABLED: (name) => parseBoolean(name, true),
  META_ADS_ORDER_VALUE_SYNC_INTERVAL_MS: (name) => parseInteger(name, 60 * 60 * 1000),
  META_ADS_ORDER_VALUE_WINDOW_DAYS: (name) => parseInteger(name, 2),
  META_ADS_SYNC_BATCH_SIZE: (name) => parseInteger(name, 5),
  META_ADS_SYNC_INITIAL_LOOKBACK_DAYS: (name) => parseInteger(name, 30),
  META_ADS_SYNC_LOOKBACK_DAYS: (name) => parseInteger(name, 7),
  META_ADS_SYNC_MAX_RETRIES: (name) => parseInteger(name, 3),
  META_ADS_TOKEN_REFRESH_LEEWAY_HOURS: (name) => parseInteger(name, 24),
  META_ADS_WORKER_LOOP: (name) => parseBoolean(name, true),
  META_ADS_WORKER_POLL_INTERVAL_MS: (name) => parseInteger(name, 60 * 1000),
  ORDER_ATTRIBUTION_MATERIALIZATION_REQUESTED_BY: (name) => parseString(name, ''),
  ORDER_ATTRIBUTION_MATERIALIZATION_WORKER_ID: (name) => parseString(name, ''),
  PORT: (name) => parseInteger(name, 8080),
  REPORTING_API_TOKEN: (name) => parseString(name, ''),
  SESSION_ATTRIBUTION_RETENTION_DAYS: (name) => parseInteger(name, 30),
  SHOPIFY_APP_API_KEY: (name) => parseString(name, ''),
  SHOPIFY_APP_API_SECRET: (name) => parseString(name, ''),
  SHOPIFY_APP_API_VERSION: (name) => parseString(name, '2025-01'),
  SHOPIFY_APP_BASE_URL: (name) => parseString(name, ''),
  SHOPIFY_APP_ENCRYPTION_KEY: (name) => parseString(name, ''),
  SHOPIFY_APP_POST_INSTALL_REDIRECT_URL: (name) => parseString(name, ''),
  SHOPIFY_APP_SCOPES: (name) => parseStringList(name, []),
  SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE: (name) => parseInteger(name, 50),
  SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES: (name) => parseInteger(name, 3),
  SHOPIFY_RECONCILIATION_BATCH_SIZE: (name) => parseInteger(name, 100),
  SHOPIFY_RECONCILIATION_LOOKBACK_DAYS: (name) => parseInteger(name, 7),
  SHOPIFY_WEBHOOK_BODY_LIMIT: (name) => parseString(name, '1mb'),
  SHOPIFY_WEBHOOK_SECRET: (name) => parseString(name, ''),
  TRACKING_ALLOWED_ORIGINS: (name) => parseStringList(name, []),
  TRACKING_BODY_LIMIT: (name) => parseString(name, '256kb'),
  TRACKING_MAX_EVENT_AGE_HOURS: (name) => parseInteger(name, 24),
  TRACKING_MAX_FUTURE_SKEW_SECONDS: (name) => parseInteger(name, 300),
  TRACKING_RATE_LIMIT_MAX: (name) => parseInteger(name, 120),
  TRACKING_RATE_LIMIT_WINDOW_MS: (name) => parseInteger(name, 60 * 1000)
};

function readEnvValue<TKey extends keyof Env>(key: TKey): Env[TKey] {
  return parsers[key](key);
}

export const env = new Proxy({} as Env, {
  get: (_target, property) => {
    if (typeof property !== 'string' || !(property in parsers)) {
      return undefined;
    }

    return readEnvValue(property as keyof Env);
  },
  has: (_target, property) => typeof property === 'string' && property in parsers,
  ownKeys: () => Object.keys(parsers),
  getOwnPropertyDescriptor: () => ({
    enumerable: true,
    configurable: true
  })
});

export function getConfiguredReportingApiToken(): string | null {
  const token = readRaw('REPORTING_API_TOKEN');
  return token && token.length > 0 ? token : null;
}

export function getApiAllowedOrigins(): string[] {
  return readEnvValue('API_ALLOWED_ORIGINS');
}
