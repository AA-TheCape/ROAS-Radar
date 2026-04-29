function readString(name: string, fallback = ""): string {
	const value = process.env[name];
	return typeof value === "string" ? value : fallback;
}

function readTrimmedString(name: string, fallback = ""): string {
	return readString(name, fallback).trim();
}

function readBoolean(name: string, fallback = false): boolean {
	const value = readTrimmedString(name);
	if (!value) {
		return fallback;
	}

	switch (value.toLowerCase()) {
		case "1":
		case "true":
		case "yes":
		case "on":
			return true;
		case "0":
		case "false":
		case "no":
		case "off":
			return false;
		default:
			return fallback;
	}
}

function readNumber(name: string, fallback: number): number {
	const value = Number(readTrimmedString(name));
	return Number.isFinite(value) ? value : fallback;
}

function readPositiveInteger(name: string, fallback: number): number {
	const value = Math.trunc(readNumber(name, fallback));
	return value > 0 ? value : fallback;
}

function readNonNegativeInteger(name: string, fallback: number): number {
	const value = Math.trunc(readNumber(name, fallback));
	return value >= 0 ? value : fallback;
}

function readList(name: string): string[] {
	return readString(name)
		.split(",")
		.map((entry) => entry.trim())
		.filter(Boolean);
}

export function getConfiguredReportingApiToken(): string | null {
	const value = readTrimmedString("REPORTING_API_TOKEN");
	return value.length > 0 ? value : null;
}

export function getApiAllowedOrigins(): string[] {
	return readList("API_ALLOWED_ORIGINS");
}

export const env = {
	PORT: readPositiveInteger("PORT", 3000),
	DATABASE_URL: readTrimmedString(
		"DATABASE_URL",
		"postgres://postgres:postgres@127.0.0.1:5432/roas_radar",
	),
	DATABASE_SSL: readBoolean("DATABASE_SSL", false),
	DATABASE_POOL_MAX: readPositiveInteger("DATABASE_POOL_MAX", 10),
	DATABASE_POOL_MIN: readNonNegativeInteger("DATABASE_POOL_MIN", 0),
	DATABASE_IDLE_TIMEOUT_MS: readPositiveInteger(
		"DATABASE_IDLE_TIMEOUT_MS",
		30_000,
	),
	DATABASE_CONNECTION_TIMEOUT_MS: readPositiveInteger(
		"DATABASE_CONNECTION_TIMEOUT_MS",
		10_000,
	),
	DATABASE_STATEMENT_TIMEOUT_MS: readPositiveInteger(
		"DATABASE_STATEMENT_TIMEOUT_MS",
		30_000,
	),
	DATABASE_QUERY_TIMEOUT_MS: readPositiveInteger(
		"DATABASE_QUERY_TIMEOUT_MS",
		30_000,
	),
	DATABASE_MAX_USES: readPositiveInteger("DATABASE_MAX_USES", 7_500),
	API_JSON_BODY_LIMIT: readTrimmedString("API_JSON_BODY_LIMIT", "1mb"),
	SHOPIFY_WEBHOOK_BODY_LIMIT: readTrimmedString(
		"SHOPIFY_WEBHOOK_BODY_LIMIT",
		"2mb",
	),
	TRACKING_BODY_LIMIT: readTrimmedString("TRACKING_BODY_LIMIT", "256kb"),
	REPORTING_API_TOKEN: getConfiguredReportingApiToken() ?? "",
	API_ALLOWED_ORIGINS: getApiAllowedOrigins(),
	APP_SESSION_TTL_HOURS: readPositiveInteger("APP_SESSION_TTL_HOURS", 24 * 14),
	TRACKING_ALLOWED_ORIGINS: readList("TRACKING_ALLOWED_ORIGINS"),
	TRACKING_MAX_EVENT_AGE_HOURS: readPositiveInteger(
		"TRACKING_MAX_EVENT_AGE_HOURS",
		168,
	),
	TRACKING_MAX_FUTURE_SKEW_SECONDS: readPositiveInteger(
		"TRACKING_MAX_FUTURE_SKEW_SECONDS",
		300,
	),
	TRACKING_RATE_LIMIT_MAX: readPositiveInteger("TRACKING_RATE_LIMIT_MAX", 120),
	TRACKING_RATE_LIMIT_WINDOW_MS: readPositiveInteger(
		"TRACKING_RATE_LIMIT_WINDOW_MS",
		60_000,
	),
	SESSION_ATTRIBUTION_RETENTION_DAYS: readPositiveInteger(
		"SESSION_ATTRIBUTION_RETENTION_DAYS",
		30,
	),
	GA4_FALLBACK_RETENTION_DAYS: readPositiveInteger(
		"GA4_FALLBACK_RETENTION_DAYS",
		30,
	),
	ATTRIBUTION_JOB_BATCH_SIZE: readPositiveInteger(
		"ATTRIBUTION_JOB_BATCH_SIZE",
		25,
	),
	ATTRIBUTION_STALE_SCAN_BATCH_SIZE: readPositiveInteger(
		"ATTRIBUTION_STALE_SCAN_BATCH_SIZE",
		50,
	),
	ATTRIBUTION_WORKER_LOOP: readBoolean("ATTRIBUTION_WORKER_LOOP", false),
	ATTRIBUTION_WORKER_POLL_INTERVAL_MS: readPositiveInteger(
		"ATTRIBUTION_WORKER_POLL_INTERVAL_MS",
		30_000,
	),
	DATA_QUALITY_CHECK_LOOP: readBoolean("DATA_QUALITY_CHECK_LOOP", false),
	DATA_QUALITY_CHECK_INTERVAL_MS: readPositiveInteger(
		"DATA_QUALITY_CHECK_INTERVAL_MS",
		3_600_000,
	),
	DATA_QUALITY_TARGET_LAG_DAYS: readNonNegativeInteger(
		"DATA_QUALITY_TARGET_LAG_DAYS",
		1,
	),
	DATA_QUALITY_ANOMALY_LOOKBACK_DAYS: readPositiveInteger(
		"DATA_QUALITY_ANOMALY_LOOKBACK_DAYS",
		7,
	),
	DATA_QUALITY_ANOMALY_THRESHOLD_RATIO: readNumber(
		"DATA_QUALITY_ANOMALY_THRESHOLD_RATIO",
		0.35,
	),
	DATA_QUALITY_ANOMALY_MIN_BASELINE: readPositiveInteger(
		"DATA_QUALITY_ANOMALY_MIN_BASELINE",
		5,
	),
	DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD: readNonNegativeInteger(
		"DATA_QUALITY_REPORTING_ANOMALY_ALERT_THRESHOLD",
		0,
	),
	DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD: readNonNegativeInteger(
		"DATA_QUALITY_ORPHAN_SESSION_ALERT_THRESHOLD",
		0,
	),
	DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD: readNonNegativeInteger(
		"DATA_QUALITY_DUPLICATE_CANONICAL_ALERT_THRESHOLD",
		0,
	),
	DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD: readNonNegativeInteger(
		"DATA_QUALITY_CONFLICTING_SHOPIFY_ALERT_THRESHOLD",
		0,
	),
	DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD: readNonNegativeInteger(
		"DATA_QUALITY_HASH_ANOMALY_ALERT_THRESHOLD",
		0,
	),
	DATA_QUALITY_SAMPLE_LIMIT: readPositiveInteger(
		"DATA_QUALITY_SAMPLE_LIMIT",
		25,
	),
	SHOPIFY_APP_API_KEY: readTrimmedString("SHOPIFY_APP_API_KEY"),
	SHOPIFY_APP_API_SECRET: readTrimmedString("SHOPIFY_APP_API_SECRET"),
	SHOPIFY_APP_API_VERSION: readTrimmedString(
		"SHOPIFY_APP_API_VERSION",
		"2025-01",
	),
	SHOPIFY_APP_BASE_URL: readTrimmedString("SHOPIFY_APP_BASE_URL"),
	SHOPIFY_APP_ENCRYPTION_KEY: readTrimmedString("SHOPIFY_APP_ENCRYPTION_KEY"),
	SHOPIFY_APP_POST_INSTALL_REDIRECT_URL: readTrimmedString(
		"SHOPIFY_APP_POST_INSTALL_REDIRECT_URL",
	),
	SHOPIFY_APP_SCOPES: readList("SHOPIFY_APP_SCOPES"),
	SHOPIFY_WEBHOOK_SECRET: readTrimmedString("SHOPIFY_WEBHOOK_SECRET"),
	SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE: readPositiveInteger(
		"SHOPIFY_ORDER_WRITEBACK_BATCH_SIZE",
		25,
	),
	SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES: readPositiveInteger(
		"SHOPIFY_ORDER_WRITEBACK_MAX_RETRIES",
		5,
	),
	SHOPIFY_RECONCILIATION_BATCH_SIZE: readPositiveInteger(
		"SHOPIFY_RECONCILIATION_BATCH_SIZE",
		50,
	),
	SHOPIFY_RECONCILIATION_LOOKBACK_DAYS: readPositiveInteger(
		"SHOPIFY_RECONCILIATION_LOOKBACK_DAYS",
		7,
	),
	META_ADS_APP_ID: readTrimmedString("META_ADS_APP_ID"),
	META_ADS_APP_SECRET: readTrimmedString("META_ADS_APP_SECRET"),
	META_ADS_APP_BASE_URL: readTrimmedString("META_ADS_APP_BASE_URL"),
	META_ADS_APP_SCOPES: readList("META_ADS_APP_SCOPES"),
	META_ADS_AD_ACCOUNT_ID: readTrimmedString("META_ADS_AD_ACCOUNT_ID"),
	META_ADS_ENCRYPTION_KEY: readTrimmedString("META_ADS_ENCRYPTION_KEY"),
	META_ADS_API_VERSION: readTrimmedString("META_ADS_API_VERSION", "v20.0"),
	META_ADS_SYNC_LOOKBACK_DAYS: readPositiveInteger(
		"META_ADS_SYNC_LOOKBACK_DAYS",
		3,
	),
	META_ADS_SYNC_INITIAL_LOOKBACK_DAYS: readPositiveInteger(
		"META_ADS_SYNC_INITIAL_LOOKBACK_DAYS",
		7,
	),
	META_ADS_SYNC_BATCH_SIZE: readPositiveInteger("META_ADS_SYNC_BATCH_SIZE", 10),
	META_ADS_SYNC_MAX_RETRIES: readPositiveInteger(
		"META_ADS_SYNC_MAX_RETRIES",
		5,
	),
	META_ADS_TOKEN_REFRESH_LEEWAY_HOURS: readPositiveInteger(
		"META_ADS_TOKEN_REFRESH_LEEWAY_HOURS",
		24,
	),
	META_ADS_WORKER_LOOP: readBoolean("META_ADS_WORKER_LOOP", false),
	META_ADS_WORKER_POLL_INTERVAL_MS: readPositiveInteger(
		"META_ADS_WORKER_POLL_INTERVAL_MS",
		300_000,
	),
	GOOGLE_ADS_CLIENT_ID: readTrimmedString("GOOGLE_ADS_CLIENT_ID"),
	GOOGLE_ADS_CLIENT_SECRET: readTrimmedString("GOOGLE_ADS_CLIENT_SECRET"),
	GOOGLE_ADS_DEVELOPER_TOKEN: readTrimmedString("GOOGLE_ADS_DEVELOPER_TOKEN"),
	GOOGLE_ADS_APP_BASE_URL: readTrimmedString("GOOGLE_ADS_APP_BASE_URL"),
	GOOGLE_ADS_APP_SCOPES: readList("GOOGLE_ADS_APP_SCOPES"),
	GOOGLE_ADS_ENCRYPTION_KEY: readTrimmedString("GOOGLE_ADS_ENCRYPTION_KEY"),
	GOOGLE_ADS_API_VERSION: readTrimmedString("GOOGLE_ADS_API_VERSION", "v18"),
	GOOGLE_ADS_SYNC_LOOKBACK_DAYS: readPositiveInteger(
		"GOOGLE_ADS_SYNC_LOOKBACK_DAYS",
		3,
	),
	GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS: readPositiveInteger(
		"GOOGLE_ADS_SYNC_INITIAL_LOOKBACK_DAYS",
		7,
	),
	GOOGLE_ADS_SYNC_BATCH_SIZE: readPositiveInteger(
		"GOOGLE_ADS_SYNC_BATCH_SIZE",
		10,
	),
	GOOGLE_ADS_SYNC_MAX_RETRIES: readPositiveInteger(
		"GOOGLE_ADS_SYNC_MAX_RETRIES",
		5,
	),
	GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS: readPositiveInteger(
		"GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS",
		30,
	),
	GOOGLE_ADS_WORKER_LOOP: readBoolean("GOOGLE_ADS_WORKER_LOOP", false),
	GOOGLE_ADS_WORKER_POLL_INTERVAL_MS: readPositiveInteger(
		"GOOGLE_ADS_WORKER_POLL_INTERVAL_MS",
		300_000,
	),
	GA4_BIGQUERY_ENABLED: readBoolean("GA4_BIGQUERY_ENABLED", false),
	GA4_BIGQUERY_LOOKBACK_HOURS: readPositiveInteger(
		"GA4_BIGQUERY_LOOKBACK_HOURS",
		24,
	),
	GA4_BIGQUERY_BACKFILL_HOURS: readPositiveInteger(
		"GA4_BIGQUERY_BACKFILL_HOURS",
		168,
	),
} as const;
