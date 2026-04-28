import { env } from '../../config/env.js';
const PROJECT_ID_PATTERN = /^[a-z][a-z0-9-]{4,61}[a-z0-9]$/;
const DATASET_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,1023}$/;
const LOCATION_PATTERN = /^[A-Za-z0-9-]{2,64}$/;
const TABLE_PATTERN = /^[A-Za-z0-9_*]+$/;
const CUSTOMER_ID_PATTERN = /^\d{10}$/;
function normalizeRequiredString(value, name) {
    const normalized = value?.trim() ?? '';
    if (!normalized) {
        throw new Error(`${name} is required when GA4 BigQuery ingestion is enabled`);
    }
    return normalized;
}
function validatePattern(value, pattern, name) {
    if (!pattern.test(value)) {
        throw new Error(`Invalid ${name} value: ${value}`);
    }
    return value;
}
function validatePositiveInteger(value, name) {
    if (value === undefined || !Number.isInteger(value) || value <= 0) {
        throw new Error(`Invalid ${name} value: ${value}`);
    }
    return value;
}
function parseCustomerIds(value) {
    const normalized = value?.trim() ?? '';
    if (!normalized) {
        return [];
    }
    return normalized.split(',').map((entry) => {
        const digitsOnly = entry.trim().replaceAll('-', '');
        if (!CUSTOMER_ID_PATTERN.test(digitsOnly)) {
            throw new Error(`Invalid GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS entry: ${entry.trim()}`);
        }
        return digitsOnly;
    });
}
function parseBoolean(value) {
    return ['1', 'true', 'yes', 'on'].includes((value ?? '').trim().toLowerCase());
}
function parsePositiveInteger(value, fallback, name) {
    const normalized = value?.trim() ?? '';
    if (!normalized) {
        return fallback;
    }
    const parsed = Number.parseInt(normalized, 10);
    return validatePositiveInteger(parsed, name);
}
export function resolveGa4BigQueryIngestionConfig(source = process.env) {
    const enabled = parseBoolean(source.GA4_BIGQUERY_ENABLED) || env.GA4_BIGQUERY_ENABLED;
    if (!enabled) {
        return { enabled: false };
    }
    const ga4ProjectId = validatePattern(normalizeRequiredString(source.GA4_BIGQUERY_PROJECT_ID, 'GA4_BIGQUERY_PROJECT_ID'), PROJECT_ID_PATTERN, 'GA4_BIGQUERY_PROJECT_ID');
    const ga4Location = validatePattern(normalizeRequiredString(source.GA4_BIGQUERY_LOCATION, 'GA4_BIGQUERY_LOCATION'), LOCATION_PATTERN, 'GA4_BIGQUERY_LOCATION');
    const ga4Dataset = validatePattern(normalizeRequiredString(source.GA4_BIGQUERY_DATASET, 'GA4_BIGQUERY_DATASET'), DATASET_PATTERN, 'GA4_BIGQUERY_DATASET');
    const ga4EventsTablePattern = validatePattern(normalizeRequiredString(source.GA4_BIGQUERY_EVENTS_TABLE_PATTERN, 'GA4_BIGQUERY_EVENTS_TABLE_PATTERN'), TABLE_PATTERN, 'GA4_BIGQUERY_EVENTS_TABLE_PATTERN');
    const ga4IntradayTablePattern = validatePattern(normalizeRequiredString(source.GA4_BIGQUERY_INTRADAY_TABLE_PATTERN, 'GA4_BIGQUERY_INTRADAY_TABLE_PATTERN'), TABLE_PATTERN, 'GA4_BIGQUERY_INTRADAY_TABLE_PATTERN');
    const ga4LookbackHours = parsePositiveInteger(source.GA4_BIGQUERY_LOOKBACK_HOURS, env.GA4_BIGQUERY_LOOKBACK_HOURS, 'GA4_BIGQUERY_LOOKBACK_HOURS');
    const ga4BackfillHours = parsePositiveInteger(source.GA4_BIGQUERY_BACKFILL_HOURS, env.GA4_BIGQUERY_BACKFILL_HOURS, 'GA4_BIGQUERY_BACKFILL_HOURS');
    const adsProjectId = validatePattern(normalizeRequiredString(source.GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID, 'GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID'), PROJECT_ID_PATTERN, 'GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID');
    const adsLocation = validatePattern(normalizeRequiredString(source.GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION, 'GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION'), LOCATION_PATTERN, 'GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION');
    const adsDataset = validatePattern(normalizeRequiredString(source.GOOGLE_ADS_TRANSFER_DATASET, 'GOOGLE_ADS_TRANSFER_DATASET'), DATASET_PATTERN, 'GOOGLE_ADS_TRANSFER_DATASET');
    const adsTablePattern = validatePattern(normalizeRequiredString(source.GOOGLE_ADS_TRANSFER_TABLE_PATTERN, 'GOOGLE_ADS_TRANSFER_TABLE_PATTERN'), TABLE_PATTERN, 'GOOGLE_ADS_TRANSFER_TABLE_PATTERN');
    const adsLookbackDays = parsePositiveInteger(source.GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS, env.GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS, 'GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS');
    return {
        enabled: true,
        ga4: {
            projectId: ga4ProjectId,
            location: ga4Location,
            dataset: ga4Dataset,
            eventsTablePattern: ga4EventsTablePattern,
            intradayTablePattern: ga4IntradayTablePattern,
            lookbackHours: ga4LookbackHours,
            backfillHours: ga4BackfillHours,
            eventsTableExpression: `\`${ga4ProjectId}.${ga4Dataset}.${ga4EventsTablePattern}\``,
            intradayTableExpression: `\`${ga4ProjectId}.${ga4Dataset}.${ga4IntradayTablePattern}\``
        },
        googleAdsTransfer: {
            projectId: adsProjectId,
            location: adsLocation,
            dataset: adsDataset,
            tablePattern: adsTablePattern,
            lookbackDays: adsLookbackDays,
            tableExpression: `\`${adsProjectId}.${adsDataset}.${adsTablePattern}\``,
            customerIds: parseCustomerIds(source.GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS)
        }
    };
}
export function assertGa4BigQueryIngestionConfig() {
    return resolveGa4BigQueryIngestionConfig();
}
