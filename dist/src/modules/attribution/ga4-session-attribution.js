import { randomUUID } from 'node:crypto';
import { logError, logInfo, logWarning, summarizeGa4IngestionResult } from '../../observability/index.js';
const GA4_SESSION_ATTRIBUTION_PIPELINE = 'ga4_session_attribution';
const GA4_INGESTION_LAG_ALERT_THRESHOLD_HOURS = 2;
// ...existing code...
function buildGa4IngestionCorrelationId() {
    return `ga4-ingestion:${randomUUID()}`;
}
export async function ingestGa4SessionAttribution(input) {
    const correlationId = buildGa4IngestionCorrelationId();
    try {
        const config = normalizeEnabledConfig(input.config);
        const now = input.now ?? new Date();
        const watermarkBefore = await readWatermarkHour();
        const windows = planGa4SessionAttributionHourlyWindows({
            now,
            watermarkHour: watermarkBefore,
            config
        });
        const hourlyResults = [];
        for (const window of windows) {
            const hourlyResult = await extractGa4SessionAttributionForHour({
                config,
                executor: input.executor,
                hourStart: window.hourStart
            });
            hourlyResults.push(hourlyResult);
            logInfo('ga4_session_attribution_ingestion_hour_completed', {
                service: process.env.K_SERVICE ?? 'roas-radar',
                pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
                correlationId,
                hourStart: hourlyResult.hourStart,
                rowCount: hourlyResult.rows.length
            });
        }
        const rowsToPersist = hourlyResults.flatMap((result) => result.rows).map(mapNormalizedRowForPersistence);
        const watermarkAfter = windows.length > 0 ? windows[windows.length - 1]?.hourStart ?? null : watermarkBefore?.toISOString() ?? null;
        const upsertedRows = await withTransaction(async (client) => {
            const startedAt = new Date();
            await markRunStarted(client, startedAt);
            for (const row of rowsToPersist) {
                await upsertGa4SessionAttributionRow(client, row);
            }
            if (input.beforeCommit) {
                await input.beforeCommit(client);
            }
            await markRunCompleted(client, new Date(), watermarkAfter);
            return rowsToPersist.length;
        });
        const result = {
            watermarkBefore: watermarkBefore?.toISOString() ?? null,
            watermarkAfter,
            processedHours: windows.map((window) => window.hourStart).sort(compareIsoAscending),
            extractedRows: hourlyResults.reduce((sum, hourlyResult) => sum + hourlyResult.rows.length, 0),
            upsertedRows
        };
        const summary = summarizeGa4IngestionResult({
            ...result,
            now,
            lagAlertThresholdHours: GA4_INGESTION_LAG_ALERT_THRESHOLD_HOURS,
            rows: rowsToPersist.map((row) => ({
                source: row.source,
                medium: row.medium,
                campaign: row.campaign,
                clickIdValue: row.click_id_value
            }))
        });
        logInfo('ga4_session_attribution_ingestion_completed', {
            service: process.env.K_SERVICE ?? 'roas-radar',
            pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
            correlationId,
            ...summary
        });
        if (summary.lagStatus === 'lagging') {
            logWarning('ga4_session_attribution_ingestion_lag_alert', {
                service: process.env.K_SERVICE ?? 'roas-radar',
                pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
                correlationId,
                ...summary,
                alertable: true
            });
        }
        return result;
    }
    catch (error) {
        logError('ga4_session_attribution_ingestion_failed', error, {
            service: process.env.K_SERVICE ?? 'roas-radar',
            pipeline: GA4_SESSION_ATTRIBUTION_PIPELINE,
            correlationId,
            alertable: true
        });
        throw error;
    }
}
