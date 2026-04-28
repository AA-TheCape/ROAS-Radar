"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.__observabilityTestUtils = exports.requestContextStorage = void 0;
exports.parseCloudTraceContext = parseCloudTraceContext;
exports.logInfo = logInfo;
exports.logWarning = logWarning;
exports.logError = logError;
exports.createRequestLoggingMiddleware = createRequestLoggingMiddleware;
exports.logHttpError = logHttpError;
exports.summarizeOrderAttributionBackfillReport = summarizeOrderAttributionBackfillReport;
exports.emitOrderAttributionBackfillJobLifecycleLog = emitOrderAttributionBackfillJobLifecycleLog;
exports.buildAttributionBacklogLog = buildAttributionBacklogLog;
exports.summarizeAttributionObservation = summarizeAttributionObservation;
exports.summarizeDualWriteConsistency = summarizeDualWriteConsistency;
exports.summarizeResolverOutcome = summarizeResolverOutcome;
exports.summarizeGa4IngestionResult = summarizeGa4IngestionResult;
const node_async_hooks_1 = require("node:async_hooks");
exports.requestContextStorage = new node_async_hooks_1.AsyncLocalStorage();
function normalizeString(value) {
    return typeof value === 'string' && value.trim() ? value.trim() : null;
}
function hasMeaningfulValue(value) {
    return normalizeString(typeof value === 'string' ? value : null) !== null;
}
function serializeValue(value) {
    if (value === undefined) {
        return undefined;
    }
    if (value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        return value;
    }
    if (value instanceof Date) {
        return value.toISOString();
    }
    if (Array.isArray(value)) {
        return value
            .map((entry) => serializeValue(entry))
            .filter((entry) => entry !== undefined);
    }
    if (typeof value === 'object') {
        const serialized = {};
        for (const [key, entry] of Object.entries(value)) {
            const normalized = serializeValue(entry);
            if (normalized !== undefined) {
                serialized[key] = normalized;
            }
        }
        return serialized;
    }
    return String(value);
}
function toSerializableFields(fields) {
    const serialized = {};
    for (const [key, value] of Object.entries(fields)) {
        serialized[key] = serializeValue(value);
    }
    return serialized;
}
function parseCloudTraceContext(header) {
    const normalized = normalizeString(header);
    if (!normalized) {
        return {};
    }
    const [traceAndSpan, options] = normalized.split(';');
    const [traceId, spanId] = traceAndSpan.split('/');
    return {
        traceId: normalizeString(traceId),
        spanId: normalizeString(spanId),
        traceSampled: options === 'o=1'
    };
}
function writeLog(severity, event, fields, stream) {
    const context = exports.requestContextStorage.getStore();
    const correlationId = normalizeString(fields.correlationId) ??
        normalizeString(fields.requestId) ??
        normalizeString(context?.requestId);
    const payload = {
        severity,
        event,
        message: event,
        timestamp: new Date().toISOString(),
        ...(correlationId ? { correlationId } : {}),
        ...(context ? { requestContext: serializeValue(context) } : {}),
        ...fields
    };
    stream.write(`${JSON.stringify(payload)}\n`);
}
function logInfo(event, fields) {
    writeLog('INFO', event, toSerializableFields(fields), process.stdout);
}
function logWarning(event, fields) {
    writeLog('WARNING', event, toSerializableFields(fields), process.stdout);
}
function logError(event, error, fields) {
    writeLog('ERROR', event, toSerializableFields({
        ...fields,
        errorName: error instanceof Error ? error.name : 'Error',
        errorMessage: error instanceof Error ? error.message : String(error),
        errorStack: error instanceof Error ? error.stack ?? null : null
    }), process.stderr);
}
function createRequestLoggingMiddleware(service) {
    return (req, res, next) => {
        const requestId = normalizeString(req.header('x-request-id')) ?? req.header('x-cloud-trace-context')?.split('/')[0] ?? null;
        const startedAt = Date.now();
        exports.requestContextStorage.run({ requestId }, () => {
            res.on('finish', () => {
                logInfo('http_request_completed', {
                    service,
                    requestId,
                    method: req.method,
                    path: req.originalUrl,
                    responseStatusCode: res.statusCode,
                    durationMs: Date.now() - startedAt
                });
            });
            next();
        });
    };
}
function logHttpError(event, error, req, fields = {}) {
    logError(event, error, {
        method: req.method,
        path: req.originalUrl,
        ...fields
    });
}
function toBackfillLifecycleStatus(stage) {
    switch (stage) {
        case 'enqueued':
            return 'queued';
        case 'started':
            return 'running';
        case 'completed':
            return 'completed';
        case 'failed':
            return 'failed';
    }
}
function summarizeOrderAttributionBackfillReport(report) {
    if (!report) {
        return {};
    }
    const rawRecoveredOrders = report.recoveredOrders;
    const recoveredOrders = typeof rawRecoveredOrders === 'number' ? rawRecoveredOrders : 0;
    const rawFailedOrders = report.failedOrders;
    const failedOrders = typeof rawFailedOrders === 'number' ? rawFailedOrders : 0;
    return {
        recoveredOrders,
        failedOrders,
        recoverableOrders: typeof report.recoverableOrders === 'number' ? report.recoverableOrders : null,
        scannedOrders: typeof report.scannedOrders === 'number' ? report.scannedOrders : null,
        dryRun: typeof report.dryRun === 'boolean' ? report.dryRun : null
    };
}
function emitOrderAttributionBackfillJobLifecycleLog(input) {
    logInfo('order_attribution_backfill_job_lifecycle', {
        service: process.env.K_SERVICE ?? 'roas-radar',
        correlationId: input.jobId,
        stage: input.stage,
        status: toBackfillLifecycleStatus(input.stage),
        jobId: input.jobId,
        workerId: input.workerId ?? null,
        submittedAt: input.submittedAt ?? null,
        startedAt: input.startedAt ?? null,
        completedAt: input.completedAt ?? null,
        options: input.options ?? null,
        ...summarizeOrderAttributionBackfillReport(input.report),
        errorMessage: input.error instanceof Error ? input.error.message : input.error ? String(input.error) : null
    });
}
function buildAttributionBacklogLog(input) {
    return JSON.stringify({
        severity: 'INFO',
        event: 'attribution_worker_backlog',
        message: 'attribution_worker_backlog',
        timestamp: new Date().toISOString(),
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
        ...input
    });
}
function summarizeAttributionObservation(payload) {
    const input = (payload ?? {});
    const source = normalizeString(input.utm_source ?? input.utmSource);
    const medium = normalizeString(input.utm_medium ?? input.utmMedium);
    const campaign = normalizeString(input.utm_campaign ?? input.utmCampaign);
    const content = normalizeString(input.utm_content ?? input.utmContent);
    const term = normalizeString(input.utm_term ?? input.utmTerm);
    const clickId = normalizeString(input.gclid) ??
        normalizeString(input.gbraid) ??
        normalizeString(input.wbraid) ??
        normalizeString(input.fbclid) ??
        normalizeString(input.ttclid) ??
        normalizeString(input.msclkid);
    return {
        hasLandingUrl: hasMeaningfulValue(input.landing_url ?? input.landingUrl),
        hasReferrerUrl: hasMeaningfulValue(input.referrer_url ?? input.referrerUrl),
        hasPageUrl: hasMeaningfulValue(input.page_url ?? input.pageUrl),
        hasSource: Boolean(source),
        hasMedium: Boolean(medium),
        hasCampaign: Boolean(campaign),
        hasContent: Boolean(content),
        hasTerm: Boolean(term),
        hasClickId: Boolean(clickId)
    };
}
function summarizeDualWriteConsistency(input) {
    return {
        browserOutcome: input.browserOutcome,
        serverOutcome: input.serverOutcome,
        dualWriteConsistent: input.browserOutcome === input.serverOutcome || input.serverOutcome === 'accepted'
    };
}
function summarizeResolverOutcome(input) {
    if (!input.winner) {
        return {
            resolverOutcome: 'unattributed',
            touchpointCount: input.touchpoints.length,
            winnerMatchSource: 'unattributed',
            fallbackUsed: false,
            ga4SkippedDueToPrecedence: Boolean(input.deterministicWinnerExists || input.shopifyHintMatchExists),
            ga4SkippedReason: input.deterministicWinnerExists
                ? 'deterministic_winner'
                : input.shopifyHintMatchExists
                    ? 'shopify_hint_fallback'
                    : 'none',
            hasSource: false,
            hasMedium: false,
            hasCampaign: false,
            hasClickId: false
        };
    }
    const winnerMatchSource = input.winner.matchSource ?? input.winner.ingestionSource ?? null;
    const fallbackUsed = winnerMatchSource === 'shopify_hint_fallback' || winnerMatchSource === 'ga4_fallback';
    return {
        resolverOutcome: input.winner.isDirect ? 'direct_winner' : 'non_direct_winner',
        touchpointCount: input.touchpoints.length,
        winningIngestionSource: input.winner.ingestionSource ?? null,
        winningSessionId: input.winner.sessionId ?? null,
        winnerMatchSource,
        fallbackUsed,
        ga4SkippedDueToPrecedence: Boolean(input.deterministicWinnerExists || input.shopifyHintMatchExists),
        ga4SkippedReason: input.deterministicWinnerExists
            ? 'deterministic_winner'
            : input.shopifyHintMatchExists
                ? 'shopify_hint_fallback'
                : 'none',
        hasSource: hasMeaningfulValue(input.winner.source),
        hasMedium: hasMeaningfulValue(input.winner.medium),
        hasCampaign: hasMeaningfulValue(input.winner.campaign),
        hasClickId: hasMeaningfulValue(input.winner.clickIdValue)
    };
}
function computeLagHours(now, watermarkAfter) {
    if (!watermarkAfter) {
        return null;
    }
    const latestCompleteHour = new Date(Math.floor(now.getTime() / (60 * 60 * 1000)) * (60 * 60 * 1000) - 60 * 60 * 1000);
    const watermarkDate = new Date(watermarkAfter);
    if (Number.isNaN(latestCompleteHour.getTime()) || Number.isNaN(watermarkDate.getTime())) {
        return null;
    }
    return Math.max(0, Math.round((latestCompleteHour.getTime() - watermarkDate.getTime()) / (60 * 60 * 1000)));
}
function summarizeGa4IngestionResult(input) {
    const rows = input.rows ?? [];
    const rowCount = rows.length;
    const countPresent = (selector) => rows.reduce((total, row) => total + Number(hasMeaningfulValue(selector(row))), 0);
    const sourcePresentRows = countPresent((row) => row.source);
    const mediumPresentRows = countPresent((row) => row.medium);
    const campaignPresentRows = countPresent((row) => row.campaign);
    const clickIdPresentRows = countPresent((row) => row.clickIdValue);
    const now = input.now ?? new Date();
    const lagHours = computeLagHours(now, input.watermarkAfter);
    const lagAlertThresholdHours = input.lagAlertThresholdHours ?? 2;
    return {
        watermarkBefore: input.watermarkBefore,
        watermarkAfter: input.watermarkAfter,
        processedHourCount: input.processedHours.length,
        processedHours: input.processedHours,
        extractedRows: input.extractedRows,
        upsertedRows: input.upsertedRows,
        lagHours,
        lagAlertThresholdHours,
        lagStatus: lagHours !== null && lagHours >= lagAlertThresholdHours ? 'lagging' : 'healthy',
        sourcePresentRows,
        mediumPresentRows,
        campaignPresentRows,
        clickIdPresentRows,
        sourceFillRate: rowCount > 0 ? sourcePresentRows / rowCount : 0,
        mediumFillRate: rowCount > 0 ? mediumPresentRows / rowCount : 0,
        campaignFillRate: rowCount > 0 ? campaignPresentRows / rowCount : 0,
        clickIdFillRate: rowCount > 0 ? clickIdPresentRows / rowCount : 0
    };
}
exports.__observabilityTestUtils = {
    buildAttributionBacklogLog,
    emitOrderAttributionBackfillJobLifecycleLog,
    parseCloudTraceContext,
    summarizeGa4IngestionResult,
    summarizeOrderAttributionBackfillReport,
    summarizeAttributionObservation,
    summarizeDualWriteConsistency,
    summarizeResolverOutcome
};
