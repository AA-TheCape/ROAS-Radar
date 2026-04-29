import { AsyncLocalStorage } from "node:async_hooks";
export const requestContextStorage = new AsyncLocalStorage();
function normalizeString(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
}
function hasMeaningfulValue(value) {
    return normalizeString(typeof value === "string" ? value : null) !== null;
}
function serializeValue(value) {
    if (value === undefined) {
        return undefined;
    }
    if (value === null ||
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean") {
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
    if (typeof value === "object") {
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
export function parseCloudTraceContext(header) {
    const normalized = normalizeString(header);
    if (!normalized) {
        return {};
    }
    const [traceAndSpan, options] = normalized.split(";");
    const [traceId, spanId] = traceAndSpan.split("/");
    return {
        traceId: normalizeString(traceId),
        spanId: normalizeString(spanId),
        traceSampled: options === "o=1",
    };
}
function writeLog(severity, event, fields, stream) {
    const context = requestContextStorage.getStore();
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
        ...fields,
    };
    stream.write(`${JSON.stringify(payload)}\n`);
}
export function logInfo(event, fields) {
    writeLog("INFO", event, toSerializableFields(fields), process.stdout);
}
export function logWarning(event, fields) {
    writeLog("WARNING", event, toSerializableFields(fields), process.stdout);
}
export function logError(event, error, fields) {
    writeLog("ERROR", event, toSerializableFields({
        ...fields,
        errorName: error instanceof Error ? error.name : "Error",
        errorMessage: error instanceof Error ? error.message : String(error),
        errorStack: error instanceof Error ? (error.stack ?? null) : null,
    }), process.stderr);
}
export function createRequestLoggingMiddleware(service) {
    return (req, res, next) => {
        const requestId = normalizeString(req.header("x-request-id")) ??
            req.header("x-cloud-trace-context")?.split("/")[0] ??
            null;
        const startedAt = Date.now();
        requestContextStorage.run({ requestId }, () => {
            res.on("finish", () => {
                logInfo("http_request_completed", {
                    service,
                    requestId,
                    method: req.method,
                    path: req.originalUrl,
                    responseStatusCode: res.statusCode,
                    durationMs: Date.now() - startedAt,
                });
            });
            next();
        });
    };
}
export function logHttpError(event, error, req, fields = {}) {
    logError(event, error, {
        method: req.method,
        path: req.originalUrl,
        ...fields,
    });
}
function toBackfillLifecycleStatus(stage) {
    switch (stage) {
        case "enqueued":
            return "queued";
        case "started":
            return "processing";
        case "completed":
            return "completed";
        case "failed":
            return "failed";
    }
}
export function summarizeOrderAttributionBackfillReport(report) {
    if (!report) {
        return {};
    }
    const rawRecoveredOrders = typeof report.recoveredOrders === "number"
        ? report.recoveredOrders
        : report.recovered;
    const recoveredOrders = typeof rawRecoveredOrders === "number" ? rawRecoveredOrders : 0;
    const rawFailedOrders = report.failedOrders;
    const failedOrders = typeof rawFailedOrders === "number" ? rawFailedOrders : 0;
    const rawFailures = Array.isArray(report.failures)
        ? report.failures
        : Array.isArray(report.sampleFailures)
            ? report.sampleFailures
            : [];
    const sampleFailures = rawFailures
        .filter((failure) => typeof failure === "object" && failure !== null)
        .map((failure) => ({
        orderId: typeof failure.orderId === "string" ? failure.orderId : null,
        code: typeof failure.code === "string" ? failure.code : null,
        message: typeof failure.message === "string" ? failure.message : null,
    }));
    const scannedOrders = typeof report.scannedOrders === "number"
        ? report.scannedOrders
        : typeof report.scanned === "number"
            ? report.scanned
            : null;
    const unrecoverableOrders = typeof report.unrecoverableOrders === "number"
        ? report.unrecoverableOrders
        : typeof report.unrecoverable === "number"
            ? report.unrecoverable
            : null;
    const writebackCompleted = typeof report.shopifyWritebackCompleted === "number"
        ? report.shopifyWritebackCompleted
        : typeof report.writebackCompleted === "number"
            ? report.writebackCompleted
            : 0;
    return {
        recoveredOrders,
        failedOrders,
        recoverableOrders: typeof report.recoverableOrders === "number"
            ? report.recoverableOrders
            : null,
        scannedOrders,
        dryRun: typeof report.dryRun === "boolean" ? report.dryRun : null,
        report: {
            scanned: scannedOrders ?? 0,
            recovered: recoveredOrders,
            unrecoverable: unrecoverableOrders ?? 0,
            writebackCompleted,
            failureCount: sampleFailures.length,
            sampleFailures,
        },
    };
}
export function emitOrderAttributionBackfillJobLifecycleLog(input) {
    logInfo("order_attribution_backfill_job_lifecycle", {
        service: process.env.K_SERVICE ?? "roas-radar",
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
        code: typeof input.error?.code === "string"
            ? input.error.code
            : null,
        errorMessage: input.error instanceof Error
            ? input.error.message
            : input.error
                ? String(input.error)
                : null,
    });
}
export function buildAttributionBacklogLog(input) {
    return JSON.stringify({
        severity: "INFO",
        event: "attribution_worker_backlog",
        message: "attribution_worker_backlog",
        timestamp: new Date().toISOString(),
        service: process.env.K_SERVICE ?? "roas-radar-attribution-worker",
        ...input,
    });
}
export function summarizeAttributionObservation(payload) {
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
        hasClickId: Boolean(clickId),
    };
}
export function summarizeDualWriteConsistency(input) {
    return {
        browserOutcome: input.browserOutcome,
        serverOutcome: input.serverOutcome,
        dualWriteConsistent: input.browserOutcome === input.serverOutcome ||
            input.serverOutcome === "accepted",
    };
}
export function summarizeResolverOutcome(input) {
    if (!input.winner) {
        return {
            resolverOutcome: "unattributed",
            touchpointCount: input.touchpoints.length,
            winnerMatchSource: "unattributed",
            fallbackUsed: false,
            ga4SkippedDueToPrecedence: Boolean(input.deterministicWinnerExists || input.shopifyHintMatchExists),
            ga4SkippedReason: input.deterministicWinnerExists
                ? "deterministic_winner"
                : input.shopifyHintMatchExists
                    ? "shopify_hint_fallback"
                    : "none",
            hasSource: false,
            hasMedium: false,
            hasCampaign: false,
            hasClickId: false,
        };
    }
    const winnerMatchSource = input.winner.matchSource ?? input.winner.ingestionSource ?? null;
    const fallbackUsed = winnerMatchSource === "shopify_hint_fallback" ||
        winnerMatchSource === "ga4_fallback";
    return {
        resolverOutcome: input.winner.isDirect
            ? "direct_winner"
            : "non_direct_winner",
        touchpointCount: input.touchpoints.length,
        winningIngestionSource: input.winner.ingestionSource ?? null,
        winningSessionId: input.winner.sessionId ?? null,
        winnerMatchSource,
        fallbackUsed,
        ga4SkippedDueToPrecedence: Boolean(input.deterministicWinnerExists || input.shopifyHintMatchExists),
        ga4SkippedReason: input.deterministicWinnerExists
            ? "deterministic_winner"
            : input.shopifyHintMatchExists
                ? "shopify_hint_fallback"
                : "none",
        hasSource: hasMeaningfulValue(input.winner.source),
        hasMedium: hasMeaningfulValue(input.winner.medium),
        hasCampaign: hasMeaningfulValue(input.winner.campaign),
        hasClickId: hasMeaningfulValue(input.winner.clickIdValue),
    };
}
function computeLagHours(now, watermarkAfter) {
    if (!watermarkAfter) {
        return null;
    }
    const latestCompleteHour = new Date(Math.floor(now.getTime() / (60 * 60 * 1000)) * (60 * 60 * 1000) -
        60 * 60 * 1000);
    const watermarkDate = new Date(watermarkAfter);
    if (Number.isNaN(latestCompleteHour.getTime()) ||
        Number.isNaN(watermarkDate.getTime())) {
        return null;
    }
    return Math.max(0, Math.round((latestCompleteHour.getTime() - watermarkDate.getTime()) /
        (60 * 60 * 1000)));
}
export function summarizeGa4IngestionResult(input) {
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
        lagStatus: lagHours !== null && lagHours >= lagAlertThresholdHours
            ? "lagging"
            : "healthy",
        sourcePresentRows,
        mediumPresentRows,
        campaignPresentRows,
        clickIdPresentRows,
        sourceFillRate: rowCount > 0 ? sourcePresentRows / rowCount : 0,
        mediumFillRate: rowCount > 0 ? mediumPresentRows / rowCount : 0,
        campaignFillRate: rowCount > 0 ? campaignPresentRows / rowCount : 0,
        clickIdFillRate: rowCount > 0 ? clickIdPresentRows / rowCount : 0,
    };
}
export const __observabilityTestUtils = {
    buildAttributionBacklogLog,
    emitOrderAttributionBackfillJobLifecycleLog,
    parseCloudTraceContext,
    summarizeGa4IngestionResult,
    summarizeOrderAttributionBackfillReport,
    summarizeAttributionObservation,
    summarizeDualWriteConsistency,
    summarizeResolverOutcome,
};
