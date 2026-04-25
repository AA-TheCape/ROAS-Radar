"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.__observabilityTestUtils = void 0;
exports.summarizeAttributionObservation = summarizeAttributionObservation;
exports.summarizeDualWriteConsistency = summarizeDualWriteConsistency;
exports.summarizeResolverOutcome = summarizeResolverOutcome;
exports.runWithRequestContext = runWithRequestContext;
exports.getRequestContext = getRequestContext;
exports.logInfo = logInfo;
exports.logWarning = logWarning;
exports.logError = logError;
exports.createRequestLoggingMiddleware = createRequestLoggingMiddleware;
exports.logHttpError = logHttpError;
exports.buildAttributionBacklogLog = buildAttributionBacklogLog;
const node_async_hooks_1 = require("node:async_hooks");
const node_crypto_1 = require("node:crypto");
const requestContextStorage = new node_async_hooks_1.AsyncLocalStorage();
function isRecord(value) {
    return typeof value === 'object' && value !== null;
}
function normalizeString(value) {
    if (typeof value !== 'string') {
        return undefined;
    }
    const normalized = value.trim();
    return normalized ? normalized : undefined;
}
function getGoogleCloudProjectId() {
    return normalizeString(process.env.GOOGLE_CLOUD_PROJECT) ?? normalizeString(process.env.GCLOUD_PROJECT);
}
function parseCloudTraceContext(headerValue) {
    const projectId = getGoogleCloudProjectId();
    const normalizedHeader = normalizeString(headerValue);
    if (!projectId || !normalizedHeader) {
        return {};
    }
    const [traceIdPart, optionsPart] = normalizedHeader.split(';', 2);
    const [traceId, spanId] = traceIdPart.split('/', 2);
    const normalizedTraceId = normalizeString(traceId);
    if (!normalizedTraceId) {
        return {};
    }
    return {
        trace: `projects/${projectId}/traces/${normalizedTraceId}`,
        spanId: normalizeString(spanId) ?? normalizeString(optionsPart)
    };
}
function serializeError(error) {
    if (error instanceof Error) {
        return {
            name: error.name,
            message: error.message,
            stack: error.stack
        };
    }
    return {
        message: String(error)
    };
}
function writeLog(severity, event, fields, stream) {
    const context = requestContextStorage.getStore();
    const payload = {
        severity,
        event,
        message: event,
        timestamp: new Date().toISOString(),
        ...(context ? { requestContext: context } : {}),
        ...fields
    };
    if (context?.trace) {
        payload['logging.googleapis.com/trace'] = context.trace;
    }
    if (context?.spanId) {
        payload['logging.googleapis.com/spanId'] = context.spanId;
    }
    stream.write(`${JSON.stringify(payload)}\n`);
}
function hasMeaningfulValue(value) {
    if (typeof value !== 'string') {
        return value !== null && value !== undefined;
    }
    return value.trim().length > 0;
}
function buildCaptureStatus(payload) {
    if (!hasMeaningfulValue(payload.roas_radar_session_id)) {
        return 'missing_session_id';
    }
    const hasMarketingDimensions = [
        payload.utm_source,
        payload.utm_medium,
        payload.utm_campaign,
        payload.utm_content,
        payload.utm_term,
        payload.gclid,
        payload.gbraid,
        payload.wbraid,
        payload.fbclid,
        payload.ttclid,
        payload.msclkid
    ].some(hasMeaningfulValue);
    const hasUrls = [payload.landing_url, payload.referrer_url, payload.page_url].some(hasMeaningfulValue);
    return hasMarketingDimensions && hasUrls ? 'complete' : 'partial';
}
function summarizeAttributionObservation(payload) {
    const observation = isRecord(payload) ? payload : {};
    return {
        captureStatus: buildCaptureStatus(observation),
        hasLandingUrl: hasMeaningfulValue(observation.landing_url),
        hasReferrerUrl: hasMeaningfulValue(observation.referrer_url),
        hasPageUrl: hasMeaningfulValue(observation.page_url),
        hasUtmSource: hasMeaningfulValue(observation.utm_source),
        hasClickId: [
            observation.gclid,
            observation.gbraid,
            observation.wbraid,
            observation.fbclid,
            observation.ttclid,
            observation.msclkid
        ].some(hasMeaningfulValue)
    };
}
function summarizeDualWriteConsistency(input) {
    const consistencyStatus = input.browserOutcome === input.serverOutcome &&
        (input.browserOutcome === 'accepted' || input.browserOutcome === 'deduplicated')
        ? 'matched'
        : 'mismatched';
    return {
        consistencyStatus,
        browserOutcome: input.browserOutcome,
        serverOutcome: input.serverOutcome
    };
}
function summarizeResolverOutcome(input) {
    if (!input.winner) {
        return {
            resolverOutcome: 'unattributed',
            touchpointCount: input.touchpoints.length
        };
    }
    return {
        resolverOutcome: input.winner.isDirect ? 'direct_winner' : 'non_direct_winner',
        touchpointCount: input.touchpoints.length,
        winningIngestionSource: input.winner.ingestionSource ?? null,
        winningSessionId: input.winner.sessionId ?? null
    };
}
function runWithRequestContext(context, callback) {
    return requestContextStorage.run(context, callback);
}
function getRequestContext() {
    return requestContextStorage.getStore();
}
function logInfo(event, fields = {}) {
    writeLog('INFO', event, fields, process.stdout);
}
function logWarning(event, fields = {}) {
    writeLog('WARNING', event, fields, process.stdout);
}
function logError(event, error, fields = {}) {
    writeLog('ERROR', event, { ...fields, error: serializeError(error) }, process.stderr);
}
function createRequestLoggingMiddleware(service) {
    return (req, res, next) => {
        const startedAt = process.hrtime.bigint();
        const requestId = normalizeString(req.header('x-request-id')) ?? (0, node_crypto_1.randomUUID)();
        const traceContext = parseCloudTraceContext(req.header('x-cloud-trace-context') ?? undefined);
        res.setHeader('x-request-id', requestId);
        runWithRequestContext({
            requestId,
            method: req.method,
            path: req.originalUrl || req.url,
            ...traceContext
        }, () => {
            res.on('finish', () => {
                const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
                logInfo('http_request_completed', {
                    service,
                    method: req.method,
                    path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path,
                    statusCode: res.statusCode,
                    statusClass: `${Math.floor(res.statusCode / 100)}xx`,
                    durationMs: Number(durationMs.toFixed(2)),
                    httpRequest: {
                        requestMethod: req.method,
                        requestUrl: req.originalUrl,
                        status: res.statusCode,
                        userAgent: req.header('user-agent') ?? undefined,
                        referer: req.header('referer') ?? undefined,
                        latency: `${Math.max(durationMs, 0).toFixed(3)}ms`
                    }
                });
            });
            next();
        });
    };
}
function logHttpError(event, error, req, extra = {}) {
    const details = isRecord(error) && 'details' in error ? { details: error.details } : {};
    const code = isRecord(error) && typeof error.code === 'string' ? { code: error.code } : {};
    const statusCode = isRecord(error) && typeof error.statusCode === 'number' ? { statusCode: error.statusCode } : {};
    logError(event, error, {
        service: process.env.K_SERVICE ?? 'roas-radar',
        method: req.method,
        path: req.baseUrl ? `${req.baseUrl}${req.path}` : req.path,
        ...statusCode,
        ...code,
        ...details,
        ...extra
    });
}
function buildAttributionBacklogLog(snapshot) {
    return JSON.stringify({
        severity: 'INFO',
        event: 'attribution_backlog_snapshot',
        message: 'attribution_backlog_snapshot',
        timestamp: new Date().toISOString(),
        service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
        ...snapshot
    });
}
exports.__observabilityTestUtils = {
    buildAttributionBacklogLog,
    parseCloudTraceContext,
    summarizeAttributionObservation,
    summarizeDualWriteConsistency,
    summarizeResolverOutcome
};
