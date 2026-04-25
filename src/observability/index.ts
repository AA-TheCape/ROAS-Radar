import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

import type { NextFunction, Request, Response } from 'express';

type RequestContext = {
  requestId: string;
  method?: string;
  path?: string;
  trace?: string;
  spanId?: string;
};

type SerializableFields = Record<string, unknown>;

type AttributionObservationInput = Partial<Record<
  | 'roas_radar_session_id'
  | 'landing_url'
  | 'referrer_url'
  | 'page_url'
  | 'utm_source'
  | 'utm_medium'
  | 'utm_campaign'
  | 'utm_content'
  | 'utm_term'
  | 'gclid'
  | 'gbraid'
  | 'wbraid'
  | 'fbclid'
  | 'ttclid'
  | 'msclkid',
  unknown
>>;

type DualWriteConsistencyInput = {
  browserOutcome: string;
  serverOutcome: string;
};

type ResolverOutcomeInput = {
  touchpoints: unknown[];
  winner: {
    isDirect?: boolean;
    ingestionSource?: string | null;
    sessionId?: string | null;
  } | null;
};

const requestContextStorage = new AsyncLocalStorage<RequestContext>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }

  const normalized = value.trim();
  return normalized ? normalized : undefined;
}

function getGoogleCloudProjectId(): string | undefined {
  return normalizeString(process.env.GOOGLE_CLOUD_PROJECT) ?? normalizeString(process.env.GCLOUD_PROJECT);
}

function parseCloudTraceContext(headerValue: string | undefined): Pick<RequestContext, 'trace' | 'spanId'> {
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

function serializeError(error: unknown): SerializableFields {
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

function writeLog(
  severity: 'INFO' | 'WARNING' | 'ERROR',
  event: string,
  fields: SerializableFields,
  stream: NodeJS.WriteStream
): void {
  const context = requestContextStorage.getStore();
  const payload: SerializableFields = {
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

function hasMeaningfulValue(value: unknown): boolean {
  if (typeof value !== 'string') {
    return value !== null && value !== undefined;
  }

  return value.trim().length > 0;
}

function buildCaptureStatus(payload: AttributionObservationInput): 'complete' | 'missing_session_id' | 'partial' {
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

export function summarizeAttributionObservation(payload: unknown): SerializableFields {
  const observation = isRecord(payload) ? (payload as AttributionObservationInput) : {};

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

export function summarizeDualWriteConsistency(input: DualWriteConsistencyInput): SerializableFields {
  const consistencyStatus =
    input.browserOutcome === input.serverOutcome &&
    (input.browserOutcome === 'accepted' || input.browserOutcome === 'deduplicated')
      ? 'matched'
      : 'mismatched';

  return {
    consistencyStatus,
    browserOutcome: input.browserOutcome,
    serverOutcome: input.serverOutcome
  };
}

export function summarizeResolverOutcome(input: ResolverOutcomeInput): SerializableFields {
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

export function runWithRequestContext<TResult>(context: RequestContext, callback: () => TResult): TResult {
  return requestContextStorage.run(context, callback);
}

export function getRequestContext(): RequestContext | undefined {
  return requestContextStorage.getStore();
}

export function logInfo(event: string, fields: SerializableFields = {}): void {
  writeLog('INFO', event, fields, process.stdout);
}

export function logWarning(event: string, fields: SerializableFields = {}): void {
  writeLog('WARNING', event, fields, process.stdout);
}

export function logError(event: string, error: unknown, fields: SerializableFields = {}): void {
  writeLog('ERROR', event, { ...fields, error: serializeError(error) }, process.stderr);
}

export function createRequestLoggingMiddleware(service: string) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const startedAt = process.hrtime.bigint();
    const requestId = normalizeString(req.header('x-request-id')) ?? randomUUID();
    const traceContext = parseCloudTraceContext(req.header('x-cloud-trace-context') ?? undefined);

    res.setHeader('x-request-id', requestId);

    runWithRequestContext(
      {
        requestId,
        method: req.method,
        path: req.originalUrl || req.url,
        ...traceContext
      },
      () => {
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
      }
    );
  };
}

export function logHttpError(
  event: string,
  error: unknown,
  req: Request,
  extra: SerializableFields = {}
): void {
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

export function buildAttributionBacklogLog(snapshot: SerializableFields): string {
  return JSON.stringify({
    severity: 'INFO',
    event: 'attribution_backlog_snapshot',
    message: 'attribution_backlog_snapshot',
    timestamp: new Date().toISOString(),
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    ...snapshot
  });
}

export const __observabilityTestUtils = {
  buildAttributionBacklogLog,
  parseCloudTraceContext,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency,
  summarizeResolverOutcome
};
