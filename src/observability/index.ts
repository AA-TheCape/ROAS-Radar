import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

import type { NextFunction, Request, Response } from 'express';

type LogSeverity = 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'CRITICAL';

export type RequestContext = {
  requestId: string;
  method?: string;
  path?: string;
  trace?: string;
  spanId?: string;
};

type LogFields = Record<string, unknown>;

const requestContextStorage = new AsyncLocalStorage<RequestContext>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function normalizeString(value: string | null | undefined): string | undefined {
  const normalized = value?.trim();
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

function serializeError(error: unknown): Record<string, unknown> {
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

function writeLog(severity: LogSeverity, event: string, fields: LogFields, stream: NodeJS.WriteStream): void {
  const context = requestContextStorage.getStore();
  const payload: Record<string, unknown> = {
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

export function runWithRequestContext<T>(context: RequestContext, callback: () => T): T {
  return requestContextStorage.run(context, callback);
}

export function getRequestContext(): RequestContext | undefined {
  return requestContextStorage.getStore();
}

export function logInfo(event: string, fields: LogFields = {}): void {
  writeLog('INFO', event, fields, process.stdout);
}

export function logWarning(event: string, fields: LogFields = {}): void {
  writeLog('WARNING', event, fields, process.stdout);
}

export function logError(event: string, error: unknown, fields: LogFields = {}): void {
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

export function logHttpError(event: string, error: unknown, req: Request, extra: LogFields = {}): void {
  const details =
    isRecord(error) && 'details' in error
      ? {
          details: (error as { details?: unknown }).details
        }
      : {};
  const code =
    isRecord(error) && typeof error.code === 'string'
      ? {
          code: error.code
        }
      : {};
  const statusCode =
    isRecord(error) && typeof error.statusCode === 'number'
      ? {
          statusCode: error.statusCode
        }
      : {};

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

export function buildAttributionBacklogLog(snapshot: {
  workerId: string;
  pendingJobs: number;
  oldestJobAgeSeconds: number;
  staleProcessingJobs: number;
}): string {
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
  parseCloudTraceContext
};
