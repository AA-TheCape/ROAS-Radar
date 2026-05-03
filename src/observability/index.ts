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

type LogSeverity = 'INFO' | 'WARNING' | 'ERROR';

type OrderAttributionBackfillReport = {
  scanned: number;
  recovered: number;
  unrecoverable: number;
  writebackCompleted: number;
  failures: unknown[];
};

type AttributionObservationPayload = Record<string, unknown>;

type DualWriteConsistencyInput = {
  browserOutcome: 'accepted' | 'deduplicated' | 'failed';
  serverOutcome: 'accepted' | 'deduplicated' | 'failed';
};

type AttributionResolverOutcomeInput = {
  shopifyOrderId: string | null;
  orderOccurredAtUtc: Date | string | null;
  tier: string | null;
  attributionReason: string | null;
  confidenceScore?: number | null;
  pipeline: string | null;
  touchpoints: Array<unknown>;
  winner?: {
    sessionId?: string | null;
    source?: string | null;
    medium?: string | null;
    campaign?: string | null;
    content?: string | null;
    term?: string | null;
    clickIdType?: string | null;
    clickIdValue?: string | null;
  } | null;
  normalizationFailures?: Array<{
    scope?: string | null;
    reason?: string | null;
    sourceKey?: string | null;
  }> | null;
};

type CampaignMetadataCoverageLogInput = {
  resolutionScope: 'campaign' | 'campaign_group';
  platform: 'google_ads' | 'meta_ads' | 'mixed';
  entityType: 'campaign';
  requestedCount: number;
  matchedCount: number;
  resolvedCount: number;
  fallbackCount: number;
  unresolvedCount: number;
  unresolvedEntityIds?: string[];
  startDate: string;
  endDate: string;
  source?: string | null;
};

type CampaignMetadataFreshnessSnapshotLogInput = {
  platform: 'google_ads' | 'meta_ads';
  entityType: 'campaign' | 'adset' | 'ad';
  freshEntityCount: number;
  staleEntityCount: number;
  freshnessThresholdHours: number;
  oldestLastSeenAt: string | null;
  newestLastSeenAt: string | null;
};

type CampaignMetadataSyncJobLifecycleLogInput = {
  stage: 'started' | 'completed' | 'failed';
  platform: 'google_ads' | 'meta_ads' | 'all';
  workerId: string;
  jobId?: string | null;
  requestedBy?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  durationMs?: number | null;
  plannedInserts?: number | null;
  plannedUpdates?: number | null;
  campaignResolvedRate?: number | null;
  overallUnresolvedRate?: number | null;
  staleEntityCount?: number | null;
  error?: unknown;
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

export function parseCloudTraceContext(headerValue: string | undefined): {
  trace?: string;
  spanId?: string;
} {
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
      stack: error.stack ?? null
    };
  }

  return {
    message: String(error)
  };
}

function writeLog(
  severity: LogSeverity,
  event: string,
  fields: Record<string, unknown>,
  stream: NodeJS.WriteStream
): void {
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

export function logInfo(event: string, fields: Record<string, unknown> = {}): void {
  writeLog('INFO', event, fields, process.stdout);
}

export function logWarning(event: string, fields: Record<string, unknown> = {}): void {
  writeLog('WARNING', event, fields, process.stdout);
}

export function logError(event: string, error: unknown, fields: Record<string, unknown> = {}): void {
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
  extra: Record<string, unknown> = {}
): void {
  const details =
    isRecord(error) && 'details' in error
      ? {
          details: error.details
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

export function buildAttributionBacklogLog(snapshot: Record<string, unknown>): string {
  return JSON.stringify({
    severity: 'INFO',
    event: 'attribution_backlog_snapshot',
    message: 'attribution_backlog_snapshot',
    timestamp: new Date().toISOString(),
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    ...snapshot
  });
}

function summarizeBackfillFailures(failures: unknown[]): {
  failures: unknown[];
  failureCount: number;
  sampleFailures: unknown[];
} {
  return {
    failures,
    failureCount: failures.length,
    sampleFailures: failures.slice(0, 5)
  };
}

export function summarizeOrderAttributionBackfillReport(
  report: OrderAttributionBackfillReport | null | undefined
): Record<string, unknown> | null {
  if (!report) {
    return null;
  }

  return {
    scanned: report.scanned,
    recovered: report.recovered,
    unrecoverable: report.unrecoverable,
    writebackCompleted: report.writebackCompleted,
    ...summarizeBackfillFailures(report.failures)
  };
}

function hasMeaningfulValue(value: unknown): boolean {
  if (typeof value !== 'string') {
    return value !== null && value !== undefined;
  }

  return value.trim().length > 0;
}

function buildCaptureStatus(payload: AttributionObservationPayload): string {
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

export function summarizeAttributionObservation(payload: unknown): Record<string, unknown> {
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

export function summarizeDualWriteConsistency(input: DualWriteConsistencyInput): Record<string, unknown> {
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

export function summarizeResolverOutcome(input: AttributionResolverOutcomeInput): Record<string, unknown> {
  const normalizationFailures = Array.isArray(input.normalizationFailures) ? input.normalizationFailures : [];
  const normalizedTier = normalizeString(input.tier) ?? 'unattributed';
  const resolverFallthroughDepth =
    normalizedTier === 'deterministic_first_party'
      ? 0
      : normalizedTier === 'deterministic_shopify_hint'
        ? 1
        : normalizedTier === 'ga4_fallback'
          ? 2
          : 3;
  const fallthroughStage =
    normalizedTier === 'deterministic_first_party'
      ? 'resolved_in_first_party'
      : normalizedTier === 'deterministic_shopify_hint'
        ? 'fell_through_to_shopify_hint'
        : normalizedTier === 'ga4_fallback'
          ? 'fell_through_to_ga4_fallback'
          : 'fell_through_to_unattributed';

  const baseFields: Record<string, unknown> = {
    attributionTier: normalizedTier,
    attributionReason: normalizeString(input.attributionReason) ?? null,
    confidenceScore: typeof input.confidenceScore === 'number' ? input.confidenceScore : null,
    pipeline: normalizeString(input.pipeline) ?? 'unknown',
    shopifyOrderId: normalizeString(input.shopifyOrderId) ?? null,
    orderOccurredAtUtc:
      input.orderOccurredAtUtc instanceof Date
        ? input.orderOccurredAtUtc.toISOString()
        : normalizeString(input.orderOccurredAtUtc) ?? null,
    resolverFallthroughDepth,
    fallthroughStage,
    normalizationFailureCount: normalizationFailures.length,
    hasNormalizationFailures: normalizationFailures.length > 0,
    firstNormalizationFailureScope: normalizeString(normalizationFailures[0]?.scope) ?? null,
    firstNormalizationFailureReason: normalizeString(normalizationFailures[0]?.reason) ?? null,
    firstNormalizationFailureSourceKey: normalizeString(normalizationFailures[0]?.sourceKey) ?? null
  };

  if (!input.winner) {
    return {
      ...baseFields,
      resolverOutcome: 'unattributed',
      touchpointCount: input.touchpoints.length
    };
  }

  return {
    ...baseFields,
    resolverOutcome: 'attributed',
    touchpointCount: input.touchpoints.length,
    sessionId: normalizeString(input.winner.sessionId) ?? null,
    source: normalizeString(input.winner.source) ?? null,
    medium: normalizeString(input.winner.medium) ?? null,
    campaign: normalizeString(input.winner.campaign) ?? null,
    content: normalizeString(input.winner.content) ?? null,
    term: normalizeString(input.winner.term) ?? null,
    clickIdType: normalizeString(input.winner.clickIdType) ?? null,
    clickIdValue: normalizeString(input.winner.clickIdValue) ?? null
  };
}

export function emitAttributionResolverOutcomeLog(input: AttributionResolverOutcomeInput): void {
  logInfo('attribution_resolver_outcome', summarizeResolverOutcome(input));
}

function normalizeBackfillErrorCode(error: unknown): string | null {
  if (isRecord(error) && typeof error.code === 'string' && error.code.trim()) {
    return error.code.trim();
  }

  if (error instanceof Error && error.name.trim()) {
    return error.name.trim();
  }

  return null;
}

function normalizeBackfillErrorMessage(error: unknown): string | null {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }

  if (typeof error === 'string' && error.trim()) {
    return error.trim();
  }

  return null;
}

function toBackfillLifecycleStatus(stage: 'enqueued' | 'started' | 'completed' | 'failed'): string {
  switch (stage) {
    case 'enqueued':
      return 'queued';
    case 'started':
      return 'processing';
    case 'completed':
      return 'completed';
    case 'failed':
      return 'failed';
  }
}

export function emitOrderAttributionBackfillJobLifecycleLog(input: {
  stage: 'enqueued' | 'started' | 'completed' | 'failed';
  jobId: string;
  workerId?: string | null;
  submittedAt?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  options: {
    startDate: string;
    endDate: string;
    dryRun: boolean;
    limit?: number | null;
    webOrdersOnly?: boolean | null;
    skipShopifyWriteback?: boolean | null;
  };
  report?: OrderAttributionBackfillReport | null;
  error?: unknown;
}): void {
  const fields: Record<string, unknown> = {
    service: process.env.K_SERVICE ?? 'roas-radar',
    stage: input.stage,
    status: toBackfillLifecycleStatus(input.stage),
    jobId: input.jobId,
    workerId: input.workerId ?? null,
    submittedAt: input.submittedAt ?? null,
    startedAt: input.startedAt ?? null,
    completedAt: input.completedAt ?? null,
    startDate: input.options.startDate,
    endDate: input.options.endDate,
    dryRun: input.options.dryRun,
    limit: input.options.limit ?? null,
    webOrdersOnly: input.options.webOrdersOnly ?? null,
    skipShopifyWriteback: input.options.skipShopifyWriteback ?? null
  };

  if (input.report) {
    fields.report = summarizeOrderAttributionBackfillReport(input.report);
  }

  if (input.stage === 'failed') {
    const errorCode = normalizeBackfillErrorCode(input.error);
    const errorMessage = normalizeBackfillErrorMessage(input.error);

    if (errorCode) {
      fields.code = errorCode;
    }

    if (errorMessage) {
      fields.failureMessage = errorMessage;
    }

    fields.alertable = true;
    logError(
      'order_attribution_backfill_job_lifecycle',
      input.error ?? new Error('Order attribution backfill job failed'),
      fields
    );
    return;
  }

  logInfo('order_attribution_backfill_job_lifecycle', fields);
}

export function emitCampaignMetadataResolutionCoverageLog(input: CampaignMetadataCoverageLogInput): void {
  const requestedCount = Math.max(0, input.requestedCount);
  const resolvedRate = requestedCount > 0 ? input.resolvedCount / requestedCount : 0;
  const fallbackRate = requestedCount > 0 ? input.fallbackCount / requestedCount : 0;
  const unresolvedRate = requestedCount > 0 ? input.unresolvedCount / requestedCount : 0;

  logInfo('campaign_metadata_resolution_coverage', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    resolutionScope: input.resolutionScope,
    platform: input.platform,
    entityType: input.entityType,
    requestedCount,
    matchedCount: input.matchedCount,
    resolvedCount: input.resolvedCount,
    fallbackCount: input.fallbackCount,
    unresolvedCount: input.unresolvedCount,
    resolvedRate,
    fallbackRate,
    unresolvedRate,
    unresolvedEntityIds: (input.unresolvedEntityIds ?? []).slice(0, 10),
    startDate: input.startDate,
    endDate: input.endDate,
    source: input.source ?? null
  });
}

export function emitCampaignMetadataFreshnessSnapshotLog(input: CampaignMetadataFreshnessSnapshotLogInput): void {
  logInfo('campaign_metadata_freshness_snapshot', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    platform: input.platform,
    entityType: input.entityType,
    freshEntityCount: input.freshEntityCount,
    staleEntityCount: input.staleEntityCount,
    freshnessThresholdHours: input.freshnessThresholdHours,
    oldestLastSeenAt: input.oldestLastSeenAt,
    newestLastSeenAt: input.newestLastSeenAt
  });
}

export function emitCampaignMetadataSyncJobLifecycleLog(input: CampaignMetadataSyncJobLifecycleLogInput): void {
  const fields: Record<string, unknown> = {
    service: process.env.K_SERVICE ?? 'roas-radar',
    stage: input.stage,
    platform: input.platform,
    workerId: input.workerId,
    jobId: input.jobId ?? null,
    requestedBy: input.requestedBy ?? null,
    startedAt: input.startedAt ?? null,
    completedAt: input.completedAt ?? null,
    durationMs: input.durationMs ?? null,
    plannedInserts: input.plannedInserts ?? null,
    plannedUpdates: input.plannedUpdates ?? null,
    campaignResolvedRate: input.campaignResolvedRate ?? null,
    overallUnresolvedRate: input.overallUnresolvedRate ?? null,
    staleEntityCount: input.staleEntityCount ?? null
  };

  if (input.stage === 'failed') {
    fields.alertable = true;
    logError(
      'campaign_metadata_sync_job_lifecycle',
      input.error ?? new Error('Campaign metadata sync failed'),
      fields
    );
    return;
  }

  logInfo('campaign_metadata_sync_job_lifecycle', fields);
}

export const __observabilityTestUtils = {
  buildAttributionBacklogLog,
  parseCloudTraceContext,
  summarizeOrderAttributionBackfillReport,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency,
  summarizeResolverOutcome,
  emitAttributionResolverOutcomeLog,
  emitCampaignMetadataResolutionCoverageLog,
  emitCampaignMetadataFreshnessSnapshotLog,
  emitCampaignMetadataSyncJobLifecycleLog
};
