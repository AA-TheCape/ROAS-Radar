import { AsyncLocalStorage } from 'node:async_hooks';
import { Buffer } from 'node:buffer';
import { randomUUID } from 'node:crypto';

import type { NextFunction, Request, RequestHandler, Response } from 'express';

import type { OrderAttributionBackfillReport } from '../../packages/attribution-schema/index.js';
import type { NormalizedRecoveryError, RecoveryCheckpoint, RecoveryRun } from '../modules/recovery/index.js';

type SerializableFields = Record<string, unknown>;

type RequestContext = {
  requestId: string;
  method: string;
  path: string;
  trace?: string;
  spanId?: string;
};

type BackfillLifecycleStage = 'enqueued' | 'started' | 'completed' | 'failed';

type OrderAttributionBackfillLifecycleOptions = {
  startDate: string;
  endDate: string;
  dryRun: boolean;
  limit: number;
  webOrdersOnly: boolean;
  skipShopifyWriteback: boolean;
};

type OrderAttributionBackfillLifecycleInput = {
  stage: BackfillLifecycleStage;
  jobId: string;
  workerId?: string;
  submittedAt?: string;
  startedAt?: string;
  completedAt?: string;
  options: OrderAttributionBackfillLifecycleOptions;
  report?: OrderAttributionBackfillReport | null;
  error?: unknown;
};

type AttributionBacklogSnapshot = {
  workerId: string;
  pendingJobs: number;
  oldestJobAgeSeconds: number;
  staleProcessingJobs: number;
};

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
  tier?: string | null;
  attributionReason?: string | null;
  confidenceScore?: number | null;
  pipeline?: string | null;
  orderOccurredAtUtc?: Date | string | null;
  shopifyOrderId?: string | null;
  normalizationFailures?: Array<{
    scope?: string | null;
    reason?: string | null;
    sourceKey?: string | null;
  }>;
};

type Ga4IngestionSummaryInput = {
  watermarkBefore: string | null;
  watermarkAfter: string | null;
  processedHours: string[];
  extractedRows: number;
  upsertedRows: number;
  now?: Date;
  lagAlertThresholdHours?: number;
  rows?: Array<{
    source: string | null;
    medium: string | null;
    campaign: string | null;
    clickIdValue: string | null;
  }>;
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

type MetaMetadataLookupSummaryLogInput = {
  resolutionScope: 'campaign_adset_metadata';
  requestedCount: number;
  normalizedRequestCount: number;
  invalidIdCount: number;
  cacheHitCount: number;
  staleCacheHitCount: number;
  recentFailureCacheHitCount: number;
  cacheMissCount: number;
  apiRequestCount: number;
  apiLookupObjectCount: number;
  apiResolvedCount: number;
  apiNotFoundCount: number;
  apiFailureCount: number;
  missingConnectionCount: number;
  unresolvedCount: number;
  unresolvedEntityIds?: string[];
  unresolvedReasons?: Record<string, number>;
};

type MetaMetadataRawIdFallbackLogInput = {
  resolutionScope: 'campaign_adset_metadata';
  startDate: string;
  endDate: string;
  source?: string | null;
  requestedCount: number;
  unresolvedCount: number;
  unresolvedEntityIds?: string[];
  unresolvedReasons?: Record<string, number>;
};

type RecoveryRunLifecycleLogInput = {
  stage: 'started' | 'completed' | 'failed' | 'cancelled';
  run: RecoveryRun;
  workerId: string;
  pagesProcessed?: number;
  durationMs?: number | null;
  error?: unknown;
};

type RecoveryRunChunkLogInput = {
  run: RecoveryRun;
  workerId: string;
  pageNumber: number;
  recordsDiscovered: number;
  recordsProcessed: number;
  done: boolean;
  durationMs: number;
  checkpoint: RecoveryCheckpoint;
};

type RecoveryRecordFailureLogInput = {
  run: RecoveryRun;
  workerId: string;
  recordId: string;
  recordType: string;
  recordKey: string;
  attemptNumber: number;
  retryable: boolean;
  nextAttemptAt?: Date | null;
  error: NormalizedRecoveryError;
};

type AttributionQaSnapshotWriteLogInput = {
  orderId: string;
  pipeline: 'realtime_queue' | 'order_backfill' | 'generated_on_read' | string;
  status: 'success' | 'failure';
  attributionTier?: string | null;
  matchSource?: string | null;
  payload?: unknown;
  error?: unknown;
};

type AttributionQaPayloadFetchLogInput = {
  endpoint: 'public_qa_payload' | 'admin_qa_debug' | string;
  orderId: string;
  status: 'success' | 'not_found' | 'failure';
  statusCode: number;
  durationMs: number;
  source?: string | null;
  payload?: unknown;
  rawEvidenceCount?: number | null;
  rawEvidenceSizeBytes?: number | null;
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

export function parseCloudTraceContext(headerValue: string | undefined): Pick<RequestContext, 'trace' | 'spanId'> {
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
      stack: error.stack ?? null
    };
  }

  return {
    message: String(error)
  };
}

function summarizeBackfillFailures(
  failures: OrderAttributionBackfillReport['failures']
): Pick<OrderAttributionBackfillReport, 'failures'> & {
  failureCount: number;
  sampleFailures: OrderAttributionBackfillReport['failures'];
} {
  return {
    failures,
    failureCount: failures.length,
    sampleFailures: failures.slice(0, 5)
  };
}

export function summarizeOrderAttributionBackfillReport(report: OrderAttributionBackfillReport): SerializableFields {
  return {
    scanned: report.scanned,
    recovered: report.recovered,
    unrecoverable: report.unrecoverable,
    writebackCompleted: report.writebackCompleted,
    ...summarizeBackfillFailures(report.failures)
  };
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

function calculateDurationMs(startedAt: string | null, completedAt: string | null): number | null {
  if (!startedAt || !completedAt) {
    return null;
  }

  const started = new Date(startedAt).getTime();
  const completed = new Date(completedAt).getTime();

  if (Number.isNaN(started) || Number.isNaN(completed)) {
    return null;
  }

  return Math.max(0, completed - started);
}

function buildRecoveryTraceFields(runId: string): SerializableFields {
  const traceId = runId.replaceAll('-', '').toLowerCase();
  const projectId = getGoogleCloudProjectId();
  const isCloudTraceCompatible = /^[a-f0-9]{32}$/.test(traceId);

  return {
    recoveryTraceId: traceId,
    ...(projectId && isCloudTraceCompatible
      ? { 'logging.googleapis.com/trace': `projects/${projectId}/traces/${traceId}` }
      : {})
  };
}

function summarizeRecoveryRun(run: RecoveryRun): SerializableFields {
  return {
    runId: run.id,
    jobType: run.jobType,
    status: run.status,
    mode: run.mode,
    initiatedBy: run.initiatedBy,
    dryRun: run.dryRun,
    scopeKey: run.scopeKey,
    timeRangeStart: run.timeRangeStart,
    timeRangeEnd: run.timeRangeEnd,
    resumeFromRunId: run.resumeFromRunId,
    rerunOfRunId: run.rerunOfRunId,
    queuedAt: run.queuedAt,
    startedAt: run.startedAt,
    completedAt: run.completedAt,
    lastHeartbeatAt: run.lastHeartbeatAt,
    durationMs: calculateDurationMs(run.startedAt, run.completedAt),
    processedCount: run.recordsProcessed,
    updatedCount: run.sideEffectsSucceeded,
    skippedCount: run.recordsSkipped,
    failedCount: run.recordsFailed,
    retriedCount: run.recordsRetried,
    discoveredCount: run.recordsDiscovered,
    claimedCount: run.recordsClaimed,
    sideEffectsAttempted: run.sideEffectsAttempted,
    sideEffectsSuppressed: run.sideEffectsSuppressed,
    errorCode: run.errorCode,
    errorMessage: run.errorMessage,
    ...buildRecoveryTraceFields(run.id)
  };
}

export function emitRecoveryRunLifecycleLog(input: RecoveryRunLifecycleLogInput): void {
  const runSummary = summarizeRecoveryRun(input.run);
  const fields: SerializableFields = {
    service: process.env.K_SERVICE ?? 'roas-radar-recovery-worker',
    stage: input.stage,
    workerId: input.workerId,
    pagesProcessed: input.pagesProcessed ?? null,
    ...runSummary,
    durationMs:
      typeof input.durationMs === 'number'
        ? Number(input.durationMs.toFixed(2))
        : runSummary.durationMs
  };

  if (input.stage === 'failed') {
    fields.alertable = true;
    fields.failureContext = {
      runId: input.run.id,
      jobType: input.run.jobType,
      workerId: input.workerId,
      status: input.run.status,
      checkpoint: input.run.checkpoint,
      counters: {
        processed: input.run.recordsProcessed,
        updated: input.run.sideEffectsSucceeded,
        skipped: input.run.recordsSkipped,
        failed: input.run.recordsFailed,
        retried: input.run.recordsRetried
      }
    };
    logError('recovery_run_lifecycle', input.error ?? new Error('Recovery run failed'), fields);
    return;
  }

  logInfo('recovery_run_lifecycle', fields);
}

export function emitRecoveryRunChunkLog(input: RecoveryRunChunkLogInput): void {
  logInfo('recovery_run_chunk_processed', {
    service: process.env.K_SERVICE ?? 'roas-radar-recovery-worker',
    ...summarizeRecoveryRun(input.run),
    workerId: input.workerId,
    pageNumber: input.pageNumber,
    recordsDiscovered: input.recordsDiscovered,
    recordsProcessed: input.recordsProcessed,
    processedCount: input.run.recordsProcessed,
    updatedCount: input.run.sideEffectsSucceeded,
    skippedCount: input.run.recordsSkipped,
    failedCount: input.run.recordsFailed,
    retriedCount: input.run.recordsRetried,
    done: input.done,
    durationMs: Number(input.durationMs.toFixed(2)),
    checkpoint: input.checkpoint
  });
}

export function emitRecoveryRecordFailureLog(input: RecoveryRecordFailureLogInput): void {
  logError('recovery_record_failure', new Error(input.error.message), {
    service: process.env.K_SERVICE ?? 'roas-radar-recovery-worker',
    workerId: input.workerId,
    runId: input.run.id,
    jobType: input.run.jobType,
    status: input.run.status,
    mode: input.run.mode,
    dryRun: input.run.dryRun,
    recordId: input.recordId,
    recordType: input.recordType,
    recordKey: input.recordKey,
    attemptNumber: input.attemptNumber,
    retryable: input.retryable,
    nextAttemptAt: input.nextAttemptAt?.toISOString() ?? null,
    errorCode: input.error.code,
    errorMessage: input.error.message,
    errorDetails: input.error.details,
    actionContext: {
      inspectRunFilter: `jsonPayload.event="recovery_run_lifecycle" jsonPayload.runId="${input.run.id}"`,
      inspectRecordFilter: `jsonPayload.event="recovery_record_failure" jsonPayload.runId="${input.run.id}" jsonPayload.recordKey="${input.recordKey}"`,
      checkpoint: input.run.checkpoint
    },
    alertable: !input.retryable,
    ...buildRecoveryTraceFields(input.run.id)
  });
}

function toBackfillLifecycleStatus(stage: BackfillLifecycleStage): 'queued' | 'processing' | 'completed' | 'failed' {
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

export function emitOrderAttributionBackfillJobLifecycleLog(input: OrderAttributionBackfillLifecycleInput): void {
  const fields: SerializableFields = {
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
    limit: input.options.limit,
    webOrdersOnly: input.options.webOrdersOnly,
    skipShopifyWriteback: input.options.skipShopifyWriteback
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

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function arrayLength(value: unknown): number {
  return Array.isArray(value) ? value.length : 0;
}

function measureJsonSizeBytes(value: unknown): number {
  try {
    return Buffer.byteLength(JSON.stringify(value) ?? 'null', 'utf8');
  } catch {
    return 0;
  }
}

function summarizeAttributionQaPayload(payload: unknown): SerializableFields {
  const record = asRecord(payload);
  const candidates = asRecord(record?.candidates);
  const diagnostics = asRecord(record?.diagnostics);

  return {
    payloadSizeBytes: payload === undefined ? 0 : measureJsonSizeBytes(payload),
    candidateCount:
      arrayLength(candidates?.deterministic_first_party) +
      arrayLength(candidates?.shopify_hint) +
      arrayLength(candidates?.ga4_fallback),
    modelSummaryCount: arrayLength(record?.model_summaries),
    creditCount: arrayLength(record?.credits),
    explainabilityRecordCount: arrayLength(record?.explainability),
    normalizationFailureCount: arrayLength(diagnostics?.normalization_failures)
  };
}

function buildCaptureStatus(payload: Record<string, unknown>): 'missing_session_id' | 'complete' | 'partial' {
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
  const baseFields: SerializableFields = {
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
    resolverOutcome: input.winner.isDirect ? 'direct_winner' : 'non_direct_winner',
    touchpointCount: input.touchpoints.length,
    winningIngestionSource: input.winner.ingestionSource ?? null,
    winningSessionId: input.winner.sessionId ?? null,
    hasWinningSessionId: Boolean(input.winner.sessionId)
  };
}

export function emitAttributionResolverOutcomeLog(input: ResolverOutcomeInput): void {
  logInfo('attribution_resolver_outcome', {
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    ...summarizeResolverOutcome(input)
  });
}

export function emitAttributionQaSnapshotWriteLog(input: AttributionQaSnapshotWriteLogInput): void {
  const fields: SerializableFields = {
    service: process.env.K_SERVICE ?? 'roas-radar-attribution-worker',
    order_id: input.orderId,
    orderId: input.orderId,
    pipeline: input.pipeline,
    status: input.status,
    attributionTier: input.attributionTier ?? null,
    matchSource: input.matchSource ?? null,
    ...summarizeAttributionQaPayload(input.payload)
  };

  if (input.status === 'failure') {
    logError('attribution_qa_snapshot_write', input.error ?? new Error('Attribution QA snapshot write failed'), {
      ...fields,
      alertable: true
    });
    return;
  }

  logInfo('attribution_qa_snapshot_write', fields);
}

export function emitAttributionQaPayloadFetchLog(input: AttributionQaPayloadFetchLogInput): void {
  const fields: SerializableFields = {
    service: process.env.K_SERVICE ?? 'roas-radar-api',
    endpoint: input.endpoint,
    order_id: input.orderId,
    orderId: input.orderId,
    status: input.status,
    statusCode: input.statusCode,
    statusClass: `${Math.floor(input.statusCode / 100)}xx`,
    source: input.source ?? null,
    durationMs: Number(input.durationMs.toFixed(2)),
    rawEvidenceCount: input.rawEvidenceCount ?? null,
    rawEvidenceSizeBytes: input.rawEvidenceSizeBytes ?? null,
    ...summarizeAttributionQaPayload(input.payload)
  };

  if (input.status === 'failure') {
    logError('attribution_qa_payload_fetch', input.error ?? new Error('Attribution QA payload fetch failed'), fields);
    return;
  }

  logInfo('attribution_qa_payload_fetch', fields);
}

export function runWithRequestContext<T>(context: RequestContext, callback: () => T): T {
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

export function createRequestLoggingMiddleware(service: string): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
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
              userAgent: req.header('user-agent') ?? null,
              referer: req.header('referer') ?? null,
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
  const details =
    isRecord(error) && 'details' in error
      ? {
          details: (error as { details?: unknown }).details ?? null
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

function computeGa4IngestionLagHours(now: Date, watermarkAfter: string | null): number | null {
  if (!watermarkAfter) {
    return null;
  }

  const latestCompleteHour = new Date(
    Math.floor(now.getTime() / (60 * 60 * 1000)) * (60 * 60 * 1000) - 60 * 60 * 1000
  );
  const watermarkDate = new Date(watermarkAfter);

  if (Number.isNaN(latestCompleteHour.getTime()) || Number.isNaN(watermarkDate.getTime())) {
    return null;
  }

  return Math.max(0, Math.round((latestCompleteHour.getTime() - watermarkDate.getTime()) / (60 * 60 * 1000)));
}

export function summarizeGa4IngestionResult(input: Ga4IngestionSummaryInput): SerializableFields {
  const rows = input.rows ?? [];
  const rowCount = rows.length;
  const countPresent = (selector: (row: (typeof rows)[number]) => string | null) =>
    rows.reduce((total, row) => total + Number(hasMeaningfulValue(selector(row))), 0);

  const sourcePresentRows = countPresent((row) => row.source);
  const mediumPresentRows = countPresent((row) => row.medium);
  const campaignPresentRows = countPresent((row) => row.campaign);
  const clickIdPresentRows = countPresent((row) => row.clickIdValue);
  const lagHours = computeGa4IngestionLagHours(input.now ?? new Date(), input.watermarkAfter);
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
  const fields: SerializableFields = {
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
    logError('campaign_metadata_sync_job_lifecycle', input.error ?? new Error('Campaign metadata sync failed'), fields);
    return;
  }

  logInfo('campaign_metadata_sync_job_lifecycle', fields);
}

export function emitMetaMetadataLookupSummaryLog(input: MetaMetadataLookupSummaryLogInput): void {
  const normalizedRequestCount = Math.max(0, input.normalizedRequestCount);
  const unresolvedCount = Math.max(0, input.unresolvedCount);

  logInfo('meta_metadata_lookup_summary', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    platform: 'meta_ads',
    resolutionScope: input.resolutionScope,
    requestedCount: Math.max(0, input.requestedCount),
    normalizedRequestCount,
    invalidIdCount: Math.max(0, input.invalidIdCount),
    cacheHitCount: Math.max(0, input.cacheHitCount),
    staleCacheHitCount: Math.max(0, input.staleCacheHitCount),
    recentFailureCacheHitCount: Math.max(0, input.recentFailureCacheHitCount),
    cacheMissCount: Math.max(0, input.cacheMissCount),
    cacheHitRate: normalizedRequestCount > 0 ? input.cacheHitCount / normalizedRequestCount : 0,
    cacheMissRate: normalizedRequestCount > 0 ? input.cacheMissCount / normalizedRequestCount : 0,
    apiRequestCount: Math.max(0, input.apiRequestCount),
    apiLookupObjectCount: Math.max(0, input.apiLookupObjectCount),
    apiResolvedCount: Math.max(0, input.apiResolvedCount),
    apiNotFoundCount: Math.max(0, input.apiNotFoundCount),
    apiFailureCount: Math.max(0, input.apiFailureCount),
    missingConnectionCount: Math.max(0, input.missingConnectionCount),
    unresolvedCount,
    unresolvedRate: normalizedRequestCount > 0 ? unresolvedCount / normalizedRequestCount : 0,
    unresolvedEntityIds: (input.unresolvedEntityIds ?? []).slice(0, 10),
    unresolvedReasons: input.unresolvedReasons ?? {}
  });
}

export function emitMetaMetadataRawIdFallbackLog(input: MetaMetadataRawIdFallbackLogInput): void {
  logInfo('meta_metadata_raw_id_fallback', {
    service: process.env.K_SERVICE ?? 'roas-radar',
    platform: 'meta_ads',
    resolutionScope: input.resolutionScope,
    requestedCount: Math.max(0, input.requestedCount),
    unresolvedCount: Math.max(0, input.unresolvedCount),
    unresolvedEntityIds: (input.unresolvedEntityIds ?? []).slice(0, 10),
    unresolvedReasons: input.unresolvedReasons ?? {},
    fallback: 'raw_id',
    startDate: input.startDate,
    endDate: input.endDate,
    source: input.source ?? null
  });
}

export function buildAttributionBacklogLog(snapshot: AttributionBacklogSnapshot): string {
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
  emitAttributionResolverOutcomeLog,
  emitOrderAttributionBackfillJobLifecycleLog,
  parseCloudTraceContext,
  summarizeGa4IngestionResult,
  summarizeOrderAttributionBackfillReport,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency,
  summarizeResolverOutcome,
  emitCampaignMetadataResolutionCoverageLog,
  emitCampaignMetadataFreshnessSnapshotLog,
  emitCampaignMetadataSyncJobLifecycleLog,
  emitMetaMetadataLookupSummaryLog,
  emitMetaMetadataRawIdFallbackLog
};
