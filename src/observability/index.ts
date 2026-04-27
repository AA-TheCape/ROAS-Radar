type ResolverOutcomeInput = {
  touchpoints: unknown[];
  winner: {
    isDirect?: boolean;
    ingestionSource?: string | null;
    sessionId?: string | null;
    matchSource?: string | null;
    source?: string | null;
    medium?: string | null;
    campaign?: string | null;
    clickIdValue?: string | null;
  } | null;
  deterministicWinnerExists?: boolean;
  shopifyHintMatchExists?: boolean;
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

export function emitOrderAttributionBackfillJobLifecycleLog(input: OrderAttributionBackfillLifecycleInput): void {
  const fields: SerializableFields = {
    service: process.env.K_SERVICE ?? 'roas-radar',
    correlationId: input.jobId,
    stage: input.stage,
    status: toBackfillLifecycleStatus(input.stage),
    jobId: input.jobId,
    // ...existing fields...
  };
  // ...existing logic...
}

function writeLog(
  severity: 'INFO' | 'WARNING' | 'ERROR',
  event: string,
  fields: SerializableFields,
  stream: NodeJS.WriteStream
): void {
  const context = requestContextStorage.getStore();
  const correlationId =
    normalizeString(fields.correlationId) ??
    normalizeString(fields.requestId) ??
    normalizeString(context?.requestId);

  const payload: SerializableFields = {
    severity,
    event,
    message: event,
    timestamp: new Date().toISOString(),
    ...(correlationId ? { correlationId } : {}),
    ...(context ? { requestContext: context } : {}),
    ...fields
  };

  // ...existing trace handling...
}

export function summarizeResolverOutcome(input: ResolverOutcomeInput): SerializableFields {
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

function computeLagHours(now: Date, watermarkAfter: string | null): number | null {
  if (!watermarkAfter) {
    return null;
  }

  const latestCompleteHour = new Date(Math.floor(now.getTime() / (60 * 60 * 1000)) * (60 * 60 * 1000) - (60 * 60 * 1000));
  const watermarkDate = new Date(watermarkAfter);

  if (Number.isNaN(latestCompleteHour.getTime()) || Number.isNaN(watermarkDate.getTime())) {
    return null;
  }

  return Math.max(0, Math.round((latestCompleteHour.getTime() - watermarkDate.getTime()) / (60 * 60 * 1000)));
}

export function summarizeGa4IngestionResult(input: Ga4IngestionSummaryInput): SerializableFields {
  const rows = input.rows ?? [];
  const rowCount = rows.length;
  const present = (value: string | null | undefined): boolean => hasMeaningfulValue(value);
  const countPresent = (selector: (row: (typeof rows)[number]) => string | null) =>
    rows.reduce((total, row) => total + Number(present(selector(row))), 0);

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

export const __observabilityTestUtils = {
  buildAttributionBacklogLog,
  emitOrderAttributionBackfillJobLifecycleLog,
  parseCloudTraceContext,
  summarizeGa4IngestionResult,
  summarizeOrderAttributionBackfillReport,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency,
  summarizeResolverOutcome
};
