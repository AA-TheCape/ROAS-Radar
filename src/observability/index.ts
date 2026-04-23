type DualWriteOutcome = 'accepted' | 'deduplicated' | 'failed' | 'rejected';

export function summarizeAttributionObservation(input: unknown): AttributionObservationSummary {
  // Normalizes camelCase and snake_case payloads into one completeness status.
}

export function summarizeDualWriteConsistency(input: {
  browserOutcome: DualWriteOutcome;
  serverOutcome: 'accepted' | 'deduplicated' | 'failed' | 'rejected';
}): DualWriteConsistencySummary {
  return {
    consistencyStatus: input.serverOutcome === 'failed' || input.serverOutcome === 'rejected' ? 'mismatched' : 'matched',
    browserOutcome: input.browserOutcome,
    serverOutcome: input.serverOutcome
  };
}

export function summarizeResolverOutcome(journey: ResolverJourneySummary) {
  return {
    resolverOutcome: !winner ? 'unattributed' : winner.isDirect ? 'direct_winner' : 'non_direct_winner',
    candidateCount: journey.touchpoints.length,
    directCandidateCount,
    nonDirectCandidateCount,
    winnerIngestionSource: winner?.ingestionSource ?? null,
    winnerAttributionReason: winner?.attributionReason ?? null,
    winnerHasClickId: Boolean(winner?.clickIdValue),
    winnerSessionId: winner?.sessionId ?? null
  };
}

export const __observabilityTestUtils = {
  buildAttributionBacklogLog,
  parseCloudTraceContext,
  summarizeAttributionObservation,
  summarizeDualWriteConsistency,
  summarizeResolverOutcome
};
