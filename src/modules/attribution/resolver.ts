import type { AttributionTouchpoint } from './engine.js';

export const DETERMINISTIC_INGESTION_SOURCES = [
  'landing_session_id',
  'checkout_token',
  'cart_token',
  'customer_identity'
] as const;

export type DeterministicIngestionSource = (typeof DETERMINISTIC_INGESTION_SOURCES)[number];

export type ResolvedAttributionTouchpoint = AttributionTouchpoint & {
  sourceTouchEventId: string | null;
  ingestionSource: DeterministicIngestionSource;
};

export type ResolvedJourney = {
  touchpoints: ResolvedAttributionTouchpoint[];
  winner: ResolvedAttributionTouchpoint | null;
  confidenceScore: number;
};

const INGESTION_SOURCE_PRECEDENCE: Record<DeterministicIngestionSource, number> = {
  landing_session_id: 0,
  checkout_token: 1,
  cart_token: 2,
  customer_identity: 3
};

function hasClickId(touchpoint: Pick<ResolvedAttributionTouchpoint, 'clickIdValue'>): boolean {
  return Boolean(touchpoint.clickIdValue);
}

function compareDatesDescending(left: Date, right: Date): number {
  return right.getTime() - left.getTime();
}

function compareDatesAscending(left: Date, right: Date): number {
  return left.getTime() - right.getTime();
}

function compareIngestionSource(left: DeterministicIngestionSource, right: DeterministicIngestionSource): number {
  return INGESTION_SOURCE_PRECEDENCE[left] - INGESTION_SOURCE_PRECEDENCE[right];
}

function compareLexical(left: string | null, right: string | null): number {
  return (left ?? '').localeCompare(right ?? '');
}

export function isDirectTouchpoint(
  touchpoint: Pick<
    AttributionTouchpoint,
    'source' | 'medium' | 'campaign' | 'content' | 'term' | 'clickIdValue'
  >
): boolean {
  return !touchpoint.source &&
    !touchpoint.medium &&
    !touchpoint.campaign &&
    !touchpoint.content &&
    !touchpoint.term &&
    !touchpoint.clickIdValue;
}

function compareDedupPriority(left: ResolvedAttributionTouchpoint, right: ResolvedAttributionTouchpoint): number {
  const sourceComparison = compareIngestionSource(left.ingestionSource, right.ingestionSource);
  if (sourceComparison !== 0) {
    return sourceComparison;
  }

  const occurredAtComparison = compareDatesDescending(left.occurredAt, right.occurredAt);
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const clickIdComparison = Number(hasClickId(right)) - Number(hasClickId(left));
  if (clickIdComparison !== 0) {
    return clickIdComparison;
  }

  return compareLexical(left.sourceTouchEventId, right.sourceTouchEventId);
}

function compareWinnerPriority(left: ResolvedAttributionTouchpoint, right: ResolvedAttributionTouchpoint): number {
  const occurredAtComparison = compareDatesDescending(left.occurredAt, right.occurredAt);
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const sourceComparison = compareIngestionSource(left.ingestionSource, right.ingestionSource);
  if (sourceComparison !== 0) {
    return sourceComparison;
  }

  const clickIdComparison = Number(hasClickId(right)) - Number(hasClickId(left));
  if (clickIdComparison !== 0) {
    return clickIdComparison;
  }

  return compareLexical(left.sessionId, right.sessionId);
}

function compareTimelineOrder(left: ResolvedAttributionTouchpoint, right: ResolvedAttributionTouchpoint): number {
  const occurredAtComparison = compareDatesAscending(left.occurredAt, right.occurredAt);
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const sourceComparison = compareIngestionSource(left.ingestionSource, right.ingestionSource);
  if (sourceComparison !== 0) {
    return sourceComparison;
  }

  const clickIdComparison = Number(hasClickId(right)) - Number(hasClickId(left));
  if (clickIdComparison !== 0) {
    return clickIdComparison;
  }

  return compareLexical(left.sessionId, right.sessionId);
}

export function dedupeDeterministicCandidates(
  candidates: ResolvedAttributionTouchpoint[]
): ResolvedAttributionTouchpoint[] {
  const deduped = new Map<string, ResolvedAttributionTouchpoint>();

  for (const candidate of candidates) {
    if (!candidate.sessionId) {
      continue;
    }

    const existing = deduped.get(candidate.sessionId);
    if (!existing || compareDedupPriority(candidate, existing) < 0) {
      deduped.set(candidate.sessionId, candidate);
    }
  }

  return Array.from(deduped.values()).sort(compareTimelineOrder);
}

export function selectLastNonDirectWinner(
  candidates: ResolvedAttributionTouchpoint[]
): ResolvedAttributionTouchpoint | null {
  const nonDirectCandidates = candidates.filter((candidate) => !candidate.isDirect);
  const directCandidates = candidates.filter((candidate) => candidate.isDirect);
  const selectionPool = nonDirectCandidates.length > 0 ? nonDirectCandidates : directCandidates;

  if (selectionPool.length === 0) {
    return null;
  }

  return selectionPool.slice().sort(compareWinnerPriority)[0] ?? null;
}

export function confidenceScoreForWinner(
  winner: Pick<ResolvedAttributionTouchpoint, 'ingestionSource'> | null
): number {
  if (!winner) {
    return 0;
  }

  switch (winner.ingestionSource) {
    case 'landing_session_id':
    case 'checkout_token':
      return 1;
    case 'cart_token':
      return 0.9;
    case 'customer_identity':
      return 0.6;
  }
}
