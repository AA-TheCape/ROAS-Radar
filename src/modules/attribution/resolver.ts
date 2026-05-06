import type { AttributionTouchpoint } from './engine.js';
import {
  CLICK_LOOKBACK_WINDOW_DAYS,
  hasClickId,
  isDirectTouchpoint as isCanonicalDirectTouchpoint,
  qualifiesSyntheticHintSignal
} from './rules.js';

export const DETERMINISTIC_INGESTION_SOURCES = [
  'landing_session_id',
  'checkout_token',
  'cart_token',
  'customer_identity'
] as const;

export type DeterministicIngestionSource = (typeof DETERMINISTIC_INGESTION_SOURCES)[number];
export type ResolvedIngestionSource =
  | DeterministicIngestionSource
  | 'shopify_marketing_hint'
  | 'ga4_fallback';
export type ResolvedAttributionTier =
  | 'deterministic_first_party'
  | 'deterministic_shopify_hint'
  | 'ga4_fallback'
  | 'unattributed';

export const ATTRIBUTION_TIER_LOOKBACK_WINDOW_DAYS = CLICK_LOOKBACK_WINDOW_DAYS;

export type ResolvedAttributionTouchpoint = AttributionTouchpoint & {
  sourceTouchEventId: string | null;
  ingestionSource: ResolvedIngestionSource;
};

export type ResolvedJourney = {
  tier: ResolvedAttributionTier;
  touchpoints: ResolvedAttributionTouchpoint[];
  winner: ResolvedAttributionTouchpoint | null;
  confidenceScore: number;
  attributionReason: string;
  orderOccurredAtUtc: Date | null;
  normalizationFailures: Array<{
    scope: 'order' | 'shopify_hint' | 'ga4_fallback';
    reason: string;
    sourceKey: string | null;
  }>;
};

export type TieredAttributionCandidate = {
  sourceKey: string;
  sessionId: string | null;
  sourceTouchEventId: string | null;
  ingestionSource: ResolvedIngestionSource;
  occurredAtUtc: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  confidenceScore: number;
  isDirect: boolean;
  isSynthetic: boolean;
};

export type TieredAttributionResolverInput = {
  orderOccurredAtUtc: Date | null;
  deterministicFirstParty: TieredAttributionCandidate[];
  shopifyHint: TieredAttributionCandidate[];
  ga4Fallback: TieredAttributionCandidate[];
  normalizationFailures?: Array<{
    scope: 'order' | 'shopify_hint' | 'ga4_fallback';
    reason: string;
    sourceKey: string | null;
  }>;
};

const INGESTION_SOURCE_PRECEDENCE: Record<DeterministicIngestionSource, number> = {
  landing_session_id: 0,
  checkout_token: 1,
  cart_token: 2,
  customer_identity: 3
};

function ingestionSourcePrecedence(source: ResolvedIngestionSource): number {
  if (source === 'shopify_marketing_hint') {
    return Number.MAX_SAFE_INTEGER - 1;
  }

  if (source === 'ga4_fallback') {
    return Number.MAX_SAFE_INTEGER;
  }

  return INGESTION_SOURCE_PRECEDENCE[source];
}

function compareDatesDescending(left: Date, right: Date): number {
  return right.getTime() - left.getTime();
}

function compareDatesAscending(left: Date, right: Date): number {
  return left.getTime() - right.getTime();
}

function compareIngestionSource(left: ResolvedIngestionSource, right: ResolvedIngestionSource): number {
  return ingestionSourcePrecedence(left) - ingestionSourcePrecedence(right);
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
  return isCanonicalDirectTouchpoint(touchpoint);
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

  const clickIdComparison = Number(hasClickId(right.clickIdValue)) - Number(hasClickId(left.clickIdValue));
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

  const clickIdComparison = Number(hasClickId(right.clickIdValue)) - Number(hasClickId(left.clickIdValue));
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

  const clickIdComparison = Number(hasClickId(right.clickIdValue)) - Number(hasClickId(left.clickIdValue));
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
    const normalizedCandidate = {
      ...candidate,
      isDirect: isCanonicalDirectTouchpoint({
        source: candidate.source,
        medium: candidate.medium,
        campaign: candidate.campaign,
        content: candidate.content,
        term: candidate.term,
        clickIdValue: candidate.clickIdValue
      })
    };

    if (!normalizedCandidate.sessionId) {
      continue;
    }

    const existing = deduped.get(normalizedCandidate.sessionId);
    if (!existing || compareDedupPriority(normalizedCandidate, existing) < 0) {
      deduped.set(normalizedCandidate.sessionId, normalizedCandidate);
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
    case 'shopify_marketing_hint':
      return 0.55;
    case 'ga4_fallback':
      return 0.35;
  }
}

function mapCandidateToResolvedTouchpoint(candidate: TieredAttributionCandidate): ResolvedAttributionTouchpoint {
  return {
    sessionId: candidate.sessionId,
    sourceTouchEventId: candidate.sourceTouchEventId,
    occurredAt: candidate.occurredAtUtc,
    source: candidate.source,
    medium: candidate.medium,
    campaign: candidate.campaign,
    content: candidate.content,
    term: candidate.term,
    clickIdType: candidate.clickIdType,
    clickIdValue: candidate.clickIdValue,
    attributionReason: candidate.attributionReason,
    engagementType: 'click',
    ingestionSource: candidate.ingestionSource,
    isDirect: isCanonicalDirectTouchpoint({
      source: candidate.source,
      medium: candidate.medium,
      campaign: candidate.campaign,
      content: candidate.content,
      term: candidate.term,
      clickIdValue: candidate.clickIdValue
    }),
    isForced: candidate.isSynthetic
  };
}

function isOnOrBeforeOrder(
  orderOccurredAtUtc: Date,
  candidateOccurredAtUtc: Date
): boolean {
  return candidateOccurredAtUtc.getTime() <= orderOccurredAtUtc.getTime();
}

function isWithinLookbackWindow(
  orderOccurredAtUtc: Date,
  candidateOccurredAtUtc: Date,
  lookbackWindowDays = ATTRIBUTION_TIER_LOOKBACK_WINDOW_DAYS
): boolean {
  const lookbackWindowMs = lookbackWindowDays * 24 * 60 * 60 * 1000;
  const deltaMs = orderOccurredAtUtc.getTime() - candidateOccurredAtUtc.getTime();

  return deltaMs >= 0 && deltaMs <= lookbackWindowMs;
}

function compareShopifyHintCandidates(left: TieredAttributionCandidate, right: TieredAttributionCandidate): number {
  if (Boolean(right.clickIdValue) !== Boolean(left.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  if (right.occurredAtUtc.getTime() !== left.occurredAtUtc.getTime()) {
    return right.occurredAtUtc.getTime() - left.occurredAtUtc.getTime();
  }

  return left.sourceKey.localeCompare(right.sourceKey);
}

function qualifiesSyntheticHintCandidate(candidate: TieredAttributionCandidate): boolean {
  return qualifiesSyntheticHintSignal(candidate);
}

function compareGa4FallbackCandidates(left: TieredAttributionCandidate, right: TieredAttributionCandidate): number {
  if (right.occurredAtUtc.getTime() !== left.occurredAtUtc.getTime()) {
    return right.occurredAtUtc.getTime() - left.occurredAtUtc.getTime();
  }

  if (Boolean(right.clickIdValue) !== Boolean(left.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  return left.sourceKey.localeCompare(right.sourceKey);
}

function dedupeTierCandidatesBySourceKey(
  candidates: TieredAttributionCandidate[],
  compare: (left: TieredAttributionCandidate, right: TieredAttributionCandidate) => number
): TieredAttributionCandidate[] {
  const deduped = new Map<string, TieredAttributionCandidate>();

  for (const candidate of candidates) {
    const existing = deduped.get(candidate.sourceKey);
    if (!existing || compare(candidate, existing) < 0) {
      deduped.set(candidate.sourceKey, candidate);
    }
  }

  return Array.from(deduped.values()).sort(compare);
}

function resolveUnattributedReason(input: TieredAttributionResolverInput): string {
  if (!input.orderOccurredAtUtc) {
    return input.normalizationFailures?.find((failure) => failure.scope === 'order')?.reason ?? 'missing_order_timestamp';
  }

  return input.normalizationFailures?.[0]?.reason ?? 'unattributed';
}

export function resolveAttributionTier(input: TieredAttributionResolverInput): ResolvedJourney {
  const orderOccurredAtUtc = input.orderOccurredAtUtc;

  if (!orderOccurredAtUtc) {
    return {
      tier: 'unattributed',
      touchpoints: [],
      winner: null,
      confidenceScore: 0,
      attributionReason: resolveUnattributedReason(input),
      orderOccurredAtUtc: null,
      normalizationFailures: input.normalizationFailures ?? []
    };
  }

  const deterministicTouchpoints = dedupeDeterministicCandidates(
    input.deterministicFirstParty
      .map(mapCandidateToResolvedTouchpoint)
      .filter((candidate) => isOnOrBeforeOrder(orderOccurredAtUtc, candidate.occurredAt))
  );
  const deterministicWinner = selectLastNonDirectWinner(deterministicTouchpoints);

  if (deterministicWinner) {
    return {
      tier: 'deterministic_first_party',
      touchpoints: deterministicTouchpoints,
      winner: deterministicWinner,
      confidenceScore: confidenceScoreForWinner(deterministicWinner),
      attributionReason: deterministicWinner.attributionReason,
      orderOccurredAtUtc,
      normalizationFailures: input.normalizationFailures ?? []
    };
  }

  const shopifyHintTouchpoints = dedupeTierCandidatesBySourceKey(
    input.shopifyHint.filter(
      (candidate) =>
        qualifiesSyntheticHintCandidate(candidate) &&
        isWithinLookbackWindow(orderOccurredAtUtc, candidate.occurredAtUtc)
    ),
    compareShopifyHintCandidates
  );
  const shopifyHintWinnerCandidate = shopifyHintTouchpoints[0] ?? null;
  const shopifyHintWinner = shopifyHintWinnerCandidate ? mapCandidateToResolvedTouchpoint(shopifyHintWinnerCandidate) : null;

  if (shopifyHintWinner) {
    return {
      tier: 'deterministic_shopify_hint',
      touchpoints: shopifyHintTouchpoints.map(mapCandidateToResolvedTouchpoint),
      winner: shopifyHintWinner,
      confidenceScore: shopifyHintWinnerCandidate?.confidenceScore ?? confidenceScoreForWinner(shopifyHintWinner),
      attributionReason: shopifyHintWinner.attributionReason,
      orderOccurredAtUtc,
      normalizationFailures: input.normalizationFailures ?? []
    };
  }

  const ga4FallbackTouchpoints = dedupeTierCandidatesBySourceKey(
    input.ga4Fallback.filter((candidate) => isWithinLookbackWindow(orderOccurredAtUtc, candidate.occurredAtUtc)),
    compareGa4FallbackCandidates
  );
  const ga4FallbackWinnerCandidate = ga4FallbackTouchpoints[0] ?? null;
  const ga4FallbackWinner = ga4FallbackWinnerCandidate ? mapCandidateToResolvedTouchpoint(ga4FallbackWinnerCandidate) : null;

  if (ga4FallbackWinner) {
    return {
      tier: 'ga4_fallback',
      touchpoints: ga4FallbackTouchpoints.map(mapCandidateToResolvedTouchpoint),
      winner: ga4FallbackWinner,
      confidenceScore: ga4FallbackWinnerCandidate?.confidenceScore ?? confidenceScoreForWinner(ga4FallbackWinner),
      attributionReason: ga4FallbackWinner.attributionReason,
      orderOccurredAtUtc,
      normalizationFailures: input.normalizationFailures ?? []
    };
  }

  return {
    tier: 'unattributed',
    touchpoints: [],
    winner: null,
    confidenceScore: 0,
    attributionReason: resolveUnattributedReason(input),
    orderOccurredAtUtc,
    normalizationFailures: input.normalizationFailures ?? []
  };
}
