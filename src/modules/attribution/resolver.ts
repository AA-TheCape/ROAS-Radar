import type { AttributionTouchpoint } from './engine.js';
import { boundConfidenceScore, confidenceScoreForWinner } from './confidence-scoring.js';
import {
  attributionEvidenceSourcePrecedence,
  attributionOriginPrecedence,
  mapResolvedIngestionSourceToAttributionOrigin
} from './precedence.js';
import {
  CLICK_LOOKBACK_WINDOW_DAYS,
  hasClickId,
  isDirectTouchpoint as isCanonicalDirectTouchpoint,
  normalizeAttributionLookbackWindows,
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
  campaignId?: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  accountId?: string | null;
  accountName?: string | null;
  channelType?: string | null;
  channelSubtype?: string | null;
  campaignMetadataSource?: string | null;
  accountMetadataSource?: string | null;
  channelMetadataSource?: string | null;
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
  lookbackWindowDays?: number;
  normalizationFailures?: Array<{
    scope: 'order' | 'shopify_hint' | 'ga4_fallback';
    reason: string;
    sourceKey: string | null;
  }>;
};

function ingestionSourcePrecedence(source: ResolvedIngestionSource): number {
  const originRank =
    1_000 -
    attributionOriginPrecedence(mapResolvedIngestionSourceToAttributionOrigin(source));
  const sourceRank = attributionEvidenceSourcePrecedence(source);

  return originRank * 10 + sourceRank;
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

function mapCandidateToResolvedTouchpoint(candidate: TieredAttributionCandidate): ResolvedAttributionTouchpoint {
  return {
    sessionId: candidate.sessionId,
    sourceTouchEventId: candidate.sourceTouchEventId,
    occurredAt: candidate.occurredAtUtc,
    source: candidate.source,
    medium: candidate.medium,
    campaign: candidate.campaign,
    campaignId: candidate.campaignId,
    content: candidate.content,
    term: candidate.term,
    clickIdType: candidate.clickIdType,
    clickIdValue: candidate.clickIdValue,
    accountId: candidate.accountId,
    accountName: candidate.accountName,
    channelType: candidate.channelType,
    channelSubtype: candidate.channelSubtype,
    campaignMetadataSource: candidate.campaignMetadataSource,
    accountMetadataSource: candidate.accountMetadataSource,
    channelMetadataSource: candidate.channelMetadataSource,
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

  const lookbackWindowDays = normalizeAttributionLookbackWindows({
    clickWindowDays: input.lookbackWindowDays
  }).clickWindowDays;

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
        isWithinLookbackWindow(orderOccurredAtUtc, candidate.occurredAtUtc, lookbackWindowDays)
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
      confidenceScore: confidenceScoreForWinner(shopifyHintWinner, shopifyHintWinnerCandidate?.confidenceScore),
      attributionReason: shopifyHintWinner.attributionReason,
      orderOccurredAtUtc,
      normalizationFailures: input.normalizationFailures ?? []
    };
  }

  const ga4FallbackTouchpoints = dedupeTierCandidatesBySourceKey(
    input.ga4Fallback.filter((candidate) =>
      isWithinLookbackWindow(orderOccurredAtUtc, candidate.occurredAtUtc, lookbackWindowDays)
    ),
    compareGa4FallbackCandidates
  );
  const ga4FallbackWinnerCandidate = ga4FallbackTouchpoints[0] ?? null;
  const ga4FallbackWinner = ga4FallbackWinnerCandidate ? mapCandidateToResolvedTouchpoint(ga4FallbackWinnerCandidate) : null;

  if (ga4FallbackWinner) {
    return {
      tier: 'ga4_fallback',
      touchpoints: ga4FallbackTouchpoints.map(mapCandidateToResolvedTouchpoint),
      winner: ga4FallbackWinner,
      confidenceScore: confidenceScoreForWinner(ga4FallbackWinner, ga4FallbackWinnerCandidate?.confidenceScore),
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

export { boundConfidenceScore, confidenceScoreForWinner };
