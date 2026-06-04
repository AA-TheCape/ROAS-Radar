import type {
  AttributionEngagementType,
  AttributionEvidenceSource,
  AttributionTouchpoint
} from './engine.js';
import {
  inferEngagementType,
  isDirectTouchpoint,
  isWithinLookbackWindow,
  normalizeAttributionLookbackWindows,
  qualifiesSyntheticHintSignal,
  type AttributionLookbackWindows
} from './rules.js';

export const CANONICAL_ATTRIBUTION_TIERS = [
  'strict',
  'probabilistic_assisted_deterministic',
  'blended_deterministic_reporting'
] as const;

export type CanonicalAttributionTier = (typeof CANONICAL_ATTRIBUTION_TIERS)[number];
export type CanonicalAttributionTierStatus = 'attributed' | 'no_eligible_touches';

export type CanonicalAttributionTierTouchpoint = AttributionTouchpoint & {
  touchpointId: string;
  evidenceSource: AttributionEvidenceSource;
  engagementType: AttributionEngagementType;
  isDirect: boolean;
  isSynthetic: boolean;
  tierSourceClass: 'strict_deterministic' | 'assisted_deterministic';
  creditWeight: number;
};

export type CanonicalAttributionTierOutput = {
  tier: CanonicalAttributionTier;
  allocationStatus: CanonicalAttributionTierStatus;
  winnerTouchpointId: string | null;
  winnerSessionId: string | null;
  touchpoints: CanonicalAttributionTierTouchpoint[];
  lookbackWindows: AttributionLookbackWindows;
  winnerSelectionRule: 'last_non_direct';
};

export type CanonicalAttributionTierOutputs = Record<CanonicalAttributionTier, CanonicalAttributionTierOutput>;

export type CanonicalAttributionTierInput = {
  orderOccurredAt: Date;
  touchpoints: AttributionTouchpoint[];
  lookbackWindows?: Partial<AttributionLookbackWindows>;
};

const EVIDENCE_SOURCE_PRECEDENCE: Record<AttributionEvidenceSource, number> = {
  landing_session_id: 0,
  checkout_token: 1,
  cart_token: 2,
  customer_identity: 3,
  shopify_marketing_hint: 4,
  ga4_fallback: 5
};

const STRICT_EVIDENCE_SOURCES = new Set<AttributionEvidenceSource>([
  'landing_session_id',
  'checkout_token',
  'cart_token',
  'customer_identity'
]);

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function inferEvidenceSource(touchpoint: AttributionTouchpoint): AttributionEvidenceSource {
  const rawEvidenceSource = touchpoint.evidenceSource ?? touchpoint.ingestionSource ?? null;

  switch (rawEvidenceSource) {
    case 'landing_session_id':
    case 'checkout_token':
    case 'cart_token':
    case 'customer_identity':
    case 'shopify_marketing_hint':
    case 'ga4_fallback':
      return rawEvidenceSource;
    default:
      return touchpoint.isForced ? 'shopify_marketing_hint' : 'customer_identity';
  }
}

function stableTouchpointId(touchpoint: AttributionTouchpoint, fallbackIndex: number): string {
  return (
    normalizeNullableString(touchpoint.touchpointId) ??
    normalizeNullableString(touchpoint.sourceTouchEventId) ??
    (normalizeNullableString(touchpoint.sessionId)
      ? `session:${touchpoint.sessionId}:${touchpoint.occurredAt.toISOString()}:${fallbackIndex}`
      : `touchpoint:${touchpoint.occurredAt.toISOString()}:${fallbackIndex}`)
  );
}

function compareLexical(left: string | null | undefined, right: string | null | undefined): number {
  return (left ?? '').localeCompare(right ?? '');
}

function compareTierWinner(
  left: CanonicalAttributionTierTouchpoint,
  right: CanonicalAttributionTierTouchpoint
): number {
  if (left.tierSourceClass !== right.tierSourceClass) {
    return left.tierSourceClass === 'strict_deterministic' ? -1 : 1;
  }

  const occurredAtComparison = right.occurredAt.getTime() - left.occurredAt.getTime();
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const evidenceComparison = EVIDENCE_SOURCE_PRECEDENCE[left.evidenceSource] - EVIDENCE_SOURCE_PRECEDENCE[right.evidenceSource];
  if (evidenceComparison !== 0) {
    return evidenceComparison;
  }

  if (Boolean(left.clickIdValue) !== Boolean(right.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  return compareLexical(left.touchpointId, right.touchpointId);
}

function compareTierTimeline(
  left: CanonicalAttributionTierTouchpoint,
  right: CanonicalAttributionTierTouchpoint
): number {
  const occurredAtComparison = left.occurredAt.getTime() - right.occurredAt.getTime();
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  return compareTierWinner(left, right);
}

function normalizeTouchpoints(input: CanonicalAttributionTierInput): CanonicalAttributionTierTouchpoint[] {
  const lookbackWindows = normalizeAttributionLookbackWindows(input.lookbackWindows);

  return input.touchpoints
    .map((touchpoint, index): CanonicalAttributionTierTouchpoint => {
      const evidenceSource = inferEvidenceSource(touchpoint);
      const isStrict = STRICT_EVIDENCE_SOURCES.has(evidenceSource);
      const engagementType = inferEngagementType({
        engagementType: touchpoint.engagementType,
        clickIdValue: touchpoint.clickIdValue
      });

      return {
        ...touchpoint,
        touchpointId: stableTouchpointId(touchpoint, index),
        evidenceSource,
        engagementType,
        isDirect: isDirectTouchpoint({
          source: touchpoint.source,
          medium: touchpoint.medium,
          campaign: touchpoint.campaign,
          content: touchpoint.content,
          term: touchpoint.term,
          clickIdValue: touchpoint.clickIdValue
        }),
        isSynthetic: Boolean(touchpoint.isSynthetic ?? touchpoint.isForced),
        tierSourceClass: isStrict ? 'strict_deterministic' : 'assisted_deterministic',
        creditWeight: 0
      };
    })
    .filter((touchpoint) => Number.isFinite(touchpoint.occurredAt.getTime()))
    .filter((touchpoint) =>
      isWithinLookbackWindow(input.orderOccurredAt, touchpoint.occurredAt, touchpoint.engagementType, lookbackWindows)
    )
    .filter((touchpoint) => {
      if (touchpoint.evidenceSource !== 'shopify_marketing_hint') {
        return true;
      }

      return touchpoint.isSynthetic && qualifiesSyntheticHintSignal(touchpoint);
    })
    .sort(compareTierTimeline);
}

function selectLastNonDirectWinner(
  touchpoints: CanonicalAttributionTierTouchpoint[]
): CanonicalAttributionTierTouchpoint | null {
  const nonDirectTouchpoints = touchpoints.filter((touchpoint) => !touchpoint.isDirect);
  const selectionPool = nonDirectTouchpoints.length > 0 ? nonDirectTouchpoints : touchpoints;

  return selectionPool.slice().sort(compareTierWinner)[0] ?? null;
}

function buildOutput(
  tier: CanonicalAttributionTier,
  touchpoints: CanonicalAttributionTierTouchpoint[],
  lookbackWindows: AttributionLookbackWindows,
  creditMode: 'winner' | 'linear'
): CanonicalAttributionTierOutput {
  const winner = selectLastNonDirectWinner(touchpoints);
  const weightedTouchpoints = touchpoints.map((touchpoint) => ({
    ...touchpoint,
    creditWeight:
      creditMode === 'linear'
        ? touchpoints.length > 0
          ? Number((1 / touchpoints.length).toFixed(8))
          : 0
        : winner?.touchpointId === touchpoint.touchpointId
          ? 1
          : 0
  }));

  return {
    tier,
    allocationStatus: winner ? 'attributed' : 'no_eligible_touches',
    winnerTouchpointId: winner?.touchpointId ?? null,
    winnerSessionId: winner?.sessionId ?? null,
    touchpoints: weightedTouchpoints,
    lookbackWindows,
    winnerSelectionRule: 'last_non_direct'
  };
}

export function computeCanonicalAttributionTiers(input: CanonicalAttributionTierInput): CanonicalAttributionTierOutputs {
  if (!(input.orderOccurredAt instanceof Date) || Number.isNaN(input.orderOccurredAt.getTime())) {
    throw new Error('orderOccurredAt must be a valid Date');
  }

  const lookbackWindows = normalizeAttributionLookbackWindows(input.lookbackWindows);
  const eligibleTouchpoints = normalizeTouchpoints(input);
  const strictTouchpoints = eligibleTouchpoints.filter((touchpoint) => touchpoint.tierSourceClass === 'strict_deterministic');
  const assistedTouchpoints = eligibleTouchpoints.filter((touchpoint) => touchpoint.evidenceSource !== 'ga4_fallback');

  return {
    strict: buildOutput('strict', strictTouchpoints, lookbackWindows, 'winner'),
    probabilistic_assisted_deterministic: buildOutput(
      'probabilistic_assisted_deterministic',
      assistedTouchpoints,
      lookbackWindows,
      'winner'
    ),
    blended_deterministic_reporting: buildOutput(
      'blended_deterministic_reporting',
      eligibleTouchpoints,
      lookbackWindows,
      'linear'
    )
  };
}
