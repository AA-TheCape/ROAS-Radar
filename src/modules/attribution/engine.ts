export const ATTRIBUTION_MODELS = [
  'first_touch',
  'last_touch',
  'last_non_direct',
  'linear',
  'clicks_only',
  'hinted_fallback_only'
] as const;

export type AttributionModel = (typeof ATTRIBUTION_MODELS)[number];
export type AttributionEvidenceSource =
  | 'landing_session_id'
  | 'checkout_token'
  | 'cart_token'
  | 'customer_identity'
  | 'shopify_marketing_hint'
  | 'ga4_fallback';
export type AttributionEngagementType = 'click' | 'view' | 'unknown';
export type AttributionAllocationStatus =
  | 'attributed'
  | 'no_eligible_touches'
  | 'blocked_by_deterministic'
  | 'unattributed';
export type AttributionLookbackRule = '28d_click' | '7d_view' | 'mixed';

export type AttributionTouchpoint = {
  touchpointId?: string | null;
  sessionId: string | null;
  occurredAt: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  isDirect: boolean;
  isForced: boolean;
  sourceTouchEventId?: string | null;
  ingestionSource?: string | null;
  evidenceSource?: AttributionEvidenceSource | null;
  engagementType?: AttributionEngagementType | null;
  isSynthetic?: boolean | null;
};

export type AttributionEngineOptions = {
  orderOccurredAt: Date;
  orderRevenue: number | string;
  attributionModels?: readonly AttributionModel[];
  normalizationFailuresCount?: number;
};

export type AttributionCredit = {
  attributionModel: AttributionModel;
  touchpointId: string | null;
  touchpointPosition: number;
  sessionId: string | null;
  touchpointOccurredAt: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  evidenceSource: AttributionEvidenceSource | null;
  engagementType: AttributionEngagementType;
  isDirect: boolean;
  isSynthetic: boolean;
  creditWeight: number;
  revenueCredit: string;
  isPrimary: boolean;
};

export type AttributionModelSummary = {
  attributionModel: AttributionModel;
  allocationStatus: AttributionAllocationStatus;
  winnerTouchpointId: string | null;
  winnerSessionId: string | null;
  winnerEvidenceSource: AttributionEvidenceSource | null;
  winnerAttributionReason: string | null;
  totalCreditWeight: number;
  totalRevenueCredited: string;
  touchpointCountConsidered: number;
  eligibleClickCount: number;
  eligibleViewCount: number;
  lookbackRuleApplied: AttributionLookbackRule;
  winnerSelectionRule: AttributionModel;
  directSuppressionApplied: boolean;
  deterministicBlockApplied: boolean;
  normalizationFailuresCount: number;
};

export type AttributionModelOutputs = Record<AttributionModel, AttributionCredit[]>;
export type AttributionModelSummaries = Record<AttributionModel, AttributionModelSummary>;

export type AttributionExecutionResult = {
  models: AttributionModel[];
  creditsByModel: AttributionModelOutputs;
  summariesByModel: AttributionModelSummaries;
};

type NormalizedTouchpoint = AttributionTouchpoint & {
  touchpointId: string;
  evidenceSource: AttributionEvidenceSource;
  engagementType: AttributionEngagementType;
  isSynthetic: boolean;
};

type StrategyContext = {
  eligibleTouchpoints: NormalizedTouchpoint[];
  eligibleClicks: NormalizedTouchpoint[];
  eligibleViews: NormalizedTouchpoint[];
  deterministicTouchpoints: NormalizedTouchpoint[];
  hintedFallbackCandidates: NormalizedTouchpoint[];
  orderRevenue: number | string;
  normalizationFailuresCount: number;
};

type StrategyResult = {
  touchpoints: NormalizedTouchpoint[];
  weights: number[];
  allocationStatus: AttributionAllocationStatus;
  directSuppressionApplied: boolean;
  deterministicBlockApplied: boolean;
  lookbackRuleApplied: AttributionLookbackRule;
};

const CLICK_LOOKBACK_WINDOW_MS = 28 * 24 * 60 * 60 * 1000;
const VIEW_LOOKBACK_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

const EVIDENCE_SOURCE_PRECEDENCE: Record<AttributionEvidenceSource, number> = {
  landing_session_id: 0,
  checkout_token: 1,
  cart_token: 2,
  customer_identity: 3,
  shopify_marketing_hint: 4,
  ga4_fallback: 5
};

const FIRST_PARTY_EVIDENCE_SOURCES = new Set<AttributionEvidenceSource>([
  'landing_session_id',
  'checkout_token',
  'cart_token',
  'customer_identity'
]);

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function revenueToCents(value: number | string): number {
  const numericValue = typeof value === 'number' ? value : Number.parseFloat(value);

  if (!Number.isFinite(numericValue) || numericValue < 0) {
    throw new Error(`orderRevenue must be a finite non-negative number, received ${String(value)}`);
  }

  return Math.round(numericValue * 100);
}

function centsToRevenue(cents: number): string {
  return (cents / 100).toFixed(2);
}

function normalizeWeights(rawWeights: number[]): number[] {
  const positiveWeights = rawWeights.map((value) => (Number.isFinite(value) && value > 0 ? value : 0));
  const totalWeight = positiveWeights.reduce((sum, value) => sum + value, 0);

  if (totalWeight <= 0) {
    return positiveWeights.map(() => 0);
  }

  return positiveWeights.map((value) => value / totalWeight);
}

function allocateRevenueAcrossWeights(totalCents: number, normalizedWeights: number[]): number[] {
  if (normalizedWeights.length === 0) {
    return [];
  }

  const provisional = normalizedWeights.map((weight, index) => {
    const exactCents = totalCents * weight;
    const wholeCents = Math.floor(exactCents);

    return {
      index,
      wholeCents,
      remainder: exactCents - wholeCents
    };
  });

  let distributedCents = provisional.reduce((sum, entry) => sum + entry.wholeCents, 0);
  const remainingCents = totalCents - distributedCents;

  const entriesByRemainder = provisional
    .slice()
    .sort((left, right) => {
      if (right.remainder !== left.remainder) {
        return right.remainder - left.remainder;
      }

      return left.index - right.index;
    })
    .slice(0, remainingCents);

  for (const entry of entriesByRemainder) {
    provisional[entry.index].wholeCents += 1;
    distributedCents += 1;
  }

  if (distributedCents !== totalCents) {
    throw new Error('Revenue allocation failed to conserve cents');
  }

  return provisional.map((entry) => entry.wholeCents);
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

function inferEngagementType(touchpoint: AttributionTouchpoint): AttributionEngagementType {
  if (touchpoint.engagementType === 'click' || touchpoint.engagementType === 'view') {
    return touchpoint.engagementType;
  }

  if (touchpoint.engagementType === 'unknown') {
    return 'unknown';
  }

  return touchpoint.clickIdValue ? 'click' : 'click';
}

function stableTouchpointId(touchpoint: AttributionTouchpoint, fallbackIndex: number): string {
  const explicitId = normalizeNullableString(touchpoint.touchpointId);
  if (explicitId) {
    return explicitId;
  }

  const eventId = normalizeNullableString(touchpoint.sourceTouchEventId);
  if (eventId) {
    return eventId;
  }

  const sessionId = normalizeNullableString(touchpoint.sessionId);
  if (sessionId) {
    return `session:${sessionId}:${touchpoint.occurredAt.toISOString()}:${fallbackIndex}`;
  }

  return `touchpoint:${touchpoint.occurredAt.toISOString()}:${fallbackIndex}`;
}

function compareLexical(left: string | null | undefined, right: string | null | undefined): number {
  return (left ?? '').localeCompare(right ?? '');
}

function compareTimelineOrder(left: NormalizedTouchpoint, right: NormalizedTouchpoint): number {
  const occurredAtComparison = left.occurredAt.getTime() - right.occurredAt.getTime();
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const evidenceComparison = EVIDENCE_SOURCE_PRECEDENCE[left.evidenceSource] - EVIDENCE_SOURCE_PRECEDENCE[right.evidenceSource];
  if (evidenceComparison !== 0) {
    return evidenceComparison;
  }

  if (left.engagementType !== right.engagementType) {
    return left.engagementType === 'click' ? -1 : 1;
  }

  if (Boolean(left.clickIdValue) !== Boolean(right.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  return compareLexical(left.touchpointId, right.touchpointId);
}

function compareLastTouchWinner(left: NormalizedTouchpoint, right: NormalizedTouchpoint): number {
  const occurredAtComparison = right.occurredAt.getTime() - left.occurredAt.getTime();
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const evidenceComparison = EVIDENCE_SOURCE_PRECEDENCE[left.evidenceSource] - EVIDENCE_SOURCE_PRECEDENCE[right.evidenceSource];
  if (evidenceComparison !== 0) {
    return evidenceComparison;
  }

  if (left.engagementType !== right.engagementType) {
    return left.engagementType === 'click' ? -1 : 1;
  }

  if (Boolean(left.clickIdValue) !== Boolean(right.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  return compareLexical(left.touchpointId, right.touchpointId);
}

function compareFirstTouchWinner(left: NormalizedTouchpoint, right: NormalizedTouchpoint): number {
  const occurredAtComparison = left.occurredAt.getTime() - right.occurredAt.getTime();
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const evidenceComparison = EVIDENCE_SOURCE_PRECEDENCE[left.evidenceSource] - EVIDENCE_SOURCE_PRECEDENCE[right.evidenceSource];
  if (evidenceComparison !== 0) {
    return evidenceComparison;
  }

  if (left.engagementType !== right.engagementType) {
    return left.engagementType === 'click' ? -1 : 1;
  }

  if (Boolean(left.clickIdValue) !== Boolean(right.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  return compareLexical(left.touchpointId, right.touchpointId);
}

function isWithinLookbackWindow(
  orderOccurredAt: Date,
  touchpoint: Pick<NormalizedTouchpoint, 'occurredAt' | 'engagementType'>
): boolean {
  const deltaMs = orderOccurredAt.getTime() - touchpoint.occurredAt.getTime();
  if (deltaMs < 0) {
    return false;
  }

  if (touchpoint.engagementType === 'click') {
    return deltaMs <= CLICK_LOOKBACK_WINDOW_MS;
  }

  if (touchpoint.engagementType === 'view') {
    return deltaMs <= VIEW_LOOKBACK_WINDOW_MS;
  }

  return false;
}

function qualifiesSyntheticHint(touchpoint: NormalizedTouchpoint): boolean {
  if (touchpoint.evidenceSource !== 'shopify_marketing_hint' || !touchpoint.isSynthetic) {
    return false;
  }

  if (touchpoint.clickIdValue) {
    return true;
  }

  if (touchpoint.source && touchpoint.medium) {
    return true;
  }

  if (touchpoint.source && touchpoint.campaign) {
    return true;
  }

  return false;
}

function normalizeTouchpoints(rawTouchpoints: AttributionTouchpoint[], orderOccurredAt: Date): NormalizedTouchpoint[] {
  return rawTouchpoints
    .map((touchpoint, index) => ({
      ...touchpoint,
      touchpointId: stableTouchpointId(touchpoint, index),
      evidenceSource: inferEvidenceSource(touchpoint),
      engagementType: inferEngagementType(touchpoint),
      isSynthetic: Boolean(touchpoint.isSynthetic ?? touchpoint.isForced)
    }))
    .filter((touchpoint) => Number.isFinite(touchpoint.occurredAt.getTime()))
    .filter((touchpoint) => isWithinLookbackWindow(orderOccurredAt, touchpoint))
    .sort(compareTimelineOrder);
}

function buildStrategyContext(rawTouchpoints: AttributionTouchpoint[], options: AttributionEngineOptions): StrategyContext {
  const eligibleTouchpoints = normalizeTouchpoints(rawTouchpoints, options.orderOccurredAt);
  const eligibleClicks = eligibleTouchpoints.filter((touchpoint) => touchpoint.engagementType === 'click');
  const eligibleViews = eligibleTouchpoints.filter((touchpoint) => touchpoint.engagementType === 'view');
  const deterministicTouchpoints = eligibleTouchpoints.filter((touchpoint) =>
    FIRST_PARTY_EVIDENCE_SOURCES.has(touchpoint.evidenceSource)
  );
  const hintedFallbackCandidates = eligibleTouchpoints.filter(qualifiesSyntheticHint);

  return {
    eligibleTouchpoints,
    eligibleClicks,
    eligibleViews,
    deterministicTouchpoints,
    hintedFallbackCandidates,
    orderRevenue: options.orderRevenue,
    normalizationFailuresCount: Math.max(0, Math.trunc(options.normalizationFailuresCount ?? 0))
  };
}

function buildZeroWeights(length: number): number[] {
  return Array.from({ length }, () => 0);
}

function buildWinnerWeights(touchpoints: NormalizedTouchpoint[], winnerId: string | null): number[] {
  return touchpoints.map((touchpoint) => (winnerId && touchpoint.touchpointId === winnerId ? 1 : 0));
}

function pickWinner(
  touchpoints: NormalizedTouchpoint[],
  comparator: (left: NormalizedTouchpoint, right: NormalizedTouchpoint) => number
): NormalizedTouchpoint | null {
  return touchpoints.slice().sort(comparator)[0] ?? null;
}

function resolveLookbackRule(clickCount: number, viewCount: number): AttributionLookbackRule {
  if (clickCount > 0 && viewCount > 0) {
    return 'mixed';
  }

  if (viewCount > 0) {
    return '7d_view';
  }

  return '28d_click';
}

function buildCredits(
  attributionModel: AttributionModel,
  touchpoints: NormalizedTouchpoint[],
  normalizedWeights: number[],
  totalRevenue: number | string
): AttributionCredit[] {
  const totalCents = revenueToCents(totalRevenue);
  const hasAttributedWeight = normalizedWeights.some((weight) => weight > 0);
  const creditedCents = hasAttributedWeight
    ? allocateRevenueAcrossWeights(totalCents, normalizedWeights)
    : normalizedWeights.map(() => 0);
  const highestCreditCents = creditedCents.reduce((max, value) => Math.max(max, value), 0);
  const primaryTouchpointIndex = highestCreditCents > 0 ? creditedCents.findIndex((value) => value === highestCreditCents) : -1;

  return touchpoints.map((touchpoint, index) => ({
    attributionModel,
    touchpointId: touchpoint.touchpointId,
    touchpointPosition: index,
    sessionId: touchpoint.sessionId,
    touchpointOccurredAt: touchpoint.occurredAt,
    source: touchpoint.source,
    medium: touchpoint.medium,
    campaign: touchpoint.campaign,
    content: touchpoint.content,
    term: touchpoint.term,
    clickIdType: touchpoint.clickIdType,
    clickIdValue: touchpoint.clickIdValue,
    attributionReason: touchpoint.attributionReason,
    evidenceSource: touchpoint.evidenceSource,
    engagementType: touchpoint.engagementType,
    isDirect: touchpoint.isDirect,
    isSynthetic: touchpoint.isSynthetic,
    creditWeight: normalizedWeights[index] ?? 0,
    revenueCredit: centsToRevenue(creditedCents[index] ?? 0),
    isPrimary: index === primaryTouchpointIndex
  }));
}

type AttributionStrategy = (context: StrategyContext) => StrategyResult;

const attributionStrategies: Record<AttributionModel, AttributionStrategy> = {
  first_touch(context) {
    const winner = pickWinner(context.eligibleTouchpoints, compareFirstTouchWinner);

    return {
      touchpoints: context.eligibleTouchpoints,
      weights: buildWinnerWeights(context.eligibleTouchpoints, winner?.touchpointId ?? null),
      allocationStatus: winner ? 'attributed' : 'no_eligible_touches',
      directSuppressionApplied: false,
      deterministicBlockApplied: false,
      lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length)
    };
  },
  last_touch(context) {
    const winner = pickWinner(context.eligibleTouchpoints, compareLastTouchWinner);

    return {
      touchpoints: context.eligibleTouchpoints,
      weights: buildWinnerWeights(context.eligibleTouchpoints, winner?.touchpointId ?? null),
      allocationStatus: winner ? 'attributed' : 'no_eligible_touches',
      directSuppressionApplied: false,
      deterministicBlockApplied: false,
      lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length)
    };
  },
  last_non_direct(context) {
    const nonDirectTouchpoints = context.eligibleTouchpoints.filter((touchpoint) => !touchpoint.isDirect);
    const winnerPool = nonDirectTouchpoints.length > 0 ? nonDirectTouchpoints : context.eligibleTouchpoints;
    const winner = pickWinner(winnerPool, compareLastTouchWinner);

    return {
      touchpoints: context.eligibleTouchpoints,
      weights: buildWinnerWeights(context.eligibleTouchpoints, winner?.touchpointId ?? null),
      allocationStatus: winner ? 'attributed' : 'no_eligible_touches',
      directSuppressionApplied: nonDirectTouchpoints.length > 0,
      deterministicBlockApplied: false,
      lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length)
    };
  },
  linear(context) {
    return {
      touchpoints: context.eligibleTouchpoints,
      weights: context.eligibleTouchpoints.map(() => 1),
      allocationStatus: context.eligibleTouchpoints.length > 0 ? 'attributed' : 'no_eligible_touches',
      directSuppressionApplied: false,
      deterministicBlockApplied: false,
      lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length)
    };
  },
  clicks_only(context) {
    const nonDirectClicks = context.eligibleClicks.filter((touchpoint) => !touchpoint.isDirect);
    const winnerPool = nonDirectClicks.length > 0 ? nonDirectClicks : context.eligibleClicks;
    const winner = pickWinner(winnerPool, compareLastTouchWinner);

    return {
      touchpoints: context.eligibleTouchpoints,
      weights: buildWinnerWeights(context.eligibleTouchpoints, winner?.touchpointId ?? null),
      allocationStatus: winner ? 'attributed' : 'no_eligible_touches',
      directSuppressionApplied: nonDirectClicks.length > 0,
      deterministicBlockApplied: false,
      lookbackRuleApplied: '28d_click'
    };
  },
  hinted_fallback_only(context) {
    if (context.deterministicTouchpoints.length > 0) {
      return {
        touchpoints: context.eligibleTouchpoints,
        weights: buildZeroWeights(context.eligibleTouchpoints.length),
        allocationStatus: 'blocked_by_deterministic',
        directSuppressionApplied: false,
        deterministicBlockApplied: true,
        lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length)
      };
    }

    const winner = pickWinner(context.hintedFallbackCandidates, compareLastTouchWinner);

    return {
      touchpoints: context.eligibleTouchpoints,
      weights: buildWinnerWeights(context.eligibleTouchpoints, winner?.touchpointId ?? null),
      allocationStatus: winner ? 'attributed' : 'unattributed',
      directSuppressionApplied: false,
      deterministicBlockApplied: false,
      lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length)
    };
  }
};

function validateRequestedModels(models: readonly AttributionModel[] | undefined): AttributionModel[] {
  if (!models || models.length === 0) {
    return [...ATTRIBUTION_MODELS];
  }

  const uniqueModels = Array.from(new Set(models));
  for (const model of uniqueModels) {
    if (!ATTRIBUTION_MODELS.includes(model)) {
      throw new Error(`Unsupported attribution model: ${model}`);
    }
  }

  return uniqueModels;
}

export function executeAttributionModels(
  rawTouchpoints: AttributionTouchpoint[],
  options: AttributionEngineOptions
): AttributionExecutionResult {
  if (!(options.orderOccurredAt instanceof Date) || Number.isNaN(options.orderOccurredAt.getTime())) {
    throw new Error('orderOccurredAt must be a valid Date');
  }

  const models = validateRequestedModels(options.attributionModels);
  const context = buildStrategyContext(rawTouchpoints, options);
  const creditsByModel = {} as AttributionModelOutputs;
  const summariesByModel = {} as AttributionModelSummaries;

  for (const model of ATTRIBUTION_MODELS) {
    creditsByModel[model] = [];
    summariesByModel[model] = {
      attributionModel: model,
      allocationStatus: 'unattributed',
      winnerTouchpointId: null,
      winnerSessionId: null,
      winnerEvidenceSource: null,
      winnerAttributionReason: null,
      totalCreditWeight: 0,
      totalRevenueCredited: '0.00',
      touchpointCountConsidered: 0,
      eligibleClickCount: context.eligibleClicks.length,
      eligibleViewCount: context.eligibleViews.length,
      lookbackRuleApplied: resolveLookbackRule(context.eligibleClicks.length, context.eligibleViews.length),
      winnerSelectionRule: model,
      directSuppressionApplied: false,
      deterministicBlockApplied: false,
      normalizationFailuresCount: context.normalizationFailuresCount
    };
  }

  for (const model of models) {
    const strategyResult = attributionStrategies[model](context);
    const normalizedWeights = normalizeWeights(strategyResult.weights);
    const credits = buildCredits(model, strategyResult.touchpoints, normalizedWeights, context.orderRevenue);
    const winner = credits.find((credit) => credit.isPrimary) ?? null;
    const totalRevenueCredited = credits.reduce((sum, credit) => sum + Number.parseFloat(credit.revenueCredit), 0);
    const totalCreditWeight = credits.reduce((sum, credit) => sum + credit.creditWeight, 0);
    const touchpointCountConsidered =
      model === 'clicks_only'
        ? context.eligibleClicks.length
        : model === 'hinted_fallback_only'
          ? context.hintedFallbackCandidates.length
          : context.eligibleTouchpoints.length;

    creditsByModel[model] = credits;
    summariesByModel[model] = {
      attributionModel: model,
      allocationStatus: strategyResult.allocationStatus,
      winnerTouchpointId: winner?.touchpointId ?? null,
      winnerSessionId: winner?.sessionId ?? null,
      winnerEvidenceSource: winner?.evidenceSource ?? null,
      winnerAttributionReason: winner?.attributionReason ?? null,
      totalCreditWeight: Number(totalCreditWeight.toFixed(8)),
      totalRevenueCredited: centsToRevenue(Math.round(totalRevenueCredited * 100)),
      touchpointCountConsidered,
      eligibleClickCount: context.eligibleClicks.length,
      eligibleViewCount: context.eligibleViews.length,
      lookbackRuleApplied: strategyResult.lookbackRuleApplied,
      winnerSelectionRule: model,
      directSuppressionApplied: strategyResult.directSuppressionApplied,
      deterministicBlockApplied: strategyResult.deterministicBlockApplied,
      normalizationFailuresCount: context.normalizationFailuresCount
    };
  }

  return {
    models,
    creditsByModel,
    summariesByModel
  };
}

export function computeAttributionOutputs(
  rawTouchpoints: AttributionTouchpoint[],
  options: AttributionEngineOptions
): AttributionModelOutputs {
  return executeAttributionModels(rawTouchpoints, options).creditsByModel;
}
