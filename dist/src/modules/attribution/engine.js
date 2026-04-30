export const ATTRIBUTION_MODELS = [
    'first_touch',
    'last_touch',
    'last_non_direct',
    'linear',
    'clicks_only',
    'hinted_fallback_only'
];
const CLICK_LOOKBACK_WINDOW_MS = 28 * 24 * 60 * 60 * 1000;
const VIEW_LOOKBACK_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
const EVIDENCE_SOURCE_PRECEDENCE = {
    landing_session_id: 0,
    checkout_token: 1,
    cart_token: 2,
    customer_identity: 3,
    shopify_marketing_hint: 4,
    ga4_fallback: 5
};
const FIRST_PARTY_EVIDENCE_SOURCES = new Set([
    'landing_session_id',
    'checkout_token',
    'cart_token',
    'customer_identity'
]);
function normalizeNullableString(value) {
    const normalized = value?.trim();
    return normalized ? normalized : null;
}
function revenueToCents(value) {
    const numericValue = typeof value === 'number' ? value : Number.parseFloat(value);
    if (!Number.isFinite(numericValue) || numericValue < 0) {
        throw new Error(`orderRevenue must be a finite non-negative number, received ${String(value)}`);
    }
    return Math.round(numericValue * 100);
}
function centsToRevenue(cents) {
    return (cents / 100).toFixed(2);
}
function normalizeWeights(rawWeights) {
    const positiveWeights = rawWeights.map((value) => (Number.isFinite(value) && value > 0 ? value : 0));
    const totalWeight = positiveWeights.reduce((sum, value) => sum + value, 0);
    if (totalWeight <= 0) {
        return positiveWeights.map(() => 0);
    }
    return positiveWeights.map((value) => value / totalWeight);
}
function allocateRevenueAcrossWeights(totalCents, normalizedWeights) {
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
function inferEvidenceSource(touchpoint) {
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
function inferEngagementType(touchpoint) {
    if (touchpoint.engagementType === 'click' || touchpoint.engagementType === 'view') {
        return touchpoint.engagementType;
    }
    if (touchpoint.engagementType === 'unknown') {
        return 'unknown';
    }
    return touchpoint.clickIdValue ? 'click' : 'click';
}
function stableTouchpointId(touchpoint, fallbackIndex) {
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
function compareLexical(left, right) {
    return (left ?? '').localeCompare(right ?? '');
}
function compareTimelineOrder(left, right) {
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
function compareLastTouchWinner(left, right) {
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
function compareFirstTouchWinner(left, right) {
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
function isWithinLookbackWindow(orderOccurredAt, touchpoint) {
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
function qualifiesSyntheticHint(touchpoint) {
    if (touchpoint.evidenceSource !== 'shopify_marketing_hint' || !touchpoint.isSynthetic) {
        return false;
    }
    if (touchpoint.clickIdType && touchpoint.clickIdValue) {
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
function normalizeTouchpoints(rawTouchpoints, orderOccurredAt) {
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
function buildStrategyContext(rawTouchpoints, options) {
    const eligibleTouchpoints = normalizeTouchpoints(rawTouchpoints, options.orderOccurredAt);
    const eligibleClicks = eligibleTouchpoints.filter((touchpoint) => touchpoint.engagementType === 'click');
    const eligibleViews = eligibleTouchpoints.filter((touchpoint) => touchpoint.engagementType === 'view');
    const deterministicTouchpoints = eligibleTouchpoints.filter((touchpoint) => FIRST_PARTY_EVIDENCE_SOURCES.has(touchpoint.evidenceSource));
    const deterministicClicks = deterministicTouchpoints.filter((touchpoint) => touchpoint.engagementType === 'click');
    const hintedFallbackCandidates = eligibleTouchpoints.filter(qualifiesSyntheticHint);
    return {
        eligibleTouchpoints,
        eligibleClicks,
        eligibleViews,
        deterministicTouchpoints,
        deterministicClicks,
        hintedFallbackCandidates,
        orderRevenue: options.orderRevenue,
        normalizationFailuresCount: Math.max(0, Math.trunc(options.normalizationFailuresCount ?? 0))
    };
}
function buildZeroWeights(length) {
    return Array.from({ length }, () => 0);
}
function buildWinnerWeights(touchpoints, winnerId) {
    return touchpoints.map((touchpoint) => (winnerId && touchpoint.touchpointId === winnerId ? 1 : 0));
}
function pickWinner(touchpoints, comparator) {
    return touchpoints.slice().sort(comparator)[0] ?? null;
}
function resolveLookbackRule(clickCount, viewCount) {
    if (clickCount > 0 && viewCount > 0) {
        return 'mixed';
    }
    if (viewCount > 0) {
        return '7d_view';
    }
    return '28d_click';
}
function summarizePool(touchpoints) {
    const eligibleClickCount = touchpoints.filter((touchpoint) => touchpoint.engagementType === 'click').length;
    const eligibleViewCount = touchpoints.filter((touchpoint) => touchpoint.engagementType === 'view').length;
    return {
        touchpointCountConsidered: touchpoints.length,
        eligibleClickCount,
        eligibleViewCount,
        lookbackRuleApplied: resolveLookbackRule(eligibleClickCount, eligibleViewCount)
    };
}
function buildCoreModelResult(touchpoints, winner, options = {}) {
    return {
        touchpoints,
        weights: buildWinnerWeights(touchpoints, winner?.touchpointId ?? null),
        allocationStatus: winner ? 'attributed' : 'no_eligible_touches',
        directSuppressionApplied: options.directSuppressionApplied ?? false,
        deterministicBlockApplied: false,
        lookbackRuleApplied: options.lookbackRuleApplied ??
            summarizePool(touchpoints).lookbackRuleApplied
    };
}
function buildCredits(attributionModel, touchpoints, normalizedWeights, totalRevenue) {
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
const attributionStrategies = {
    first_touch(context) {
        return buildCoreModelResult(context.deterministicTouchpoints, pickWinner(context.deterministicTouchpoints, compareFirstTouchWinner));
    },
    last_touch(context) {
        return buildCoreModelResult(context.deterministicTouchpoints, pickWinner(context.deterministicTouchpoints, compareLastTouchWinner));
    },
    last_non_direct(context) {
        const nonDirectTouchpoints = context.deterministicTouchpoints.filter((touchpoint) => !touchpoint.isDirect);
        const winnerPool = nonDirectTouchpoints.length > 0 ? nonDirectTouchpoints : context.deterministicTouchpoints;
        const winner = pickWinner(winnerPool, compareLastTouchWinner);
        return buildCoreModelResult(context.deterministicTouchpoints, winner, {
            directSuppressionApplied: nonDirectTouchpoints.length > 0
        });
    },
    linear(context) {
        return {
            touchpoints: context.deterministicTouchpoints,
            weights: context.deterministicTouchpoints.map(() => 1),
            allocationStatus: context.deterministicTouchpoints.length > 0 ? 'attributed' : 'no_eligible_touches',
            directSuppressionApplied: false,
            deterministicBlockApplied: false,
            lookbackRuleApplied: summarizePool(context.deterministicTouchpoints).lookbackRuleApplied
        };
    },
    clicks_only(context) {
        const nonDirectClicks = context.deterministicClicks.filter((touchpoint) => !touchpoint.isDirect);
        const winnerPool = nonDirectClicks.length > 0 ? nonDirectClicks : context.deterministicClicks;
        const winner = pickWinner(winnerPool, compareLastTouchWinner);
        return {
            touchpoints: context.deterministicTouchpoints,
            weights: buildWinnerWeights(context.deterministicTouchpoints, winner?.touchpointId ?? null),
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
            touchpoints: context.hintedFallbackCandidates,
            weights: buildWinnerWeights(context.hintedFallbackCandidates, winner?.touchpointId ?? null),
            allocationStatus: winner ? 'attributed' : 'unattributed',
            directSuppressionApplied: false,
            deterministicBlockApplied: false,
            lookbackRuleApplied: summarizePool(context.hintedFallbackCandidates).lookbackRuleApplied
        };
    }
};
function validateRequestedModels(models) {
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
export function executeAttributionModels(rawTouchpoints, options) {
    if (!(options.orderOccurredAt instanceof Date) || Number.isNaN(options.orderOccurredAt.getTime())) {
        throw new Error('orderOccurredAt must be a valid Date');
    }
    const models = validateRequestedModels(options.attributionModels);
    const context = buildStrategyContext(rawTouchpoints, options);
    const creditsByModel = {};
    const summariesByModel = {};
    const allEligiblePoolSummary = summarizePool(context.eligibleTouchpoints);
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
            eligibleClickCount: 0,
            eligibleViewCount: 0,
            lookbackRuleApplied: allEligiblePoolSummary.lookbackRuleApplied,
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
        const poolSummary = model === 'hinted_fallback_only' && strategyResult.allocationStatus === 'blocked_by_deterministic'
            ? allEligiblePoolSummary
            : summarizePool(strategyResult.touchpoints);
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
            touchpointCountConsidered: poolSummary.touchpointCountConsidered,
            eligibleClickCount: poolSummary.eligibleClickCount,
            eligibleViewCount: poolSummary.eligibleViewCount,
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
export function computeAttributionOutputs(rawTouchpoints, options) {
    return executeAttributionModels(rawTouchpoints, options).creditsByModel;
}
