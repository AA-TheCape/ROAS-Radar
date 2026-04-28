"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ATTRIBUTION_MATCH_SOURCES = exports.DETERMINISTIC_INGESTION_SOURCES = void 0;
exports.isDirectTouchpoint = isDirectTouchpoint;
exports.dedupeDeterministicCandidates = dedupeDeterministicCandidates;
exports.selectLastNonDirectWinner = selectLastNonDirectWinner;
exports.confidenceScoreForWinner = confidenceScoreForWinner;
exports.confidenceLabelForScore = confidenceLabelForScore;
exports.isEligibleGa4FallbackCandidate = isEligibleGa4FallbackCandidate;
exports.selectGa4FallbackWinner = selectGa4FallbackWinner;
exports.DETERMINISTIC_INGESTION_SOURCES = [
    'landing_session_id',
    'checkout_token',
    'cart_token',
    'customer_identity'
];
exports.ATTRIBUTION_MATCH_SOURCES = [
    ...exports.DETERMINISTIC_INGESTION_SOURCES,
    'shopify_hint_fallback',
    'ga4_fallback',
    'unattributed'
];
const INGESTION_SOURCE_PRECEDENCE = {
    landing_session_id: 0,
    checkout_token: 1,
    cart_token: 2,
    customer_identity: 3
};
function hasClickId(touchpoint) {
    return Boolean(touchpoint.clickIdValue);
}
function compareDatesDescending(left, right) {
    return right.getTime() - left.getTime();
}
function compareDatesAscending(left, right) {
    return left.getTime() - right.getTime();
}
function compareIngestionSource(left, right) {
    const leftPrecedence = left ? INGESTION_SOURCE_PRECEDENCE[left] : Number.MAX_SAFE_INTEGER;
    const rightPrecedence = right ? INGESTION_SOURCE_PRECEDENCE[right] : Number.MAX_SAFE_INTEGER;
    return leftPrecedence - rightPrecedence;
}
function compareLexical(left, right) {
    return (left ?? '').localeCompare(right ?? '');
}
function isDirectTouchpoint(touchpoint) {
    return !touchpoint.source &&
        !touchpoint.medium &&
        !touchpoint.campaign &&
        !touchpoint.content &&
        !touchpoint.term &&
        !touchpoint.clickIdValue;
}
function compareDedupPriority(left, right) {
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
function compareWinnerPriority(left, right) {
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
function compareTimelineOrder(left, right) {
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
function dedupeDeterministicCandidates(candidates) {
    const deduped = new Map();
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
function selectLastNonDirectWinner(candidates) {
    const nonDirectCandidates = candidates.filter((candidate) => !candidate.isDirect);
    const directCandidates = candidates.filter((candidate) => candidate.isDirect);
    const selectionPool = nonDirectCandidates.length > 0 ? nonDirectCandidates : directCandidates;
    if (selectionPool.length === 0) {
        return null;
    }
    return selectionPool.slice().sort(compareWinnerPriority)[0] ?? null;
}
function confidenceScoreForWinner(winner) {
    if (!winner) {
        return 0;
    }
    switch (winner.matchSource) {
        case 'landing_session_id':
        case 'checkout_token':
            return 1;
        case 'cart_token':
            return 0.9;
        case 'customer_identity':
            return 0.6;
        case 'shopify_hint_fallback':
            return winner.clickIdValue ? 0.55 : 0.4;
        case 'ga4_fallback':
            return winner.clickIdValue ? 0.35 : 0.25;
        case 'unattributed':
            return 0;
    }
}
function confidenceLabelForScore(score) {
    if (score >= 0.9) {
        return 'high';
    }
    if (score >= 0.6) {
        return 'medium';
    }
    if (score > 0) {
        return 'low';
    }
    return 'none';
}
function hasAttributionSignal(candidate) {
    return Boolean(candidate.clickIdValue ||
        candidate.source ||
        candidate.medium ||
        candidate.campaign ||
        candidate.content ||
        candidate.term);
}
function populatedDimensionCount(candidate) {
    return [candidate.source, candidate.medium, candidate.campaign, candidate.content, candidate.term].filter(Boolean).length;
}
function compareGa4FallbackCandidates(left, right) {
    const occurredAtComparison = compareDatesDescending(new Date(left.occurredAt), new Date(right.occurredAt));
    if (occurredAtComparison !== 0) {
        return occurredAtComparison;
    }
    const clickIdComparison = Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
    if (clickIdComparison !== 0) {
        return clickIdComparison;
    }
    const dimensionComparison = populatedDimensionCount(right) - populatedDimensionCount(left);
    if (dimensionComparison !== 0) {
        return dimensionComparison;
    }
    const sessionIdComparison = compareLexical(left.ga4SessionId, right.ga4SessionId);
    if (sessionIdComparison !== 0) {
        return sessionIdComparison;
    }
    const clientIdComparison = compareLexical(left.ga4ClientId, right.ga4ClientId);
    if (clientIdComparison !== 0) {
        return clientIdComparison;
    }
    const transactionIdComparison = compareLexical(left.transactionId, right.transactionId);
    if (transactionIdComparison !== 0) {
        return transactionIdComparison;
    }
    return 0;
}
function isEligibleGa4FallbackCandidate(candidate, orderOccurredAt) {
    const candidateOccurredAt = new Date(candidate.occurredAt);
    if (candidateOccurredAt.getTime() > orderOccurredAt.getTime()) {
        return false;
    }
    if (!candidate.sessionHasRequiredFields) {
        return false;
    }
    if (!hasAttributionSignal(candidate)) {
        return false;
    }
    if (!candidate.ga4ClientId && !candidate.ga4SessionId && !candidate.transactionId) {
        return false;
    }
    return true;
}
function selectGa4FallbackWinner(candidates, orderOccurredAt) {
    const eligibleCandidates = candidates.filter((candidate) => isEligibleGa4FallbackCandidate(candidate, orderOccurredAt));
    if (eligibleCandidates.length === 0) {
        return null;
    }
    return eligibleCandidates.slice().sort(compareGa4FallbackCandidates)[0] ?? null;
}
