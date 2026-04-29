import type { AttributionTouchpoint } from "./engine.js";
import type { PersistedGa4FallbackCandidate } from "./ga4-fallback-candidates.js";

export const DETERMINISTIC_INGESTION_SOURCES = [
	"landing_session_id",
	"checkout_token",
	"cart_token",
	"customer_identity",
] as const;

export type DeterministicIngestionSource =
	(typeof DETERMINISTIC_INGESTION_SOURCES)[number];
export const ATTRIBUTION_MATCH_SOURCES = [
	...DETERMINISTIC_INGESTION_SOURCES,
	"shopify_hint_fallback",
	"ga4_fallback",
	"unattributed",
] as const;

export type AttributionMatchSource = (typeof ATTRIBUTION_MATCH_SOURCES)[number];
export type AttributionConfidenceLabel = "high" | "medium" | "low" | "none";

export type ResolvedAttributionTouchpoint = AttributionTouchpoint & {
	sourceTouchEventId: string | null;
	ingestionSource: DeterministicIngestionSource | null;
	matchSource: AttributionMatchSource;
	confidenceLabel: AttributionConfidenceLabel;
	ga4ClientId: string | null;
	ga4SessionId: string | null;
};

export type ResolvedJourney = {
	touchpoints: ResolvedAttributionTouchpoint[];
	winner: ResolvedAttributionTouchpoint | null;
	confidenceScore: number;
	confidenceLabel: AttributionConfidenceLabel;
};

const INGESTION_SOURCE_PRECEDENCE: Record<
	DeterministicIngestionSource,
	number
> = {
	landing_session_id: 0,
	checkout_token: 1,
	cart_token: 2,
	customer_identity: 3,
};

function hasClickId(
	touchpoint: Pick<ResolvedAttributionTouchpoint, "clickIdValue">,
): boolean {
	return Boolean(touchpoint.clickIdValue);
}

function compareDatesDescending(left: Date, right: Date): number {
	return right.getTime() - left.getTime();
}

function compareDatesAscending(left: Date, right: Date): number {
	return left.getTime() - right.getTime();
}

function compareIngestionSource(
	left: DeterministicIngestionSource | null,
	right: DeterministicIngestionSource | null,
): number {
	const leftPrecedence = left
		? INGESTION_SOURCE_PRECEDENCE[left]
		: Number.MAX_SAFE_INTEGER;
	const rightPrecedence = right
		? INGESTION_SOURCE_PRECEDENCE[right]
		: Number.MAX_SAFE_INTEGER;
	return leftPrecedence - rightPrecedence;
}

function compareLexical(left: string | null, right: string | null): number {
	return (left ?? "").localeCompare(right ?? "");
}

export function isDirectTouchpoint(
	touchpoint: Pick<
		AttributionTouchpoint,
		"source" | "medium" | "campaign" | "content" | "term" | "clickIdValue"
	>,
): boolean {
	return (
		!touchpoint.source &&
		!touchpoint.medium &&
		!touchpoint.campaign &&
		!touchpoint.content &&
		!touchpoint.term &&
		!touchpoint.clickIdValue
	);
}

function compareDedupPriority(
	left: ResolvedAttributionTouchpoint,
	right: ResolvedAttributionTouchpoint,
): number {
	const sourceComparison = compareIngestionSource(
		left.ingestionSource,
		right.ingestionSource,
	);
	if (sourceComparison !== 0) {
		return sourceComparison;
	}

	const occurredAtComparison = compareDatesDescending(
		left.occurredAt,
		right.occurredAt,
	);
	if (occurredAtComparison !== 0) {
		return occurredAtComparison;
	}

	const clickIdComparison =
		Number(hasClickId(right)) - Number(hasClickId(left));
	if (clickIdComparison !== 0) {
		return clickIdComparison;
	}

	return compareLexical(left.sourceTouchEventId, right.sourceTouchEventId);
}

function compareWinnerPriority(
	left: ResolvedAttributionTouchpoint,
	right: ResolvedAttributionTouchpoint,
): number {
	const occurredAtComparison = compareDatesDescending(
		left.occurredAt,
		right.occurredAt,
	);
	if (occurredAtComparison !== 0) {
		return occurredAtComparison;
	}

	const sourceComparison = compareIngestionSource(
		left.ingestionSource,
		right.ingestionSource,
	);
	if (sourceComparison !== 0) {
		return sourceComparison;
	}

	const clickIdComparison =
		Number(hasClickId(right)) - Number(hasClickId(left));
	if (clickIdComparison !== 0) {
		return clickIdComparison;
	}

	return compareLexical(left.sessionId, right.sessionId);
}

function compareTimelineOrder(
	left: ResolvedAttributionTouchpoint,
	right: ResolvedAttributionTouchpoint,
): number {
	const occurredAtComparison = compareDatesAscending(
		left.occurredAt,
		right.occurredAt,
	);
	if (occurredAtComparison !== 0) {
		return occurredAtComparison;
	}

	const sourceComparison = compareIngestionSource(
		left.ingestionSource,
		right.ingestionSource,
	);
	if (sourceComparison !== 0) {
		return sourceComparison;
	}

	const clickIdComparison =
		Number(hasClickId(right)) - Number(hasClickId(left));
	if (clickIdComparison !== 0) {
		return clickIdComparison;
	}

	return compareLexical(left.sessionId, right.sessionId);
}

export function dedupeDeterministicCandidates(
	candidates: ResolvedAttributionTouchpoint[],
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
	candidates: ResolvedAttributionTouchpoint[],
): ResolvedAttributionTouchpoint | null {
	const nonDirectCandidates = candidates.filter(
		(candidate) => !candidate.isDirect,
	);
	const directCandidates = candidates.filter((candidate) => candidate.isDirect);
	const selectionPool =
		nonDirectCandidates.length > 0 ? nonDirectCandidates : directCandidates;

	if (selectionPool.length === 0) {
		return null;
	}

	return selectionPool.slice().sort(compareWinnerPriority)[0] ?? null;
}

export function confidenceScoreForWinner(
	winner: Pick<
		ResolvedAttributionTouchpoint,
		"matchSource" | "clickIdValue"
	> | null,
): number {
	if (!winner) {
		return 0;
	}

	switch (winner.matchSource) {
		case "landing_session_id":
		case "checkout_token":
			return 1;
		case "cart_token":
			return 0.9;
		case "customer_identity":
			return 0.6;
		case "shopify_hint_fallback":
			return winner.clickIdValue ? 0.55 : 0.4;
		case "ga4_fallback":
			return winner.clickIdValue ? 0.35 : 0.25;
		case "unattributed":
			return 0;
	}
}

export function confidenceLabelForScore(
	score: number,
): AttributionConfidenceLabel {
	if (score >= 0.9) {
		return "high";
	}

	if (score >= 0.6) {
		return "medium";
	}

	if (score > 0) {
		return "low";
	}

	return "none";
}

function hasAttributionSignal(
	candidate: Pick<
		PersistedGa4FallbackCandidate,
		"source" | "medium" | "campaign" | "content" | "term" | "clickIdValue"
	>,
): boolean {
	return Boolean(
		candidate.clickIdValue ||
			candidate.source ||
			candidate.medium ||
			candidate.campaign ||
			candidate.content ||
			candidate.term,
	);
}

function populatedDimensionCount(
	candidate: Pick<
		PersistedGa4FallbackCandidate,
		"source" | "medium" | "campaign" | "content" | "term"
	>,
): number {
	return [
		candidate.source,
		candidate.medium,
		candidate.campaign,
		candidate.content,
		candidate.term,
	].filter(Boolean).length;
}

function compareGa4FallbackCandidates(
	left: PersistedGa4FallbackCandidate,
	right: PersistedGa4FallbackCandidate,
): number {
	const occurredAtComparison = compareDatesDescending(
		new Date(left.occurredAt),
		new Date(right.occurredAt),
	);
	if (occurredAtComparison !== 0) {
		return occurredAtComparison;
	}

	const clickIdComparison =
		Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
	if (clickIdComparison !== 0) {
		return clickIdComparison;
	}

	const dimensionComparison =
		populatedDimensionCount(right) - populatedDimensionCount(left);
	if (dimensionComparison !== 0) {
		return dimensionComparison;
	}

	const sessionIdComparison = compareLexical(
		left.ga4SessionId,
		right.ga4SessionId,
	);
	if (sessionIdComparison !== 0) {
		return sessionIdComparison;
	}

	const clientIdComparison = compareLexical(
		left.ga4ClientId,
		right.ga4ClientId,
	);
	if (clientIdComparison !== 0) {
		return clientIdComparison;
	}

	const transactionIdComparison = compareLexical(
		left.transactionId,
		right.transactionId,
	);
	if (transactionIdComparison !== 0) {
		return transactionIdComparison;
	}

	return 0;
}

export function isEligibleGa4FallbackCandidate(
	candidate: PersistedGa4FallbackCandidate,
	orderOccurredAt: Date,
): boolean {
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

	if (
		!candidate.ga4ClientId &&
		!candidate.ga4SessionId &&
		!candidate.transactionId
	) {
		return false;
	}

	return true;
}

export function selectGa4FallbackWinner(
	candidates: PersistedGa4FallbackCandidate[],
	orderOccurredAt: Date,
): PersistedGa4FallbackCandidate | null {
	const eligibleCandidates = candidates.filter((candidate) =>
		isEligibleGa4FallbackCandidate(candidate, orderOccurredAt),
	);
	if (eligibleCandidates.length === 0) {
		return null;
	}

	return (
		eligibleCandidates.slice().sort(compareGa4FallbackCandidates)[0] ?? null
	);
}
