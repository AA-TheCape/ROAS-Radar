export const ATTRIBUTION_ORIGINS = [
	"unattributed",
	"unknown",
	"ad_platform",
	"ga4_fallback",
	"shopify_marketing_hint",
	"deterministic_first_party",
] as const;

export type AttributionOrigin = (typeof ATTRIBUTION_ORIGINS)[number];

export const ATTRIBUTION_EVIDENCE_SOURCES = [
	"landing_session_id",
	"checkout_token",
	"cart_token",
	"customer_identity",
	"shopify_marketing_hint",
	"ga4_fallback",
] as const;

export type AttributionEvidenceSource =
	(typeof ATTRIBUTION_EVIDENCE_SOURCES)[number];

export type AttributionOriginInput = {
	attributionTier?: string | null;
	attributionSource?: string | null;
	matchSource?: string | null;
	attributionReason?: string | null;
};

export type AttributionUpdateCandidate = {
	origin: AttributionOrigin;
	attribution: AttributionComparableFields;
};

export type AttributionComparableFields = {
	sessionId: string | null;
	source: string | null;
	medium: string | null;
	campaign: string | null;
	content: string | null;
	term: string | null;
	clickIdType: string | null;
	clickIdValue: string | null;
	attributionReason: string | null;
};

const ORIGIN_PRECEDENCE: Record<AttributionOrigin, number> = {
	unattributed: 0,
	unknown: 10,
	ad_platform: 20,
	ga4_fallback: 30,
	shopify_marketing_hint: 40,
	deterministic_first_party: 50,
};

const EVIDENCE_SOURCE_ORDER: Record<AttributionEvidenceSource, number> = {
	landing_session_id: 0,
	checkout_token: 1,
	cart_token: 2,
	customer_identity: 3,
	shopify_marketing_hint: 4,
	ga4_fallback: 5,
};

function normalizeNullableString(
	value: string | null | undefined,
): string | null {
	const normalized = value?.trim().toLowerCase();
	return normalized ? normalized : null;
}

export function attributionOriginPrecedence(origin: AttributionOrigin): number {
	return ORIGIN_PRECEDENCE[origin];
}

export function attributionEvidenceSourcePrecedence(
	source: AttributionEvidenceSource,
): number {
	return EVIDENCE_SOURCE_ORDER[source];
}

export function compareAttributionEvidenceSources(
	left: AttributionEvidenceSource,
	right: AttributionEvidenceSource,
): number {
	return (
		attributionEvidenceSourcePrecedence(left) -
		attributionEvidenceSourcePrecedence(right)
	);
}

export function classifyAttributionOrigin(
	input: AttributionOriginInput,
): AttributionOrigin {
	const tier = normalizeNullableString(input.attributionTier);
	const source = normalizeNullableString(input.attributionSource);
	const matchSource = normalizeNullableString(input.matchSource);
	const reason = normalizeNullableString(input.attributionReason);

	if (
		tier === "deterministic_first_party" ||
		source === "landing_session_id" ||
		source === "checkout_token" ||
		source === "cart_token" ||
		source === "stitched_identity_journey" ||
		matchSource === "landing_session_id" ||
		matchSource === "checkout_token" ||
		matchSource === "cart_token" ||
		matchSource === "customer_identity" ||
		reason === "matched_by_landing_session" ||
		reason === "matched_by_checkout_token" ||
		reason === "matched_by_cart_token" ||
		reason === "matched_by_customer_identity"
	) {
		return "deterministic_first_party";
	}

	if (
		tier === "deterministic_shopify_hint" ||
		source === "shopify_marketing_hint" ||
		matchSource === "shopify_marketing_hint" ||
		matchSource === "shopify_hint_fallback" ||
		reason === "shopify_hint_derived"
	) {
		return "shopify_marketing_hint";
	}

	if (
		tier === "ga4_fallback" ||
		source === "ga4_fallback" ||
		matchSource === "ga4_fallback" ||
		reason === "ga4_fallback_match"
	) {
		return "ga4_fallback";
	}

	if (
		source === "meta_ads" ||
		source === "google_ads" ||
		source === "tiktok_ads" ||
		source === "ad_platform" ||
		matchSource === "meta_ads" ||
		matchSource === "google_ads" ||
		matchSource === "tiktok_ads" ||
		matchSource === "ad_platform"
	) {
		return "ad_platform";
	}

	if (
		tier === "unattributed" ||
		source === "unattributed" ||
		matchSource === "unattributed" ||
		reason === "unattributed"
	) {
		return "unattributed";
	}

	return "unknown";
}

export function mapResolvedIngestionSourceToAttributionOrigin(
	source: AttributionEvidenceSource,
): AttributionOrigin {
	switch (source) {
		case "landing_session_id":
		case "checkout_token":
		case "cart_token":
		case "customer_identity":
			return "deterministic_first_party";
		case "shopify_marketing_hint":
			return "shopify_marketing_hint";
		case "ga4_fallback":
			return "ga4_fallback";
	}
}

export function attributionFieldsEqual(
	left: AttributionComparableFields,
	right: AttributionComparableFields,
): boolean {
	return (
		left.sessionId === right.sessionId &&
		left.source === right.source &&
		left.medium === right.medium &&
		left.campaign === right.campaign &&
		left.content === right.content &&
		left.term === right.term &&
		left.clickIdType === right.clickIdType &&
		left.clickIdValue === right.clickIdValue &&
		left.attributionReason === right.attributionReason
	);
}

export function shouldApplyAttributionUpdate(input: {
	current: AttributionUpdateCandidate | null;
	proposed: AttributionUpdateCandidate;
}): boolean {
	if (input.proposed.origin === "unattributed") {
		return !input.current || input.current.origin === "unknown";
	}

	if (!input.current) {
		return true;
	}

	const currentPrecedence = attributionOriginPrecedence(input.current.origin);
	const proposedPrecedence = attributionOriginPrecedence(input.proposed.origin);

	if (proposedPrecedence > currentPrecedence) {
		return true;
	}

	if (proposedPrecedence < currentPrecedence) {
		return false;
	}

	return (
		input.current.origin === input.proposed.origin &&
		!attributionFieldsEqual(
			input.current.attribution,
			input.proposed.attribution,
		)
	);
}
