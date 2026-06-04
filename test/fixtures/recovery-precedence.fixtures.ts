import type {
	AttributionComparableFields,
	AttributionOrigin,
} from "../../src/modules/attribution/precedence.js";

export type ContradictoryAttributionFixture = {
	name: string;
	currentOrigin: AttributionOrigin;
	proposedOrigin: AttributionOrigin;
	current: AttributionComparableFields;
	proposed: AttributionComparableFields;
	shouldApply: boolean;
};

function fields(
	overrides: Partial<AttributionComparableFields>,
): AttributionComparableFields {
	return {
		sessionId: null,
		source: null,
		medium: null,
		campaign: null,
		content: null,
		term: null,
		clickIdType: null,
		clickIdValue: null,
		attributionReason: null,
		...overrides,
	};
}

export const contradictoryRecoveryAttributionFixtures: ContradictoryAttributionFixture[] =
	[
		{
			name: "ga4 fallback cannot overwrite deterministic first-party checkout attribution",
			currentOrigin: "deterministic_first_party",
			proposedOrigin: "ga4_fallback",
			current: fields({
				sessionId: "11111111-1111-4111-8111-111111111111",
				source: "google",
				medium: "cpc",
				campaign: "checkout-winner",
				clickIdType: "gclid",
				clickIdValue: "GCLID-FIRST-PARTY",
				attributionReason: "matched_by_checkout_token",
			}),
			proposed: fields({
				source: "meta",
				medium: "paid_social",
				campaign: "ga4-lower-priority",
				clickIdType: "fbclid",
				clickIdValue: "FBCLID-GA4",
				attributionReason: "ga4_fallback_match",
			}),
			shouldApply: false,
		},
		{
			name: "Shopify marketing hint can repair lower-priority ad-platform attribution",
			currentOrigin: "ad_platform",
			proposedOrigin: "shopify_marketing_hint",
			current: fields({
				source: "google_ads",
				medium: "cpc",
				campaign: "platform-import",
				attributionReason: "platform_reported",
			}),
			proposed: fields({
				sessionId: "22222222-2222-4222-8222-222222222222",
				source: "google",
				medium: "cpc",
				campaign: "shopify-hint-winner",
				clickIdType: "gclid",
				clickIdValue: "GCLID-SHOPIFY-HINT",
				attributionReason: "shopify_hint_derived",
			}),
			shouldApply: true,
		},
		{
			name: "GA4 fallback can fill unattributed records but not replace Shopify hints",
			currentOrigin: "unattributed",
			proposedOrigin: "ga4_fallback",
			current: fields({ attributionReason: "unattributed" }),
			proposed: fields({
				source: "google",
				medium: "organic",
				campaign: "ga4-fill",
				attributionReason: "ga4_fallback_match",
			}),
			shouldApply: true,
		},
		{
			name: "same-origin retry is idempotent when the recovered fields match",
			currentOrigin: "shopify_marketing_hint",
			proposedOrigin: "shopify_marketing_hint",
			current: fields({
				sessionId: "33333333-3333-4333-8333-333333333333",
				source: "google",
				medium: "cpc",
				campaign: "same-hint",
				clickIdType: "gclid",
				clickIdValue: "GCLID-SAME",
				attributionReason: "shopify_hint_derived",
			}),
			proposed: fields({
				sessionId: "33333333-3333-4333-8333-333333333333",
				source: "google",
				medium: "cpc",
				campaign: "same-hint",
				clickIdType: "gclid",
				clickIdValue: "GCLID-SAME",
				attributionReason: "shopify_hint_derived",
			}),
			shouldApply: false,
		},
	];
