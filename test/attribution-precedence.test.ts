import assert from "node:assert/strict";
import test from "node:test";

import {
	type AttributionComparableFields,
	type AttributionOrigin,
	classifyAttributionOrigin,
	shouldApplyAttributionUpdate,
} from "../src/modules/attribution/precedence.js";

function attribution(
	overrides: Partial<AttributionComparableFields> = {},
): AttributionComparableFields {
	return {
		sessionId: null,
		source: "google",
		medium: "cpc",
		campaign: "spring",
		content: null,
		term: null,
		clickIdType: "gclid",
		clickIdValue: "GCLID-1",
		attributionReason: "shopify_hint_derived",
		...overrides,
	};
}

function candidate(origin: AttributionOrigin, fields = attribution()) {
	return {
		origin,
		attribution: fields,
	};
}

test("classifyAttributionOrigin recognizes stored Shopify, GA4, ad platform, and first-party sources", () => {
	assert.equal(
		classifyAttributionOrigin({
			attributionTier: "deterministic_shopify_hint",
			attributionSource: "shopify_marketing_hint",
		}),
		"shopify_marketing_hint",
	);
	assert.equal(
		classifyAttributionOrigin({
			attributionTier: "ga4_fallback",
			matchSource: "ga4_fallback",
		}),
		"ga4_fallback",
	);
	assert.equal(
		classifyAttributionOrigin({
			matchSource: "google_ads",
		}),
		"ad_platform",
	);
	assert.equal(
		classifyAttributionOrigin({
			attributionReason: "matched_by_checkout_token",
		}),
		"deterministic_first_party",
	);
});

test("shouldApplyAttributionUpdate lets Shopify hints replace GA4 and ad-platform attribution", () => {
	const proposed = candidate("shopify_marketing_hint");

	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate(
				"ga4_fallback",
				attribution({ attributionReason: "ga4_fallback_match" }),
			),
			proposed,
		}),
		true,
	);
	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate("ad_platform"),
			proposed,
		}),
		true,
	);
});

test("shouldApplyAttributionUpdate is idempotent and preserves stronger first-party attribution", () => {
	const proposed = candidate("shopify_marketing_hint");

	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate("shopify_marketing_hint"),
			proposed,
		}),
		false,
	);
	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate("deterministic_first_party"),
			proposed,
		}),
		false,
	);
	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate(
				"shopify_marketing_hint",
				attribution({ campaign: "old" }),
			),
			proposed,
		}),
		true,
	);
});
