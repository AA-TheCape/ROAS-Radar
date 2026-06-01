import assert from "node:assert/strict";
import test from "node:test";

import {
	type AttributionComparableFields,
	type AttributionOrigin,
	ATTRIBUTION_EVIDENCE_SOURCES,
	ATTRIBUTION_ORIGINS,
	classifyAttributionOrigin,
	compareAttributionEvidenceSources,
	attributionOriginPrecedence,
	shouldApplyAttributionUpdate,
} from "../src/modules/attribution/precedence.js";
import { contradictoryRecoveryAttributionFixtures } from "./fixtures/recovery-precedence.fixtures.js";

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

test("shouldApplyAttributionUpdate covers every origin conflict permutation", () => {
	for (const currentOrigin of ATTRIBUTION_ORIGINS) {
		for (const proposedOrigin of ATTRIBUTION_ORIGINS) {
			const current = candidate(currentOrigin);
			const proposed = candidate(proposedOrigin);
			const currentPrecedence = attributionOriginPrecedence(currentOrigin);
			const proposedPrecedence = attributionOriginPrecedence(proposedOrigin);
			const expected =
				proposedOrigin === "unattributed"
					? currentOrigin === "unknown"
					: proposedPrecedence > currentPrecedence;

			assert.equal(
				shouldApplyAttributionUpdate({ current, proposed }),
				expected,
				`${proposedOrigin} proposed over ${currentOrigin}`,
			);
		}
	}
});

test("shouldApplyAttributionUpdate persists an initial unattributed fallback", () => {
	assert.equal(
		shouldApplyAttributionUpdate({
			current: null,
			proposed: candidate(
				"unattributed",
				attribution({
					sessionId: null,
					source: null,
					medium: null,
					campaign: null,
					clickIdType: null,
					clickIdValue: null,
					attributionReason: "unattributed",
				}),
			),
		}),
		true,
	);
});

test("shouldApplyAttributionUpdate uses stable same-origin tie-break behavior", () => {
	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate("ga4_fallback", attribution({ campaign: "old" })),
			proposed: candidate("ga4_fallback", attribution({ campaign: "new" })),
		}),
		true,
	);
	assert.equal(
		shouldApplyAttributionUpdate({
			current: candidate("ga4_fallback"),
			proposed: candidate("ga4_fallback"),
		}),
		false,
	);
});

test("compareAttributionEvidenceSources exposes the shared source ordering", () => {
	const sorted = ATTRIBUTION_EVIDENCE_SOURCES.slice().sort(
		compareAttributionEvidenceSources,
	);

	assert.deepEqual(sorted, [
		"landing_session_id",
		"checkout_token",
		"cart_token",
		"customer_identity",
		"shopify_marketing_hint",
		"ga4_fallback",
	]);
	assert.equal(
		compareAttributionEvidenceSources(
			"shopify_marketing_hint",
			"ga4_fallback",
		) < 0,
		true,
	);
});

test("contradictory recovery fixtures enforce source-priority decisions", () => {
	for (const fixture of contradictoryRecoveryAttributionFixtures) {
		assert.equal(
			shouldApplyAttributionUpdate({
				current: {
					origin: fixture.currentOrigin,
					attribution: fixture.current,
				},
				proposed: {
					origin: fixture.proposedOrigin,
					attribution: fixture.proposed,
				},
			}),
			fixture.shouldApply,
			fixture.name,
		);
	}
});
