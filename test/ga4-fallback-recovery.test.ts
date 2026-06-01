import assert from "node:assert/strict";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@localhost:5432/roas_radar_test";

function buildSnapshot(overrides: {
	sessionId?: string | null;
	source?: string | null;
	medium?: string | null;
	campaign?: string | null;
	clickIdValue?: string | null;
	orderTier?: string | null;
	orderSource?: string | null;
}) {
	return {
		origin: "unattributed" as const,
		result: {
			sessionId: overrides.sessionId ?? null,
			source: overrides.source ?? null,
			medium: overrides.medium ?? null,
			campaign: overrides.campaign ?? null,
			content: null,
			term: null,
			clickIdType: overrides.clickIdValue ? "gclid" : null,
			clickIdValue: overrides.clickIdValue ?? null,
			confidenceScore: null,
			attributionReason: null,
			matchSource: null,
			confidenceLabel: null,
			attributedAt: null,
		},
		order: {
			tier: overrides.orderTier ?? null,
			source: overrides.orderSource ?? null,
			reason: null,
			snapshot: null,
		},
	};
}

test("GA4 fallback recovery treats only blank or explicit unattributed snapshots as eligible", async () => {
	const { __ga4FallbackRecoveryTestUtils } = await import(
		"../src/modules/attribution/ga4-fallback-recovery.js"
	);

	assert.equal(
		__ga4FallbackRecoveryTestUtils.isCurrentlyUnattributedSnapshot(null),
		true,
	);
	assert.equal(
		__ga4FallbackRecoveryTestUtils.isCurrentlyUnattributedSnapshot(
			buildSnapshot({}),
		),
		true,
	);
	assert.equal(
		__ga4FallbackRecoveryTestUtils.isCurrentlyUnattributedSnapshot(
			buildSnapshot({
				orderTier: "unattributed",
				orderSource: "unattributed",
			}),
		),
		true,
	);
	assert.equal(
		__ga4FallbackRecoveryTestUtils.isCurrentlyUnattributedSnapshot(
			buildSnapshot({
				source: "google",
				medium: "cpc",
				orderTier: "ga4_fallback",
				orderSource: "ga4_fallback",
			}),
		),
		false,
	);
	assert.equal(
		__ga4FallbackRecoveryTestUtils.isCurrentlyUnattributedSnapshot(
			buildSnapshot({
				campaign: "shopify-hint",
				orderTier: "deterministic_shopify_hint",
				orderSource: "shopify_marketing_hint",
			}),
		),
		false,
	);
});
