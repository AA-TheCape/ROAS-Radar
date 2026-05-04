import assert from "node:assert/strict";
import test from "node:test";

import {
	ATTRIBUTION_MODELS,
	type AttributionTouchpoint,
	computeAttributionOutputs,
} from "../src/modules/attribution/engine.js";

function buildTouchpoint(
	sessionId: string,
	occurredAt: string,
	overrides: Partial<AttributionTouchpoint> = {},
): AttributionTouchpoint {
	return {
		sessionId,
		occurredAt: new Date(occurredAt),
		source: "google",
		medium: "cpc",
		campaign: sessionId,
		content: null,
		term: null,
		clickIdType: null,
		clickIdValue: null,
		attributionReason: "matched_by_customer_identity",
		isDirect: false,
		isForced: false,
		...overrides,
	};
}

function revenueCreditsForModel(
	touchpoints: AttributionTouchpoint[],
	attributionModel: (typeof ATTRIBUTION_MODELS)[number],
	orderRevenue: string,
	orderOccurredAt: string,
	options: Parameters<typeof computeAttributionOutputs>[1] = {
		orderRevenue,
		orderOccurredAt: new Date(orderOccurredAt),
	},
): string[] {
	return computeAttributionOutputs(touchpoints, options)[attributionModel].map(
		(credit) => credit.revenueCredit,
	);
}

test("first touch attributes all revenue to the earliest touchpoint", () => {
	const touchpoints = [
		buildTouchpoint("session-a", "2026-04-01T00:00:00.000Z"),
		buildTouchpoint("session-b", "2026-04-03T00:00:00.000Z"),
		buildTouchpoint("session-c", "2026-04-05T00:00:00.000Z"),
	];

	assert.deepEqual(
		revenueCreditsForModel(
			touchpoints,
			"first_touch",
			"100.00",
			"2026-04-06T00:00:00.000Z",
		),
		["100.00", "0.00", "0.00"],
	);
});

test("last touch attributes all revenue to the most recent touchpoint", () => {
	const touchpoints = [
		buildTouchpoint("session-a", "2026-04-01T00:00:00.000Z"),
		buildTouchpoint("session-b", "2026-04-03T00:00:00.000Z"),
		buildTouchpoint("session-c", "2026-04-05T00:00:00.000Z"),
	];

	assert.deepEqual(
		revenueCreditsForModel(
			touchpoints,
			"last_touch",
			"100.00",
			"2026-04-06T00:00:00.000Z",
		),
		["0.00", "0.00", "100.00"],
	);
});

test("linear attribution splits revenue evenly and preserves cents deterministically", () => {
	const touchpoints = [
		buildTouchpoint("session-a", "2026-04-01T00:00:00.000Z"),
		buildTouchpoint("session-b", "2026-04-03T00:00:00.000Z"),
		buildTouchpoint("session-c", "2026-04-05T00:00:00.000Z"),
	];

	assert.deepEqual(
		revenueCreditsForModel(
			touchpoints,
			"linear",
			"100.00",
			"2026-04-06T00:00:00.000Z",
		),
		["33.34", "33.33", "33.33"],
	);
});

test("time decay weighs recent touchpoints more heavily", () => {
	const touchpoints = [
		buildTouchpoint("session-a", "2026-04-02T00:00:00.000Z"),
		buildTouchpoint("session-b", "2026-04-03T00:00:00.000Z"),
		buildTouchpoint("session-c", "2026-04-04T00:00:00.000Z"),
	];

	assert.deepEqual(
		revenueCreditsForModel(
			touchpoints,
			"time_decay",
			"100.00",
			"2026-04-04T00:00:00.000Z",
			{
				orderRevenue: "100.00",
				orderOccurredAt: new Date("2026-04-04T00:00:00.000Z"),
				timeDecayHalfLifeDays: 1,
			},
		),
		["14.29", "28.57", "57.14"],
	);
});

test("position based attribution applies a U-shape split", () => {
	const touchpoints = [
		buildTouchpoint("session-a", "2026-04-01T00:00:00.000Z"),
		buildTouchpoint("session-b", "2026-04-02T00:00:00.000Z"),
		buildTouchpoint("session-c", "2026-04-03T00:00:00.000Z"),
		buildTouchpoint("session-d", "2026-04-04T00:00:00.000Z"),
	];

	assert.deepEqual(
		revenueCreditsForModel(
			touchpoints,
			"position_based",
			"100.00",
			"2026-04-05T00:00:00.000Z",
		),
		["40.00", "10.00", "10.00", "40.00"],
	);
});

test("rule based weighted attribution applies deterministic custom multipliers", () => {
	const touchpoints = [
		buildTouchpoint("session-a", "2026-04-01T00:00:00.000Z"),
		buildTouchpoint("session-b", "2026-04-02T00:00:00.000Z", {
			source: null,
			medium: null,
			campaign: null,
			isDirect: true,
		}),
		buildTouchpoint("session-c", "2026-04-03T00:00:00.000Z", {
			clickIdType: "gclid",
			clickIdValue: "gclid-123",
		}),
	];

	assert.deepEqual(
		revenueCreditsForModel(
			touchpoints,
			"rule_based_weighted",
			"90.00",
			"2026-04-04T00:00:00.000Z",
			{
				orderRevenue: "90.00",
				orderOccurredAt: new Date("2026-04-04T00:00:00.000Z"),
				ruleBasedWeightConfig: {
					firstTouchWeight: 0.2,
					middleTouchWeight: 0.3,
					lastTouchWeight: 0.5,
					clickIdBonusMultiplier: 2,
					directDiscountMultiplier: 0.5,
				},
			},
		),
		["13.33", "10.00", "66.67"],
	);
});

test("all models conserve exact order revenue including unattributed fallback journeys", () => {
	const outputs = computeAttributionOutputs([], {
		orderRevenue: "123.45",
		orderOccurredAt: new Date("2026-04-04T00:00:00.000Z"),
	});

	for (const attributionModel of ATTRIBUTION_MODELS) {
		const total = outputs[attributionModel].reduce(
			(sum, credit) => sum + Number(credit.revenueCredit),
			0,
		);

		assert.equal(Number(total.toFixed(2)), 123.45);
		assert.equal(outputs[attributionModel].length, 1);
		assert.equal(
			outputs[attributionModel][0].attributionReason,
			"unattributed",
		);
	}
});
