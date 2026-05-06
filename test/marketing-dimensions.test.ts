import assert from "node:assert/strict";
import test from "node:test";

import {
	buildCanonicalSpendDimensions,
	buildCanonicalTouchpointDimensions,
} from "../src/modules/marketing-dimensions/index.js";

test("buildCanonicalTouchpointDimensions infers google paid search from gclid when utm tags are absent", () => {
	const dimensions = buildCanonicalTouchpointDimensions({
		source: null,
		medium: null,
		campaign: " Spring Sale ",
		content: " Hero A ",
		term: "running shoes",
		gclid: "gclid-123",
	});

	assert.deepEqual(dimensions, {
		source: "google",
		medium: "cpc",
		campaign: "spring sale",
		content: "hero a",
		term: "running shoes",
		clickIdType: "gclid",
		clickIdValue: "gclid-123",
	});
});

test("buildCanonicalTouchpointDimensions treats gbraid-only touches as google paid search", () => {
	const dimensions = buildCanonicalTouchpointDimensions({
		source: null,
		medium: null,
		campaign: null,
		gbraid: "GBRAID-123",
	});

	assert.deepEqual(dimensions, {
		source: "google",
		medium: "cpc",
		campaign: null,
		content: null,
		term: null,
		clickIdType: "gbraid",
		clickIdValue: "GBRAID-123",
	});
});

test("buildCanonicalTouchpointDimensions categorizes present-but-unknown source and medium as unmapped", () => {
	const dimensions = buildCanonicalTouchpointDimensions({
		source: "Reddit",
		medium: "Boosted",
		campaign: "Launch",
	});

	assert.equal(dimensions.source, "unmapped");
	assert.equal(dimensions.medium, "unmapped");
	assert.equal(dimensions.campaign, "launch");
	assert.equal(dimensions.content, null);
	assert.equal(dimensions.term, null);
});

test("buildCanonicalSpendDimensions emits explicit unknown buckets for missing creative dimensions", () => {
	const dimensions = buildCanonicalSpendDimensions({
		source: "meta",
		medium: "paid_social",
		campaign: "Prospecting",
		content: null,
		term: null,
	});

	assert.deepEqual(dimensions, {
		source: "meta",
		medium: "paid_social",
		campaign: "prospecting",
		content: "unknown",
		term: "unknown",
	});
});
