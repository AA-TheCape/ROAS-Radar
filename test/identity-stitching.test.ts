import assert from "node:assert/strict";
import test from "node:test";

import {
	__identityTestUtils,
	buildIdentityEdgeIngestionMetricsLog,
	hashIdentityEmail,
	resolveIdentityStitch,
} from "../src/modules/identity/index.js";

test("hashIdentityEmail normalizes casing and whitespace before hashing", () => {
	const firstHash = hashIdentityEmail(" Buyer@Example.com ");
	const secondHash = hashIdentityEmail("buyer@example.com");

	assert.equal(firstHash, secondHash);
	assert.match(firstHash ?? "", /^[0-9a-f]{64}$/);
});

test("resolveIdentityStitch reuses an existing identity when identifiers agree", () => {
	const emailHash = hashIdentityEmail("buyer@example.com");
	const decision = resolveIdentityStitch(
		[
			{
				id: "identity-1",
				hashed_email: emailHash,
				shopify_customer_id: "shopify-123",
			},
		],
		{
			shopifyCustomerId: "shopify-123",
			email: "buyer@example.com",
		},
	);

	assert.deepEqual(decision, {
		outcome: "linked",
		identityId: "identity-1",
		emailHash,
		shopifyCustomerId: "shopify-123",
		operation: "reuse",
	});
});

test("resolveIdentityStitch rejects a customer id that conflicts with an existing hashed email", () => {
	const decision = resolveIdentityStitch(
		[
			{
				id: "identity-1",
				hashed_email: hashIdentityEmail("first@example.com"),
				shopify_customer_id: "shopify-123",
			},
		],
		{
			shopifyCustomerId: "shopify-123",
			email: "second@example.com",
		},
	);

	assert.equal(decision.outcome, "conflict");
	assert.equal(decision.reason, "customer_id_conflicts_with_existing_email");
});

test("resolveIdentityStitch rejects an email hash that conflicts with an existing customer id", () => {
	const decision = resolveIdentityStitch(
		[
			{
				id: "identity-1",
				hashed_email: hashIdentityEmail("buyer@example.com"),
				shopify_customer_id: "shopify-123",
			},
		],
		{
			shopifyCustomerId: "shopify-456",
			email: "buyer@example.com",
		},
	);

	assert.equal(decision.outcome, "conflict");
	assert.equal(
		decision.reason,
		"email_hash_conflicts_with_existing_customer_id",
	);
});

test("resolveIdentityStitch rejects automatic merges across two existing identities", () => {
	const decision = resolveIdentityStitch(
		[
			{
				id: "identity-1",
				hashed_email: null,
				shopify_customer_id: "shopify-123",
			},
			{
				id: "identity-2",
				hashed_email: hashIdentityEmail("buyer@example.com"),
				shopify_customer_id: null,
			},
		],
		{
			shopifyCustomerId: "shopify-123",
			email: "buyer@example.com",
		},
	);

	assert.equal(decision.outcome, "conflict");
	assert.equal(decision.reason, "identifiers_resolve_to_different_identities");
});

test("buildIdentityEdgeIngestionMetricsLog emits structured counters", () => {
	const payload = JSON.parse(
		buildIdentityEdgeIngestionMetricsLog({
			sourceTable: "tracking_events",
			evidenceSource: "tracking_event",
			outcome: "linked",
			deduplicated: false,
			processedNodes: 3,
			attachedNodes: 2,
			rehomedNodes: 1,
			quarantinedNodes: 0,
			journeyId: "123e4567-e89b-42d3-a456-426614174000",
		}),
	) as Record<string, unknown>;

	assert.equal(payload.event, "identity_edge_ingestion_processed");
	assert.equal(payload.evidenceSource, "tracking_event");
	assert.equal(payload.sourceTable, "tracking_events");
	assert.equal(payload.outcome, "linked");
	assert.equal(payload.processedNodes, 3);
	assert.equal(payload.attachedNodes, 2);
	assert.equal(payload.rehomedNodes, 1);
	assert.equal(payload.quarantinedNodes, 0);
});

test("buildNormalizedIdentityNodes normalizes, deduplicates, and drops invalid identifiers", () => {
	const nodes = __identityTestUtils.buildNormalizedIdentityNodes({
		sourceTimestamp: "2026-04-25T12:00:00.000Z",
		evidenceSource: "tracking_event",
		sourceTable: "tracking_events",
		sourceRecordId: "event-normalization",
		idempotencyKey: "identity-normalization-1",
		sessionId: "123e4567-e89b-42d3-a456-426614174999",
		checkoutToken: " co-1 ",
		cartToken: "ca-1",
		shopifyCustomerId: " sc-1 ",
		email: " Buyer@Example.com ",
		phone: "not-a-phone",
		hashedEmail: hashIdentityEmail("buyer@example.com"),
	});

	assert.deepEqual(nodes, [
		{ nodeType: "session_id", nodeKey: "123e4567-e89b-42d3-a456-426614174999" },
		{ nodeType: "checkout_token", nodeKey: "co-1" },
		{ nodeType: "cart_token", nodeKey: "ca-1" },
		{ nodeType: "shopify_customer_id", nodeKey: "sc-1" },
		{
			nodeType: "hashed_email",
			nodeKey: hashIdentityEmail("buyer@example.com") as string,
		},
	]);
});

test("selectBestJourneyId breaks ties by precedence, latest observation, then lexical journey id", () => {
	const rankedWinner = __identityTestUtils.selectBestJourneyId(
		[
			{
				node_id: "node-1",
				node_type: "hashed_email",
				journey_id: "journey-b",
				edge_id: "edge-1",
				edge_type: "deterministic",
				precedence_rank: 70,
				authoritative_shopify_customer_id: null,
				last_observed_at: new Date("2026-04-20T10:00:00.000Z"),
			},
			{
				node_id: "node-2",
				node_type: "checkout_token",
				journey_id: "journey-a",
				edge_id: "edge-2",
				edge_type: "deterministic",
				precedence_rank: 40,
				authoritative_shopify_customer_id: null,
				last_observed_at: new Date("2026-04-20T11:00:00.000Z"),
			},
		],
		new Map([
			[
				"journey-b",
				{
					journey_id: "journey-b",
					max_precedence_rank: 70,
					latest_observed_at: new Date("2026-04-20T10:00:00.000Z"),
				},
			],
			[
				"journey-a",
				{
					journey_id: "journey-a",
					max_precedence_rank: 40,
					latest_observed_at: new Date("2026-04-20T11:00:00.000Z"),
				},
			],
		]),
	);

	assert.equal(rankedWinner, "journey-b");

	const recencyWinner = __identityTestUtils.selectBestJourneyId(
		[
			{
				node_id: "node-3",
				node_type: "hashed_email",
				journey_id: "journey-c",
				edge_id: "edge-3",
				edge_type: "deterministic",
				precedence_rank: 70,
				authoritative_shopify_customer_id: null,
				last_observed_at: new Date("2026-04-20T10:00:00.000Z"),
			},
			{
				node_id: "node-4",
				node_type: "hashed_email",
				journey_id: "journey-d",
				edge_id: "edge-4",
				edge_type: "deterministic",
				precedence_rank: 70,
				authoritative_shopify_customer_id: null,
				last_observed_at: new Date("2026-04-20T11:00:00.000Z"),
			},
		],
		new Map([
			[
				"journey-c",
				{
					journey_id: "journey-c",
					max_precedence_rank: 70,
					latest_observed_at: new Date("2026-04-20T10:00:00.000Z"),
				},
			],
			[
				"journey-d",
				{
					journey_id: "journey-d",
					max_precedence_rank: 70,
					latest_observed_at: new Date("2026-04-20T11:00:00.000Z"),
				},
			],
		]),
	);

	assert.equal(recencyWinner, "journey-d");

	const lexicalWinner = __identityTestUtils.selectBestJourneyId(
		[
			{
				node_id: "node-5",
				node_type: "hashed_email",
				journey_id: "journey-z",
				edge_id: "edge-5",
				edge_type: "deterministic",
				precedence_rank: 70,
				authoritative_shopify_customer_id: null,
				last_observed_at: new Date("2026-04-20T11:00:00.000Z"),
			},
			{
				node_id: "node-6",
				node_type: "hashed_email",
				journey_id: "journey-a",
				edge_id: "edge-6",
				edge_type: "deterministic",
				precedence_rank: 70,
				authoritative_shopify_customer_id: null,
				last_observed_at: new Date("2026-04-20T11:00:00.000Z"),
			},
		],
		new Map([
			[
				"journey-z",
				{
					journey_id: "journey-z",
					max_precedence_rank: 70,
					latest_observed_at: new Date("2026-04-20T11:00:00.000Z"),
				},
			],
			[
				"journey-a",
				{
					journey_id: "journey-a",
					max_precedence_rank: 70,
					latest_observed_at: new Date("2026-04-20T11:00:00.000Z"),
				},
			],
		]),
	);

	assert.equal(lexicalWinner, "journey-a");
});
