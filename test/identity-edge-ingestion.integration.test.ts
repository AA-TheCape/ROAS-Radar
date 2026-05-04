import assert from "node:assert/strict";
import test from "node:test";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

import { resetE2EDatabase } from "./e2e-harness.ts";

async function getIdentityModules() {
	const [{ withTransaction }, { ingestIdentityEdges, hashIdentityEmail }] =
		await Promise.all([
			import("../src/db/pool.js"),
			import("../src/modules/identity/index.js"),
		]);

	return {
		withTransaction,
		ingestIdentityEdges,
		hashIdentityEmail,
	};
}

async function captureStructuredLogs<T>(
	callback: () => Promise<T>,
): Promise<{ entries: Array<Record<string, unknown>>; result: T }> {
	const stdoutChunks: string[] = [];
	const stderrChunks: string[] = [];
	const originalStdoutWrite = process.stdout.write.bind(process.stdout);
	const originalStderrWrite = process.stderr.write.bind(process.stderr);

	process.stdout.write = ((chunk: string | Uint8Array) => {
		stdoutChunks.push(
			typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"),
		);
		return true;
	}) as typeof process.stdout.write;
	process.stderr.write = ((chunk: string | Uint8Array) => {
		stderrChunks.push(
			typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"),
		);
		return true;
	}) as typeof process.stderr.write;

	try {
		const result = await callback();
		const entries = [...stdoutChunks, ...stderrChunks]
			.join("")
			.trim()
			.split("\n")
			.map((line) => line.trim())
			.filter((line) => line.startsWith("{") && line.endsWith("}"))
			.map((line) => JSON.parse(line) as Record<string, unknown>);

		return { entries, result };
	} finally {
		process.stdout.write = originalStdoutWrite as typeof process.stdout.write;
		process.stderr.write = originalStderrWrite as typeof process.stderr.write;
	}
}

async function fetchJourneyState(journeyId: string) {
	const { pool } = await import("../src/db/pool.js");

	const [journeyResult, edgeResult, ingestionResult] = await Promise.all([
		pool.query<{
			id: string;
			status: string;
			authoritative_shopify_customer_id: string | null;
			primary_email_hash: string | null;
			primary_phone_hash: string | null;
			merged_into_journey_id: string | null;
		}>(
			`
        SELECT
          id::text AS id,
          status,
          authoritative_shopify_customer_id,
          primary_email_hash,
          primary_phone_hash,
          merged_into_journey_id::text AS merged_into_journey_id
        FROM identity_journeys
        WHERE id = $1::uuid
      `,
			[journeyId],
		),
		pool.query<{
			node_type: string;
			node_key: string;
			edge_type: string;
			is_active: boolean;
			conflict_code: string | null;
			is_ambiguous: boolean;
		}>(
			`
        SELECT
          n.node_type,
          n.node_key,
          e.edge_type,
          e.is_active,
          e.conflict_code,
          n.is_ambiguous
        FROM identity_edges e
        INNER JOIN identity_nodes n ON n.id = e.node_id
        WHERE e.journey_id = $1::uuid
        ORDER BY n.node_type ASC, e.created_at ASC
      `,
			[journeyId],
		),
		pool.query<{
			idempotency_key: string;
			status: string;
			outcome_reason: string | null;
			rehomed_nodes: number;
			quarantined_nodes: number;
		}>(
			`
        SELECT
          idempotency_key,
          status,
          outcome_reason,
          rehomed_nodes,
          quarantined_nodes
        FROM identity_edge_ingestion_runs
        WHERE journey_id = $1::uuid
        ORDER BY id ASC
      `,
			[journeyId],
		),
	]);

	return {
		journey: journeyResult.rows[0] ?? null,
		edges: edgeResult.rows,
		ingestions: ingestionResult.rows,
	};
}

test.beforeEach(async () => {
	await resetE2EDatabase();
});

test.after(async () => {
	await resetE2EDatabase();
	const { pool } = await import("../src/db/pool.js");
	await pool.end();
});

test("identity edge ingestion is idempotent for repeated runs", async () => {
	const sessionId = "123e4567-e89b-42d3-a456-426614174000";
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

	const first = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T10:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-1",
			idempotencyKey: "identity-replay-1",
			sessionId,
			checkoutToken: "co-1",
			cartToken: "ca-1",
		}),
	);
	const second = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T10:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-1",
			idempotencyKey: "identity-replay-1",
			sessionId,
			checkoutToken: "co-1",
			cartToken: "ca-1",
		}),
	);

	assert.equal(first.outcome, "linked");
	assert.equal(second.outcome, "linked");
	assert.equal(second.deduplicated, true);

	const { pool } = await import("../src/db/pool.js");
	const countResult = await pool.query<{
		node_count: string;
		edge_count: string;
		run_count: string;
	}>(
		`
      SELECT
        (SELECT COUNT(*)::text FROM identity_nodes) AS node_count,
        (SELECT COUNT(*)::text FROM identity_edges) AS edge_count,
        (SELECT COUNT(*)::text FROM identity_edge_ingestion_runs) AS run_count
    `,
	);

	assert.equal(countResult.rows[0]?.node_count, "3");
	assert.equal(countResult.rows[0]?.edge_count, "3");
	assert.equal(countResult.rows[0]?.run_count, "1");
});

test("identity edge ingestion reprocesses an existing non-terminal run instead of replaying a null journey result", async () => {
	const sessionId = "123e4567-e89b-42d3-a456-426614174099";
	const { pool } = await import("../src/db/pool.js");
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

	await pool.query(
		`
      INSERT INTO identity_edge_ingestion_runs (
        idempotency_key,
        evidence_source,
        source_table,
        source_record_id,
        source_timestamp,
        status,
        journey_id,
        outcome_reason,
        processed_nodes,
        attached_nodes,
        rehomed_nodes,
        quarantined_nodes,
        processed_at,
        created_at,
        updated_at
      )
      VALUES (
        'identity-interrupted-1',
        'tracking_event',
        'tracking_events',
        'event-interrupted-stale',
        '2026-04-20T09:59:00.000Z',
        'started',
        NULL,
        'linked',
        99,
        99,
        99,
        99,
        '2026-04-20T09:59:30.000Z',
        now(),
        now()
      )
    `,
	);

	const result = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T10:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-interrupted-retry",
			idempotencyKey: "identity-interrupted-1",
			sessionId,
			checkoutToken: "co-interrupted-1",
		}),
	);

	assert.equal(result.outcome, "linked");
	assert.equal(result.deduplicated, false);
	assert.ok(result.journeyId);
	assert.deepEqual(result.linkedSessionIds, []);

	const ingestionRun = await pool.query<{
		status: string;
		journey_id: string | null;
		outcome_reason: string | null;
		processed_nodes: number;
		attached_nodes: number;
		rehomed_nodes: number;
		quarantined_nodes: number;
		processed_at: Date | null;
		source_record_id: string | null;
	}>(
		`
      SELECT
        status,
        journey_id::text AS journey_id,
        outcome_reason,
        processed_nodes,
        attached_nodes,
        rehomed_nodes,
        quarantined_nodes,
        processed_at,
        source_record_id
      FROM identity_edge_ingestion_runs
      WHERE idempotency_key = 'identity-interrupted-1'
    `,
	);

	assert.equal(ingestionRun.rowCount, 1);
	assert.equal(ingestionRun.rows[0]?.status, "completed");
	assert.equal(ingestionRun.rows[0]?.journey_id, result.journeyId);
	assert.equal(ingestionRun.rows[0]?.outcome_reason, "created_new_journey");
	assert.equal(ingestionRun.rows[0]?.processed_nodes, 2);
	assert.equal(ingestionRun.rows[0]?.attached_nodes, 2);
	assert.equal(ingestionRun.rows[0]?.rehomed_nodes, 0);
	assert.equal(ingestionRun.rows[0]?.quarantined_nodes, 0);
	assert.ok(ingestionRun.rows[0]?.processed_at);
	assert.equal(
		ingestionRun.rows[0]?.source_record_id,
		"event-interrupted-retry",
	);
});

test("identity edge ingestion preserves first_seen and last_seen across out-of-order events", async () => {
	const sessionId = "123e4567-e89b-42d3-a456-426614174001";
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-24T12:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-newer",
			idempotencyKey: "identity-order-1",
			sessionId,
			checkoutToken: "co-2",
		}),
	);

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-18T08:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-older",
			idempotencyKey: "identity-order-2",
			sessionId,
			cartToken: "ca-2",
		}),
	);

	const { pool } = await import("../src/db/pool.js");
	const nodeResult = await pool.query<{
		first_seen_at: Date;
		last_seen_at: Date;
	}>(
		`
      SELECT first_seen_at, last_seen_at
      FROM identity_nodes
      WHERE node_type = 'session_id'
        AND node_key = $1
    `,
		[sessionId],
	);
	const edgeResult = await pool.query<{
		first_observed_at: Date;
		last_observed_at: Date;
	}>(
		`
      SELECT e.first_observed_at, e.last_observed_at
      FROM identity_edges e
      INNER JOIN identity_nodes n ON n.id = e.node_id
      WHERE n.node_type = 'session_id'
        AND n.node_key = $1
        AND e.is_active = true
    `,
		[sessionId],
	);

	assert.equal(
		nodeResult.rows[0]?.first_seen_at.toISOString(),
		"2026-04-18T08:00:00.000Z",
	);
	assert.equal(
		nodeResult.rows[0]?.last_seen_at.toISOString(),
		"2026-04-24T12:00:00.000Z",
	);
	assert.equal(
		edgeResult.rows[0]?.first_observed_at.toISOString(),
		"2026-04-18T08:00:00.000Z",
	);
	assert.equal(
		edgeResult.rows[0]?.last_observed_at.toISOString(),
		"2026-04-24T12:00:00.000Z",
	);
});

test("identity edge ingestion emits structured processing metrics", async () => {
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();
	const { entries, result } = await captureStructuredLogs(() =>
		withTransaction((client) =>
			ingestIdentityEdges(client, {
				sourceTimestamp: "2026-04-22T14:00:00.000Z",
				evidenceSource: "tracking_event",
				sourceTable: "tracking_events",
				sourceRecordId: "event-metrics",
				idempotencyKey: "identity-metrics-1",
				sessionId: "123e4567-e89b-42d3-a456-426614174002",
				checkoutToken: "co-3",
				cartToken: "ca-3",
			}),
		),
	);

	assert.equal(result.outcome, "linked");

	const identityMetricsLog = entries.find(
		(entry) => entry.event === "identity_edge_ingestion_processed",
	);
	assert.ok(identityMetricsLog);
	assert.equal(identityMetricsLog?.evidenceSource, "tracking_event");
	assert.equal(identityMetricsLog?.outcome, "linked");
	assert.equal(identityMetricsLog?.processedNodes, 3);
	assert.equal(identityMetricsLog?.attachedNodes, 3);
	assert.equal(identityMetricsLog?.rehomedNodes, 0);
	assert.equal(identityMetricsLog?.quarantinedNodes, 0);
});

test("anonymous session and token evidence is promoted in-place when Shopify authority arrives later", async () => {
	const sessionId = "123e4567-e89b-42d3-a456-426614174010";
	const { withTransaction, ingestIdentityEdges, hashIdentityEmail } =
		await getIdentityModules();
	const emailHash = hashIdentityEmail("buyer@example.com");

	const anonymous = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T09:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "anon-session",
			idempotencyKey: "identity-example-1-anon",
			sessionId,
			checkoutToken: "co-example-1",
			cartToken: "ca-example-1",
		}),
	);
	const promoted = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T10:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-example-1",
			idempotencyKey: "identity-example-1-authoritative",
			sessionId,
			checkoutToken: "co-example-1",
			cartToken: "ca-example-1",
			shopifyCustomerId: "sc-example-1",
			email: "buyer@example.com",
		}),
	);

	assert.equal(anonymous.outcome, "linked");
	assert.equal(promoted.outcome, "linked");
	assert.equal(promoted.journeyId, anonymous.journeyId);

	const { pool } = await import("../src/db/pool.js");
	const [journeyCountResult, state] = await Promise.all([
		pool.query<{ journey_count: string }>(
			"SELECT COUNT(*)::text AS journey_count FROM identity_journeys",
		),
		fetchJourneyState(promoted.journeyId as string),
	]);

	assert.equal(journeyCountResult.rows[0]?.journey_count, "1");
	assert.equal(
		state.journey?.authoritative_shopify_customer_id,
		"sc-example-1",
	);
	assert.equal(state.journey?.primary_email_hash, emailHash);
	assert.ok(
		state.edges.some(
			(edge) =>
				edge.node_type === "shopify_customer_id" &&
				edge.node_key === "sc-example-1" &&
				edge.edge_type === "authoritative" &&
				edge.is_active,
		),
	);
	assert.ok(
		state.edges.some(
			(edge) =>
				edge.node_type === "checkout_token" &&
				edge.node_key === "co-example-1" &&
				edge.edge_type === "deterministic" &&
				edge.is_active,
		),
	);
});

test("Shopify authority rehomes lower-rank identifiers and persists the authoritative merge reason", async () => {
	const { withTransaction, ingestIdentityEdges, hashIdentityEmail } =
		await getIdentityModules();
	const emailHash = hashIdentityEmail("buyer@example.com");

	const anonymous = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T09:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "anon-email",
			idempotencyKey: "identity-authority-anon",
			email: "buyer@example.com",
		}),
	);
	const authoritative = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T10:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-sc1",
			idempotencyKey: "identity-authority-customer",
			shopifyCustomerId: "sc-1",
		}),
	);
	const promoted = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T11:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-promote",
			idempotencyKey: "identity-authority-promote",
			shopifyCustomerId: "sc-1",
			email: "buyer@example.com",
		}),
	);

	assert.equal(promoted.outcome, "linked");
	assert.equal(promoted.journeyId, authoritative.journeyId);

	const anonymousState = await fetchJourneyState(anonymous.journeyId as string);
	const authoritativeState = await fetchJourneyState(
		authoritative.journeyId as string,
	);

	assert.equal(anonymousState.journey?.status, "merged");
	assert.equal(
		anonymousState.journey?.merged_into_journey_id,
		authoritative.journeyId,
	);
	assert.equal(
		authoritativeState.journey?.authoritative_shopify_customer_id,
		"sc-1",
	);
	assert.equal(authoritativeState.journey?.primary_email_hash, emailHash);
	assert.ok(
		authoritativeState.edges.some(
			(edge) =>
				edge.node_type === "hashed_email" &&
				edge.node_key === emailHash &&
				edge.edge_type === "promoted" &&
				edge.is_active,
		),
	);

	const promotionRun = authoritativeState.ingestions.find(
		(run) => run.idempotency_key === "identity-authority-promote",
	);
	assert.equal(promotionRun?.status, "completed");
	assert.equal(
		promotionRun?.outcome_reason,
		"shopify_customer_id_authoritative_winner",
	);
	assert.equal(promotionRun?.rehomed_nodes, 1);
	assert.equal(promotionRun?.quarantined_nodes, 0);
});

test("non-authoritative candidates resolve deterministically by precedence and persist the merge reason", async () => {
	const { withTransaction, ingestIdentityEdges, hashIdentityEmail } =
		await getIdentityModules();
	const emailHash = hashIdentityEmail("buyer@example.com");

	const emailJourney = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-21T09:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "anon-email-2",
			idempotencyKey: "identity-precedence-email",
			email: "buyer@example.com",
		}),
	);
	const checkoutJourney = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-21T10:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "anon-checkout",
			idempotencyKey: "identity-precedence-checkout",
			checkoutToken: "co-merge-1",
		}),
	);
	const merged = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-21T11:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "anon-join",
			idempotencyKey: "identity-precedence-join",
			email: "buyer@example.com",
			checkoutToken: "co-merge-1",
		}),
	);

	assert.equal(merged.outcome, "linked");
	assert.equal(merged.journeyId, emailJourney.journeyId);

	const checkoutState = await fetchJourneyState(
		checkoutJourney.journeyId as string,
	);
	const winnerState = await fetchJourneyState(emailJourney.journeyId as string);

	assert.equal(checkoutState.journey?.status, "merged");
	assert.equal(
		checkoutState.journey?.merged_into_journey_id,
		emailJourney.journeyId,
	);
	assert.ok(
		winnerState.edges.some(
			(edge) =>
				edge.node_type === "checkout_token" &&
				edge.node_key === "co-merge-1" &&
				edge.edge_type === "promoted" &&
				edge.is_active,
		),
	);

	const mergeRun = winnerState.ingestions.find(
		(run) => run.idempotency_key === "identity-precedence-join",
	);
	assert.equal(mergeRun?.outcome_reason, "non_authoritative_precedence_winner");
	assert.equal(mergeRun?.rehomed_nodes, 1);
});

test("shared phone numbers across authoritative Shopify customers are quarantined instead of merged", async () => {
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();
	const phoneHash =
		"5f2c5aeb6d5456f74d87e7013dc76ce8998f8e76c11908ba3b0fdb0c0b1c3f34";

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T09:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-phone-1",
			idempotencyKey: "identity-phone-j1",
			shopifyCustomerId: "sc-1",
			phoneHash,
		}),
	);
	const secondJourney = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T10:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-phone-2",
			idempotencyKey: "identity-phone-j2",
			shopifyCustomerId: "sc-2",
		}),
	);
	const conflictHandled = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T11:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-phone-3",
			idempotencyKey: "identity-phone-quarantine",
			shopifyCustomerId: "sc-2",
			phoneHash,
		}),
	);

	assert.equal(conflictHandled.outcome, "linked");
	assert.equal(conflictHandled.journeyId, secondJourney.journeyId);

	const { pool } = await import("../src/db/pool.js");
	const phoneNodeResult = await pool.query<{
		is_ambiguous: boolean;
		edge_type: string;
		conflict_code: string | null;
		journey_id: string;
	}>(
		`
      SELECT
        n.is_ambiguous,
        e.edge_type,
        e.conflict_code,
        e.journey_id::text AS journey_id
      FROM identity_nodes n
      INNER JOIN identity_edges e ON e.node_id = n.id
      WHERE n.node_type = 'phone_hash'
        AND n.node_key = $1
        AND e.is_active = true
    `,
		[phoneHash],
	);

	assert.equal(phoneNodeResult.rows[0]?.is_ambiguous, true);
	assert.equal(phoneNodeResult.rows[0]?.edge_type, "quarantined");
	assert.equal(
		phoneNodeResult.rows[0]?.conflict_code,
		"phone_hash_conflicts_across_authoritative_customers",
	);
	assert.notEqual(phoneNodeResult.rows[0]?.journey_id, secondJourney.journeyId);

	const secondState = await fetchJourneyState(
		secondJourney.journeyId as string,
	);
	const quarantineRun = secondState.ingestions.find(
		(run) => run.idempotency_key === "identity-phone-quarantine",
	);
	assert.equal(
		quarantineRun?.outcome_reason,
		"shopify_customer_id_authoritative_winner",
	);
	assert.equal(quarantineRun?.quarantined_nodes, 1);
});

test("shared hashed emails across authoritative Shopify customers are quarantined and ignored on future merges", async () => {
	const { withTransaction, ingestIdentityEdges, hashIdentityEmail } =
		await getIdentityModules();
	const { pool } = await import("../src/db/pool.js");
	const emailHash = hashIdentityEmail("shared@example.com") as string;

	await pool.query(
		`
      INSERT INTO shopify_orders (
        shopify_order_id,
        shopify_order_number,
        shopify_customer_id,
        email_hash,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        created_at_shopify,
        updated_at_shopify,
        payload_size_bytes,
        source_name
      )
      VALUES
        (
          'order-email-1',
          '1001',
          'sc-email-1',
          $1,
          'USD',
          50.00,
          50.00,
          '2026-04-22T09:00:00.000Z',
          '2026-04-22T09:00:00.000Z',
          '2026-04-22T09:00:00.000Z',
          octet_length(convert_to('{}', 'utf8')),
          'web'
        ),
        (
          'order-email-2',
          '1002',
          'sc-email-2',
          $1,
          'USD',
          75.00,
          75.00,
          '2026-04-22T10:00:00.000Z',
          '2026-04-22T10:00:00.000Z',
          '2026-04-22T10:00:00.000Z',
          octet_length(convert_to('{}', 'utf8')),
          'web'
        ),
        (
          'order-email-3',
          '1003',
          'sc-email-2',
          $1,
          'USD',
          80.00,
          80.00,
          '2026-04-22T11:00:00.000Z',
          '2026-04-22T11:00:00.000Z',
          '2026-04-22T11:00:00.000Z',
          octet_length(convert_to('{}', 'utf8')),
          'web'
        ),
        (
          'order-email-after-quarantine',
          '1004',
          'sc-email-3',
          $1,
          'USD',
          90.00,
          90.00,
          '2026-04-22T12:00:00.000Z',
          '2026-04-22T12:00:00.000Z',
          '2026-04-22T12:00:00.000Z',
          octet_length(convert_to('{}', 'utf8')),
          'web'
        )
    `,
		[emailHash],
	);

	const firstJourney = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T09:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-email-1",
			idempotencyKey: "identity-email-j1",
			shopifyCustomerId: "sc-email-1",
			hashedEmail: emailHash,
		}),
	);
	const secondJourney = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T10:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-email-2",
			idempotencyKey: "identity-email-j2",
			shopifyCustomerId: "sc-email-2",
		}),
	);
	const quarantined = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T11:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-email-3",
			idempotencyKey: "identity-email-quarantine",
			shopifyCustomerId: "sc-email-2",
			hashedEmail: emailHash,
		}),
	);
	const futureAnonymous = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-22T12:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-email-after-quarantine",
			idempotencyKey: "identity-email-post-quarantine",
			sessionId: "123e4567-e89b-42d3-a456-426614174011",
			shopifyCustomerId: "sc-email-3",
			hashedEmail: emailHash,
		}),
	);

	assert.equal(quarantined.outcome, "linked");
	assert.equal(quarantined.journeyId, secondJourney.journeyId);
	assert.ok(futureAnonymous.journeyId);
	assert.notEqual(futureAnonymous.journeyId, firstJourney.journeyId);
	assert.notEqual(futureAnonymous.journeyId, secondJourney.journeyId);

	const [emailNodeResult, futureState, orderAssignments] = await Promise.all([
		pool.query<{
			is_ambiguous: boolean;
			edge_type: string;
			conflict_code: string | null;
			journey_id: string;
		}>(
			`
        SELECT
          n.is_ambiguous,
          e.edge_type,
          e.conflict_code,
          e.journey_id::text AS journey_id
        FROM identity_nodes n
        INNER JOIN identity_edges e ON e.node_id = n.id
        WHERE n.node_type = 'hashed_email'
          AND n.node_key = $1
          AND e.is_active = true
      `,
			[emailHash],
		),
		fetchJourneyState(futureAnonymous.journeyId as string),
		pool.query<{
			shopify_order_id: string;
			shopify_customer_id: string | null;
			identity_journey_id: string | null;
		}>(
			`
        SELECT
          shopify_order_id,
          shopify_customer_id,
          identity_journey_id::text AS identity_journey_id
        FROM shopify_orders
        WHERE shopify_order_id IN (
          'order-email-1',
          'order-email-2',
          'order-email-3',
          'order-email-after-quarantine'
        )
        ORDER BY shopify_order_id ASC
      `,
		),
	]);

	assert.equal(emailNodeResult.rows[0]?.is_ambiguous, true);
	assert.equal(emailNodeResult.rows[0]?.edge_type, "quarantined");
	assert.equal(
		emailNodeResult.rows[0]?.conflict_code,
		"hashed_email_conflicts_across_authoritative_customers",
	);
	assert.equal(emailNodeResult.rows[0]?.journey_id, firstJourney.journeyId);
	assert.equal(
		futureState.edges.some(
			(edge) =>
				edge.node_type === "hashed_email" &&
				edge.node_key === emailHash &&
				edge.is_active,
		),
		false,
	);
	assert.deepEqual(orderAssignments.rows, [
		{
			shopify_order_id: "order-email-1",
			shopify_customer_id: "sc-email-1",
			identity_journey_id: firstJourney.journeyId,
		},
		{
			shopify_order_id: "order-email-2",
			shopify_customer_id: "sc-email-2",
			identity_journey_id: secondJourney.journeyId,
		},
		{
			shopify_order_id: "order-email-3",
			shopify_customer_id: "sc-email-2",
			identity_journey_id: secondJourney.journeyId,
		},
		{
			shopify_order_id: "order-email-after-quarantine",
			shopify_customer_id: "sc-email-3",
			identity_journey_id: futureAnonymous.journeyId,
		},
	]);
});

test("different authoritative Shopify journeys hard-stop instead of auto-merging", async () => {
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-23T09:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-conflict-1",
			idempotencyKey: "identity-conflict-j1",
			shopifyCustomerId: "sc-1",
			sessionId: "123e4567-e89b-42d3-a456-426614174003",
		}),
	);
	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-23T10:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-conflict-2",
			idempotencyKey: "identity-conflict-j2",
			shopifyCustomerId: "sc-2",
			checkoutToken: "co-conflict-1",
		}),
	);

	const conflicted = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-23T11:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-conflict",
			idempotencyKey: "identity-conflict-join",
			sessionId: "123e4567-e89b-42d3-a456-426614174003",
			checkoutToken: "co-conflict-1",
		}),
	);

	assert.equal(conflicted.outcome, "conflict");
	assert.equal(conflicted.reason, "authoritative_shopify_customer_conflict");

	const { pool } = await import("../src/db/pool.js");
	const ingestionRun = await pool.query<{
		status: string;
		journey_id: string | null;
		outcome_reason: string | null;
	}>(
		`
      SELECT
        status,
        journey_id::text AS journey_id,
        outcome_reason
      FROM identity_edge_ingestion_runs
      WHERE idempotency_key = 'identity-conflict-join'
    `,
	);
	const activeEdges = await pool.query<{ active_edge_count: string }>(
		`
      SELECT COUNT(*)::text AS active_edge_count
      FROM identity_edges
      WHERE is_active = true
    `,
	);

	assert.equal(ingestionRun.rows[0]?.status, "conflicted");
	assert.equal(ingestionRun.rows[0]?.journey_id, null);
	assert.equal(
		ingestionRun.rows[0]?.outcome_reason,
		"authoritative_shopify_customer_conflict",
	);
	assert.equal(activeEdges.rows[0]?.active_edge_count, "4");
});

test("concurrent authoritative conflict attempts both hard-stop without mutating active edge ownership", async () => {
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-23T09:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-concurrency-1",
			idempotencyKey: "identity-concurrency-seed-1",
			shopifyCustomerId: "sc-concurrency-1",
			sessionId: "123e4567-e89b-42d3-a456-426614174012",
		}),
	);
	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-23T10:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-concurrency-2",
			idempotencyKey: "identity-concurrency-seed-2",
			shopifyCustomerId: "sc-concurrency-2",
			checkoutToken: "co-concurrency-1",
		}),
	);

	const { pool } = await import("../src/db/pool.js");
	const runConflictAttempt = async (idempotencyKey: string) => {
		const client = await pool.connect();

		try {
			await client.query("BEGIN");
			const result = await ingestIdentityEdges(client, {
				sourceTimestamp: "2026-04-23T11:00:00.000Z",
				evidenceSource: "tracking_event",
				sourceTable: "tracking_events",
				sourceRecordId: idempotencyKey,
				idempotencyKey,
				sessionId: "123e4567-e89b-42d3-a456-426614174012",
				checkoutToken: "co-concurrency-1",
			});
			await client.query("COMMIT");
			return result;
		} catch (error) {
			await client.query("ROLLBACK");
			throw error;
		} finally {
			client.release();
		}
	};

	const [firstConflict, secondConflict] = await Promise.all([
		runConflictAttempt("identity-concurrency-join-1"),
		runConflictAttempt("identity-concurrency-join-2"),
	]);

	assert.equal(firstConflict.outcome, "conflict");
	assert.equal(firstConflict.reason, "authoritative_shopify_customer_conflict");
	assert.equal(secondConflict.outcome, "conflict");
	assert.equal(
		secondConflict.reason,
		"authoritative_shopify_customer_conflict",
	);

	const [activeEdges, conflictRuns] = await Promise.all([
		pool.query<{ active_edge_count: string }>(
			`
        SELECT COUNT(*)::text AS active_edge_count
        FROM identity_edges
        WHERE is_active = true
      `,
		),
		pool.query<{ conflict_count: string }>(
			`
        SELECT COUNT(*)::text AS conflict_count
        FROM identity_edge_ingestion_runs
        WHERE idempotency_key IN ('identity-concurrency-join-1', 'identity-concurrency-join-2')
          AND status = 'conflicted'
          AND outcome_reason = 'authoritative_shopify_customer_conflict'
      `,
		),
	]);

	assert.equal(activeEdges.rows[0]?.active_edge_count, "4");
	assert.equal(conflictRuns.rows[0]?.conflict_count, "2");
});

test("qualifying identity events only relink historical sessions inside the inclusive 30-day lookback and ignore late older events", async () => {
	const sessionAtBoundary = "123e4567-e89b-42d3-a456-426614174101";
	const sessionOutsideWindow = "123e4567-e89b-42d3-a456-426614174102";
	const { withTransaction, ingestIdentityEdges } = await getIdentityModules();

	await withTransaction(async (client) => {
		await client.query(
			`
        INSERT INTO tracking_sessions (id, first_seen_at, last_seen_at)
        VALUES
          ($1::uuid, '2026-03-26T12:00:00.000Z', '2026-03-26T12:05:00.000Z'),
          ($2::uuid, '2026-03-26T11:59:59.000Z', '2026-03-26T12:04:59.000Z')
      `,
			[sessionAtBoundary, sessionOutsideWindow],
		);
	});

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-03-26T12:00:00.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-boundary",
			idempotencyKey: "identity-lookback-boundary-session",
			sessionId: sessionAtBoundary,
			checkoutToken: "co-boundary-1",
		}),
	);

	const outsideAnonymous = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-03-26T11:59:59.000Z",
			evidenceSource: "tracking_event",
			sourceTable: "tracking_events",
			sourceRecordId: "event-outside",
			idempotencyKey: "identity-lookback-outside-session",
			sessionId: sessionOutsideWindow,
			checkoutToken: "co-outside-1",
		}),
	);

	const authoritative = await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-25T12:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-lookback-1",
			idempotencyKey: "identity-lookback-authoritative",
			sessionId: sessionAtBoundary,
			checkoutToken: "co-boundary-1",
			shopifyCustomerId: "sc-lookback-1",
			hashedEmail: "b".repeat(64),
		}),
	);

	await withTransaction((client) =>
		ingestIdentityEdges(client, {
			sourceTimestamp: "2026-04-20T12:00:00.000Z",
			evidenceSource: "shopify_order_webhook",
			sourceTable: "shopify_orders",
			sourceRecordId: "order-lookback-late",
			idempotencyKey: "identity-lookback-late-event",
			shopifyCustomerId: "sc-lookback-1",
			hashedEmail: "b".repeat(64),
		}),
	);

	assert.equal(authoritative.outcome, "linked");

	const { pool } = await import("../src/db/pool.js");
	const [sessionResult, journeyWindowResult] = await Promise.all([
		pool.query<{
			id: string;
			identity_journey_id: string | null;
		}>(
			`
        SELECT
          id::text AS id,
          identity_journey_id::text AS identity_journey_id
        FROM tracking_sessions
        WHERE id IN ($1::uuid, $2::uuid)
        ORDER BY id ASC
      `,
			[sessionAtBoundary, sessionOutsideWindow],
		),
		pool.query<{
			lookback_window_started_at: Date;
			lookback_window_expires_at: Date;
			last_touch_eligible_at: Date;
		}>(
			`
        SELECT
          lookback_window_started_at,
          lookback_window_expires_at,
          last_touch_eligible_at
        FROM identity_journeys
        WHERE id = $1::uuid
      `,
			[authoritative.journeyId],
		),
	]);

	assert.deepEqual(sessionResult.rows, [
		{
			id: sessionAtBoundary,
			identity_journey_id: authoritative.journeyId,
		},
		{
			id: sessionOutsideWindow,
			identity_journey_id: outsideAnonymous.journeyId,
		},
	]);
	assert.equal(
		journeyWindowResult.rows[0]?.lookback_window_started_at.toISOString(),
		"2026-03-26T12:00:00.000Z",
	);
	assert.equal(
		journeyWindowResult.rows[0]?.lookback_window_expires_at.toISOString(),
		"2026-04-25T12:00:00.000Z",
	);
	assert.equal(
		journeyWindowResult.rows[0]?.last_touch_eligible_at.toISOString(),
		"2026-04-25T12:00:00.000Z",
	);
});
