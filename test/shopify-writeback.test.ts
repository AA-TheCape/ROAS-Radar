import assert from "node:assert/strict";
import test from "node:test";
import type { Pool } from "pg";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

async function getModules() {
	const poolModule = await import("../src/db/pool.js");
	const writebackModule = await import("../src/modules/shopify/writeback.js");

	return {
		pool: poolModule.pool,
		enqueueShopifyOrderWriteback: writebackModule.enqueueShopifyOrderWriteback,
		processShopifyOrderWritebackQueue:
			writebackModule.processShopifyOrderWritebackQueue,
		reconcileRecentShopifyOrderAttributes:
			writebackModule.reconcileRecentShopifyOrderAttributes,
		testUtils: writebackModule.__shopifyWritebackTestUtils,
	};
}

function buildCanonicalShopifyAttributes(
	sessionId: string,
): Array<{ key: string; value: string }> {
	return [
		{ key: "schema_version", value: "1" },
		{ key: "roas_radar_session_id", value: sessionId },
		{
			key: "landing_url",
			value:
				"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale",
		},
		{ key: "referrer_url", value: "https://www.google.com/search?q=widget" },
		{
			key: "page_url",
			value:
				"https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123",
		},
		{ key: "utm_source", value: "google" },
		{ key: "utm_medium", value: "cpc" },
		{ key: "utm_campaign", value: "spring-sale" },
		{ key: "utm_content", value: "hero" },
		{ key: "utm_term", value: "widgets" },
		{ key: "gclid", value: "GCLID-123" },
		{ key: "gbraid", value: "GBRAID-123" },
		{ key: "wbraid", value: "WBRAID-123" },
	];
}

function buildRawPayload(
	attributes: Array<{ key: string; value: string }> = [],
): string {
	return JSON.stringify({
		id: "shopify-order-payload",
		note_attributes: attributes.map((attribute) => ({
			name: attribute.key,
			value: attribute.value,
		})),
	});
}

async function ensureDeadLetterTables(pool: Pool): Promise<void> {
	await pool.query(`
    CREATE TABLE IF NOT EXISTS event_dead_letters (
      id bigserial PRIMARY KEY,
      event_type text NOT NULL,
      source_table text NOT NULL,
      source_record_id text NOT NULL,
      source_queue_key text,
      status text NOT NULL DEFAULT 'pending_replay',
      first_failed_at timestamptz NOT NULL DEFAULT now(),
      last_failed_at timestamptz NOT NULL DEFAULT now(),
      last_error_message text,
      payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      error_context jsonb NOT NULL DEFAULT '{}'::jsonb,
      failure_count integer NOT NULL DEFAULT 1,
      replayed_at timestamptz,
      last_replay_run_id bigint,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

	await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS event_dead_letters_source_uidx
      ON event_dead_letters (event_type, source_table, source_record_id)
  `);

	await pool.query(`
    CREATE TABLE IF NOT EXISTS event_replay_runs (
      id bigserial PRIMARY KEY,
      filters jsonb NOT NULL DEFAULT '{}'::jsonb,
      candidate_count integer NOT NULL DEFAULT 0,
      replayed_count integer NOT NULL DEFAULT 0,
      skipped_count integer NOT NULL DEFAULT 0,
      failed_count integer NOT NULL DEFAULT 0,
      started_at timestamptz NOT NULL DEFAULT now(),
      completed_at timestamptz
    )
  `);

	await pool.query(`
    CREATE TABLE IF NOT EXISTS event_replay_run_items (
      id bigserial PRIMARY KEY,
      replay_run_id bigint NOT NULL REFERENCES event_replay_runs(id) ON DELETE CASCADE,
      dead_letter_id bigint NOT NULL REFERENCES event_dead_letters(id) ON DELETE CASCADE,
      source_table text NOT NULL,
      source_record_id text NOT NULL,
      outcome text NOT NULL,
      message text,
      created_at timestamptz NOT NULL DEFAULT now()
    )
  `);
}

async function resetDatabase(): Promise<void> {
	const { pool } = await getModules();
	await ensureDeadLetterTables(pool);

	await pool.query(`
    TRUNCATE TABLE
      shopify_order_writeback_jobs,
      shopify_orders,
      session_attribution_touch_events,
      session_attribution_identities,
      tracking_events,
      tracking_sessions,
      event_replay_run_items,
      event_replay_runs,
      event_dead_letters
    RESTART IDENTITY CASCADE
  `);
}

async function insertTrackingSession(
	pool: Pool,
	sessionId: string,
): Promise<void> {
	await pool.query(
		`
      INSERT INTO tracking_sessions (
        id,
        first_seen_at,
        last_seen_at,
        landing_page,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid
      )
      VALUES (
        $1::uuid,
        '2026-04-20T10:00:00.000Z',
        '2026-04-20T10:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123'
      )
    `,
		[sessionId],
	);
}

async function insertSessionAttributionIdentity(
	pool: Pool,
	sessionId: string,
): Promise<void> {
	await pool.query(
		`
      INSERT INTO session_attribution_identities (
        roas_radar_session_id,
        first_captured_at,
        last_captured_at,
        retained_until,
        landing_url,
        referrer_url,
        initial_utm_source,
        initial_utm_medium,
        initial_utm_campaign,
        initial_utm_content,
        initial_utm_term,
        initial_gclid,
        initial_gbraid,
        initial_wbraid
      )
      VALUES (
        $1::uuid,
        '2026-04-20T10:00:00.000Z',
        '2026-04-20T10:05:00.000Z',
        '2026-05-20T10:05:00.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123'
      )
    `,
		[sessionId],
	);
}

async function insertSessionAttributionTouchEvent(
	pool: Pool,
	sessionId: string,
): Promise<void> {
	await pool.query(
		`
      INSERT INTO session_attribution_touch_events (
        roas_radar_session_id,
        event_type,
        occurred_at,
        captured_at,
        retained_until,
        page_url,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        payload_size_bytes,
        raw_payload
      )
      VALUES (
        $1::uuid,
        'page_view',
        '2026-04-20T10:04:00.000Z',
        '2026-04-20T10:04:05.000Z',
        '2026-05-20T10:04:05.000Z',
        'https://store.example/products/widget?utm_source=google&utm_medium=cpc&utm_campaign=spring-sale&gbraid=GBRAID-123',
        'https://www.google.com/search?q=widget',
        'google',
        'cpc',
        'spring-sale',
        'hero',
        'widgets',
        'GCLID-123',
        'GBRAID-123',
        'WBRAID-123',
        octet_length(convert_to('{}', 'utf8')),
        '{}'::jsonb
      )
    `,
		[sessionId],
	);
}

async function insertShopifyOrder(
	pool: Pool,
	input: {
		shopifyOrderId: string;
		sessionId?: string | null;
		rawPayload?: string;
		processedAt?: string;
	},
): Promise<void> {
	await pool.query(
		`
      INSERT INTO shopify_orders (
        shopify_order_id,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        landing_session_id,
        raw_payload,
        ingested_at
      )
      VALUES ($1, 'USD', '100.00', '100.00', $2, $3::uuid, $4::jsonb, now())
    `,
		[
			input.shopifyOrderId,
			input.processedAt ?? "2026-04-22T12:00:00.000Z",
			input.sessionId ?? null,
			input.rawPayload ?? buildRawPayload(),
		],
	);
}

async function seedCanonicalOrder(
	pool: Pool,
	shopifyOrderId: string,
	sessionId: string,
	rawPayload?: string,
): Promise<void> {
	await insertTrackingSession(pool, sessionId);
	await insertSessionAttributionIdentity(pool, sessionId);
	await insertSessionAttributionTouchEvent(pool, sessionId);
	await insertShopifyOrder(pool, {
		shopifyOrderId,
		sessionId,
		rawPayload,
	});
}

test.beforeEach(async () => {
	const { testUtils } = await getModules();
	testUtils.reset();
	await resetDatabase();
});

test.after(async () => {
	const { pool } = await getModules();
	await pool.end();
});

test("enqueueShopifyOrderWriteback upserts one queue row per order idempotently", async () => {
	const { pool, enqueueShopifyOrderWriteback } = await getModules();
	const sessionId = "123e4567-e89b-42d3-a456-426614174001";
	await seedCanonicalOrder(pool, "1001", sessionId);

	await enqueueShopifyOrderWriteback("1001", "shopify_order_upserted");
	await enqueueShopifyOrderWriteback(
		"1001",
		"reconciliation_missing_canonical_attributes",
	);

	const result = await pool.query<{
		total: string;
		requested_reason: string;
		queue_key: string;
		status: string;
	}>(
		`
      SELECT
        COUNT(*)::text AS total,
        MAX(requested_reason) AS requested_reason,
        MAX(queue_key) AS queue_key,
        MAX(status) AS status
      FROM shopify_order_writeback_jobs
      WHERE shopify_order_id = '1001'
    `,
	);

	assert.equal(result.rows[0].total, "1");
	assert.equal(
		result.rows[0].requested_reason,
		"reconciliation_missing_canonical_attributes",
	);
	assert.equal(result.rows[0].queue_key, "shopify_order:1001");
	assert.equal(result.rows[0].status, "pending");
});

test("processShopifyOrderWritebackQueue builds canonical attributes and completes claimed jobs", async () => {
	const {
		pool,
		enqueueShopifyOrderWriteback,
		processShopifyOrderWritebackQueue,
		testUtils,
	} = await getModules();
	const sessionId = "123e4567-e89b-42d3-a456-426614174002";
	await seedCanonicalOrder(pool, "1002", sessionId);

	testUtils.setWritebackProcessor(async () => undefined);

	await enqueueShopifyOrderWriteback("1002", "shopify_order_upserted");
	const report = await processShopifyOrderWritebackQueue({
		workerId: "test-shopify-writeback-worker",
		limit: 10,
		now: new Date("2100-04-23T00:00:00.000Z"),
	});

	assert.equal(report.claimedJobs, 1);
	assert.equal(report.completedJobs, 1);
	assert.equal(report.retriedJobs, 0);
	assert.equal(report.deadLetteredJobs, 0);

	const appliedWritebacks = testUtils.getAppliedWritebacks();
	assert.equal(appliedWritebacks.length, 1);
	assert.equal(appliedWritebacks[0].shopifyOrderId, "1002");
	assert.deepEqual(
		appliedWritebacks[0].attributes,
		buildCanonicalShopifyAttributes(sessionId),
	);

	const queueState = await pool.query<{ status: string; attempts: number }>(
		`
      SELECT status, attempts
      FROM shopify_order_writeback_jobs
      WHERE shopify_order_id = '1002'
    `,
	);

	assert.equal(queueState.rows[0].status, "completed");
	assert.equal(queueState.rows[0].attempts, 0);
});

test("processShopifyOrderWritebackQueue marks transient failures for retry", async () => {
	const {
		pool,
		enqueueShopifyOrderWriteback,
		processShopifyOrderWritebackQueue,
		testUtils,
	} = await getModules();
	const sessionId = "123e4567-e89b-42d3-a456-426614174003";
	await seedCanonicalOrder(pool, "1003", sessionId);

	testUtils.setWritebackProcessor(async () => {
		throw Object.assign(new Error("shopify temporarily unavailable"), {
			retryable: true,
			statusCode: 503,
		});
	});

	await enqueueShopifyOrderWriteback("1003", "shopify_order_upserted");
	const report = await processShopifyOrderWritebackQueue({
		workerId: "test-shopify-writeback-retry",
		limit: 10,
		now: new Date("2100-04-23T00:00:00.000Z"),
	});

	assert.equal(report.claimedJobs, 1);
	assert.equal(report.retriedJobs, 1);
	assert.equal(report.deadLetteredJobs, 0);

	const queueState = await pool.query<{
		status: string;
		attempts: number;
		last_error: string | null;
	}>(
		`
      SELECT status, attempts, last_error
      FROM shopify_order_writeback_jobs
      WHERE shopify_order_id = '1003'
    `,
	);

	assert.equal(queueState.rows[0].status, "retry");
	assert.equal(queueState.rows[0].attempts, 1);
	assert.equal(
		queueState.rows[0].last_error,
		"shopify temporarily unavailable",
	);
});

test("processShopifyOrderWritebackQueue dead-letters terminal failures with retained payload context", async () => {
	const {
		pool,
		enqueueShopifyOrderWriteback,
		processShopifyOrderWritebackQueue,
		testUtils,
	} = await getModules();
	const sessionId = "123e4567-e89b-42d3-a456-426614174004";
	await seedCanonicalOrder(pool, "1004", sessionId);

	testUtils.setWritebackProcessor(async () => {
		throw Object.assign(new Error("shopify rejected note attributes"), {
			retryable: false,
			statusCode: 422,
		});
	});

	await enqueueShopifyOrderWriteback("1004", "shopify_order_upserted");
	const report = await processShopifyOrderWritebackQueue({
		workerId: "test-shopify-writeback-dead-letter",
		limit: 10,
		now: new Date("2100-04-23T00:00:00.000Z"),
	});

	assert.equal(report.claimedJobs, 1);
	assert.equal(report.deadLetteredJobs, 1);
	assert.equal(report.retriedJobs, 0);

	const queueState = await pool.query<{
		id: string;
		status: string;
		attempts: number;
		dead_lettered_at: Date | null;
	}>(
		`
      SELECT id::text AS id, status, attempts, dead_lettered_at
      FROM shopify_order_writeback_jobs
      WHERE shopify_order_id = '1004'
    `,
	);

	assert.equal(queueState.rows[0].status, "failed");
	assert.equal(queueState.rows[0].attempts, 1);
	assert.ok(queueState.rows[0].dead_lettered_at);

	const deadLetter = await pool.query<{
		event_type: string;
		source_table: string;
		source_record_id: string;
		shopify_order_id: string;
	}>(
		`
      SELECT
        event_type,
        source_table,
        source_record_id,
        payload->>'shopifyOrderId' AS shopify_order_id
      FROM event_dead_letters
      WHERE source_table = 'shopify_order_writeback_jobs'
      ORDER BY id DESC
      LIMIT 1
    `,
	);

	assert.equal(deadLetter.rows[0].event_type, "shopify_writeback_failed");
	assert.equal(deadLetter.rows[0].source_table, "shopify_order_writeback_jobs");
	assert.equal(deadLetter.rows[0].source_record_id, queueState.rows[0].id);
	assert.equal(deadLetter.rows[0].shopify_order_id, "1004");
});

test("reconcileRecentShopifyOrderAttributes requeues recent orders with missing canonical Shopify attributes idempotently", async () => {
	const { pool, reconcileRecentShopifyOrderAttributes } = await getModules();
	const sessionId = "123e4567-e89b-42d3-a456-426614174005";
	await seedCanonicalOrder(pool, "1001", sessionId, buildRawPayload());

	const firstReport = await reconcileRecentShopifyOrderAttributes({
		workerId: "test-shopify-reconciliation-1",
		limit: 10,
		lookbackDays: 30,
		now: new Date("2026-04-23T00:00:00.000Z"),
	});

	assert.equal(firstReport.scannedOrders, 1);
	assert.equal(firstReport.ordersNeedingWriteback, 1);
	assert.equal(firstReport.requeuedOrders, 1);

	const firstQueueState = await pool.query<{ total: string }>(`
    SELECT COUNT(*)::text AS total
    FROM shopify_order_writeback_jobs
    WHERE shopify_order_id = '1001'
  `);
	assert.equal(firstQueueState.rows[0].total, "1");

	const secondReport = await reconcileRecentShopifyOrderAttributes({
		workerId: "test-shopify-reconciliation-2",
		limit: 10,
		lookbackDays: 30,
		now: new Date("2026-04-23T00:00:00.000Z"),
	});

	assert.equal(secondReport.requeuedOrders, 1);

	const secondQueueState = await pool.query<{ total: string }>(`
    SELECT COUNT(*)::text AS total
    FROM shopify_order_writeback_jobs
    WHERE shopify_order_id = '1001'
  `);
	assert.equal(secondQueueState.rows[0].total, "1");
});

test("reconcileRecentShopifyOrderAttributes reports up-to-date, skipped, and failed orders separately", async () => {
	const { pool, reconcileRecentShopifyOrderAttributes } = await getModules();

	const needsWritebackSession = "123e4567-e89b-42d3-a456-426614174006";
	const upToDateSession = "123e4567-e89b-42d3-a456-426614174007";
	const failedSession = "123e4567-e89b-42d3-a456-426614174008";

	await seedCanonicalOrder(
		pool,
		"2001",
		needsWritebackSession,
		buildRawPayload(),
	);
	await seedCanonicalOrder(
		pool,
		"2002",
		upToDateSession,
		buildRawPayload(buildCanonicalShopifyAttributes(upToDateSession)),
	);
	await insertShopifyOrder(pool, {
		shopifyOrderId: "2003",
		sessionId: null,
		rawPayload: buildRawPayload(),
	});
	await insertTrackingSession(pool, failedSession);
	await insertShopifyOrder(pool, {
		shopifyOrderId: "2004",
		sessionId: failedSession,
		rawPayload: buildRawPayload(),
	});

	const report = await reconcileRecentShopifyOrderAttributes({
		workerId: "test-shopify-reconciliation-report",
		limit: 10,
		lookbackDays: 30,
		now: new Date("2026-04-24T00:00:00.000Z"),
	});

	assert.equal(report.scannedOrders, 4);
	assert.equal(report.ordersNeedingWriteback, 1);
	assert.equal(report.requeuedOrders, 1);
	assert.equal(report.upToDateOrders, 1);
	assert.equal(report.skippedOrders, 1);
	assert.equal(report.failedOrders, 1);
});
