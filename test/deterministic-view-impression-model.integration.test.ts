import assert from "node:assert/strict";
import test from "node:test";
import type { PoolClient } from "pg";

import { buildRawPayloadFixture } from "./integration-test-helpers.js";

process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test";

const { pool } = await import("../src/db/pool.js");
const { persistDeterministicViewImpressionModelOutputs } = await import(
	"../src/modules/attribution/deterministic-view-impression-model.js"
);
const { resetE2EDatabase } = await import("./e2e-harness.js");

async function seedOrderAndRun(): Promise<{ runId: string; orderId: string }> {
	const orderId = "deterministic-order-1";
	const orderFixture = buildRawPayloadFixture({ id: orderId }, orderId);
	const run = await pool.query<{ id: string }>(
		`
      INSERT INTO attribution_runs (
        run_status,
        trigger_source,
        run_metadata
      )
      VALUES ('running', 'deterministic_test', '{"deterministicViewImpressionAttributionEnabled":true}'::jsonb)
      RETURNING id::text
    `,
	);

	await pool.query(
		`
      INSERT INTO shopify_orders (
        shopify_order_id,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        source_name,
        payload_external_id,
        payload_size_bytes,
        payload_hash,
        raw_payload,
        ingested_at
      )
      VALUES (
        $1,
        'USD',
        '200.00',
        '200.00',
        '2026-05-26T12:00:00Z',
        'web',
        $2,
        $3,
        $4,
        $5::jsonb,
        now()
      )
    `,
		[
			orderId,
			orderFixture.payloadExternalId,
			orderFixture.payloadSizeBytes,
			orderFixture.payloadHash,
			orderFixture.rawPayloadJson,
		],
	);

	await pool.query(
		`
      INSERT INTO attribution_order_inputs (
        run_id,
        order_id,
        order_occurred_at_utc,
        order_timestamp_source,
        currency_code,
        subtotal_amount,
        total_amount,
        source_name,
        raw_order_ref
      )
      VALUES (
        $1::uuid,
        $2,
        '2026-05-26T12:00:00Z',
        'processed_at',
        'USD',
        '200.00',
        '200.00',
        'web',
        '{}'::jsonb
      )
    `,
		[run.rows[0].id, orderId],
	);

	await pool.query(
		`
      INSERT INTO attribution_touchpoint_inputs (
        run_id,
        order_id,
        touchpoint_id,
        touchpoint_occurred_at_utc,
        touchpoint_captured_at_utc,
        touchpoint_source_kind,
        ingestion_source,
        source,
        medium,
        campaign,
        content,
        term,
        evidence_source,
        is_direct,
        engagement_type,
        is_eligible,
        attribution_reason
      )
      VALUES (
        $1::uuid,
        $2,
        'touch-1',
        '2026-05-25T12:00:00Z',
        '2026-05-25T12:00:01Z',
        'session_event',
        'browser',
        'meta',
        'paid_social',
        'campaign-1',
        'adset-1',
        'ad-1',
        'landing_session_id',
        false,
        'click',
        true,
        'matched_by_landing_session'
      )
    `,
		[run.rows[0].id, orderId],
	);

	return { runId: run.rows[0].id, orderId };
}

async function seedDeterministicFacts(): Promise<void> {
	const source = await pool.query<{ id: string }>(
		`
      INSERT INTO deterministic_event_sources (
        source_key,
        platform,
        account_id,
        evidence_origin,
        source_type,
        external_request_id,
        api_version,
        api_endpoint,
        api_request_timestamp_utc,
        api_account_id,
        api_request_id
      )
      VALUES (
        'deterministic-source-1',
        'meta_ads',
        'act_123',
        'api',
        'ads_insights',
        'trace-123',
        'v20.0',
        'insights',
        '2026-05-26T13:00:00Z',
        'act_123',
        'trace-123'
      )
      RETURNING id::text
    `,
	);

	await pool.query(
		`
      INSERT INTO deterministic_event_facts (
        source_id,
        platform,
        account_id,
        campaign_id,
        adset_id,
        ad_id,
        event_type,
        fact_date,
        event_count,
        evidence_origin,
        platform_verified,
        normalization_status
      )
      VALUES
        ($1::bigint, 'meta_ads', 'act_123', 'campaign-1', 'adset-1', 'ad-1', 'view', '2026-05-25', 6, 'api', true, 'normalized'),
        ($1::bigint, 'meta_ads', 'act_123', 'campaign-1', 'adset-1', 'ad-1', 'impression', '2026-05-25', 12, 'api', true, 'normalized'),
        ($1::bigint, 'meta_ads', 'act_123', 'campaign-1', 'adset-1', 'ad-1', 'view', '2026-05-19', 50, 'api', true, 'normalized'),
        ($1::bigint, 'meta_ads', 'act_123', 'campaign-1', 'adset-1', 'ad-1', 'view', '2026-05-25', 4, 'api', false, 'normalized'),
        ($1::bigint, 'meta_ads', 'act_123', 'campaign-other', NULL, NULL, 'view', '2026-05-25', 99, 'api', true, 'normalized')
    `,
		[source.rows[0].id],
	);
}

test(
	"deterministic view/impression model persists idempotent outputs apart from click model tables",
	{ concurrency: false },
	async () => {
		await resetE2EDatabase();
		const { runId, orderId } = await seedOrderAndRun();
		await seedDeterministicFacts();

		const first = await persistDeterministicViewImpressionModelOutputs(
			pool as unknown as PoolClient,
			{
				runId,
				orderId,
				orderOccurredAtUtc: "2026-05-26T12:00:00.000Z",
				enabled: true,
			},
		);
		const second = await persistDeterministicViewImpressionModelOutputs(
			pool as unknown as PoolClient,
			{
				runId,
				orderId,
				orderOccurredAtUtc: "2026-05-26T12:00:00.000Z",
				enabled: true,
			},
		);

		assert.deepEqual(first, { enabled: true, insertedRows: 2 });
		assert.deepEqual(second, { enabled: true, insertedRows: 2 });

		const outputs = await pool.query<{
			model_key: string;
			event_type: string;
			contribution_weight: string;
			contributed_event_count: string;
		}>(
			`
        SELECT
          model_key,
          event_type,
          contribution_weight::text,
          contributed_event_count::text
        FROM deterministic_model_outputs
        WHERE run_id = $1::uuid
          AND order_id = $2
        ORDER BY model_key ASC
      `,
			[runId, orderId],
		);

		assert.deepEqual(outputs.rows, [
			{
				model_key: "deterministic_impressions",
				event_type: "impression",
				contribution_weight: "1.00000000",
				contributed_event_count: "12.000000",
			},
			{
				model_key: "deterministic_views",
				event_type: "view",
				contribution_weight: "1.00000000",
				contributed_event_count: "6.000000",
			},
		]);

		const counts = await pool.query<{
			deterministic_outputs: string;
			click_summaries: string;
			click_credits: string;
		}>(
			`
        SELECT
          (SELECT count(*)::text FROM deterministic_model_outputs) AS deterministic_outputs,
          (SELECT count(*)::text FROM attribution_model_summaries) AS click_summaries,
          (SELECT count(*)::text FROM attribution_model_credits) AS click_credits
      `,
		);

		assert.deepEqual(counts.rows[0], {
			deterministic_outputs: "2",
			click_summaries: "0",
			click_credits: "0",
		});
	},
);

test(
	"deterministic absence path returns safely without click model side effects",
	{ concurrency: false },
	async () => {
		await resetE2EDatabase();
		const { runId, orderId } = await seedOrderAndRun();

		const result = await persistDeterministicViewImpressionModelOutputs(
			pool as unknown as PoolClient,
			{
				runId,
				orderId,
				orderOccurredAtUtc: "2026-05-26T12:00:00.000Z",
				enabled: true,
			},
		);

		assert.deepEqual(result, { enabled: true, insertedRows: 0 });

		const counts = await pool.query<{
			deterministic_outputs: string;
			click_summaries: string;
			click_credits: string;
		}>(
			`
        SELECT
          (SELECT count(*)::text FROM deterministic_model_outputs) AS deterministic_outputs,
          (SELECT count(*)::text FROM attribution_model_summaries) AS click_summaries,
          (SELECT count(*)::text FROM attribution_model_credits) AS click_credits
      `,
		);

		assert.deepEqual(counts.rows[0], {
			deterministic_outputs: "0",
			click_summaries: "0",
			click_credits: "0",
		});
	},
);

test.after(async () => {
	await pool.end();
});
