import assert from "node:assert/strict";
import test from "node:test";

import {
  ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
  ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
  attributionEngineV1JsonSchemas,
  normalizeAttributionCaptureV1,
  normalizeAttributionCreditRecordV1,
  normalizeAttributionConsentState,
  normalizeAttributionDecimalString,
  normalizeAttributionExplainRecordV1,
  normalizeAttributionHintInputV1,
  normalizeAttributionOrderInputV1,
  normalizeAttributionResultRecordV1,
  normalizeAttributionTouchpointInputV1,
  normalizeAttributionUtcTimestamp,
  normalizeOrderAttributionBackfillRequest,
  orderAttributionBackfillEnqueueResponseSchema,
  orderAttributionBackfillJobResponseSchema
} from "../packages/attribution-schema/index.js";

test("attribution consent state defaults to unknown and accepts explicit opt-out", () => {
	assert.equal(normalizeAttributionConsentState(undefined), "unknown");
	assert.equal(normalizeAttributionConsentState("denied"), "denied");
	assert.throws(() => normalizeAttributionConsentState("revoked"));
});

test("attribution capture normalization keeps marketing identifiers under denied consent", () => {
	const capture = normalizeAttributionCaptureV1({
		schema_version: 1,
		roas_radar_session_id: "123e4567-e89b-42d3-a456-426614174000",
		occurred_at: "2026-04-23T12:00:00.000Z",
		captured_at: "2026-04-23T12:00:05.000Z",
		landing_url:
			"https://example.com/?utm_source=Google&utm_medium=CPC&gclid=ABC123",
		referrer_url: "https://google.com/search?q=widget",
		page_url: "https://example.com/products/widget?gclid=ABC123",
		utm_source: "Google",
		utm_medium: "CPC",
		utm_campaign: "Spring",
		utm_content: "Hero",
		utm_term: "Widget",
		gclid: "ABC123",
		gbraid: "GB-123",
		wbraid: "WB-456",
		fbclid: null,
		ttclid: null,
		msclkid: null,
	});

	assert.equal(capture.utm_source, "google");
	assert.equal(capture.utm_medium, "cpc");
	assert.equal(capture.gclid, "ABC123");
	assert.equal(capture.gbraid, "GB-123");
	assert.equal(capture.wbraid, "WB-456");
});

test("attribution capture normalization converts empty strings to null and removes URL fragments", () => {
	const capture = normalizeAttributionCaptureV1({
		schema_version: 1,
		roas_radar_session_id: "123e4567-e89b-42d3-a456-426614174000",
		occurred_at: "2026-04-23T12:00:00Z",
		captured_at: "2026-04-23T12:00:05Z",
		landing_url: " https://example.com/landing?utm_source=Email#hero ",
		referrer_url: "   ",
		page_url: "https://example.com/products/widget?gbraid=GB-123#wrapper",
		utm_source: " Email ",
		utm_medium: " Newsletter ",
		utm_campaign: "",
		utm_content: "   ",
		utm_term: undefined,
		gclid: "",
		gbraid: " GB-123 ",
		wbraid: "   ",
		fbclid: null,
		ttclid: undefined,
		msclkid: "",
	});

	assert.equal(
		capture.landing_url,
		"https://example.com/landing?utm_source=Email",
	);
	assert.equal(capture.referrer_url, null);
	assert.equal(
		capture.page_url,
		"https://example.com/products/widget?gbraid=GB-123",
	);
	assert.equal(capture.utm_source, "email");
	assert.equal(capture.utm_medium, "newsletter");
	assert.equal(capture.utm_campaign, null);
	assert.equal(capture.utm_content, null);
	assert.equal(capture.gclid, null);
	assert.equal(capture.gbraid, "GB-123");
	assert.equal(capture.wbraid, null);
	assert.equal(capture.msclkid, null);
});

test('attribution shared helpers normalize decimal strings and enforce UTC timestamps with offsets', () => {
  assert.equal(normalizeAttributionDecimalString('120'), '120');
  assert.equal(normalizeAttributionDecimalString(120), '120.00');
  assert.equal(normalizeAttributionUtcTimestamp('2026-04-30T12:00:00-05:00'), '2026-04-30T17:00:00.000Z');

  assert.throws(() => normalizeAttributionDecimalString('abc'), /invalid_decimal_string/);
  assert.throws(() => normalizeAttributionUtcTimestamp('2026-04-30T12:00:00'), /invalid_iso_timestamp/);
});

test('order attribution backfill request normalizes defaults', () => {
  const request = normalizeOrderAttributionBackfillRequest({
    startDate: '2026-04-01',
    endDate: '2026-04-15'
  });

	assert.deepEqual(request, {
		startDate: "2026-04-01",
		endDate: "2026-04-15",
		dryRun: true,
		limit: ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
		webOrdersOnly: true,
		skipShopifyWriteback: false,
	});
});

test("order attribution backfill request preserves explicit execution flags at the limit cap", () => {
	const request = normalizeOrderAttributionBackfillRequest({
		startDate: "2026-04-01",
		endDate: "2026-04-15",
		dryRun: false,
		limit: ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
		webOrdersOnly: false,
		skipShopifyWriteback: true,
	});

	assert.deepEqual(request, {
		startDate: "2026-04-01",
		endDate: "2026-04-15",
		dryRun: false,
		limit: ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
		webOrdersOnly: false,
		skipShopifyWriteback: true,
	});
});

test("order attribution backfill request rejects invalid date windows and oversized limits", () => {
	assert.throws(
		() =>
			normalizeOrderAttributionBackfillRequest({
				startDate: "2026-04-15",
				endDate: "2026-04-01",
			}),
		/Start date must be on or before end date\./,
	);

	assert.throws(
		() =>
			normalizeOrderAttributionBackfillRequest({
				startDate: "2026-04-01",
				endDate: "2026-04-15",
				limit: ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT + 1,
			}),
		/Limit must be 5000 or less\./,
	);
});

test("order attribution backfill request rejects non-positive limits", () => {
	assert.throws(
		() =>
			normalizeOrderAttributionBackfillRequest({
				startDate: "2026-04-01",
				endDate: "2026-04-15",
				limit: 0,
			}),
		/Limit must be greater than 0\./,
	);
});

test("order attribution backfill responses accept normalized enqueue and job payloads", () => {
	const enqueueResponse = orderAttributionBackfillEnqueueResponseSchema.parse({
		ok: true,
		jobId: "0ed2f8d7-3867-4bad-a91b-487080ec2a47",
		status: "queued",
		submittedAt: "2026-04-25T12:34:56Z",
		submittedBy: "admin@example.com",
		options: {
			startDate: "2026-04-01",
			endDate: "2026-04-15",
			dryRun: true,
			limit: 250,
			webOrdersOnly: true,
			skipShopifyWriteback: false,
		},
	});

	const jobResponse = orderAttributionBackfillJobResponseSchema.parse({
		...enqueueResponse,
		status: "completed",
		startedAt: "2026-04-25T12:35:00Z",
		completedAt: "2026-04-25T12:36:00Z",
		report: {
			scanned: 250,
			recovered: 120,
			unrecoverable: 130,
			writebackCompleted: 120,
			failures: [
				{
					orderId: "12345",
					code: "shopify_writeback_failed",
					message: "Writeback failed",
				},
			],
		},
		error: null,
	});

	assert.equal(enqueueResponse.submittedAt, "2026-04-25T12:34:56.000Z");
	assert.equal(jobResponse.startedAt, "2026-04-25T12:35:00.000Z");
	assert.equal(
		jobResponse.report?.failures[0]?.code,
		"shopify_writeback_failed",
	);
});

test("order attribution backfill job responses accept queued and processing payloads without reports", () => {
	const queuedJob = orderAttributionBackfillJobResponseSchema.parse({
		ok: true,
		jobId: "0ed2f8d7-3867-4bad-a91b-487080ec2a47",
		status: "queued",
		submittedAt: "2026-04-25T12:34:56Z",
		submittedBy: "admin@example.com",
		startedAt: null,
		completedAt: null,
		options: {
			startDate: "2026-04-01",
			endDate: "2026-04-15",
			dryRun: true,
			limit: 500,
			webOrdersOnly: true,
			skipShopifyWriteback: false,
		},
		report: null,
		error: null,
	});

	const processingJob = orderAttributionBackfillJobResponseSchema.parse({
		...queuedJob,
		status: "processing",
		startedAt: "2026-04-25T12:35:00Z",
	});

	assert.equal(queuedJob.startedAt, null);
	assert.equal(queuedJob.report, null);
	assert.equal(processingJob.startedAt, "2026-04-25T12:35:00.000Z");
	assert.equal(processingJob.completedAt, null);
	assert.equal(processingJob.error, null);
});

test('attribution v1 order, touchpoint, and hint schemas normalize canonical preprocessing records', () => {
  const order = normalizeAttributionOrderInputV1({
    schema_version: 1,
    order_id: 'shopify-order-1',
    order_platform: 'shopify',
    order_occurred_at_utc: '2026-04-30T12:00:00Z',
    order_timestamp_source: 'processed_at',
    currency_code: 'usd',
    subtotal_amount: '100.0',
    total_amount: 120,
    landing_session_id: '123e4567-e89b-42d3-a456-426614174000',
    checkout_token: ' checkout-1 ',
    cart_token: null,
    shopify_customer_id: 'customer-1',
    email_hash: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    source_name: 'web',
    identity_journey_id: '123e4567-e89b-42d3-a456-426614174111',
    raw_order_ref: {
      source: 'shopify_orders.raw_payload'
    }
  });

  const hint = normalizeAttributionHintInputV1({
    hint_source_system: 'shopify_order',
    hint_type: 'landing_site',
    source: 'Google',
    medium: 'CPC',
    campaign: 'Brand',
    content: null,
    term: null,
    click_id_type: 'gclid',
    click_id_value: 'ABC123',
    hint_confidence_score: '0.55',
    hint_confidence_label: 'medium',
    raw_hint_keys: ['utm_source', 'gclid']
  });

  const touchpoint = normalizeAttributionTouchpointInputV1({
    schema_version: 1,
    touchpoint_id: 'event:1',
    session_id: '123e4567-e89b-42d3-a456-426614174000',
    identity_journey_id: '123e4567-e89b-42d3-a456-426614174111',
    touchpoint_occurred_at_utc: '2026-04-30T11:00:00Z',
    touchpoint_captured_at_utc: '2026-04-30T11:00:01Z',
    touchpoint_source_kind: 'session_event',
    ingestion_source: 'browser',
    source: 'Google',
    medium: 'CPC',
    campaign: 'Brand',
    content: null,
    term: null,
    click_id_type: 'gclid',
    click_id_value: 'ABC123',
    evidence_source: 'checkout_token',
    is_direct: false,
    engagement_type: 'click',
    is_synthetic: false,
    is_eligible: true,
    ineligibility_reason: null,
    attribution_reason: 'matched_by_checkout_token',
    attribution_hint: hint
  });

  assert.equal(order.currency_code, 'USD');
  assert.equal(order.total_amount, '120.00');
  assert.equal(hint.source, 'google');
  assert.equal(touchpoint.source, 'google');
  assert.equal(touchpoint.touchpoint_occurred_at_utc, '2026-04-30T11:00:00.000Z');
});

test('attribution v1 canonical schemas normalize omitted nullable fields to explicit nulls', () => {
  const order = normalizeAttributionOrderInputV1({
    schema_version: 1,
    order_id: 'shopify-order-2',
    order_platform: 'shopify',
    order_occurred_at_utc: '2026-04-30T12:00:00Z',
    order_timestamp_source: 'created_at_shopify',
    currency_code: 'usd',
    subtotal_amount: '10.00',
    total_amount: '10.00',
    landing_session_id: undefined,
    checkout_token: undefined,
    cart_token: undefined,
    shopify_customer_id: undefined,
    email_hash: undefined,
    source_name: undefined,
    identity_journey_id: undefined,
    raw_order_ref: undefined
  });

  const touchpoint = normalizeAttributionTouchpointInputV1({
    schema_version: 1,
    touchpoint_id: 'event:2',
    session_id: undefined,
    identity_journey_id: undefined,
    touchpoint_occurred_at_utc: '2026-04-30T11:00:00Z',
    touchpoint_captured_at_utc: '2026-04-30T11:00:01Z',
    touchpoint_source_kind: 'shopify_hint',
    ingestion_source: 'shopify_marketing_hint',
    source: undefined,
    medium: undefined,
    campaign: undefined,
    content: undefined,
    term: undefined,
    click_id_type: undefined,
    click_id_value: undefined,
    evidence_source: 'shopify_marketing_hint',
    is_direct: true,
    engagement_type: 'unknown',
    is_eligible: true,
    attribution_hint: undefined,
    ineligibility_reason: undefined,
    attribution_reason: undefined
  });

  const credit = normalizeAttributionCreditRecordV1({
    run_id: '11111111-1111-4111-8111-111111111111',
    attribution_spec_version: 'v1',
    order_id: 'order-2',
    model_key: 'hinted_fallback_only',
    touchpoint_id: 'tp-2',
    session_id: undefined,
    touchpoint_position: 1,
    occurred_at_utc: '2026-04-28T10:00:00Z',
    source: undefined,
    medium: undefined,
    campaign: undefined,
    content: undefined,
    term: undefined,
    click_id_type: undefined,
    click_id_value: undefined,
    touch_type: 'view',
    is_direct: true,
    evidence_source: 'shopify_marketing_hint',
    is_synthetic: true,
    attribution_reason: 'synthetic_hint',
    credit_weight: '1.00',
    revenue_credit: '10.00',
    is_primary: true
  });

  const explain = normalizeAttributionExplainRecordV1({
    run_id: '11111111-1111-4111-8111-111111111111',
    order_id: 'order-2',
    touchpoint_id: undefined,
    model_key: undefined,
    explain_stage: 'fallback',
    decision: 'fallback_used',
    decision_reason: 'synthetic_hint',
    details_json: {},
    order_occurred_at_utc: undefined,
    created_at_utc: '2026-04-30T12:05:01Z'
  });

  assert.equal(order.raw_order_ref, null);
  assert.equal(order.checkout_token, null);
  assert.equal(touchpoint.attribution_hint, null);
  assert.equal(touchpoint.click_id_type, null);
  assert.equal(credit.source, null);
  assert.equal(credit.click_id_type, null);
  assert.equal(explain.touchpoint_id, null);
  assert.equal(explain.model_key, null);
  assert.equal(explain.order_occurred_at_utc, null);
});

test('attribution v1 result, credit, and explainability schemas normalize canonical output records', () => {
  const result = normalizeAttributionResultRecordV1({
    run_id: '11111111-1111-4111-8111-111111111111',
    attribution_spec_version: 'v1',
    order_id: 'order-1',
    model_key: 'last_non_direct',
    allocation_status: 'attributed',
    winner_touchpoint_id: 'tp-1',
    winner_session_id: '22222222-2222-4222-8222-222222222222',
    winner_evidence_source: 'landing_session_id',
    winner_attribution_reason: 'matched_by_landing_session',
    total_credit_weight: '1',
    total_revenue_credited: 120,
    touchpoint_count_considered: 2,
    eligible_click_count: 2,
    eligible_view_count: 0,
    lookback_rule_applied: '28d_click',
    winner_selection_rule: 'last_non_direct',
    direct_suppression_applied: true,
    deterministic_block_applied: false,
    normalization_failures_count: 0,
    generated_at_utc: '2026-04-30T12:05:00Z'
  });

  const credit = normalizeAttributionCreditRecordV1({
    run_id: '11111111-1111-4111-8111-111111111111',
    attribution_spec_version: 'v1',
    order_id: 'order-1',
    model_key: 'last_non_direct',
    touchpoint_id: 'tp-1',
    session_id: '22222222-2222-4222-8222-222222222222',
    touchpoint_position: 1,
    occurred_at_utc: '2026-04-28T10:00:00Z',
    source: 'google',
    medium: 'cpc',
    campaign: 'spring-sale',
    content: 'hero',
    term: 'widget',
    click_id_type: 'gclid',
    click_id_value: 'ABC123',
    touch_type: 'click',
    is_direct: false,
    evidence_source: 'landing_session_id',
    is_synthetic: false,
    attribution_reason: 'matched_by_landing_session',
    credit_weight: 1,
    revenue_credit: '120.00',
    is_primary: true
  });

  const explain = normalizeAttributionExplainRecordV1({
    run_id: '11111111-1111-4111-8111-111111111111',
    order_id: 'order-1',
    touchpoint_id: 'tp-1',
    model_key: 'last_non_direct',
    explain_stage: 'model_scoring',
    decision: 'winner',
    decision_reason: 'matched_by_landing_session',
    details_json: {
      creditWeight: 1
    },
    order_occurred_at_utc: '2026-04-30T12:00:00Z',
    created_at_utc: '2026-04-30T12:05:01Z'
  });

  assert.equal(result.total_credit_weight, '1');
  assert.equal(result.total_revenue_credited, '120.00');
  assert.equal(result.generated_at_utc, '2026-04-30T12:05:00.000Z');
  assert.equal(credit.credit_weight, '1.00');
  assert.equal(credit.occurred_at_utc, '2026-04-28T10:00:00.000Z');
  assert.equal(explain.created_at_utc, '2026-04-30T12:05:01.000Z');
});

test('attribution v1 schemas reject timestamps without timezone offsets', () => {
  assert.throws(
    () =>
      normalizeAttributionOrderInputV1({
        schema_version: 1,
        order_id: 'shopify-order-3',
        order_platform: 'shopify',
        order_occurred_at_utc: '2026-04-30T12:00:00',
        order_timestamp_source: 'processed_at',
        currency_code: 'USD',
        subtotal_amount: '10.00',
        total_amount: '10.00',
        landing_session_id: null,
        checkout_token: null,
        cart_token: null,
        shopify_customer_id: null,
        email_hash: null,
        source_name: null,
        identity_journey_id: null,
        raw_order_ref: null
      }),
    /Invalid ISO-8601 timestamp/
  );

  assert.throws(
    () =>
      normalizeAttributionResultRecordV1({
        run_id: '11111111-1111-4111-8111-111111111111',
        attribution_spec_version: 'v1',
        order_id: 'order-3',
        model_key: 'last_touch',
        allocation_status: 'unattributed',
        winner_touchpoint_id: null,
        winner_session_id: null,
        winner_evidence_source: null,
        winner_attribution_reason: null,
        total_credit_weight: '0.00',
        total_revenue_credited: '0.00',
        touchpoint_count_considered: 0,
        eligible_click_count: 0,
        eligible_view_count: 0,
        lookback_rule_applied: 'mixed',
        winner_selection_rule: 'last_touch',
        direct_suppression_applied: false,
        deterministic_block_applied: false,
        normalization_failures_count: 0,
        generated_at_utc: '2026-04-30T12:05:00'
      }),
    /Invalid ISO-8601 timestamp/
  );
});

test('attribution engine package publishes six JSON schema documents for canonical v1 records', () => {
  assert.deepEqual(Object.keys(attributionEngineV1JsonSchemas).sort(), [
    'AttributionCreditRecordV1',
    'AttributionExplainRecordV1',
    'AttributionHintInputV1',
    'AttributionOrderInputV1',
    'AttributionResultRecordV1',
    'AttributionTouchpointInputV1'
  ]);

  assert.equal(attributionEngineV1JsonSchemas.AttributionOrderInputV1.title, 'AttributionOrderInputV1');
  assert.equal(attributionEngineV1JsonSchemas.AttributionTouchpointInputV1.type, 'object');
  assert.equal(attributionEngineV1JsonSchemas.AttributionResultRecordV1.additionalProperties, false);
});
