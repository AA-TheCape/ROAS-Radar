import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
  ORDER_ATTRIBUTION_BACKFILL_MAX_LIMIT,
  RECOVERY_SOURCE_PRECEDENCE,
  attributionEngineV1JsonSchemas,
  attributionQaPayloadV1JsonSchema,
  attributionQaPayloadV1NoMatchFixture,
  attributionQaPayloadV1SuccessFixture,
  campaignMetadataRefreshPayloadJsonSchema,
  ga4EnrichmentFieldsJsonSchema,
  normalizeCampaignMetadataRefreshPayload,
  normalizeAttributionCaptureV1,
  normalizeAttributionCreditRecordV1,
  normalizeAttributionConsentState,
  normalizeAttributionDecimalString,
  normalizeAttributionExplainRecordV1,
  normalizeAttributionHintInputV1,
  normalizeAttributionOrderInputV1,
  normalizeAttributionQaPayloadV1,
  normalizeAttributionResultRecordV1,
  normalizeAttributionTouchpointInputV1,
  normalizeAttributionUtcTimestamp,
  normalizeGa4EnrichmentFields,
  normalizeMetaDeterministicAttributionAggregateV1,
  normalizeMetaDeterministicAttributionIdentityTupleV1,
  normalizeOrderAttributionBackfillRequest,
  normalizeRecoveryJobReport,
  normalizeRecoveryJobRequest,
  normalizeShopifyAttributionHint,
  normalizeShopifyRawPayloadSnapshot,
  orderAttributionBackfillEnqueueResponseSchema,
  orderAttributionBackfillJobResponseSchema,
  recoveryJobReportJsonSchema,
  recoveryJobRequestJsonSchema,
  shopifyAttributionHintJsonSchema,
  shopifyRawPayloadSnapshotJsonSchema
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

test("recovery job contracts normalize requests and require Shopify > GA4 > ad platforms precedence", () => {
	const request = normalizeRecoveryJobRequest({
		schemaVersion: 1,
		jobType: "shopify_attribution_hint_recovery",
		initiatedBy: "ops@example.com",
		timeRangeStart: "2026-05-01T00:00:00Z",
		timeRangeEnd: "2026-05-02T00:00:00Z",
	});

	assert.equal(request.mode, "manual");
	assert.equal(request.dryRun, true);
	assert.equal(request.scopeKey, "global");
	assert.deepEqual(RECOVERY_SOURCE_PRECEDENCE, [
		"shopify",
		"ga4",
		"ad_platforms",
	]);

	const report = normalizeRecoveryJobReport({
		schemaVersion: 1,
		jobId: "run-1",
		jobType: "shopify_attribution_hint_recovery",
		status: "succeeded",
		startedAt: "2026-05-01T00:00:01Z",
		completedAt: "2026-05-01T00:00:02Z",
		dryRun: true,
		sourcePrecedence: ["shopify", "ga4", "ad_platforms"],
		counters: {
			recordsDiscovered: 1,
			recordsProcessed: 1,
			recordsSucceeded: 1,
			recordsFailed: 0,
			recordsSkipped: 0,
			sideEffectsAttempted: 0,
			sideEffectsSucceeded: 0,
			sideEffectsSuppressed: 1,
		},
	});

	assert.equal(report.completedAt, "2026-05-01T00:00:02.000Z");
	assert.throws(
		() =>
			normalizeRecoveryJobReport({
				...report,
				sourcePrecedence: ["ga4", "shopify", "ad_platforms"],
			}),
		/Invalid literal value/,
	);
});

test("recovery job contracts reject invalid windows and classify failure retryability", () => {
	assert.throws(
		() =>
			normalizeRecoveryJobRequest({
				schemaVersion: 1,
				jobType: "ga4_fallback_unattributed_recovery",
				initiatedBy: "ops@example.com",
				timeRangeStart: "2026-05-02T00:00:00Z",
				timeRangeEnd: "2026-05-01T00:00:00Z",
			}),
		/timeRangeStart must be on or before timeRangeEnd/,
	);

	const report = normalizeRecoveryJobReport({
		schemaVersion: 1,
		jobId: "run-2",
		jobType: "ga4_fallback_unattributed_recovery",
		status: "dead_lettered",
		startedAt: "2026-05-01T00:00:00Z",
		completedAt: "2026-05-01T00:05:00Z",
		dryRun: false,
		sourcePrecedence: ["shopify", "ga4", "ad_platforms"],
		counters: {
			recordsDiscovered: 2,
			recordsProcessed: 2,
			recordsSucceeded: 1,
			recordsFailed: 1,
			recordsSkipped: 0,
			sideEffectsAttempted: 1,
			sideEffectsSucceeded: 1,
			sideEffectsSuppressed: 0,
		},
		failures: [
			{
				recordType: "shopify_order",
				recordKey: "1001",
				code: "ga4_export_lag",
				message: "GA4 export is not available yet",
				sourceSystem: "ga4",
				retryable: true,
			},
			{
				recordType: "shopify_order",
				recordKey: "1002",
				code: "invalid_order_payload",
				message: "Order payload is missing required ids",
				sourceSystem: "shopify",
				retryable: false,
			},
		],
	});

	assert.equal(report.status, "dead_lettered");
	assert.equal(report.failures[0].retryable, true);
	assert.equal(report.failures[1].retryable, false);
	assert.throws(
		() =>
			normalizeRecoveryJobReport({
				...report,
				failures: [
					{
						...report.failures[0],
						sourceSystem: "warehouse",
					},
				],
			}),
		/Invalid enum value/,
	);
});

test("recovery source payload contracts normalize Shopify, GA4, and ad metadata fields", () => {
	const shopifySnapshot = normalizeShopifyRawPayloadSnapshot({
		schemaVersion: 1,
		snapshotId: "snapshot-1",
		shopDomain: "example.myshopify.com",
		shopifyOrderId: "1001",
		capturedAt: "2026-05-01T00:00:00Z",
		payloadVersion: 1,
		payloadSha256: "a".repeat(64),
		storageUri: null,
		rawPayload: { id: 1001 },
		normalized: {
			orderName: "#1001",
			processedAt: "2026-05-01T00:00:00Z",
			createdAtShopify: null,
			currencyCode: "usd",
			totalPrice: "120.00",
			subtotalPrice: "100.00",
			landingSite: "https://example.com/?utm_source=Google",
			referringSite: null,
			checkoutToken: "checkout-1",
			cartToken: null,
			customerId: "customer-1",
			sourceName: "web",
		},
	});
	const shopifyHint = normalizeShopifyAttributionHint({
		schemaVersion: 1,
		shopifyOrderId: "1001",
		extractedAt: "2026-05-01T00:00:01Z",
		hintSource: "landing_site",
		source: "Google",
		medium: "CPC",
		campaign: "Brand",
		content: null,
		term: null,
		clickIdType: "gclid",
		clickIdValue: "ABC123",
		landingSite: "https://example.com/?gclid=ABC123",
		referringSite: null,
		confidenceScore: 0.8,
		confidenceLabel: "high",
		sourcePrecedenceRank: 1,
	});
	const ga4 = normalizeGa4EnrichmentFields({
		schemaVersion: 1,
		shopifyOrderId: "1001",
		ga4PropertyId: "properties/123",
		ga4EventDate: "2026-05-01",
		enrichedAt: "2026-05-01T00:00:02Z",
		clientId: "client-1",
		sessionId: null,
		userPseudoId: null,
		transactionId: "1001",
		source: "Newsletter",
		medium: "Email",
		campaign: "May",
		content: null,
		term: null,
		clickIdType: null,
		clickIdValue: null,
		collectedTrafficSource: { manual_source: "newsletter" },
		sourcePrecedenceRank: 2,
	});
	const metadata = normalizeCampaignMetadataRefreshPayload({
		schemaVersion: 1,
		requestedBy: "ops@example.com",
		workerId: "metadata-refresh",
		mode: "api_refresh",
		platforms: ["google_ads", "meta_ads"],
		startDate: "2026-05-01",
		endDate: "2026-05-02",
		sourcePrecedenceRank: 3,
	});

	assert.equal(shopifySnapshot.normalized.currencyCode, "USD");
	assert.deepEqual(shopifySnapshot.rawPayload, { id: 1001 });
	assert.equal(shopifyHint.source, "google");
	assert.equal(shopifyHint.sourcePrecedenceRank, 1);
	assert.equal(ga4.medium, "email");
	assert.equal(ga4.sourcePrecedenceRank, 2);
	assert.deepEqual(metadata.campaignIds, []);
	assert.equal(metadata.sourcePrecedenceRank, 3);
});

test("committed recovery JSON Schema files cover shared recovery contract titles", () => {
	const expectedSchemas = [
		["docs/json-schema/recovery-job-request-v1.schema.json", recoveryJobRequestJsonSchema],
		["docs/json-schema/recovery-job-report-v1.schema.json", recoveryJobReportJsonSchema],
		["docs/json-schema/shopify-raw-payload-snapshot-v1.schema.json", shopifyRawPayloadSnapshotJsonSchema],
		["docs/json-schema/shopify-attribution-hint-v1.schema.json", shopifyAttributionHintJsonSchema],
		["docs/json-schema/ga4-enrichment-fields-v1.schema.json", ga4EnrichmentFieldsJsonSchema],
		["docs/json-schema/campaign-metadata-refresh-payload-v1.schema.json", campaignMetadataRefreshPayloadJsonSchema],
	] as const;

	for (const [path, exportedSchema] of expectedSchemas) {
		const committed = JSON.parse(readFileSync(path, "utf8"));

		assert.equal(committed.$schema, "https://json-schema.org/draft/2020-12/schema");
		assert.equal(committed.title, exportedSchema.title);
	}
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

test('Meta deterministic attribution schemas enforce identity, verification, and window contracts', () => {
  const identity = normalizeMetaDeterministicAttributionIdentityTupleV1({
    organization_id: 1,
    ad_account_id: 'act_123',
    report_date: '2026-05-20',
    attribution_family: 'deterministic_views',
    attribution_window: '7d_view',
    campaign_id: 'campaign-1',
    adset_id: null,
    ad_id: null
  });

  const aggregate = normalizeMetaDeterministicAttributionAggregateV1({
    schema_version: 1,
    platform: 'meta_ads',
    organization_id: 1,
    meta_connection_id: 2,
    source_id: 3,
    raw_event_id: 4,
    fact_id: null,
    ad_account_id: 'act_123',
    report_date: '2026-05-20',
    campaign_id: 'campaign-1',
    campaign_name: 'Campaign',
    adset_id: null,
    adset_name: null,
    ad_id: null,
    ad_name: null,
    event_type: 'view',
    attribution_family: 'deterministic_views',
    attribution_window: '7d_view',
    attribution_window_days: 7,
    aggregate_count: 42,
    evidence_origin: 'api',
    platform_verified: true,
    verification_status: 'verified',
    verified_by_source_id: 3,
    verified_at_utc: '2026-05-21T12:00:00Z',
    raw_record_metadata: {
      sourceId: 3,
      sourceTable: 'deterministic_event_sources',
      rawTable: 'raw_deterministic_events',
      rawEventId: 4,
      apiVersion: 'v20.0',
      apiEndpoint: 'insights',
      apiAccountId: 'act_123',
      apiRequestTimestampUtc: '2026-05-21T11:59:59Z',
      requestId: 'trace-123'
    }
  });

  assert.equal(identity.campaign_id, 'campaign-1');
  assert.equal(aggregate.verified_at_utc, '2026-05-21T12:00:00.000Z');
  assert.equal(aggregate.attribution_window_days, 7);

  assert.throws(
    () =>
      normalizeAttributionResultRecordV1({
        run_id: '11111111-1111-4111-8111-111111111111',
        attribution_spec_version: 'v1',
        order_id: 'order-1',
        model_key: 'deterministic_views',
        allocation_status: 'attributed',
        winner_touchpoint_id: 'touch-1',
        winner_session_id: null,
        winner_evidence_source: 'landing_session_id',
        winner_attribution_reason: 'matched_by_landing_session',
        total_credit_weight: '1.00',
        total_revenue_credited: '200.00',
        touchpoint_count_considered: 1,
        eligible_click_count: 1,
        eligible_view_count: 0,
        lookback_rule_applied: '28d_click',
        winner_selection_rule: 'last_touch',
        direct_suppression_applied: false,
        deterministic_block_applied: false,
        normalization_failures_count: 0,
        generated_at_utc: '2026-05-21T12:00:00Z'
      }),
    /Invalid enum value/
  );

  assert.throws(
    () =>
      normalizeAttributionCreditRecordV1({
        run_id: '11111111-1111-4111-8111-111111111111',
        attribution_spec_version: 'v1',
        order_id: 'order-1',
        model_key: 'last_touch',
        touchpoint_id: 'touch-1',
        session_id: null,
        touchpoint_position: 1,
        occurred_at_utc: '2026-05-21T12:00:00Z',
        source: 'meta',
        medium: 'paid_social',
        campaign: 'campaign-1',
        content: null,
        term: null,
        click_id_type: null,
        click_id_value: null,
        touch_type: 'view',
        is_direct: false,
        evidence_source: 'deterministic_views',
        is_synthetic: false,
        attribution_reason: 'deterministic_views',
        credit_weight: '1.00',
        revenue_credit: '200.00',
        is_primary: true
      }),
    /Invalid enum value/
  );

  assert.throws(
    () =>
      normalizeMetaDeterministicAttributionAggregateV1({
        ...aggregate,
        event_type: 'impression'
      }),
    /event_type must match attribution_family/
  );

  assert.throws(
    () =>
      normalizeMetaDeterministicAttributionAggregateV1({
        ...aggregate,
        verified_by_source_id: null
      }),
    /verified Meta aggregate rows require verified status/
  );

  assert.throws(
    () =>
      normalizeMetaDeterministicAttributionAggregateV1({
        ...aggregate,
        raw_record_metadata: {
          ...aggregate.raw_record_metadata,
          requestId: null
        }
      }),
    /verified Meta aggregate rows require raw payload and Meta API provenance metadata/
  );

  assert.throws(
    () =>
      normalizeMetaDeterministicAttributionIdentityTupleV1({
        ...identity,
        campaign_id: null,
        ad_id: null
      }),
    /campaign_id or ad_id is required/
  );
});

test('attribution engine package publishes JSON schema documents for canonical v1 records', () => {
  assert.deepEqual(Object.keys(attributionEngineV1JsonSchemas).sort(), [
    'AttributionCreditRecordV1',
    'AttributionExplainRecordV1',
    'AttributionHintInputV1',
    'AttributionOrderInputV1',
    'AttributionQaPayloadV1',
    'AttributionResultRecordV1',
    'AttributionTouchpointInputV1',
    'MetaDeterministicAttributionAggregateV1',
    'MetaDeterministicAttributionIdentityTupleV1'
  ]);

  assert.equal(attributionEngineV1JsonSchemas.AttributionOrderInputV1.title, 'AttributionOrderInputV1');
  assert.equal(attributionEngineV1JsonSchemas.AttributionTouchpointInputV1.type, 'object');
  assert.equal(attributionEngineV1JsonSchemas.AttributionResultRecordV1.additionalProperties, false);
  assert.equal(attributionQaPayloadV1JsonSchema.title, 'AttributionQaPayloadV1');
  assert.equal(
    attributionEngineV1JsonSchemas.MetaDeterministicAttributionAggregateV1.title,
    'MetaDeterministicAttributionAggregateV1'
  );
});

test('attribution QA payload fixtures validate success and no-match outcomes', () => {
  const success = normalizeAttributionQaPayloadV1(attributionQaPayloadV1SuccessFixture);
  const noMatch = normalizeAttributionQaPayloadV1(attributionQaPayloadV1NoMatchFixture);
  const selectedSuccessCandidates = [
    ...success.candidates.deterministic_first_party,
    ...success.candidates.shopify_hint,
    ...success.candidates.ga4_fallback
  ].filter((candidate) => candidate.selected);
  const selectedNoMatchCandidates = [
    ...noMatch.candidates.deterministic_first_party,
    ...noMatch.candidates.shopify_hint,
    ...noMatch.candidates.ga4_fallback
  ].filter((candidate) => candidate.selected);

  assert.equal(success.outcome.status, 'success');
  assert.equal(success.outcome.attribution_tier, 'deterministic_first_party');
  assert.equal(success.order.currency_code, 'USD');
  assert.equal(success.candidates.deterministic_first_party[0]?.source, 'google');
  assert.equal(success.generated_at_utc, '2026-04-30T12:30:00.000Z');
  assert.equal(selectedSuccessCandidates.length, 1);
  assert.ok(success.outcome.winner_touchpoint_id || success.outcome.winner_session_id);

  assert.equal(noMatch.outcome.status, 'no_match');
  assert.equal(noMatch.outcome.attribution_tier, 'unattributed');
  assert.equal(noMatch.outcome.winner_touchpoint_id, null);
  assert.equal(noMatch.outcome.winner_session_id, null);
  assert.equal(noMatch.outcome.confidence_score, 0);
  assert.equal(noMatch.outcome.confidence_label, 'none');
  assert.equal(selectedNoMatchCandidates.length, 0);
  assert.equal(noMatch.credits.length, 0);
});

test('attribution QA payload serialization round-trips normalized schema fields', () => {
  const payload = normalizeAttributionQaPayloadV1({
    ...attributionQaPayloadV1SuccessFixture,
    generated_at_utc: '2026-04-30T12:30:00-05:00',
    order: {
      ...attributionQaPayloadV1SuccessFixture.order,
      currency_code: 'usd',
      subtotal_amount: 180,
      total_amount: 195,
      identifiers: {
        ...attributionQaPayloadV1SuccessFixture.order.identifiers,
        checkout_token: undefined,
        cart_token: '   ',
        email_hash: undefined
      }
    },
    candidates: {
      ...attributionQaPayloadV1SuccessFixture.candidates,
      deterministic_first_party: attributionQaPayloadV1SuccessFixture.candidates.deterministic_first_party.map(
        (candidate) => ({
          ...candidate,
          occurred_at_utc: '2026-04-30T11:15:00-05:00',
          source: ' Google ',
          medium: ' CPC ',
          content: undefined,
          click_id_type: undefined
        })
      )
    }
  });
  const serialized = JSON.parse(JSON.stringify(payload));
  const reparsed = normalizeAttributionQaPayloadV1(serialized);

  assert.deepEqual(reparsed, serialized);
  assert.equal(reparsed.generated_at_utc, '2026-04-30T17:30:00.000Z');
  assert.equal(reparsed.order.currency_code, 'USD');
  assert.equal(reparsed.order.subtotal_amount, '180.00');
  assert.equal(reparsed.order.identifiers.checkout_token, null);
  assert.equal(reparsed.order.identifiers.cart_token, null);
  assert.equal(reparsed.order.identifiers.email_hash, null);
  assert.equal(reparsed.candidates.deterministic_first_party[0]?.source, 'google');
  assert.equal(reparsed.candidates.deterministic_first_party[0]?.medium, 'cpc');
  assert.equal(reparsed.candidates.deterministic_first_party[0]?.content, null);
  assert.equal(reparsed.candidates.deterministic_first_party[0]?.click_id_type, null);
  assert.equal(reparsed.candidates.deterministic_first_party[0]?.occurred_at_utc, '2026-04-30T16:15:00.000Z');
});

test('attribution QA payload enforces success and no-match invariants', () => {
  assert.throws(
    () =>
      normalizeAttributionQaPayloadV1({
        ...attributionQaPayloadV1SuccessFixture,
        outcome: {
          ...attributionQaPayloadV1SuccessFixture.outcome,
          status: 'no_match'
        }
      }),
    /no_match payloads must be unattributed/
  );

  assert.throws(
    () =>
      normalizeAttributionQaPayloadV1({
        ...attributionQaPayloadV1NoMatchFixture,
        outcome: {
          ...attributionQaPayloadV1NoMatchFixture.outcome,
          status: 'success',
          attribution_tier: 'deterministic_first_party',
          match_source: 'landing_session_id',
          confidence_score: 1,
          confidence_label: 'high'
        }
      }),
    /success payloads require winner_touchpoint_id or winner_session_id/
  );
});
