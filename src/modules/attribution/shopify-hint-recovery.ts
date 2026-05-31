import { createHash } from "node:crypto";
import type { PoolClient } from "pg";

import { withTransaction } from "../../db/pool.js";
import {
	type RecoveryCheckpoint,
	type RecoveryExecutionResult,
	type RecoveryJobDefinition,
	PostgresRecoveryJobStore,
	type RecoveryRecordResult,
	type RecoveryStartResult,
	createRecoveryJobOrchestrator,
} from "../recovery/index.js";
import { extractAttributionCandidatesForOrder } from "./candidate-extraction.js";
import { buildAttributionConfidenceLabel } from "./order-attribution-audit.js";
import {
	type AttributionComparableFields,
	type AttributionOrigin,
	classifyAttributionOrigin,
	shouldApplyAttributionUpdate,
} from "./precedence.js";
import { type ResolvedJourney, resolveAttributionTier } from "./resolver.js";

export const SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE =
	"shopify_attribution_hint_recovery";

const ATTRIBUTION_MODEL_VERSION = 1;

type ShopifyAttributionRecoveryOrder = {
	order_row_id: string;
	shopify_order_id: string;
	total_price: string;
	processed_at: Date | null;
	created_at_shopify: Date | null;
	ingested_at: Date;
	landing_session_id: string | null;
	checkout_token: string | null;
	cart_token: string | null;
	email_hash: string | null;
	customer_identity_id: string | null;
	identity_journey_id: string | null;
	source_name: string | null;
	raw_payload: unknown;
	order_occurred_at: Date;
};

type CurrentAttributionRow = {
	session_id: string | null;
	attributed_source: string | null;
	attributed_medium: string | null;
	attributed_campaign: string | null;
	attributed_content: string | null;
	attributed_term: string | null;
	attributed_click_id_type: string | null;
	attributed_click_id_value: string | null;
	confidence_score: string | null;
	attribution_reason: string | null;
	match_source: string | null;
	confidence_label: string | null;
	attributed_at: Date | null;
	order_attribution_tier: string | null;
	order_attribution_source: string | null;
	order_attribution_reason: string | null;
	order_attribution_snapshot: unknown;
};

export type ShopifyAttributionRecoveryOptions = {
	timeRangeStart: Date | string;
	timeRangeEnd: Date | string;
	initiatedBy: string;
	workerId?: string;
	dryRun?: boolean;
	scopeKey?: string;
	pageSize?: number;
	now?: Date;
};

type Snapshot = {
	origin: AttributionOrigin;
	result: {
		sessionId: string | null;
		source: string | null;
		medium: string | null;
		campaign: string | null;
		content: string | null;
		term: string | null;
		clickIdType: string | null;
		clickIdValue: string | null;
		confidenceScore: number | null;
		attributionReason: string | null;
		matchSource: string | null;
		confidenceLabel: string | null;
		attributedAt: string | null;
	};
	order: {
		tier: string | null;
		source: string | null;
		reason: string | null;
		snapshot: unknown;
	};
};

function normalizeNullableString(
	value: string | null | undefined,
): string | null {
	const normalized = value?.trim();
	return normalized ? normalized : null;
}

function normalizeCheckpointString(
	checkpoint: RecoveryCheckpoint,
	key: string,
): string | null {
	const value = checkpoint[key];
	return typeof value === "string" && value.trim() ? value.trim() : null;
}

function stableHash(input: unknown): string {
	return createHash("sha256").update(JSON.stringify(input)).digest("hex");
}

function buildRecordResult(input: RecoveryRecordResult): RecoveryRecordResult {
	return input;
}

function buildComparableFromSnapshot(
	snapshot: Snapshot,
): AttributionComparableFields {
	return {
		sessionId: snapshot.result.sessionId,
		source: snapshot.result.source,
		medium: snapshot.result.medium,
		campaign: snapshot.result.campaign,
		content: snapshot.result.content,
		term: snapshot.result.term,
		clickIdType: snapshot.result.clickIdType,
		clickIdValue: snapshot.result.clickIdValue,
		attributionReason: snapshot.result.attributionReason,
	};
}

function serializeTouchpoint(journey: ResolvedJourney) {
	const winner = journey.winner;

	if (!winner) {
		return null;
	}

	return {
		sessionId: winner.sessionId,
		sourceTouchEventId: winner.sourceTouchEventId,
		occurredAt: winner.occurredAt.toISOString(),
		source: winner.source,
		medium: winner.medium,
		campaign: winner.campaign,
		content: winner.content,
		term: winner.term,
		clickIdType: winner.clickIdType,
		clickIdValue: winner.clickIdValue,
		attributionReason: winner.attributionReason,
		ingestionSource: winner.ingestionSource,
		isDirect: winner.isDirect,
	};
}

function buildJourneySnapshot(
	journey: ResolvedJourney,
): Record<string, unknown> {
	return {
		tier: journey.tier,
		attributionReason: journey.attributionReason,
		orderOccurredAtUtc: journey.orderOccurredAtUtc?.toISOString() ?? null,
		normalizationFailures: journey.normalizationFailures,
		confidenceScore: journey.confidenceScore,
		winner: serializeTouchpoint(journey),
		timeline: journey.touchpoints.map((touchpoint) => ({
			sessionId: touchpoint.sessionId,
			sourceTouchEventId: touchpoint.sourceTouchEventId,
			occurredAt: touchpoint.occurredAt.toISOString(),
			source: touchpoint.source,
			medium: touchpoint.medium,
			campaign: touchpoint.campaign,
			content: touchpoint.content,
			term: touchpoint.term,
			clickIdType: touchpoint.clickIdType,
			clickIdValue: touchpoint.clickIdValue,
			attributionReason: touchpoint.attributionReason,
			ingestionSource: touchpoint.ingestionSource,
			isDirect: touchpoint.isDirect,
		})),
	};
}

function buildCurrentSnapshot(
	row: CurrentAttributionRow | null,
): Snapshot | null {
	if (!row) {
		return null;
	}

	const origin = classifyAttributionOrigin({
		attributionTier: row.order_attribution_tier,
		attributionSource: row.order_attribution_source,
		matchSource: row.match_source,
		attributionReason: row.attribution_reason ?? row.order_attribution_reason,
	});

	return {
		origin,
		result: {
			sessionId: row.session_id,
			source: row.attributed_source,
			medium: row.attributed_medium,
			campaign: row.attributed_campaign,
			content: row.attributed_content,
			term: row.attributed_term,
			clickIdType: row.attributed_click_id_type,
			clickIdValue: row.attributed_click_id_value,
			confidenceScore:
				row.confidence_score === null ? null : Number(row.confidence_score),
			attributionReason: row.attribution_reason,
			matchSource: row.match_source,
			confidenceLabel: row.confidence_label,
			attributedAt: row.attributed_at?.toISOString() ?? null,
		},
		order: {
			tier: row.order_attribution_tier,
			source: row.order_attribution_source,
			reason: row.order_attribution_reason,
			snapshot: row.order_attribution_snapshot,
		},
	};
}

function buildProposedSnapshot(
	journey: ResolvedJourney,
	matchedAt: Date,
): Snapshot {
	const winner = journey.winner;
	const confidenceLabel = buildAttributionConfidenceLabel(
		journey.confidenceScore,
	);

	return {
		origin: "shopify_marketing_hint",
		result: {
			sessionId: winner?.sessionId ?? null,
			source: normalizeNullableString(winner?.source),
			medium: normalizeNullableString(winner?.medium),
			campaign: normalizeNullableString(winner?.campaign),
			content: normalizeNullableString(winner?.content),
			term: normalizeNullableString(winner?.term),
			clickIdType: normalizeNullableString(winner?.clickIdType),
			clickIdValue: normalizeNullableString(winner?.clickIdValue),
			confidenceScore: journey.confidenceScore,
			attributionReason: winner?.attributionReason ?? journey.attributionReason,
			matchSource: journey.attributionReason,
			confidenceLabel,
			attributedAt: matchedAt.toISOString(),
		},
		order: {
			tier: "deterministic_shopify_hint",
			source: "shopify_marketing_hint",
			reason: journey.attributionReason,
			snapshot: buildJourneySnapshot(journey),
		},
	};
}

async function fetchCurrentAttribution(
	client: PoolClient,
	shopifyOrderId: string,
): Promise<CurrentAttributionRow | null> {
	const result = await client.query<CurrentAttributionRow>(
		`
      SELECT
        results.session_id::text,
        results.attributed_source,
        results.attributed_medium,
        results.attributed_campaign,
        results.attributed_content,
        results.attributed_term,
        results.attributed_click_id_type,
        results.attributed_click_id_value,
        results.confidence_score::text,
        results.attribution_reason,
        results.match_source,
        results.confidence_label,
        results.attributed_at,
        orders.attribution_tier AS order_attribution_tier,
        orders.attribution_source AS order_attribution_source,
        orders.attribution_reason AS order_attribution_reason,
        orders.attribution_snapshot AS order_attribution_snapshot
      FROM shopify_orders orders
      LEFT JOIN attribution_results results
        ON results.shopify_order_id = orders.shopify_order_id
      WHERE orders.shopify_order_id = $1
      LIMIT 1
    `,
		[shopifyOrderId],
	);

	return result.rows[0] ?? null;
}

async function resolveShopifyHintJourney(
	client: PoolClient,
	order: ShopifyAttributionRecoveryOrder,
): Promise<ResolvedJourney> {
	const candidates = await extractAttributionCandidatesForOrder(
		client,
		{
			shopifyOrderId: order.shopify_order_id,
			processedAt: order.processed_at,
			createdAtShopify: order.created_at_shopify,
			ingestedAt: order.ingested_at,
			landingSessionId: order.landing_session_id,
			checkoutToken: order.checkout_token,
			cartToken: order.cart_token,
			emailHash: order.email_hash,
			customerIdentityId: order.customer_identity_id,
			identityJourneyId: order.identity_journey_id,
			sourceName: order.source_name,
			rawPayload: order.raw_payload,
		},
		{
			loadDeterministicFirstPartyCandidates: async () => [],
			loadGa4Candidates: async () => [],
		},
	);

	return resolveAttributionTier(candidates);
}

async function persistRecoveredAttribution(input: {
	client: PoolClient;
	order: ShopifyAttributionRecoveryOrder;
	before: Snapshot | null;
	after: Snapshot;
	runId: string;
	jobType: string;
	changedBy: string;
	now: Date;
}): Promise<void> {
	const { client, order, after, before } = input;

	await client.query(
		"DELETE FROM attribution_order_credits WHERE shopify_order_id = $1",
		[order.shopify_order_id],
	);

	await client.query(
		`
      INSERT INTO attribution_order_credits (
        shopify_order_id,
        attribution_model,
        touchpoint_position,
        session_id,
        touchpoint_occurred_at,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        credit_weight,
        revenue_credit,
        is_primary,
        attribution_reason,
        model_version,
        match_source,
        confidence_label
      )
      VALUES (
        $1,
        'last_non_direct',
        1,
        $2::uuid,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        1,
        $11,
        true,
        $12,
        $13,
        $14,
        $15
      )
    `,
		[
			order.shopify_order_id,
			after.result.sessionId,
			order.order_occurred_at,
			after.result.source,
			after.result.medium,
			after.result.campaign,
			after.result.content,
			after.result.term,
			after.result.clickIdType,
			after.result.clickIdValue,
			order.total_price,
			after.result.attributionReason,
			ATTRIBUTION_MODEL_VERSION,
			after.result.matchSource,
			after.result.confidenceLabel,
		],
	);

	await client.query(
		`
      INSERT INTO attribution_results (
        shopify_order_id,
        session_id,
        attribution_model,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        confidence_score,
        attribution_reason,
        attributed_at,
        reprocess_version,
        model_version,
        match_source,
        confidence_label
      )
      VALUES (
        $1,
        $2::uuid,
        'last_non_direct',
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        1,
        $13,
        $14,
        $15
      )
      ON CONFLICT (shopify_order_id)
      DO UPDATE SET
        session_id = EXCLUDED.session_id,
        attribution_model = EXCLUDED.attribution_model,
        attributed_source = EXCLUDED.attributed_source,
        attributed_medium = EXCLUDED.attributed_medium,
        attributed_campaign = EXCLUDED.attributed_campaign,
        attributed_content = EXCLUDED.attributed_content,
        attributed_term = EXCLUDED.attributed_term,
        attributed_click_id_type = EXCLUDED.attributed_click_id_type,
        attributed_click_id_value = EXCLUDED.attributed_click_id_value,
        confidence_score = EXCLUDED.confidence_score,
        attribution_reason = EXCLUDED.attribution_reason,
        attributed_at = EXCLUDED.attributed_at,
        model_version = EXCLUDED.model_version,
        match_source = EXCLUDED.match_source,
        confidence_label = EXCLUDED.confidence_label
    `,
		[
			order.shopify_order_id,
			after.result.sessionId,
			after.result.source,
			after.result.medium,
			after.result.campaign,
			after.result.content,
			after.result.term,
			after.result.clickIdType,
			after.result.clickIdValue,
			after.result.confidenceScore,
			after.result.attributionReason,
			input.now,
			ATTRIBUTION_MODEL_VERSION,
			after.result.matchSource,
			after.result.confidenceLabel,
		],
	);

	await client.query(
		`
      UPDATE shopify_orders
      SET
        attribution_tier = $2,
        attribution_source = $3,
        attribution_matched_at = $4,
        attribution_reason = $5,
        attribution_snapshot = $6::jsonb,
        attribution_snapshot_updated_at = $4
      WHERE shopify_order_id = $1
    `,
		[
			order.shopify_order_id,
			after.order.tier,
			after.order.source,
			input.now,
			after.order.reason,
			JSON.stringify(after.order.snapshot),
		],
	);

	await client.query(
		`
      INSERT INTO attribution_recovery_audit_logs (
        recovery_run_id,
        job_type,
        shopify_order_id,
        changed_by,
        change_reason,
        before_attribution,
        after_attribution,
        created_at
      )
      VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
    `,
		[
			input.runId,
			input.jobType,
			order.shopify_order_id,
			input.changedBy,
			"shopify_hint_precedence_recovery",
			JSON.stringify(before),
			JSON.stringify(after),
			input.now,
		],
	);
}

function buildRecoveryDefinition(
	pageSize?: number,
): RecoveryJobDefinition<ShopifyAttributionRecoveryOrder> {
	return {
		jobType: SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
		pageSize,
		fetchPage: async (context) => {
			const lastOccurredAt = normalizeCheckpointString(
				context.checkpoint,
				"lastOccurredAt",
			);
			const lastOrderRowId = normalizeCheckpointString(
				context.checkpoint,
				"lastOrderRowId",
			);

			return withTransaction(async (client) => {
				const result = await client.query<ShopifyAttributionRecoveryOrder>(
					`
            SELECT
              o.id::text AS order_row_id,
              o.shopify_order_id,
              o.total_price,
              o.processed_at,
              o.created_at_shopify,
              o.ingested_at,
              o.landing_session_id::text AS landing_session_id,
              o.checkout_token,
              o.cart_token,
              o.email_hash,
              o.customer_identity_id::text AS customer_identity_id,
              o.identity_journey_id::text AS identity_journey_id,
              o.source_name,
              o.raw_payload,
              COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at
            FROM shopify_orders o
            WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1::timestamptz
              AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= $2::timestamptz
              AND (
                o.raw_payload ? 'landing_site'
                OR o.raw_payload ? 'note_attributes'
                OR o.raw_payload ? 'attributes'
              )
              AND (
                $3::timestamptz IS NULL
                OR COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) > $3::timestamptz
                OR (
                  COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) = $3::timestamptz
                  AND o.id > $4::bigint
                )
              )
            ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) ASC, o.id ASC
            LIMIT $5
          `,
					[
						context.run.timeRangeStart,
						context.run.timeRangeEnd,
						lastOccurredAt,
						lastOrderRowId,
						context.pageSize,
					],
				);

				const last = result.rows[result.rows.length - 1];

				return {
					records: result.rows,
					checkpoint: last
						? {
								lastOccurredAt: last.order_occurred_at.toISOString(),
								lastOrderRowId: last.order_row_id,
							}
						: context.checkpoint,
					done: result.rows.length < context.pageSize,
				};
			});
		},
		identifyRecord: (record, context) => ({
			recordType: "shopify_order",
			recordKey: record.shopify_order_id,
			sourceFingerprint: stableHash(record.raw_payload),
			sideEffectKey: context.buildRecordIdempotencyKey(
				"shopify_order_attribution",
				record.shopify_order_id,
			),
		}),
		processRecord: async (record, context) =>
			withTransaction(async (client) => {
				const journey = await resolveShopifyHintJourney(client, record);

				if (journey.tier !== "deterministic_shopify_hint" || !journey.winner) {
					return buildRecordResult({
						status: "skipped",
						sideEffectAttempted: false,
						result: {
							reason: "no_shopify_hint_attribution",
							attributionTier: journey.tier,
						},
					});
				}

				const current = buildCurrentSnapshot(
					await fetchCurrentAttribution(client, record.shopify_order_id),
				);
				const proposed = buildProposedSnapshot(journey, context.now);
				const shouldApply = shouldApplyAttributionUpdate({
					current: current
						? {
								origin: current.origin,
								attribution: buildComparableFromSnapshot(current),
							}
						: null,
					proposed: {
						origin: proposed.origin,
						attribution: buildComparableFromSnapshot(proposed),
					},
				});

				if (!shouldApply) {
					return buildRecordResult({
						status: "skipped",
						sideEffectAttempted: false,
						result: {
							reason: "precedence_or_idempotency_skipped",
							currentOrigin: current?.origin ?? null,
							proposedOrigin: proposed.origin,
						},
					});
				}

				if (!context.dryRun) {
					await persistRecoveredAttribution({
						client,
						order: record,
						before: current,
						after: proposed,
						runId: context.run.id,
						jobType: context.run.jobType,
						changedBy: context.run.initiatedBy,
						now: context.now,
					});
				}

				return buildRecordResult({
					status: "succeeded",
					sideEffectAttempted: !context.dryRun,
					sideEffectSucceeded: !context.dryRun,
					result: {
						reason: context.dryRun
							? "shopify_hint_recovery_preview"
							: "attribution_recovered_from_shopify_hint",
						beforeOrigin: current?.origin ?? null,
						afterOrigin: proposed.origin,
					},
				});
			}),
	};
}

export async function runShopifyAttributionRecovery(
	options: ShopifyAttributionRecoveryOptions,
): Promise<RecoveryStartResult | RecoveryExecutionResult> {
	const orchestrator = createRecoveryJobOrchestrator(
		buildRecoveryDefinition(options.pageSize),
	);

	return orchestrator.startAndExecute(
		{
			jobType: SHOPIFY_ATTRIBUTION_RECOVERY_JOB_TYPE,
			mode: "manual",
			initiatedBy: options.initiatedBy,
			dryRun: options.dryRun ?? true,
			timeRangeStart: options.timeRangeStart,
			timeRangeEnd: options.timeRangeEnd,
			scopeKey: options.scopeKey ?? "shopify-attribution-hints",
			inputParameters: {
				pageSize: options.pageSize ?? null,
			},
			now: options.now,
		},
		options.workerId ?? "shopify-attribution-recovery",
	);
}

function readStoredPageSize(value: unknown): number | undefined {
	if (
		typeof value === "number" &&
		Number.isInteger(value) &&
		value > 0
	) {
		return value;
	}

	return undefined;
}

export async function executeShopifyAttributionRecoveryRun(
	runId: string,
	workerId = "shopify-attribution-recovery",
	now = new Date(),
): Promise<RecoveryExecutionResult> {
	const store = new PostgresRecoveryJobStore();
	const run = await store.getRun(runId);
	const pageSize = readStoredPageSize(run?.inputParameters.pageSize);
	const orchestrator = createRecoveryJobOrchestrator(
		buildRecoveryDefinition(pageSize),
		store,
	);

	return orchestrator.execute(runId, workerId, now);
}
