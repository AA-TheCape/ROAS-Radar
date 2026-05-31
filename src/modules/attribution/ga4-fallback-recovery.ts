import { createHash } from "node:crypto";
import type { PoolClient } from "pg";

import { withTransaction } from "../../db/pool.js";
import {
	type RecoveryCheckpoint,
	type RecoveryExecutionResult,
	type RecoveryJobDefinition,
	type RecoveryRecordResult,
	type RecoveryStartResult,
	createRecoveryJobOrchestrator,
} from "../recovery/index.js";
import {
	extractAttributionCandidatesForOrder,
	resolveOrderOccurredAtUtc,
	type Ga4AttributionCandidateInput,
} from "./candidate-extraction.js";
import { lookupGa4FallbackCandidates } from "./ga4-fallback-candidates.js";
import { buildAttributionConfidenceLabel } from "./order-attribution-audit.js";
import {
	type AttributionComparableFields,
	type AttributionOrigin,
	shouldApplyAttributionUpdate,
} from "./precedence.js";
import { type ResolvedJourney, resolveAttributionTier } from "./resolver.js";

export const GA4_FALLBACK_RECOVERY_JOB_TYPE = "ga4_fallback_unattributed_recovery";

const ATTRIBUTION_MODEL_VERSION = 1;

type Ga4FallbackRecoveryOrder = {
	order_row_id: string;
	shopify_order_id: string;
	total_price: string;
	processed_at: Date | null;
	created_at_shopify: Date | null;
	ingested_at: Date;
	email_hash: string | null;
	customer_identity_id: string | null;
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

export type Ga4FallbackRecoveryOptions = {
	timeRangeStart: Date | string;
	timeRangeEnd: Date | string;
	initiatedBy: string;
	workerId?: string;
	dryRun?: boolean;
	scopeKey?: string;
	pageSize?: number;
	lookbackDays?: number;
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

function normalizePositiveInteger(value: number | undefined): number | null {
	if (!Number.isFinite(value)) {
		return null;
	}

	return Math.max(1, Math.trunc(value ?? 1));
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

function isUnattributedValue(value: string | null): boolean {
	return value === null || value.trim().toLowerCase() === "unattributed";
}

export function isCurrentlyUnattributedSnapshot(
	snapshot: Snapshot | null,
): boolean {
	if (!snapshot) {
		return true;
	}

	const hasResultAttribution = [
		snapshot.result.sessionId,
		snapshot.result.source,
		snapshot.result.medium,
		snapshot.result.campaign,
		snapshot.result.content,
		snapshot.result.term,
		snapshot.result.clickIdType,
		snapshot.result.clickIdValue,
	].some((value) => normalizeNullableString(value) !== null);

	return (
		!hasResultAttribution &&
		isUnattributedValue(snapshot.order.tier) &&
		isUnattributedValue(snapshot.order.source)
	);
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

function buildCurrentSnapshot(row: CurrentAttributionRow | null): Snapshot | null {
	if (!row) {
		return null;
	}

	return {
		origin: isCurrentlyUnattributedSnapshot({
			origin: "unattributed",
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
		})
			? "unattributed"
			: "unknown",
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

	return {
		origin: "ga4_fallback",
		result: {
			sessionId: null,
			source: normalizeNullableString(winner?.source),
			medium: normalizeNullableString(winner?.medium),
			campaign: normalizeNullableString(winner?.campaign),
			content: normalizeNullableString(winner?.content),
			term: normalizeNullableString(winner?.term),
			clickIdType: normalizeNullableString(winner?.clickIdType),
			clickIdValue: normalizeNullableString(winner?.clickIdValue),
			confidenceScore: journey.confidenceScore,
			attributionReason: winner?.attributionReason ?? journey.attributionReason,
			matchSource: "ga4_fallback",
			confidenceLabel: buildAttributionConfidenceLabel(journey.confidenceScore),
			attributedAt: matchedAt.toISOString(),
		},
		order: {
			tier: "ga4_fallback",
			source: "ga4_fallback",
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

async function resolveGa4FallbackJourney(
	client: PoolClient,
	order: Ga4FallbackRecoveryOrder,
	lookbackDays?: number | null,
): Promise<ResolvedJourney> {
	const orderTimestamps = resolveOrderOccurredAtUtc({
		shopifyOrderId: order.shopify_order_id,
		processedAt: order.processed_at,
		createdAtShopify: order.created_at_shopify,
		ingestedAt: order.ingested_at,
		landingSessionId: null,
		checkoutToken: null,
		cartToken: null,
		emailHash: order.email_hash,
		customerIdentityId: order.customer_identity_id,
		rawPayload: null,
	});

	const extracted = await extractAttributionCandidatesForOrder(
		client,
		{
			shopifyOrderId: order.shopify_order_id,
			processedAt: order.processed_at,
			createdAtShopify: order.created_at_shopify,
			ingestedAt: order.ingested_at,
			landingSessionId: null,
			checkoutToken: null,
			cartToken: null,
			emailHash: order.email_hash,
			customerIdentityId: order.customer_identity_id,
			rawPayload: null,
		},
		{
			loadDeterministicFirstPartyCandidates: async () => [],
			loadGa4Candidates: async (): Promise<Ga4AttributionCandidateInput[]> => {
				if (!orderTimestamps.orderOccurredAtUtc) {
					return [];
				}

				const candidates = await lookupGa4FallbackCandidates(
					{
						orderOccurredAt: orderTimestamps.orderOccurredAtUtc.toISOString(),
						customerIdentityId: order.customer_identity_id,
						emailHash: order.email_hash,
						transactionId: order.shopify_order_id,
						lookbackDays: lookbackDays ?? undefined,
					},
					client,
				);

				return candidates.map((candidate) => ({
					stableIdentifier: candidate.candidateKey,
					occurredAt: candidate.occurredAt,
					source: candidate.source,
					medium: candidate.medium,
					campaign: candidate.campaign,
					content: candidate.content,
					term: candidate.term,
					clickIdType: candidate.clickIdType,
					clickIdValue: candidate.clickIdValue,
					attributionReason: "ga4_fallback_match",
				}));
			},
		},
	);

	return resolveAttributionTier({
		orderOccurredAtUtc: extracted.orderOccurredAtUtc,
		deterministicFirstParty: [],
		shopifyHint: [],
		ga4Fallback: extracted.ga4Fallback,
		normalizationFailures: extracted.normalizationFailures,
	});
}

async function persistRecoveredAttribution(input: {
	client: PoolClient;
	order: Ga4FallbackRecoveryOrder;
	before: Snapshot | null;
	after: Snapshot;
	runId: string;
	jobType: string;
	changedBy: string;
	now: Date;
}): Promise<void> {
	const { client, order, after, before } = input;

	await client.query("DELETE FROM attribution_order_credits WHERE shopify_order_id = $1", [
		order.shopify_order_id,
	]);

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
        NULL,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        1,
        $10,
        true,
        $11,
        $12,
        $13,
        $14
      )
    `,
		[
			order.shopify_order_id,
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
        NULL,
        'last_non_direct',
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        1,
        $12,
        $13,
        $14
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
			"ga4_fallback_unattributed_recovery",
			JSON.stringify(before),
			JSON.stringify(after),
			input.now,
		],
	);
}

function buildRecoveryDefinition(
	pageSize?: number,
	lookbackDays?: number | null,
): RecoveryJobDefinition<Ga4FallbackRecoveryOrder> {
	return {
		jobType: GA4_FALLBACK_RECOVERY_JOB_TYPE,
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
				const result = await client.query<Ga4FallbackRecoveryOrder>(
					`
            SELECT
              o.id::text AS order_row_id,
              o.shopify_order_id,
              o.total_price,
              o.processed_at,
              o.created_at_shopify,
              o.ingested_at,
              o.email_hash,
              o.customer_identity_id::text AS customer_identity_id,
              COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at
            FROM shopify_orders o
            LEFT JOIN attribution_results results
              ON results.shopify_order_id = o.shopify_order_id
            WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1::timestamptz
              AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= $2::timestamptz
              AND COALESCE(o.attribution_tier, 'unattributed') = 'unattributed'
              AND COALESCE(o.attribution_source, 'unattributed') = 'unattributed'
              AND (
                results.shopify_order_id IS NULL
                OR (
                  results.session_id IS NULL
                  AND results.attributed_source IS NULL
                  AND results.attributed_medium IS NULL
                  AND results.attributed_campaign IS NULL
                  AND results.attributed_content IS NULL
                  AND results.attributed_term IS NULL
                  AND results.attributed_click_id_value IS NULL
                )
              )
              AND (
                o.customer_identity_id IS NOT NULL
                OR o.email_hash IS NOT NULL
                OR o.shopify_order_id IS NOT NULL
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
			sourceFingerprint: stableHash({
				shopifyOrderId: record.shopify_order_id,
				orderOccurredAt: record.order_occurred_at.toISOString(),
				customerIdentityId: record.customer_identity_id,
				emailHash: record.email_hash,
			}),
			sideEffectKey: context.buildRecordIdempotencyKey(
				"ga4_fallback_attribution",
				record.shopify_order_id,
			),
		}),
		processRecord: async (record, context) =>
			withTransaction(async (client) => {
				const journey = await resolveGa4FallbackJourney(
					client,
					record,
					lookbackDays,
				);

				if (journey.tier !== "ga4_fallback" || !journey.winner) {
					return buildRecordResult({
						status: "skipped",
						sideEffectAttempted: false,
						result: {
							reason: "no_ga4_fallback_attribution",
							attributionTier: journey.tier,
						},
					});
				}

				const current = buildCurrentSnapshot(
					await fetchCurrentAttribution(client, record.shopify_order_id),
				);

				if (!isCurrentlyUnattributedSnapshot(current)) {
					return buildRecordResult({
						status: "skipped",
						sideEffectAttempted: false,
						result: {
							reason: "current_attribution_not_unattributed",
							currentOrigin: current?.origin ?? null,
						},
					});
				}

				const proposed = buildProposedSnapshot(journey, context.now);
				const shouldApply = shouldApplyAttributionUpdate({
					current: null,
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
							? "ga4_fallback_recovery_preview"
							: "attribution_recovered_from_ga4_fallback",
						beforeOrigin: current?.origin ?? null,
						afterOrigin: proposed.origin,
						confidenceScore: proposed.result.confidenceScore,
						matchSource: proposed.result.matchSource,
					},
				});
			}),
	};
}

export async function runGa4FallbackRecovery(
	options: Ga4FallbackRecoveryOptions,
): Promise<RecoveryStartResult | RecoveryExecutionResult> {
	const normalizedLookbackDays = normalizePositiveInteger(options.lookbackDays);
	const orchestrator = createRecoveryJobOrchestrator(
		buildRecoveryDefinition(options.pageSize, normalizedLookbackDays),
	);

	return orchestrator.startAndExecute(
		{
			jobType: GA4_FALLBACK_RECOVERY_JOB_TYPE,
			mode: "manual",
			initiatedBy: options.initiatedBy,
			dryRun: options.dryRun ?? true,
			timeRangeStart: options.timeRangeStart,
			timeRangeEnd: options.timeRangeEnd,
			scopeKey: options.scopeKey ?? "ga4-fallback-unattributed",
			inputParameters: {
				pageSize: options.pageSize ?? null,
				lookbackDays: normalizedLookbackDays,
			},
			now: options.now,
		},
		options.workerId ?? "ga4-fallback-recovery",
	);
}

export const __ga4FallbackRecoveryTestUtils = {
	isCurrentlyUnattributedSnapshot,
};
