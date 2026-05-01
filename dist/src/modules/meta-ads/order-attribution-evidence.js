import { createHash } from 'node:crypto';
import { Router } from 'express';
import { z } from 'zod';
import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
import { logInfo, logWarning } from '../../observability/index.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
import { normalizeTimestampToUtc, resolveOrderOccurredAtUtc } from '../attribution/candidate-extraction.js';
const META_ATTRIBUTION_RULE_VERSION = 'meta_platform_reported_v1';
const CANONICAL_THRESHOLD = 0.5;
const PARALLEL_THRESHOLD = 0.35;
const DEFAULT_ATTRIBUTION_WINDOW_DAYS = 7;
const APPROVED_MATCH_BASES = [
    'fbclid',
    'fbc',
    'fbp',
    'external_id',
    'email_hash',
    'phone_hash',
    'meta_order_reference',
    'conversion_api_event_id'
];
const ORDER_JOINABLE_SOURCE_KINDS = new Set(['order_scoped', 'order_joinable']);
class MetaAttributionEvidenceHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'MetaAttributionEvidenceHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const metaAttributionEvidenceSchema = z.object({
    organizationId: z.coerce.number().int().positive().optional(),
    shopifyOrderId: z.string().trim().min(1).max(255),
    metaConnectionId: z.coerce.number().int().positive().optional(),
    rawRecordId: z.coerce.number().int().positive().optional(),
    syncJobId: z.coerce.number().int().positive().optional(),
    ingestionRunId: z.coerce.number().int().positive().optional(),
    metaSignalId: z.string().trim().min(1).max(255),
    sourceKind: z.enum(['order_scoped', 'order_joinable', 'aggregate_only', 'unknown']).optional(),
    reportedAtUtc: z.string().trim().min(1),
    sourceReceivedAt: z.string().trim().min(1).optional(),
    metaTouchpointOccurredAtUtc: z.string().trim().min(1).optional(),
    eventOrReportTimestampUtc: z.string().trim().min(1).optional(),
    reportedConversionTimestampUtc: z.string().trim().min(1).optional(),
    attributionWindowDays: z.coerce.number().int().min(0).max(90).optional(),
    metaAttributionReason: z.string().trim().min(1).max(255),
    campaignId: z.string().trim().min(1).max(255),
    campaignName: z.string().trim().min(1).max(255).optional(),
    adAccountId: z.string().trim().min(1).max(255),
    adId: z.string().trim().min(1).max(255).optional(),
    adSetId: z.string().trim().min(1).max(255).optional(),
    currencyCode: z.string().trim().min(1).max(12).optional(),
    reportedConversionValue: z
        .union([z.coerce.number().finite(), z.null()])
        .optional(),
    reportedEventName: z.string().trim().min(1).max(255).optional(),
    isViewThrough: z.boolean().optional(),
    isClickThrough: z.boolean().optional(),
    matchBasis: z.string().trim().min(1).max(120).optional(),
    observedMatchBases: z.array(z.string().trim().min(1).max(120)).optional(),
    confidenceScore: z
        .union([z.coerce.number().min(0).max(1), z.null()])
        .optional(),
    rawPayloadReference: z.string().trim().min(1).max(500).optional(),
    rawPayloadHashes: z.array(z.string().trim().min(1).max(200)).optional(),
    sourceRecordIds: z.array(z.unknown()).optional(),
    sourceSnapshot: z.record(z.unknown()).optional(),
    rawPayload: z.unknown().optional(),
    ruleVersion: z.string().trim().min(1).max(120).optional()
});
function normalizeNullableString(value) {
    const normalized = value?.trim();
    return normalized ? normalized : null;
}
function parseInput(input) {
    try {
        return metaAttributionEvidenceSchema.parse(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new MetaAttributionEvidenceHttpError(400, 'invalid_request', 'Invalid Meta attribution evidence payload', error.flatten());
        }
        throw error;
    }
}
function parseRequiredTimestamp(value, field) {
    const normalized = normalizeTimestampToUtc(value);
    if (!normalized) {
        throw new MetaAttributionEvidenceHttpError(400, 'invalid_request', `${field} must be an ISO-8601 timestamp with an explicit UTC offset`);
    }
    return normalized;
}
function parseOptionalTimestamp(value, field) {
    if (!value) {
        return null;
    }
    return parseRequiredTimestamp(value, field);
}
function normalizeAdAccountId(value) {
    return value.replace(/^act_/i, '').trim();
}
function normalizeCurrencyCode(value, normalizationFailures) {
    const normalized = normalizeNullableString(value)?.toUpperCase() ?? null;
    if (!normalized) {
        return null;
    }
    if (!/^[A-Z]{3}$/.test(normalized)) {
        normalizationFailures.push('invalid_currency_code');
        return null;
    }
    return normalized;
}
function normalizeMatchBasis(value, normalizationFailures) {
    const normalized = normalizeNullableString(value)?.toLowerCase() ?? null;
    if (!normalized) {
        return null;
    }
    if (APPROVED_MATCH_BASES.includes(normalized)) {
        return normalized;
    }
    normalizationFailures.push('unsupported_match_basis');
    return null;
}
function normalizeObservedMatchBases(input, strongestBasis, normalizationFailures) {
    const deduped = new Set();
    for (const entry of input ?? []) {
        const normalized = normalizeMatchBasis(entry, normalizationFailures);
        if (normalized) {
            deduped.add(normalized);
        }
    }
    if (strongestBasis) {
        deduped.add(strongestBasis);
    }
    return Array.from(deduped);
}
function dedupeStrings(values) {
    const deduped = new Set();
    for (const value of values) {
        const normalized = value.trim();
        if (normalized) {
            deduped.add(normalized);
        }
    }
    return Array.from(deduped);
}
function buildInlinePayloadHash(rawPayload) {
    if (rawPayload === undefined) {
        return null;
    }
    return createHash('sha256').update(JSON.stringify(rawPayload)).digest('hex');
}
function buildSourceSnapshot(payload) {
    return {
        ...(payload.sourceSnapshot ?? {}),
        ...(payload.rawPayload === undefined ? {} : { rawPayload: payload.rawPayload })
    };
}
function buildEvidenceSnapshotHash(input) {
    return createHash('sha256')
        .update(JSON.stringify({
        metaSignalId: input.metaSignalId,
        shopifyOrderId: input.shopifyOrderId,
        reportedAtUtc: input.reportedAtUtc.toISOString(),
        metaTouchpointOccurredAtUtc: input.metaTouchpointOccurredAtUtc?.toISOString() ?? null,
        matchBasis: input.matchBasis,
        confidenceScore: input.confidenceScore,
        sourceKind: input.sourceKind,
        rawPayloadHashes: input.rawPayloadHashes,
        sourceRecordIds: input.sourceRecordIds,
        ruleVersion: input.ruleVersion
    }))
        .digest('hex');
}
function evaluateEligibility(input) {
    const hasOrderTimestamp = input.orderOccurredAtUtc !== null;
    const hasMetaTouchpoint = input.metaTouchpointOccurredAtUtc !== null;
    const hasApprovedMatchBasis = input.matchBasis !== null;
    const hasRawPayloadTraceability = Boolean(input.rawPayloadReference || input.rawRecordId !== null);
    const hasIngestionRunReference = input.ingestionRunId !== null;
    const isOrderJoinable = ORDER_JOINABLE_SOURCE_KINDS.has(input.sourceKind);
    const hasConfidenceScore = input.confidenceScore !== null;
    const touchpointBeforeOrder = hasOrderTimestamp &&
        hasMetaTouchpoint &&
        input.metaTouchpointOccurredAtUtc.getTime() <= input.orderOccurredAtUtc.getTime();
    const withinAttributionWindow = hasOrderTimestamp &&
        hasMetaTouchpoint &&
        touchpointBeforeOrder &&
        input.metaTouchpointOccurredAtUtc.getTime() >=
            input.orderOccurredAtUtc.getTime() - input.attributionWindowDays * 24 * 60 * 60 * 1000;
    const confidenceAtLeastCanonical = (input.confidenceScore ?? -1) >= CANONICAL_THRESHOLD;
    const confidenceWithinParallelBand = input.confidenceScore !== null &&
        input.confidenceScore >= PARALLEL_THRESHOLD &&
        input.confidenceScore < CANONICAL_THRESHOLD;
    const disqualificationReasons = dedupeStrings([
        ...input.normalizationFailures,
        ...(hasOrderTimestamp ? [] : ['missing_order_timestamp']),
        ...(hasMetaTouchpoint ? [] : ['missing_meta_touchpoint_timestamp']),
        ...(hasApprovedMatchBasis ? [] : ['missing_approved_match_basis']),
        ...(hasRawPayloadTraceability ? [] : ['missing_raw_payload_traceability']),
        ...(hasIngestionRunReference ? [] : ['missing_ingestion_run_reference']),
        ...(isOrderJoinable ? [] : ['aggregate_only_or_non_joinable_source']),
        ...(hasConfidenceScore ? [] : ['missing_confidence_score']),
        ...(hasOrderTimestamp && hasMetaTouchpoint && !touchpointBeforeOrder ? ['meta_touchpoint_after_order'] : []),
        ...(hasOrderTimestamp && hasMetaTouchpoint && touchpointBeforeOrder && !withinAttributionWindow
            ? ['outside_attribution_window']
            : []),
        ...(input.confidenceScore !== null && input.confidenceScore < PARALLEL_THRESHOLD ? ['confidence_below_parallel_floor'] : [])
    ]);
    if (disqualificationReasons.length > 0) {
        return {
            eligibilityOutcome: 'ineligible',
            eligibilityReasons: disqualificationReasons,
            disqualificationReasons,
            parallelOnlyReasons: [],
            eligibilitySignals: {
                hasOrderTimestamp,
                hasMetaTouchpoint,
                hasApprovedMatchBasis,
                hasRawPayloadTraceability,
                hasIngestionRunReference,
                isOrderJoinable,
                touchpointBeforeOrder,
                withinAttributionWindow,
                hasConfidenceScore,
                confidenceAtLeastCanonical,
                confidenceWithinParallelBand
            }
        };
    }
    if (confidenceAtLeastCanonical) {
        return {
            eligibilityOutcome: 'eligible_canonical',
            eligibilityReasons: ['passed_meta_hard_guards', 'passed_meta_canonical_threshold'],
            disqualificationReasons: [],
            parallelOnlyReasons: [],
            eligibilitySignals: {
                hasOrderTimestamp,
                hasMetaTouchpoint,
                hasApprovedMatchBasis,
                hasRawPayloadTraceability,
                hasIngestionRunReference,
                isOrderJoinable,
                touchpointBeforeOrder,
                withinAttributionWindow,
                hasConfidenceScore,
                confidenceAtLeastCanonical,
                confidenceWithinParallelBand
            }
        };
    }
    return {
        eligibilityOutcome: 'eligible_parallel_only',
        eligibilityReasons: ['below_meta_canonical_threshold'],
        disqualificationReasons: [],
        parallelOnlyReasons: ['confidence_below_canonical_threshold'],
        eligibilitySignals: {
            hasOrderTimestamp,
            hasMetaTouchpoint,
            hasApprovedMatchBasis,
            hasRawPayloadTraceability,
            hasIngestionRunReference,
            isOrderJoinable,
            touchpointBeforeOrder,
            withinAttributionWindow,
            hasConfidenceScore,
            confidenceAtLeastCanonical,
            confidenceWithinParallelBand
        }
    };
}
async function loadOrderTimestamp(shopifyOrderId) {
    const result = await query(`
      SELECT
        shopify_order_id,
        processed_at,
        created_at_shopify,
        ingested_at
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `, [shopifyOrderId]);
    const row = result.rows[0] ?? null;
    if (!row) {
        throw new MetaAttributionEvidenceHttpError(404, 'order_not_found', 'Shopify order was not found');
    }
    return row;
}
function normalizePayload(payload, orderRow) {
    const normalizationFailures = [];
    const orderOccurredAt = resolveOrderOccurredAtUtc({
        shopifyOrderId: orderRow.shopify_order_id,
        processedAt: orderRow.processed_at,
        createdAtShopify: orderRow.created_at_shopify,
        ingestedAt: orderRow.ingested_at,
        landingSessionId: null,
        checkoutToken: null,
        cartToken: null
    }).orderOccurredAtUtc;
    const reportedAtUtc = parseRequiredTimestamp(payload.reportedAtUtc, 'reportedAtUtc');
    const sourceReceivedAt = parseOptionalTimestamp(payload.sourceReceivedAt, 'sourceReceivedAt');
    const metaTouchpointOccurredAtUtc = parseOptionalTimestamp(payload.metaTouchpointOccurredAtUtc, 'metaTouchpointOccurredAtUtc');
    const eventOrReportTimestampUtc = parseOptionalTimestamp(payload.eventOrReportTimestampUtc, 'eventOrReportTimestampUtc');
    const reportedConversionTimestampUtc = parseOptionalTimestamp(payload.reportedConversionTimestampUtc, 'reportedConversionTimestampUtc');
    const matchBasis = normalizeMatchBasis(payload.matchBasis, normalizationFailures);
    const observedMatchBases = normalizeObservedMatchBases(payload.observedMatchBases, matchBasis, normalizationFailures);
    const currencyCode = normalizeCurrencyCode(payload.currencyCode, normalizationFailures);
    const inlinePayloadHash = buildInlinePayloadHash(payload.rawPayload);
    const rawPayloadHashes = dedupeStrings([...(payload.rawPayloadHashes ?? []), ...(inlinePayloadHash ? [inlinePayloadHash] : [])]);
    const rawPayloadReference = normalizeNullableString(payload.rawPayloadReference) ??
        (payload.rawPayload === undefined ? null : `inline_request_payload:${payload.metaSignalId.trim()}`);
    const sourceRecordIds = payload.sourceRecordIds ?? [];
    const sourceSnapshotJson = buildSourceSnapshot(payload);
    const ruleVersion = normalizeNullableString(payload.ruleVersion) ?? META_ATTRIBUTION_RULE_VERSION;
    const confidenceScore = payload.confidenceScore ?? null;
    const attributionWindowDays = payload.attributionWindowDays ?? DEFAULT_ATTRIBUTION_WINDOW_DAYS;
    const evaluation = evaluateEligibility({
        orderOccurredAtUtc: orderOccurredAt,
        metaTouchpointOccurredAtUtc,
        attributionWindowDays,
        sourceKind: payload.sourceKind ?? 'order_scoped',
        matchBasis,
        confidenceScore,
        rawPayloadReference,
        rawRecordId: payload.rawRecordId ?? null,
        ingestionRunId: payload.ingestionRunId ?? null,
        normalizationFailures
    });
    return {
        organizationId: payload.organizationId ?? env.DEFAULT_ORGANIZATION_ID,
        shopifyOrderId: payload.shopifyOrderId.trim(),
        metaConnectionId: payload.metaConnectionId ?? null,
        rawRecordId: payload.rawRecordId ?? null,
        syncJobId: payload.syncJobId ?? null,
        ingestionRunId: payload.ingestionRunId ?? null,
        metaSignalId: payload.metaSignalId.trim(),
        sourceKind: payload.sourceKind ?? 'order_scoped',
        reportedAtUtc,
        sourceReceivedAt,
        orderOccurredAtUtc: orderOccurredAt,
        metaTouchpointOccurredAtUtc,
        eventOrReportTimestampUtc,
        reportedConversionTimestampUtc,
        attributionWindowDays,
        metaAttributionReason: payload.metaAttributionReason.trim(),
        campaignId: payload.campaignId.trim(),
        campaignName: normalizeNullableString(payload.campaignName),
        adAccountId: normalizeAdAccountId(payload.adAccountId),
        adId: normalizeNullableString(payload.adId),
        adSetId: normalizeNullableString(payload.adSetId),
        currencyCode,
        reportedConversionValue: payload.reportedConversionValue ?? null,
        reportedEventName: normalizeNullableString(payload.reportedEventName),
        isViewThrough: payload.isViewThrough ?? false,
        isClickThrough: payload.isClickThrough ?? false,
        matchBasis,
        observedMatchBases,
        confidenceScore,
        eligibilityOutcome: evaluation.eligibilityOutcome,
        eligibilityReasons: evaluation.eligibilityReasons,
        disqualificationReasons: evaluation.disqualificationReasons,
        parallelOnlyReasons: evaluation.parallelOnlyReasons,
        normalizationFailures,
        eligibilitySignals: evaluation.eligibilitySignals,
        sourceRecordIds,
        rawPayloadReference,
        rawPayloadHashes,
        evidenceSnapshotHash: buildEvidenceSnapshotHash({
            metaSignalId: payload.metaSignalId.trim(),
            shopifyOrderId: payload.shopifyOrderId.trim(),
            reportedAtUtc,
            metaTouchpointOccurredAtUtc,
            matchBasis,
            confidenceScore,
            sourceKind: payload.sourceKind ?? 'order_scoped',
            rawPayloadHashes,
            sourceRecordIds,
            ruleVersion
        }),
        sourceSnapshotJson,
        ruleVersion
    };
}
async function upsertMetaAttributionEvidence(normalized) {
    return withTransaction(async (client) => {
        const result = await client.query(`
        INSERT INTO meta_order_attribution_evidence (
          organization_id,
          shopify_order_id,
          meta_connection_id,
          raw_record_id,
          sync_job_id,
          ingestion_run_id,
          meta_signal_id,
          platform,
          source_kind,
          reported_at_utc,
          source_received_at,
          order_occurred_at_utc,
          meta_touchpoint_occurred_at_utc,
          event_or_report_timestamp_utc,
          reported_conversion_timestamp_utc,
          attribution_window_days,
          meta_attribution_reason,
          campaign_id,
          campaign_name,
          ad_account_id,
          ad_id,
          ad_set_id,
          currency_code,
          reported_conversion_value,
          reported_event_name,
          is_view_through,
          is_click_through,
          match_basis,
          observed_match_bases,
          confidence_score,
          eligibility_outcome,
          eligibility_reasons,
          disqualification_reasons,
          parallel_only_reasons,
          normalization_failures,
          eligibility_signals,
          source_record_ids,
          raw_payload_reference,
          raw_payload_hashes,
          evidence_snapshot_hash,
          source_snapshot_json,
          rule_version,
          updated_at
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5,
          $6,
          $7,
          'meta_ads',
          $8,
          $9,
          $10,
          $11,
          $12,
          $13,
          $14,
          $15,
          $16,
          $17,
          $18,
          $19,
          $20,
          $21,
          $22,
          $23,
          $24,
          $25,
          $26,
          $27,
          $28::text[],
          $29,
          $30,
          $31::jsonb,
          $32::jsonb,
          $33::jsonb,
          $34::jsonb,
          $35::jsonb,
          $36::jsonb,
          $37,
          $38::jsonb,
          $39,
          $40::jsonb,
          $41,
          now()
        )
        ON CONFLICT (organization_id, meta_signal_id)
        DO UPDATE SET
          shopify_order_id = EXCLUDED.shopify_order_id,
          meta_connection_id = EXCLUDED.meta_connection_id,
          raw_record_id = EXCLUDED.raw_record_id,
          sync_job_id = EXCLUDED.sync_job_id,
          ingestion_run_id = EXCLUDED.ingestion_run_id,
          source_kind = EXCLUDED.source_kind,
          reported_at_utc = EXCLUDED.reported_at_utc,
          source_received_at = EXCLUDED.source_received_at,
          order_occurred_at_utc = EXCLUDED.order_occurred_at_utc,
          meta_touchpoint_occurred_at_utc = EXCLUDED.meta_touchpoint_occurred_at_utc,
          event_or_report_timestamp_utc = EXCLUDED.event_or_report_timestamp_utc,
          reported_conversion_timestamp_utc = EXCLUDED.reported_conversion_timestamp_utc,
          attribution_window_days = EXCLUDED.attribution_window_days,
          meta_attribution_reason = EXCLUDED.meta_attribution_reason,
          campaign_id = EXCLUDED.campaign_id,
          campaign_name = EXCLUDED.campaign_name,
          ad_account_id = EXCLUDED.ad_account_id,
          ad_id = EXCLUDED.ad_id,
          ad_set_id = EXCLUDED.ad_set_id,
          currency_code = EXCLUDED.currency_code,
          reported_conversion_value = EXCLUDED.reported_conversion_value,
          reported_event_name = EXCLUDED.reported_event_name,
          is_view_through = EXCLUDED.is_view_through,
          is_click_through = EXCLUDED.is_click_through,
          match_basis = EXCLUDED.match_basis,
          observed_match_bases = EXCLUDED.observed_match_bases,
          confidence_score = EXCLUDED.confidence_score,
          eligibility_outcome = EXCLUDED.eligibility_outcome,
          eligibility_reasons = EXCLUDED.eligibility_reasons,
          disqualification_reasons = EXCLUDED.disqualification_reasons,
          parallel_only_reasons = EXCLUDED.parallel_only_reasons,
          normalization_failures = EXCLUDED.normalization_failures,
          eligibility_signals = EXCLUDED.eligibility_signals,
          source_record_ids = EXCLUDED.source_record_ids,
          raw_payload_reference = EXCLUDED.raw_payload_reference,
          raw_payload_hashes = EXCLUDED.raw_payload_hashes,
          evidence_snapshot_hash = EXCLUDED.evidence_snapshot_hash,
          source_snapshot_json = EXCLUDED.source_snapshot_json,
          rule_version = EXCLUDED.rule_version,
          updated_at = now()
        RETURNING
          id::text,
          eligibility_outcome,
          confidence_score,
          order_occurred_at_utc,
          meta_touchpoint_occurred_at_utc,
          match_basis,
          currency_code
      `, [
            normalized.organizationId,
            normalized.shopifyOrderId,
            normalized.metaConnectionId,
            normalized.rawRecordId,
            normalized.syncJobId,
            normalized.ingestionRunId,
            normalized.metaSignalId,
            normalized.sourceKind,
            normalized.reportedAtUtc,
            normalized.sourceReceivedAt,
            normalized.orderOccurredAtUtc,
            normalized.metaTouchpointOccurredAtUtc,
            normalized.eventOrReportTimestampUtc,
            normalized.reportedConversionTimestampUtc,
            normalized.attributionWindowDays,
            normalized.metaAttributionReason,
            normalized.campaignId,
            normalized.campaignName,
            normalized.adAccountId,
            normalized.adId,
            normalized.adSetId,
            normalized.currencyCode,
            normalized.reportedConversionValue,
            normalized.reportedEventName,
            normalized.isViewThrough,
            normalized.isClickThrough,
            normalized.matchBasis,
            normalized.observedMatchBases,
            normalized.confidenceScore,
            normalized.eligibilityOutcome,
            JSON.stringify(normalized.eligibilityReasons),
            JSON.stringify(normalized.disqualificationReasons),
            JSON.stringify(normalized.parallelOnlyReasons),
            JSON.stringify(normalized.normalizationFailures),
            JSON.stringify(normalized.eligibilitySignals),
            JSON.stringify(normalized.sourceRecordIds),
            normalized.rawPayloadReference,
            JSON.stringify(normalized.rawPayloadHashes),
            normalized.evidenceSnapshotHash,
            JSON.stringify(normalized.sourceSnapshotJson),
            normalized.ruleVersion
        ]);
        return result.rows[0];
    });
}
export function createMetaAttributionEvidenceAdminRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.use(requireAdmin);
    router.post('/attribution-evidence', async (req, res, next) => {
        try {
            const payload = parseInput(req.body ?? {});
            const orderRow = await loadOrderTimestamp(payload.shopifyOrderId.trim());
            const normalized = normalizePayload(payload, orderRow);
            const stored = await upsertMetaAttributionEvidence(normalized);
            logInfo('meta_order_attribution_evidence_ingested', {
                service: process.env.K_SERVICE ?? 'roas-radar-api',
                organizationId: normalized.organizationId,
                shopifyOrderId: normalized.shopifyOrderId,
                metaSignalId: normalized.metaSignalId,
                eligibilityOutcome: normalized.eligibilityOutcome,
                confidenceScore: normalized.confidenceScore,
                rawPayloadTraceable: Boolean(normalized.rawPayloadReference || normalized.rawRecordId !== null),
                ingestionRunId: normalized.ingestionRunId,
                ruleVersion: normalized.ruleVersion
            });
            res.status(201).json({
                ok: true,
                evidenceId: stored.id,
                eligibilityOutcome: stored.eligibility_outcome,
                confidenceScore: stored.confidence_score === null ? null : Number(stored.confidence_score),
                normalized: {
                    shopifyOrderId: normalized.shopifyOrderId,
                    metaSignalId: normalized.metaSignalId,
                    orderOccurredAtUtc: stored.order_occurred_at_utc?.toISOString() ?? null,
                    metaTouchpointOccurredAtUtc: stored.meta_touchpoint_occurred_at_utc?.toISOString() ?? null,
                    matchBasis: stored.match_basis,
                    currencyCode: stored.currency_code
                }
            });
        }
        catch (error) {
            if (error instanceof MetaAttributionEvidenceHttpError && error.statusCode < 500) {
                logWarning('meta_order_attribution_evidence_rejected', {
                    service: process.env.K_SERVICE ?? 'roas-radar-api',
                    statusCode: error.statusCode,
                    code: error.code,
                    message: error.message,
                    shopifyOrderId: normalizeNullableString(req.body?.shopifyOrderId),
                    metaSignalId: normalizeNullableString(req.body?.metaSignalId),
                    details: error.details ?? null
                });
            }
            next(error);
        }
    });
    return router;
}
export const __metaAttributionEvidenceTestUtils = {
    normalizeAdAccountId,
    normalizeCurrencyCode,
    normalizeMatchBasis,
    normalizeObservedMatchBases,
    evaluateEligibility,
    normalizePayload
};
