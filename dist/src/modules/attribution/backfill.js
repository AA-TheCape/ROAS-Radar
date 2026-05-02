import { withTransaction } from '../../db/pool.js';
import { emitAttributionResolverOutcomeLog, logError, logInfo } from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { formatDateInTimezone, getReportingTimezone } from '../settings/index.js';
import { applyShopifyOrderWriteback } from '../shopify/writeback.js';
import { extractAttributionCandidatesForOrder } from './candidate-extraction.js';
import { buildEmptyOrderAttributionBackfillProgress, parseOrderAttributionBackfillProgress } from './backfill-progress.js';
import { ATTRIBUTION_MODELS, computeAttributionOutputs, computeSingleWinnerCredits } from './engine.js';
import { buildAttributionConfidenceLabel, buildAttributionMatchSource, buildOrderAttributionAuditRecord } from './order-attribution-audit.js';
import { insertAttributionDecisionArtifact } from './decision-artifacts.js';
import { resolveAttributionTierForVersion } from './resolver.js';
import { ATTRIBUTION_RESOLVER_RULE_VERSION } from './rule-version.js';
const ATTRIBUTION_MODEL_VERSION = 1;
const MAX_PREVIEW_ORDERS = 25;
const MAX_REPORTED_FAILURES = 100;
const MISSING_ATTRIBUTION_SQL = `
  attribution.shopify_order_id IS NULL
  OR (
    attribution.session_id IS NULL
    AND attribution.attributed_source IS NULL
    AND attribution.attributed_medium IS NULL
    AND attribution.attributed_campaign IS NULL
    AND attribution.attributed_content IS NULL
    AND attribution.attributed_term IS NULL
    AND attribution.attributed_click_id_value IS NULL
  )
`;
export class OrderAttributionBackfillRunError extends Error {
    code;
    report;
    constructor(message, options) {
        super(message, options.cause === undefined ? undefined : { cause: options.cause });
        this.name = 'OrderAttributionBackfillRunError';
        this.code = options.code;
        this.report = options.report;
    }
}
function normalizeFailureCode(error, fallback) {
    if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
        return error.code.trim();
    }
    if (error instanceof Error && error.name.trim()) {
        return error.name.trim();
    }
    return fallback;
}
function normalizeFailureMessage(error, fallback) {
    if (error instanceof Error && error.message.trim()) {
        return error.message.trim();
    }
    if (typeof error === 'string' && error.trim()) {
        return error.trim();
    }
    return fallback;
}
function buildEmptyScopeMetrics() {
    return {
        totalOrdersInScope: 0,
        ordersMissingAttribution: 0,
        ordersWithAttribution: 0,
        completenessRate: 1
    };
}
function buildOrderAttributionBackfillReport(input) {
    return {
        requestedBy: input.requestedBy,
        workerId: input.workerId,
        dryRun: input.dryRun,
        scope: {
            windowStart: input.windowStart.toISOString(),
            windowEnd: input.windowEnd.toISOString(),
            onlyWebOrders: input.onlyWebOrders,
            limit: input.limit
        },
        beforeMetrics: input.beforeMetrics,
        afterMetrics: input.afterMetrics,
        scannedOrders: input.scannedOrders,
        recoverableOrders: input.recoverableOrders,
        recoveredOrders: input.recoveredOrders,
        unrecoverableOrders: input.unrecoverableOrders,
        failedOrders: input.failedOrders,
        shopifyWritebackCompleted: input.shopifyWritebackCompleted,
        shopifyWritebackSkipped: input.shopifyWritebackSkipped,
        shopifyWritebackFailed: input.shopifyWritebackFailed,
        failures: input.failures,
        preview: input.preview
    };
}
function recordFailure(failures, failure) {
    if (failures.length >= MAX_REPORTED_FAILURES) {
        return;
    }
    failures.push(failure);
}
function normalizeNullableString(value) {
    const normalized = value?.trim();
    return normalized ? normalized : null;
}
function resolveOrderOccurredAt(order) {
    return order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
}
function isSameResolvedTouchpoint(left, right) {
    return (left.sessionId === right.sessionId &&
        left.sourceTouchEventId === right.sourceTouchEventId &&
        left.ingestionSource === right.ingestionSource &&
        left.occurredAt.getTime() === right.occurredAt.getTime());
}
function serializeResolvedTouchpoint(touchpoint) {
    return {
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
        isDirect: touchpoint.isDirect
    };
}
async function fetchOrder(client, shopifyOrderId) {
    const result = await client.query(`
      SELECT
        id::text,
        shopify_order_id,
        total_price,
        processed_at,
        created_at_shopify,
        ingested_at,
        payload_hash,
        landing_session_id::text AS landing_session_id,
        checkout_token,
        cart_token,
        email_hash,
        customer_identity_id::text AS customer_identity_id,
        source_name,
        attribution_tier,
        raw_payload
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `, [shopifyOrderId]);
    return result.rows[0] ?? null;
}
async function resolveAttributionJourney(client, order) {
    const resolverInput = await extractAttributionCandidatesForOrder(client, {
        shopifyOrderId: order.shopify_order_id,
        processedAt: order.processed_at,
        createdAtShopify: order.created_at_shopify,
        ingestedAt: order.ingested_at,
        landingSessionId: order.landing_session_id,
        checkoutToken: order.checkout_token,
        cartToken: order.cart_token,
        emailHash: order.email_hash,
        customerIdentityId: order.customer_identity_id,
        sourceName: order.source_name,
        rawPayload: order.raw_payload
    });
    return {
        resolverInput,
        journey: resolveAttributionTierForVersion(resolverInput, ATTRIBUTION_RESOLVER_RULE_VERSION)
    };
}
function selectPrimaryCredit(credits) {
    return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}
async function persistAttribution(client, order, evaluation, backfillRunId) {
    const { journey, resolverInput } = evaluation;
    const orderOccurredAt = journey.orderOccurredAtUtc ?? resolveOrderOccurredAt(order);
    const outputs = computeAttributionOutputs(journey.touchpoints, {
        orderOccurredAt,
        orderRevenue: order.total_price
    });
    if (journey.winner) {
        const winner = journey.winner;
        const winnerIndex = journey.touchpoints.findIndex((touchpoint) => isSameResolvedTouchpoint(touchpoint, winner));
        if (winnerIndex >= 0) {
            outputs.last_touch = computeSingleWinnerCredits('last_touch', journey.touchpoints, winnerIndex, order.total_price);
        }
    }
    const primaryCredit = selectPrimaryCredit(outputs.last_touch);
    if (!primaryCredit) {
        throw new Error(`Failed to compute attribution credits for Shopify order ${order.shopify_order_id}`);
    }
    const matchedAt = new Date();
    const orderAttributionAudit = buildOrderAttributionAuditRecord(journey, matchedAt);
    const matchSource = buildAttributionMatchSource(journey);
    const confidenceLabel = buildAttributionConfidenceLabel(journey.confidenceScore);
    const decisionArtifactId = await insertAttributionDecisionArtifact({
        client,
        order: {
            shopifyOrderId: order.shopify_order_id,
            payloadHash: order.payload_hash,
            attributionTier: order.attribution_tier
        },
        journey,
        resolverInput,
        orderAttributionAudit,
        resolverRunSource: backfillRunId ? 'manual_backfill' : 'forward_processing',
        resolverTriggeredBy: `backfill:${backfillRunId ?? 'adhoc'}`,
        backfillRunId
    });
    await client.query('DELETE FROM attribution_order_credits WHERE shopify_order_id = $1', [order.shopify_order_id]);
    for (const model of ATTRIBUTION_MODELS) {
        for (const credit of outputs[model]) {
            await client.query(`
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
            $2,
            $3,
            $4::uuid,
            $5,
            $6,
            $7,
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
            $19
          )
        `, [
                order.shopify_order_id,
                credit.attributionModel,
                credit.touchpointPosition,
                credit.sessionId,
                credit.touchpointOccurredAt,
                normalizeNullableString(credit.source),
                normalizeNullableString(credit.medium),
                normalizeNullableString(credit.campaign),
                normalizeNullableString(credit.content),
                normalizeNullableString(credit.term),
                normalizeNullableString(credit.clickIdType),
                normalizeNullableString(credit.clickIdValue),
                credit.creditWeight,
                credit.revenueCredit,
                credit.isPrimary,
                credit.attributionReason,
                ATTRIBUTION_MODEL_VERSION,
                matchSource,
                confidenceLabel
            ]);
        }
    }
    await client.query(`
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
        confidence_label,
        meta_attribution_evaluation_outcome,
        meta_attribution_affected_canonical,
        attribution_decision_artifact_id,
        resolver_rule_version
      )
      VALUES (
        $1,
        $2::uuid,
        'last_touch',
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
        $15,
        $16,
        $17,
        $18::uuid,
        $19
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
        confidence_label = EXCLUDED.confidence_label,
        meta_attribution_evaluation_outcome = EXCLUDED.meta_attribution_evaluation_outcome,
        meta_attribution_affected_canonical = EXCLUDED.meta_attribution_affected_canonical,
        attribution_decision_artifact_id = EXCLUDED.attribution_decision_artifact_id,
        resolver_rule_version = EXCLUDED.resolver_rule_version
    `, [
        order.shopify_order_id,
        primaryCredit.sessionId,
        normalizeNullableString(primaryCredit.source),
        normalizeNullableString(primaryCredit.medium),
        normalizeNullableString(primaryCredit.campaign),
        normalizeNullableString(primaryCredit.content),
        normalizeNullableString(primaryCredit.term),
        normalizeNullableString(primaryCredit.clickIdType),
        normalizeNullableString(primaryCredit.clickIdValue),
        journey.confidenceScore,
        primaryCredit.attributionReason,
        matchedAt,
        ATTRIBUTION_MODEL_VERSION,
        matchSource,
        confidenceLabel,
        'not_evaluated',
        false,
        decisionArtifactId,
        journey.resolverRuleVersion
    ]);
    await client.query(`
      UPDATE shopify_orders
      SET
        attribution_tier = $2,
        attribution_source = $3,
        attribution_matched_at = $4,
        attribution_reason = $5,
        attribution_snapshot = $6::jsonb,
        attribution_snapshot_updated_at = $4,
        attribution_resolver_rule_version = $7,
        meta_attribution_evaluation_outcome = 'not_evaluated',
        meta_attribution_present = false,
        meta_attribution_affected_canonical = false,
        latest_attribution_decision_artifact_id = $8::uuid
      WHERE shopify_order_id = $1
    `, [
        order.shopify_order_id,
        orderAttributionAudit.tier,
        orderAttributionAudit.source,
        orderAttributionAudit.matchedAt,
        orderAttributionAudit.reason,
        JSON.stringify({
            tier: journey.tier,
            resolverRuleVersion: journey.resolverRuleVersion,
            decisionArtifactId,
            attributionReason: journey.attributionReason,
            orderOccurredAtUtc: journey.orderOccurredAtUtc?.toISOString() ?? null,
            normalizationFailures: journey.normalizationFailures,
            confidenceScore: journey.confidenceScore,
            winner: journey.winner ? serializeResolvedTouchpoint(journey.winner) : null,
            timeline: journey.touchpoints.map(serializeResolvedTouchpoint)
        }),
        journey.resolverRuleVersion,
        decisionArtifactId
    ]);
    emitAttributionResolverOutcomeLog({
        shopifyOrderId: order.shopify_order_id,
        orderOccurredAtUtc: journey.orderOccurredAtUtc,
        tier: journey.tier,
        attributionReason: journey.attributionReason,
        confidenceScore: journey.confidenceScore,
        resolverRuleVersion: journey.resolverRuleVersion,
        decisionArtifactId,
        pipeline: 'order_backfill',
        touchpoints: journey.touchpoints,
        winner: journey.winner,
        normalizationFailures: journey.normalizationFailures
    });
}
async function fetchScopeMetrics(client, options) {
    const result = await client.query(`
      WITH scoped_orders AS (
        SELECT o.shopify_order_id
        FROM shopify_orders o
        WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1
          AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= $2
          AND ($3::boolean = false OR COALESCE(o.source_name, '') = 'web')
      )
      SELECT
        COUNT(*)::text AS total_orders_in_scope,
        COUNT(*) FILTER (WHERE ${MISSING_ATTRIBUTION_SQL})::text AS orders_missing_attribution,
        COUNT(*) FILTER (WHERE NOT (${MISSING_ATTRIBUTION_SQL}))::text AS orders_with_attribution
      FROM scoped_orders scoped
      LEFT JOIN attribution_results attribution
        ON attribution.shopify_order_id = scoped.shopify_order_id
    `, [options.windowStart, options.windowEnd, options.onlyWebOrders]);
    const row = result.rows[0];
    const totalOrdersInScope = Number(row?.total_orders_in_scope ?? '0');
    const ordersMissingAttribution = Number(row?.orders_missing_attribution ?? '0');
    const ordersWithAttribution = Number(row?.orders_with_attribution ?? '0');
    return {
        totalOrdersInScope,
        ordersMissingAttribution,
        ordersWithAttribution,
        completenessRate: totalOrdersInScope > 0 ? ordersWithAttribution / totalOrdersInScope : 1
    };
}
async function fetchBackfillCandidates(client, options) {
    const result = await client.query(`
      SELECT
        o.id::text AS order_row_id,
        o.shopify_order_id,
        COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at
      FROM shopify_orders o
      WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1
        AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= $2
        AND ($3::boolean = false OR COALESCE(o.source_name, '') = 'web')
        AND (
          $4::timestamptz IS NULL
          OR COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) > $4::timestamptz
          OR (
            COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) = $4::timestamptz
            AND o.id > $5::bigint
          )
        )
      ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) ASC, o.id ASC
      LIMIT $6
    `, [
        options.windowStart,
        options.windowEnd,
        options.onlyWebOrders,
        options.checkpoint.lastOrderOccurredAt,
        options.checkpoint.lastOrderRowId ?? '0',
        options.limit
    ]);
    return result.rows;
}
function previewRowForOrder(order, journey) {
    return {
        shopifyOrderId: order.shopify_order_id,
        orderOccurredAt: resolveOrderOccurredAt(order).toISOString(),
        recoverable: Boolean(journey.winner),
        touchpointCount: journey.touchpoints.length,
        winnerSessionId: journey.winner?.sessionId ?? null,
        attributionReason: journey.winner?.attributionReason ?? 'unattributed'
    };
}
export async function backfillRecentOrdersWithRecoveredAttribution(options) {
    if (!(options.windowStart instanceof Date) || Number.isNaN(options.windowStart.getTime())) {
        throw new Error('windowStart must be a valid Date');
    }
    if (!(options.windowEnd instanceof Date) || Number.isNaN(options.windowEnd.getTime())) {
        throw new Error('windowEnd must be a valid Date');
    }
    if (options.windowStart > options.windowEnd) {
        throw new Error('windowStart must be on or before windowEnd');
    }
    const limit = Math.max(1, options.limit ?? 500);
    const dryRun = options.dryRun ?? false;
    const onlyWebOrders = options.onlyWebOrders ?? true;
    const writeToShopifyWhenAvailable = options.writeToShopifyWhenAvailable ?? true;
    const applyWriteback = options.applyWriteback ?? applyShopifyOrderWriteback;
    const progress = parseOrderAttributionBackfillProgress(options.progress ?? buildEmptyOrderAttributionBackfillProgress());
    const publishProgress = options.onProgress
        ? async () => options.onProgress?.(parseOrderAttributionBackfillProgress(progress))
        : null;
    let beforeMetrics = progress.beforeMetrics ?? buildEmptyScopeMetrics();
    let afterMetrics = buildEmptyScopeMetrics();
    let scannedOrders = progress.scannedOrders;
    let recoverableOrders = progress.recoverableOrders;
    let recoveredOrders = progress.recoveredOrders;
    let unrecoverableOrders = progress.unrecoverableOrders;
    let failedOrders = progress.failedOrders;
    let shopifyWritebackCompleted = progress.shopifyWritebackCompleted;
    let shopifyWritebackSkipped = progress.shopifyWritebackSkipped;
    let shopifyWritebackFailed = progress.shopifyWritebackFailed;
    const failures = [...progress.failures];
    const preview = [...progress.preview];
    try {
        logInfo('order_attribution_backfill_started', {
            requestedBy: options.requestedBy,
            workerId: options.workerId,
            dryRun,
            onlyWebOrders,
            limit,
            windowStart: options.windowStart.toISOString(),
            windowEnd: options.windowEnd.toISOString()
        });
        if (progress.beforeMetrics === null) {
            beforeMetrics = await withTransaction((client) => fetchScopeMetrics(client, {
                windowStart: options.windowStart,
                windowEnd: options.windowEnd,
                onlyWebOrders
            }));
            progress.beforeMetrics = beforeMetrics;
            if (publishProgress) {
                await publishProgress();
            }
        }
        const reportingDates = new Set();
        const reportingTimezone = dryRun ? null : await withTransaction((client) => getReportingTimezone(client));
        while (!progress.cursor.completed) {
            const candidateRows = await withTransaction((client) => fetchBackfillCandidates(client, {
                windowStart: options.windowStart,
                windowEnd: options.windowEnd,
                onlyWebOrders,
                limit,
                checkpoint: progress.cursor
            }));
            if (candidateRows.length === 0) {
                progress.cursor.completed = true;
                if (publishProgress) {
                    await publishProgress();
                }
                break;
            }
            for (const candidate of candidateRows) {
                try {
                    const resolved = await withTransaction(async (client) => {
                        const order = await fetchOrder(client, candidate.shopify_order_id);
                        if (!order) {
                            return null;
                        }
                        const evaluation = await resolveAttributionJourney(client, order);
                        return { order, evaluation };
                    });
                    scannedOrders += 1;
                    if (!resolved) {
                        failedOrders += 1;
                        recordFailure(failures, {
                            orderId: candidate.shopify_order_id,
                            code: 'order_not_found',
                            message: `Shopify order ${candidate.shopify_order_id} was not found during backfill processing`
                        });
                        continue;
                    }
                    if (preview.length < MAX_PREVIEW_ORDERS) {
                        preview.push(previewRowForOrder(resolved.order, resolved.evaluation.journey));
                    }
                    if (resolved.evaluation.journey.winner) {
                        recoverableOrders += 1;
                    }
                    else {
                        unrecoverableOrders += 1;
                    }
                    if (!dryRun) {
                        await withTransaction(async (client) => {
                            await persistAttribution(client, resolved.order, resolved.evaluation, options.runId ?? null);
                            if (reportingTimezone) {
                                reportingDates.add(formatDateInTimezone(resolveOrderOccurredAt(resolved.order), reportingTimezone));
                            }
                        });
                        recoveredOrders += 1;
                        if (writeToShopifyWhenAvailable) {
                            try {
                                const writeback = await applyWriteback({
                                    workerId: options.workerId,
                                    shopifyOrderId: resolved.order.shopify_order_id,
                                    requestedReason: 'recent_order_attribution_backfill'
                                });
                                if (writeback.status === 'completed') {
                                    shopifyWritebackCompleted += 1;
                                }
                                else {
                                    shopifyWritebackSkipped += 1;
                                }
                            }
                            catch (error) {
                                shopifyWritebackFailed += 1;
                                recordFailure(failures, {
                                    orderId: resolved.order.shopify_order_id,
                                    code: normalizeFailureCode(error, 'shopify_writeback_failed'),
                                    message: normalizeFailureMessage(error, `Shopify writeback failed for Shopify order ${resolved.order.shopify_order_id}`)
                                });
                                logError('order_attribution_backfill_shopify_writeback_failed', error, {
                                    requestedBy: options.requestedBy,
                                    workerId: options.workerId,
                                    shopifyOrderId: resolved.order.shopify_order_id
                                });
                            }
                        }
                    }
                }
                catch (error) {
                    scannedOrders += 1;
                    failedOrders += 1;
                    recordFailure(failures, {
                        orderId: candidate.shopify_order_id,
                        code: normalizeFailureCode(error, 'order_attribution_backfill_failed'),
                        message: normalizeFailureMessage(error, `Failed to backfill Shopify order ${candidate.shopify_order_id}`)
                    });
                    logError('order_attribution_backfill_order_failed', error, {
                        requestedBy: options.requestedBy,
                        workerId: options.workerId,
                        shopifyOrderId: candidate.shopify_order_id
                    });
                }
            }
            const lastCandidate = candidateRows[candidateRows.length - 1];
            progress.scannedOrders = scannedOrders;
            progress.recoverableOrders = recoverableOrders;
            progress.recoveredOrders = recoveredOrders;
            progress.unrecoverableOrders = unrecoverableOrders;
            progress.failedOrders = failedOrders;
            progress.shopifyWritebackCompleted = shopifyWritebackCompleted;
            progress.shopifyWritebackSkipped = shopifyWritebackSkipped;
            progress.shopifyWritebackFailed = shopifyWritebackFailed;
            progress.failures = failures;
            progress.preview = preview;
            progress.cursor.lastOrderOccurredAt = lastCandidate.order_occurred_at.toISOString();
            progress.cursor.lastOrderRowId = lastCandidate.order_row_id;
            progress.cursor.batchesProcessed += 1;
            if (publishProgress) {
                await publishProgress();
            }
        }
        if (!dryRun && reportingDates.size > 0) {
            await withTransaction((client) => refreshDailyReportingMetrics(client, [...reportingDates]));
        }
        afterMetrics = dryRun
            ? beforeMetrics
            : await withTransaction((client) => fetchScopeMetrics(client, {
                windowStart: options.windowStart,
                windowEnd: options.windowEnd,
                onlyWebOrders
            }));
        const report = buildOrderAttributionBackfillReport({
            requestedBy: options.requestedBy,
            workerId: options.workerId,
            dryRun,
            windowStart: options.windowStart,
            windowEnd: options.windowEnd,
            onlyWebOrders,
            limit,
            beforeMetrics,
            afterMetrics,
            scannedOrders,
            recoverableOrders,
            recoveredOrders,
            unrecoverableOrders,
            failedOrders,
            shopifyWritebackCompleted,
            shopifyWritebackSkipped,
            shopifyWritebackFailed,
            failures,
            preview
        });
        logInfo(dryRun ? 'order_attribution_backfill_dry_run_completed' : 'order_attribution_backfill_completed', report);
        return report;
    }
    catch (error) {
        const partialReport = buildOrderAttributionBackfillReport({
            requestedBy: options.requestedBy,
            workerId: options.workerId,
            dryRun,
            windowStart: options.windowStart,
            windowEnd: options.windowEnd,
            onlyWebOrders,
            limit,
            beforeMetrics,
            afterMetrics,
            scannedOrders,
            recoverableOrders,
            recoveredOrders,
            unrecoverableOrders,
            failedOrders,
            shopifyWritebackCompleted,
            shopifyWritebackSkipped,
            shopifyWritebackFailed,
            failures,
            preview
        });
        logError('order_attribution_backfill_run_failed', error, {
            requestedBy: options.requestedBy,
            workerId: options.workerId,
            scannedOrders,
            recoveredOrders,
            unrecoverableOrders,
            writebackCompleted: shopifyWritebackCompleted,
            failedOrders
        });
        throw new OrderAttributionBackfillRunError(normalizeFailureMessage(error, 'Order attribution backfill job failed'), {
            code: normalizeFailureCode(error, 'order_attribution_backfill_run_failed'),
            report: toOrderAttributionBackfillJobReport(partialReport),
            cause: error
        });
    }
}
export function toOrderAttributionBackfillJobReport(report) {
    return {
        scanned: report.scannedOrders,
        recovered: report.recoveredOrders,
        unrecoverable: report.unrecoverableOrders,
        writebackCompleted: report.shopifyWritebackCompleted,
        failures: report.failures
    };
}
