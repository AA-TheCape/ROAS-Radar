import { Buffer } from 'node:buffer';
import { Router } from 'express';
import { z } from 'zod';
import { ATTRIBUTION_MODEL_KEYS, normalizeAttributionCreditRecordV1, normalizeAttributionExplainRecordV1, normalizeAttributionResultRecordV1 } from '../../../packages/attribution-schema/index.js';
import { query } from '../../db/pool.js';
import { attachAuthContext, requireAuthenticated } from '../auth/index.js';
import { getReportingTimezone } from '../settings/index.js';
class AttributionReadHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'AttributionReadHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const modelKeySchema = z.enum(ATTRIBUTION_MODEL_KEYS);
const uuidSchema = z.string().uuid();
const resultsQuerySchema = z
    .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    modelKey: modelKeySchema,
    runId: uuidSchema.optional(),
    orderId: z.string().trim().min(1).optional(),
    source: z.string().trim().min(1).optional(),
    medium: z.string().trim().min(1).optional(),
    campaign: z.string().trim().min(1).optional(),
    limit: z.coerce.number().int().positive().max(200).optional().default(50),
    cursor: z.string().trim().min(1).optional()
})
    .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'startDate must be on or before endDate',
            path: ['startDate']
        });
    }
});
const channelTotalsQuerySchema = z
    .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    runId: uuidSchema.optional(),
    orderId: z.string().trim().min(1).optional(),
    source: z.string().trim().min(1).optional(),
    medium: z.string().trim().min(1).optional(),
    campaign: z.string().trim().min(1).optional()
})
    .superRefine((value, ctx) => {
    if (value.startDate > value.endDate) {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'startDate must be on or before endDate',
            path: ['startDate']
        });
    }
});
const explainabilityParamsSchema = z.object({
    orderId: z.string().trim().min(1)
});
const explainabilityQuerySchema = z.object({
    runId: uuidSchema.optional(),
    modelKey: modelKeySchema.optional()
});
const cursorSchema = z.object({
    orderOccurredAtUtc: z.string().datetime(),
    orderId: z.string().min(1),
    runId: uuidSchema
});
function parseInput(schema, input) {
    try {
        return schema.parse(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new AttributionReadHttpError(400, 'invalid_request', 'Invalid attribution query parameters', error.flatten());
        }
        throw error;
    }
}
function asObjectRecord(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}
function encodeCursor(input) {
    return Buffer.from(JSON.stringify(input), 'utf8').toString('base64url');
}
function decodeCursor(cursor) {
    if (!cursor) {
        return null;
    }
    try {
        const decoded = Buffer.from(cursor, 'base64url').toString('utf8');
        return cursorSchema.parse(JSON.parse(decoded));
    }
    catch (error) {
        throw new AttributionReadHttpError(400, 'invalid_cursor', 'Invalid attribution pagination cursor', {
            cursor,
            reason: error instanceof Error ? error.message : String(error)
        });
    }
}
function buildResultsFilters(input, reportingTimezone, cursor) {
    const params = [input.modelKey, input.startDate, input.endDate, reportingTimezone];
    const filters = [
        'summary.model_key = $1',
        "timezone($4::text, summary.order_occurred_at_utc) >= $2::date",
        "timezone($4::text, summary.order_occurred_at_utc) < ($3::date + interval '1 day')"
    ];
    if (input.runId) {
        params.push(input.runId);
        filters.push(`summary.run_id = $${params.length}::uuid`);
    }
    if (input.orderId) {
        params.push(input.orderId);
        filters.push(`summary.order_id = $${params.length}`);
    }
    if (input.source || input.medium || input.campaign) {
        const dimensionFilters = [
            'filter_credit.run_id = summary.run_id',
            'filter_credit.order_id = summary.order_id',
            'filter_credit.model_key = summary.model_key'
        ];
        if (input.source) {
            params.push(input.source);
            dimensionFilters.push(`filter_credit.source = $${params.length}`);
        }
        if (input.medium) {
            params.push(input.medium);
            dimensionFilters.push(`filter_credit.medium = $${params.length}`);
        }
        if (input.campaign) {
            params.push(input.campaign);
            dimensionFilters.push(`filter_credit.campaign = $${params.length}`);
        }
        filters.push(`EXISTS (
        SELECT 1
        FROM attribution_model_credits filter_credit
        WHERE ${dimensionFilters.join(' AND ')}
      )`);
    }
    if (cursor) {
        params.push(cursor.orderOccurredAtUtc, cursor.orderId, cursor.runId);
        const occurredAtParam = params.length - 2;
        const orderIdParam = params.length - 1;
        const runIdParam = params.length;
        filters.push(`(summary.order_occurred_at_utc, summary.order_id, summary.run_id) < ($${occurredAtParam}::timestamptz, $${orderIdParam}, $${runIdParam}::uuid)`);
    }
    return {
        sql: `WHERE ${filters.join(' AND ')}`,
        params
    };
}
function buildChannelTotalsFilters(input, reportingTimezone) {
    const params = [input.startDate, input.endDate, reportingTimezone];
    const filters = [
        "timezone($3::text, summary.order_occurred_at_utc) >= $1::date",
        "timezone($3::text, summary.order_occurred_at_utc) < ($2::date + interval '1 day')"
    ];
    if (input.runId) {
        params.push(input.runId);
        filters.push(`summary.run_id = $${params.length}::uuid`);
    }
    if (input.orderId) {
        params.push(input.orderId);
        filters.push(`summary.order_id = $${params.length}`);
    }
    if (input.source) {
        params.push(input.source);
        filters.push(`credit.source = $${params.length}`);
    }
    if (input.medium) {
        params.push(input.medium);
        filters.push(`credit.medium = $${params.length}`);
    }
    if (input.campaign) {
        params.push(input.campaign);
        filters.push(`credit.campaign = $${params.length}`);
    }
    return {
        sql: `WHERE ${filters.join(' AND ')}`,
        params
    };
}
async function resolveExplainabilityRunId(orderId, requestedRunId) {
    if (requestedRunId) {
        return requestedRunId;
    }
    const result = await query(`
      SELECT summary.run_id::text AS run_id
      FROM attribution_model_summaries summary
      WHERE summary.order_id = $1
      ORDER BY summary.generated_at_utc DESC, summary.run_id DESC
      LIMIT 1
    `, [orderId]);
    const runId = result.rows[0]?.run_id;
    if (!runId) {
        throw new AttributionReadHttpError(404, 'attribution_order_not_found', `No attribution results were found for order ${orderId}`);
    }
    return runId;
}
export function createAttributionReadRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.use(requireAuthenticated);
    router.get('/results', async (req, res, next) => {
        try {
            const input = parseInput(resultsQuerySchema, req.query);
            const cursor = decodeCursor(input.cursor);
            const reportingTimezone = await getReportingTimezone();
            const filters = buildResultsFilters(input, reportingTimezone, cursor);
            const result = await query(`
          SELECT
            summary.run_id::text AS run_id,
            summary.attribution_spec_version,
            summary.order_id,
            summary.model_key,
            summary.allocation_status,
            summary.winner_touchpoint_id,
            summary.winner_session_id::text AS winner_session_id,
            summary.winner_evidence_source,
            summary.winner_attribution_reason,
            summary.total_credit_weight::text AS total_credit_weight,
            summary.total_revenue_credited::text AS total_revenue_credited,
            summary.touchpoint_count_considered,
            summary.eligible_click_count,
            summary.eligible_view_count,
            summary.lookback_rule_applied,
            summary.winner_selection_rule,
            summary.direct_suppression_applied,
            summary.deterministic_block_applied,
            summary.normalization_failures_count,
            summary.generated_at_utc,
            summary.order_occurred_at_utc,
            runs.run_status,
            runs.trigger_source,
            runs.submitted_by,
            runs.window_start_utc,
            runs.window_end_utc,
            runs.lookback_click_window_days,
            runs.lookback_view_window_days,
            runs.created_at_utc AS run_created_at_utc,
            runs.completed_at_utc,
            primary_credit.touchpoint_id AS primary_touchpoint_id,
            primary_credit.session_id::text AS primary_session_id,
            primary_credit.occurred_at_utc AS primary_occurred_at_utc,
            primary_credit.source AS primary_source,
            primary_credit.medium AS primary_medium,
            primary_credit.campaign AS primary_campaign,
            primary_credit.content AS primary_content,
            primary_credit.term AS primary_term,
            primary_credit.click_id_type AS primary_click_id_type,
            primary_credit.click_id_value AS primary_click_id_value,
            primary_credit.touch_type AS primary_touch_type,
            primary_credit.is_direct AS primary_is_direct,
            primary_credit.is_synthetic AS primary_is_synthetic,
            primary_credit.attribution_reason AS primary_attribution_reason
          FROM attribution_model_summaries summary
          INNER JOIN attribution_runs runs
            ON runs.id = summary.run_id
          LEFT JOIN LATERAL (
            SELECT
              credit.touchpoint_id,
              credit.session_id,
              credit.occurred_at_utc,
              credit.source,
              credit.medium,
              credit.campaign,
              credit.content,
              credit.term,
              credit.click_id_type,
              credit.click_id_value,
              credit.touch_type,
              credit.is_direct,
              credit.is_synthetic,
              credit.attribution_reason
            FROM attribution_model_credits credit
            WHERE credit.run_id = summary.run_id
              AND credit.order_id = summary.order_id
              AND credit.model_key = summary.model_key
            ORDER BY credit.is_primary DESC, credit.touchpoint_position ASC
            LIMIT 1
          ) primary_credit
            ON TRUE
          ${filters.sql}
          ORDER BY summary.order_occurred_at_utc DESC, summary.order_id DESC, summary.run_id DESC
          LIMIT $${filters.params.length + 1}
        `, [...filters.params, input.limit + 1]);
            const hasMore = result.rows.length > input.limit;
            const rows = result.rows.slice(0, input.limit);
            const lastRow = rows[rows.length - 1];
            res.json({
                rows: rows.map((row) => ({
                    record: normalizeAttributionResultRecordV1({
                        run_id: row.run_id,
                        attribution_spec_version: row.attribution_spec_version,
                        order_id: row.order_id,
                        model_key: row.model_key,
                        allocation_status: row.allocation_status,
                        winner_touchpoint_id: row.winner_touchpoint_id,
                        winner_session_id: row.winner_session_id,
                        winner_evidence_source: row.winner_evidence_source,
                        winner_attribution_reason: row.winner_attribution_reason,
                        total_credit_weight: row.total_credit_weight,
                        total_revenue_credited: row.total_revenue_credited,
                        touchpoint_count_considered: row.touchpoint_count_considered,
                        eligible_click_count: row.eligible_click_count,
                        eligible_view_count: row.eligible_view_count,
                        lookback_rule_applied: row.lookback_rule_applied,
                        winner_selection_rule: row.winner_selection_rule,
                        direct_suppression_applied: row.direct_suppression_applied,
                        deterministic_block_applied: row.deterministic_block_applied,
                        normalization_failures_count: row.normalization_failures_count,
                        generated_at_utc: row.generated_at_utc.toISOString()
                    }),
                    orderOccurredAtUtc: row.order_occurred_at_utc.toISOString(),
                    run: {
                        id: row.run_id,
                        status: row.run_status,
                        triggerSource: row.trigger_source,
                        submittedBy: row.submitted_by,
                        windowStartUtc: row.window_start_utc?.toISOString() ?? null,
                        windowEndUtc: row.window_end_utc?.toISOString() ?? null,
                        lookbackClickWindowDays: row.lookback_click_window_days,
                        lookbackViewWindowDays: row.lookback_view_window_days,
                        createdAtUtc: row.run_created_at_utc.toISOString(),
                        completedAtUtc: row.completed_at_utc?.toISOString() ?? null
                    },
                    model: {
                        key: row.model_key,
                        winnerSelectionRule: row.winner_selection_rule,
                        lookbackRuleApplied: row.lookback_rule_applied
                    },
                    primaryTouchpoint: row.primary_touchpoint_id === null ||
                        row.primary_touch_type === null ||
                        row.primary_is_direct === null ||
                        row.primary_is_synthetic === null ||
                        row.winner_evidence_source === null ||
                        row.primary_occurred_at_utc === null
                        ? null
                        : normalizeAttributionCreditRecordV1({
                            run_id: row.run_id,
                            attribution_spec_version: row.attribution_spec_version,
                            order_id: row.order_id,
                            model_key: row.model_key,
                            touchpoint_id: row.primary_touchpoint_id,
                            session_id: row.primary_session_id,
                            touchpoint_position: 1,
                            occurred_at_utc: row.primary_occurred_at_utc.toISOString(),
                            source: row.primary_source,
                            medium: row.primary_medium,
                            campaign: row.primary_campaign,
                            content: row.primary_content,
                            term: row.primary_term,
                            click_id_type: row.primary_click_id_type,
                            click_id_value: row.primary_click_id_value,
                            touch_type: row.primary_touch_type,
                            is_direct: row.primary_is_direct,
                            evidence_source: row.winner_evidence_source,
                            is_synthetic: row.primary_is_synthetic,
                            attribution_reason: row.primary_attribution_reason ?? 'unknown',
                            credit_weight: row.total_credit_weight,
                            revenue_credit: row.total_revenue_credited,
                            is_primary: true
                        })
                })),
                nextCursor: hasMore && lastRow
                    ? encodeCursor({
                        orderOccurredAtUtc: lastRow.order_occurred_at_utc.toISOString(),
                        orderId: lastRow.order_id,
                        runId: lastRow.run_id
                    })
                    : null
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/channel-totals', async (req, res, next) => {
        try {
            const input = parseInput(channelTotalsQuerySchema, req.query);
            const reportingTimezone = await getReportingTimezone();
            const filters = buildChannelTotalsFilters(input, reportingTimezone);
            const result = await query(`
          SELECT
            credit.model_key,
            credit.source,
            credit.medium,
            COUNT(DISTINCT credit.order_id)::int AS order_count,
            COALESCE(SUM(credit.revenue_credit), 0)::text AS revenue_credited,
            COALESCE(SUM(credit.credit_weight), 0)::text AS credit_weight_total,
            MIN(runs.lookback_click_window_days)::int AS lookback_click_window_days,
            MIN(runs.lookback_view_window_days)::int AS lookback_view_window_days
          FROM attribution_model_credits credit
          INNER JOIN attribution_model_summaries summary
            ON summary.run_id = credit.run_id
           AND summary.order_id = credit.order_id
           AND summary.model_key = credit.model_key
          INNER JOIN attribution_runs runs
            ON runs.id = summary.run_id
          ${filters.sql}
          GROUP BY credit.model_key, credit.source, credit.medium
          ORDER BY
            credit.model_key ASC,
            SUM(credit.revenue_credit) DESC,
            credit.source ASC NULLS LAST,
            credit.medium ASC NULLS LAST
        `, filters.params);
            res.json({
                rows: result.rows.map((row) => ({
                    modelKey: row.model_key,
                    source: row.source,
                    medium: row.medium,
                    orderCount: row.order_count,
                    revenueCredited: row.revenue_credited,
                    creditWeightTotal: row.credit_weight_total
                })),
                lookbackClickWindowDays: result.rows[0]?.lookback_click_window_days ?? 28,
                lookbackViewWindowDays: result.rows[0]?.lookback_view_window_days ?? 7
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/orders/:orderId/explainability', async (req, res, next) => {
        try {
            const { orderId } = parseInput(explainabilityParamsSchema, req.params);
            const input = parseInput(explainabilityQuerySchema, req.query);
            const runId = await resolveExplainabilityRunId(orderId, input.runId);
            const runResult = await query(`
          SELECT
            id::text AS id,
            attribution_spec_version,
            run_status,
            trigger_source,
            submitted_by,
            window_start_utc,
            window_end_utc,
            lookback_click_window_days,
            lookback_view_window_days,
            created_at_utc,
            completed_at_utc
          FROM attribution_runs
          WHERE id = $1::uuid
          LIMIT 1
        `, [runId]);
            const run = runResult.rows[0];
            if (!run) {
                throw new AttributionReadHttpError(404, 'attribution_run_not_found', `Attribution run ${runId} was not found`);
            }
            const modelFilterSql = input.modelKey ? 'AND model_key = $3' : '';
            const modelFilterParams = input.modelKey ? [runId, orderId, input.modelKey] : [runId, orderId];
            const [summaryResult, touchpointResult, creditResult, explainResult] = await Promise.all([
                query(`
            SELECT
              summary.run_id::text AS run_id,
              summary.attribution_spec_version,
              summary.order_id,
              summary.model_key,
              summary.allocation_status,
              summary.winner_touchpoint_id,
              summary.winner_session_id::text AS winner_session_id,
              summary.winner_evidence_source,
              summary.winner_attribution_reason,
              summary.total_credit_weight::text AS total_credit_weight,
              summary.total_revenue_credited::text AS total_revenue_credited,
              summary.touchpoint_count_considered,
              summary.eligible_click_count,
              summary.eligible_view_count,
              summary.lookback_rule_applied,
              summary.winner_selection_rule,
              summary.direct_suppression_applied,
              summary.deterministic_block_applied,
              summary.normalization_failures_count,
              summary.generated_at_utc,
              summary.order_occurred_at_utc,
              $4::text AS run_status,
              $5::text AS trigger_source,
              $6::text AS submitted_by,
              $7::timestamptz AS window_start_utc,
              $8::timestamptz AS window_end_utc,
              $9::int AS lookback_click_window_days,
              $10::int AS lookback_view_window_days,
              $11::timestamptz AS run_created_at_utc,
              $12::timestamptz AS completed_at_utc,
              NULL::text AS primary_touchpoint_id,
              NULL::text AS primary_session_id,
              NULL::timestamptz AS primary_occurred_at_utc,
              NULL::text AS primary_source,
              NULL::text AS primary_medium,
              NULL::text AS primary_campaign,
              NULL::text AS primary_content,
              NULL::text AS primary_term,
              NULL::text AS primary_click_id_type,
              NULL::text AS primary_click_id_value,
              NULL::text AS primary_touch_type,
              NULL::boolean AS primary_is_direct,
              NULL::boolean AS primary_is_synthetic,
              NULL::text AS primary_attribution_reason
            FROM attribution_model_summaries summary
            WHERE summary.run_id = $1::uuid
              AND summary.order_id = $2
              ${input.modelKey ? 'AND summary.model_key = $3' : ''}
            ORDER BY summary.model_key ASC
          `, [
                    ...modelFilterParams,
                    run.run_status,
                    run.trigger_source,
                    run.submitted_by,
                    run.window_start_utc?.toISOString() ?? null,
                    run.window_end_utc?.toISOString() ?? null,
                    run.lookback_click_window_days,
                    run.lookback_view_window_days,
                    run.created_at_utc.toISOString(),
                    run.completed_at_utc?.toISOString() ?? null
                ]),
                query(`
            SELECT
              run_id::text AS run_id,
              order_id,
              touchpoint_id,
              session_id::text AS session_id,
              identity_journey_id::text AS identity_journey_id,
              touchpoint_occurred_at_utc,
              touchpoint_captured_at_utc,
              touchpoint_source_kind,
              ingestion_source,
              source,
              medium,
              campaign,
              content,
              term,
              click_id_type,
              click_id_value,
              evidence_source,
              is_direct,
              engagement_type,
              is_synthetic,
              is_eligible,
              ineligibility_reason,
              attribution_reason,
              attribution_hint
            FROM attribution_touchpoint_inputs
            WHERE run_id = $1::uuid
              AND order_id = $2
            ORDER BY touchpoint_occurred_at_utc ASC, touchpoint_id ASC
          `, [runId, orderId]),
                query(`
            SELECT
              run_id::text AS run_id,
              attribution_spec_version,
              order_id,
              model_key,
              touchpoint_id,
              session_id::text AS session_id,
              touchpoint_position,
              occurred_at_utc,
              source,
              medium,
              campaign,
              content,
              term,
              click_id_type,
              click_id_value,
              touch_type,
              is_direct,
              evidence_source,
              is_synthetic,
              attribution_reason,
              credit_weight::text AS credit_weight,
              revenue_credit::text AS revenue_credit,
              is_primary
            FROM attribution_model_credits
            WHERE run_id = $1::uuid
              AND order_id = $2
              ${modelFilterSql}
            ORDER BY model_key ASC, touchpoint_position ASC
          `, modelFilterParams),
                query(`
            SELECT
              run_id::text AS run_id,
              order_id,
              touchpoint_id,
              model_key,
              explain_stage,
              decision,
              decision_reason,
              details_json,
              order_occurred_at_utc,
              created_at_utc
            FROM attribution_explain_records
            WHERE run_id = $1::uuid
              AND order_id = $2
              ${input.modelKey ? 'AND (model_key = $3 OR model_key IS NULL)' : ''}
            ORDER BY created_at_utc ASC, model_key ASC NULLS FIRST, touchpoint_id ASC NULLS FIRST
          `, modelFilterParams)
            ]);
            if (summaryResult.rows.length === 0) {
                throw new AttributionReadHttpError(404, 'attribution_order_not_found', `No attribution results were found for order ${orderId} in run ${runId}`);
            }
            res.json({
                orderId,
                selectedRunReason: input.runId ? 'explicit_run_id' : 'latest_run_for_order',
                run: {
                    id: run.id,
                    attributionSpecVersion: run.attribution_spec_version,
                    status: run.run_status,
                    triggerSource: run.trigger_source,
                    submittedBy: run.submitted_by,
                    windowStartUtc: run.window_start_utc?.toISOString() ?? null,
                    windowEndUtc: run.window_end_utc?.toISOString() ?? null,
                    lookbackClickWindowDays: run.lookback_click_window_days,
                    lookbackViewWindowDays: run.lookback_view_window_days,
                    createdAtUtc: run.created_at_utc.toISOString(),
                    completedAtUtc: run.completed_at_utc?.toISOString() ?? null
                },
                summaries: summaryResult.rows.map((row) => normalizeAttributionResultRecordV1({
                    run_id: row.run_id,
                    attribution_spec_version: row.attribution_spec_version,
                    order_id: row.order_id,
                    model_key: row.model_key,
                    allocation_status: row.allocation_status,
                    winner_touchpoint_id: row.winner_touchpoint_id,
                    winner_session_id: row.winner_session_id,
                    winner_evidence_source: row.winner_evidence_source,
                    winner_attribution_reason: row.winner_attribution_reason,
                    total_credit_weight: row.total_credit_weight,
                    total_revenue_credited: row.total_revenue_credited,
                    touchpoint_count_considered: row.touchpoint_count_considered,
                    eligible_click_count: row.eligible_click_count,
                    eligible_view_count: row.eligible_view_count,
                    lookback_rule_applied: row.lookback_rule_applied,
                    winner_selection_rule: row.winner_selection_rule,
                    direct_suppression_applied: row.direct_suppression_applied,
                    deterministic_block_applied: row.deterministic_block_applied,
                    normalization_failures_count: row.normalization_failures_count,
                    generated_at_utc: row.generated_at_utc.toISOString()
                })),
                touchpoints: touchpointResult.rows.map((row) => ({
                    runId: row.run_id,
                    orderId: row.order_id,
                    touchpointId: row.touchpoint_id,
                    sessionId: row.session_id,
                    identityJourneyId: row.identity_journey_id,
                    touchpointOccurredAtUtc: row.touchpoint_occurred_at_utc.toISOString(),
                    touchpointCapturedAtUtc: row.touchpoint_captured_at_utc.toISOString(),
                    touchpointSourceKind: row.touchpoint_source_kind,
                    ingestionSource: row.ingestion_source,
                    source: row.source,
                    medium: row.medium,
                    campaign: row.campaign,
                    content: row.content,
                    term: row.term,
                    clickIdType: row.click_id_type,
                    clickIdValue: row.click_id_value,
                    evidenceSource: row.evidence_source,
                    isDirect: row.is_direct,
                    engagementType: row.engagement_type,
                    isSynthetic: row.is_synthetic,
                    isEligible: row.is_eligible,
                    ineligibilityReason: row.ineligibility_reason,
                    attributionReason: row.attribution_reason,
                    attributionHint: asObjectRecord(row.attribution_hint)
                })),
                credits: creditResult.rows.map((row) => normalizeAttributionCreditRecordV1({
                    run_id: row.run_id,
                    attribution_spec_version: row.attribution_spec_version,
                    order_id: row.order_id,
                    model_key: row.model_key,
                    touchpoint_id: row.touchpoint_id,
                    session_id: row.session_id,
                    touchpoint_position: row.touchpoint_position,
                    occurred_at_utc: row.occurred_at_utc.toISOString(),
                    source: row.source,
                    medium: row.medium,
                    campaign: row.campaign,
                    content: row.content,
                    term: row.term,
                    click_id_type: row.click_id_type,
                    click_id_value: row.click_id_value,
                    touch_type: row.touch_type,
                    is_direct: row.is_direct,
                    evidence_source: row.evidence_source,
                    is_synthetic: row.is_synthetic,
                    attribution_reason: row.attribution_reason,
                    credit_weight: row.credit_weight,
                    revenue_credit: row.revenue_credit,
                    is_primary: row.is_primary
                })),
                explainability: explainResult.rows.map((row) => normalizeAttributionExplainRecordV1({
                    run_id: row.run_id,
                    order_id: row.order_id,
                    touchpoint_id: row.touchpoint_id,
                    model_key: row.model_key,
                    explain_stage: row.explain_stage,
                    decision: row.decision,
                    decision_reason: row.decision_reason,
                    details_json: asObjectRecord(row.details_json),
                    order_occurred_at_utc: row.order_occurred_at_utc?.toISOString() ?? null,
                    created_at_utc: row.created_at_utc.toISOString()
                }))
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
