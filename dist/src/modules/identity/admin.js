import { Router } from 'express';
import { z } from 'zod';
import { query } from '../../db/pool.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
class IdentityAdminHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'IdentityAdminHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const baseHealthQueryObjectSchema = z.object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    source: z.string().trim().min(1).optional()
});
function withValidDateRange(schema) {
    return schema.superRefine((value, ctx) => {
        if (value.startDate > value.endDate) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'startDate must be on or before endDate',
                path: ['startDate']
            });
        }
    });
}
const baseHealthQuerySchema = withValidDateRange(baseHealthQueryObjectSchema);
const conflictsQuerySchema = withValidDateRange(baseHealthQueryObjectSchema.extend({
    limit: z.coerce.number().int().positive().max(100).optional().default(25)
}));
function parseInput(schema, input) {
    try {
        return schema.parse(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new IdentityAdminHttpError(400, 'invalid_request', 'Invalid identity health query parameters', error.flatten());
        }
        throw error;
    }
}
async function fetchIdentityHealthOverview(input) {
    const sourceFilter = input.source
        ? `
      AND (runs.evidence_source = $3 OR runs.source_table = $3)
    `
        : '';
    const params = input.source ? [input.startDate, input.endDate, input.source] : [input.startDate, input.endDate];
    const summaryResult = await query(`
      SELECT
        COUNT(*)::bigint AS total_ingestions,
        COUNT(*) FILTER (WHERE runs.status = 'completed' AND COALESCE(runs.outcome_reason, 'linked') <> 'missing_identifiers')::bigint AS linked_ingestions,
        COUNT(*) FILTER (WHERE runs.outcome_reason = 'missing_identifiers')::bigint AS skipped_ingestions,
        COUNT(*) FILTER (WHERE runs.status = 'conflicted')::bigint AS conflict_ingestions,
        COUNT(*) FILTER (WHERE runs.rehomed_nodes > 0)::bigint AS merge_runs,
        COALESCE(SUM(runs.rehomed_nodes), 0)::bigint AS rehomed_nodes,
        COALESCE(SUM(runs.quarantined_nodes), 0)::bigint AS quarantined_nodes,
        (
          SELECT COUNT(*)::bigint
          FROM identity_edges edge
          WHERE edge.conflict_code IS NOT NULL
            AND edge.is_active = true
        ) AS unresolved_conflicts
      FROM identity_edge_ingestion_runs runs
      WHERE runs.source_timestamp >= $1::date
        AND runs.source_timestamp < ($2::date + INTERVAL '1 day')
        ${sourceFilter}
    `, params);
    const seriesResult = await query(`
      SELECT
        to_char(date_trunc('day', runs.source_timestamp AT TIME ZONE 'UTC'), 'YYYY-MM-DD') AS bucket_date,
        COUNT(*) FILTER (WHERE runs.status = 'completed' AND COALESCE(runs.outcome_reason, 'linked') <> 'missing_identifiers')::bigint AS linked_count,
        COUNT(*) FILTER (WHERE runs.outcome_reason = 'missing_identifiers')::bigint AS skipped_count,
        COUNT(*) FILTER (WHERE runs.status = 'conflicted')::bigint AS conflict_count,
        COUNT(*) FILTER (WHERE runs.rehomed_nodes > 0)::bigint AS merge_runs,
        COALESCE(SUM(runs.rehomed_nodes), 0)::bigint AS rehomed_nodes,
        COALESCE(SUM(runs.quarantined_nodes), 0)::bigint AS quarantined_nodes
      FROM identity_edge_ingestion_runs runs
      WHERE runs.source_timestamp >= $1::date
        AND runs.source_timestamp < ($2::date + INTERVAL '1 day')
        ${sourceFilter}
      GROUP BY 1
      ORDER BY 1 ASC
    `, params);
    const unlinkedSessionsFilter = input.source
        ? `
      AND (sessions.initial_utm_source = $3 OR $3 = 'tracking_sessions')
    `
        : '';
    const unlinkedSessionsResult = await query(`
      SELECT
        COUNT(*) FILTER (WHERE sessions.identity_journey_id IS NULL)::bigint AS unlinked_sessions,
        COUNT(*) FILTER (WHERE sessions.identity_journey_id IS NOT NULL)::bigint AS linked_sessions
      FROM tracking_sessions sessions
      WHERE sessions.first_seen_at >= $1::date
        AND sessions.first_seen_at < ($2::date + INTERVAL '1 day')
        ${unlinkedSessionsFilter}
    `, params);
    const backfillRunSourceFilter = input.source
        ? `
      AND (
        $3 = 'backfill'
        OR EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(COALESCE(runs.options->'sources', '[]'::jsonb)) AS source_filter(value)
          WHERE source_filter.value = $3
        )
      )
    `
        : '';
    const backfillStatusResult = await query(`
      SELECT
        COUNT(*) FILTER (WHERE runs.status = 'processing')::bigint AS active_runs,
        COUNT(*) FILTER (WHERE runs.status = 'failed')::bigint AS failed_runs,
        COUNT(*) FILTER (WHERE runs.status = 'completed')::bigint AS completed_runs
      FROM identity_graph_backfill_runs runs
      WHERE runs.started_at >= $1::date
        AND runs.started_at < ($2::date + INTERVAL '1 day')
        ${backfillRunSourceFilter}
    `, params);
    const latestBackfillResult = await query(`
      SELECT
        runs.id::text AS id,
        runs.status,
        runs.requested_by,
        runs.worker_id,
        runs.options,
        runs.error_code,
        runs.error_message,
        runs.started_at,
        runs.completed_at,
        runs.updated_at
      FROM identity_graph_backfill_runs runs
      WHERE runs.started_at >= $1::date
        AND runs.started_at < ($2::date + INTERVAL '1 day')
        ${backfillRunSourceFilter}
      ORDER BY runs.started_at DESC
      LIMIT 1
    `, params);
    const summary = summaryResult.rows[0] ?? {
        total_ingestions: 0,
        linked_ingestions: 0,
        skipped_ingestions: 0,
        conflict_ingestions: 0,
        merge_runs: 0,
        rehomed_nodes: 0,
        quarantined_nodes: 0,
        unresolved_conflicts: 0
    };
    const unlinkedSessions = unlinkedSessionsResult.rows[0] ?? {
        unlinked_sessions: 0,
        linked_sessions: 0
    };
    const backfillStatus = backfillStatusResult.rows[0] ?? {
        active_runs: 0,
        failed_runs: 0,
        completed_runs: 0
    };
    const latestBackfill = latestBackfillResult.rows[0] ?? null;
    const latestBackfillOptions = latestBackfill && typeof latestBackfill.options === 'object' && latestBackfill.options !== null
        ? latestBackfill.options
        : null;
    return {
        range: {
            startDate: input.startDate,
            endDate: input.endDate
        },
        source: input.source ?? null,
        summary: {
            totalIngestions: Number(summary.total_ingestions),
            linkedIngestions: Number(summary.linked_ingestions),
            skippedIngestions: Number(summary.skipped_ingestions),
            conflictIngestions: Number(summary.conflict_ingestions),
            mergeRuns: Number(summary.merge_runs),
            rehomedNodes: Number(summary.rehomed_nodes),
            quarantinedNodes: Number(summary.quarantined_nodes),
            unresolvedConflicts: Number(summary.unresolved_conflicts),
            unlinkedSessions: Number(unlinkedSessions.unlinked_sessions),
            linkedSessions: Number(unlinkedSessions.linked_sessions)
        },
        series: seriesResult.rows.map((row) => ({
            date: row.bucket_date,
            linked: Number(row.linked_count),
            skipped: Number(row.skipped_count),
            conflicts: Number(row.conflict_count),
            mergeRuns: Number(row.merge_runs),
            rehomedNodes: Number(row.rehomed_nodes),
            quarantinedNodes: Number(row.quarantined_nodes)
        })),
        backfill: {
            activeRuns: Number(backfillStatus.active_runs),
            failedRuns: Number(backfillStatus.failed_runs),
            completedRuns: Number(backfillStatus.completed_runs),
            latestRun: latestBackfill
                ? {
                    runId: latestBackfill.id,
                    status: latestBackfill.status,
                    requestedBy: latestBackfill.requested_by,
                    workerId: latestBackfill.worker_id ?? 'identity-graph-backfill',
                    sources: Array.isArray(latestBackfillOptions?.sources)
                        ? latestBackfillOptions.sources.filter((value) => typeof value === 'string')
                        : [],
                    startedAt: latestBackfill.started_at.toISOString(),
                    completedAt: latestBackfill.completed_at?.toISOString() ?? null,
                    updatedAt: latestBackfill.updated_at.toISOString(),
                    errorCode: latestBackfill.error_code,
                    errorMessage: latestBackfill.error_message
                }
                : null
        }
    };
}
async function fetchIdentityConflictDetails(input) {
    const sourceFilter = input.source
        ? `
      AND (edge.evidence_source = $3 OR edge.source_table = $3)
    `
        : '';
    const params = input.source
        ? [input.startDate, input.endDate, input.source, input.limit]
        : [input.startDate, input.endDate, input.limit];
    const limitPlaceholder = input.source ? '$4' : '$3';
    const result = await query(`
      SELECT
        edge.id::text AS edge_id,
        journey.id::text AS journey_id,
        journey.status AS journey_status,
        journey.authoritative_shopify_customer_id,
        node.node_type,
        node.node_key,
        edge.evidence_source,
        edge.source_table,
        edge.source_record_id,
        edge.conflict_code,
        edge.first_observed_at,
        edge.last_observed_at,
        edge.updated_at
      FROM identity_edges edge
      INNER JOIN identity_nodes node
        ON node.id = edge.node_id
      INNER JOIN identity_journeys journey
        ON journey.id = edge.journey_id
      WHERE edge.conflict_code IS NOT NULL
        AND edge.updated_at >= $1::date
        AND edge.updated_at < ($2::date + INTERVAL '1 day')
        ${sourceFilter}
      ORDER BY edge.updated_at DESC, edge.id DESC
      LIMIT ${limitPlaceholder}::int
    `, params);
    return {
        range: {
            startDate: input.startDate,
            endDate: input.endDate
        },
        source: input.source ?? null,
        conflicts: result.rows.map((row) => ({
            edgeId: row.edge_id,
            journeyId: row.journey_id,
            journeyStatus: row.journey_status,
            authoritativeShopifyCustomerId: row.authoritative_shopify_customer_id,
            nodeType: row.node_type,
            nodeKey: row.node_key,
            evidenceSource: row.evidence_source,
            sourceTable: row.source_table,
            sourceRecordId: row.source_record_id,
            conflictCode: row.conflict_code,
            firstObservedAt: row.first_observed_at.toISOString(),
            lastObservedAt: row.last_observed_at.toISOString(),
            updatedAt: row.updated_at.toISOString()
        }))
    };
}
export function createIdentityAdminRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.use(requireAdmin);
    router.get('/health', async (req, res, next) => {
        try {
            const input = parseInput(baseHealthQuerySchema, req.query);
            const response = await fetchIdentityHealthOverview(input);
            res.status(200).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/health/conflicts', async (req, res, next) => {
        try {
            const input = parseInput(conflictsQuerySchema, req.query);
            const response = await fetchIdentityConflictDetails(input);
            res.status(200).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
