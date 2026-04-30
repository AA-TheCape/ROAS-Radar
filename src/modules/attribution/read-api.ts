import { Buffer } from 'node:buffer';

import { Router } from 'express';
import { z } from 'zod';

import {
  ATTRIBUTION_MODEL_KEYS,
  normalizeAttributionCreditRecordV1,
  normalizeAttributionExplainRecordV1,
  normalizeAttributionResultRecordV1
} from '../../../packages/attribution-schema/index.js';
import { query } from '../../db/pool.js';
import { attachAuthContext, requireAuthenticated } from '../auth/index.js';
import { getReportingTimezone } from '../settings/index.js';

class AttributionReadHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
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

type ResultsSummaryRow = {
  run_id: string;
  attribution_spec_version: 'v1';
  order_id: string;
  model_key: z.infer<typeof modelKeySchema>;
  allocation_status: 'attributed' | 'no_eligible_touches' | 'blocked_by_deterministic' | 'unattributed';
  winner_touchpoint_id: string | null;
  winner_session_id: string | null;
  winner_evidence_source: string | null;
  winner_attribution_reason: string | null;
  total_credit_weight: string;
  total_revenue_credited: string;
  touchpoint_count_considered: number;
  eligible_click_count: number;
  eligible_view_count: number;
  lookback_rule_applied: '28d_click' | '7d_view' | 'mixed';
  winner_selection_rule: z.infer<typeof modelKeySchema>;
  direct_suppression_applied: boolean;
  deterministic_block_applied: boolean;
  normalization_failures_count: number;
  generated_at_utc: Date;
  order_occurred_at_utc: Date;
  run_status: string;
  trigger_source: string;
  submitted_by: string;
  window_start_utc: Date | null;
  window_end_utc: Date | null;
  lookback_click_window_days: number;
  lookback_view_window_days: number;
  run_created_at_utc: Date;
  completed_at_utc: Date | null;
  primary_touchpoint_id: string | null;
  primary_session_id: string | null;
  primary_occurred_at_utc: Date | null;
  primary_source: string | null;
  primary_medium: string | null;
  primary_campaign: string | null;
  primary_content: string | null;
  primary_term: string | null;
  primary_click_id_type: string | null;
  primary_click_id_value: string | null;
  primary_touch_type: 'click' | 'view' | null;
  primary_is_direct: boolean | null;
  primary_is_synthetic: boolean | null;
  primary_attribution_reason: string | null;
};

type RunRow = {
  id: string;
  attribution_spec_version: 'v1';
  run_status: string;
  trigger_source: string;
  submitted_by: string;
  window_start_utc: Date | null;
  window_end_utc: Date | null;
  lookback_click_window_days: number;
  lookback_view_window_days: number;
  created_at_utc: Date;
  completed_at_utc: Date | null;
};

type TouchpointRow = {
  run_id: string;
  order_id: string;
  touchpoint_id: string;
  session_id: string | null;
  identity_journey_id: string | null;
  touchpoint_occurred_at_utc: Date;
  touchpoint_captured_at_utc: Date;
  touchpoint_source_kind: string;
  ingestion_source: string;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
  evidence_source: string;
  is_direct: boolean;
  engagement_type: string;
  is_synthetic: boolean;
  is_eligible: boolean;
  ineligibility_reason: string | null;
  attribution_reason: string | null;
  attribution_hint: unknown;
};

type CreditRow = {
  run_id: string;
  attribution_spec_version: 'v1';
  order_id: string;
  model_key: z.infer<typeof modelKeySchema>;
  touchpoint_id: string;
  session_id: string | null;
  touchpoint_position: number;
  occurred_at_utc: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
  touch_type: 'click' | 'view';
  is_direct: boolean;
  evidence_source: string;
  is_synthetic: boolean;
  attribution_reason: string;
  credit_weight: string;
  revenue_credit: string;
  is_primary: boolean;
};

type ExplainRow = {
  run_id: string;
  order_id: string;
  touchpoint_id: string | null;
  model_key: z.infer<typeof modelKeySchema> | null;
  explain_stage: 'candidate_extraction' | 'eligibility_filter' | 'model_scoring' | 'fallback';
  decision: 'included' | 'excluded' | 'winner' | 'fallback_used' | 'no_credit';
  decision_reason: string;
  details_json: unknown;
  order_occurred_at_utc: Date | null;
  created_at_utc: Date;
};

type ChannelTotalsRow = {
  model_key: z.infer<typeof modelKeySchema>;
  source: string | null;
  medium: string | null;
  order_count: number;
  revenue_credited: string;
  credit_weight_total: string;
  lookback_click_window_days: number;
  lookback_view_window_days: number;
};

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new AttributionReadHttpError(400, 'invalid_request', 'Invalid attribution query parameters', error.flatten());
    }

    throw error;
  }
}

function asObjectRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function encodeCursor(input: z.infer<typeof cursorSchema>): string {
  return Buffer.from(JSON.stringify(input), 'utf8').toString('base64url');
}

function decodeCursor(cursor: string | undefined): z.infer<typeof cursorSchema> | null {
  if (!cursor) {
    return null;
  }

  try {
    const decoded = Buffer.from(cursor, 'base64url').toString('utf8');
    return cursorSchema.parse(JSON.parse(decoded));
  } catch (error) {
    throw new AttributionReadHttpError(400, 'invalid_cursor', 'Invalid attribution pagination cursor', {
      cursor,
      reason: error instanceof Error ? error.message : String(error)
    });
  }
}

function buildResultsFilters(
  input: z.infer<typeof resultsQuerySchema>,
  reportingTimezone: string,
  cursor: z.infer<typeof cursorSchema> | null
): { sql: string; params: unknown[] } {
  const params: unknown[] = [input.modelKey, input.startDate, input.endDate, reportingTimezone];
  const filters: string[] = [
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
    const dimensionFilters: string[] = [
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

    filters.push(
      `EXISTS (
        SELECT 1
        FROM attribution_model_credits filter_credit
        WHERE ${dimensionFilters.join(' AND ')}
      )`
    );
  }

  if (cursor) {
    params.push(cursor.orderOccurredAtUtc, cursor.orderId, cursor.runId);
    const occurredAtParam = params.length - 2;
    const orderIdParam = params.length - 1;
    const runIdParam = params.length;
    filters.push(
      `(summary.order_occurred_at_utc, summary.order_id, summary.run_id) < ($${occurredAtParam}::timestamptz, $${orderIdParam}, $${runIdParam}::uuid)`
    );
  }

  return {
    sql: `WHERE ${filters.join(' AND ')}`,
    params
  };
}

function buildChannelTotalsFilters(
  input: z.infer<typeof channelTotalsQuerySchema>,
  reportingTimezone: string
): { sql: string; params: unknown[] } {
  const params: unknown[] = [input.startDate, input.endDate, reportingTimezone];
  const filters: string[] = [
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

async function resolveExplainabilityRunId(orderId: string, requestedRunId?: string): Promise<string> {
  if (requestedRunId) {
    return requestedRunId;
  }

  const result = await query<{ run_id: string }>(
    `
      SELECT summary.run_id::text AS run_id
      FROM attribution_model_summaries summary
      WHERE summary.order_id = $1
      ORDER BY summary.generated_at_utc DESC, summary.run_id DESC
      LIMIT 1
    `,
    [orderId]
  );

  const runId = result.rows[0]?.run_id;
  if (!runId) {
    throw new AttributionReadHttpError(404, 'attribution_order_not_found', `No attribution results were found for order ${orderId}`);
  }

  return runId;
}

export function createAttributionReadRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);

  router.get('/results', async (req, res, next) => {
    try {
      const input = parseInput(resultsQuerySchema, req.query);
      const cursor = decodeCursor(input.cursor);
      const reportingTimezone = await getReportingTimezone();
      const filters = buildResultsFilters(input, reportingTimezone, cursor);

      const result = await query<ResultsSummaryRow>(
        `
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
        `,
        [...filters.params, input.limit + 1]
      );

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
          primaryTouchpoint:
            row.primary_touchpoint_id === null ||
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
        nextCursor:
          hasMore && lastRow
            ? encodeCursor({
                orderOccurredAtUtc: lastRow.order_occurred_at_utc.toISOString(),
                orderId: lastRow.order_id,
                runId: lastRow.run_id
              })
            : null
      });
    } catch (error) {
      next(error);
    }
  });

  router.get('/channel-totals', async (req, res, next) => {
    try {
      const input = parseInput(channelTotalsQuerySchema, req.query);
      const reportingTimezone = await getReportingTimezone();
      const filters = buildChannelTotalsFilters(input, reportingTimezone);

      const result = await query<ChannelTotalsRow>(
        `
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
        `,
        filters.params
      );

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
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders/:orderId/explainability', async (req, res, next) => {
    try {
      const { orderId } = parseInput(explainabilityParamsSchema, req.params);
      const input = parseInput(explainabilityQuerySchema, req.query);
      const runId = await resolveExplainabilityRunId(orderId, input.runId);

      const runResult = await query<RunRow>(
        `
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
        `,
        [runId]
      );

      const run = runResult.rows[0];
      if (!run) {
        throw new AttributionReadHttpError(404, 'attribution_run_not_found', `Attribution run ${runId} was not found`);
      }

      const modelFilterSql = input.modelKey ? 'AND model_key = $3' : '';
      const modelFilterParams = input.modelKey ? [runId, orderId, input.modelKey] : [runId, orderId];

      const [summaryResult, touchpointResult, creditResult, explainResult] = await Promise.all([
        query<ResultsSummaryRow>(
          `
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
          `,
          [
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
          ]
        ),
        query<TouchpointRow>(
          `
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
          `,
          [runId, orderId]
        ),
        query<CreditRow>(
          `
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
          `,
          modelFilterParams
        ),
        query<ExplainRow>(
          `
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
          `,
          modelFilterParams
        )
      ]);

      if (summaryResult.rows.length === 0) {
        throw new AttributionReadHttpError(
          404,
          'attribution_order_not_found',
          `No attribution results were found for order ${orderId} in run ${runId}`
        );
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
        summaries: summaryResult.rows.map((row) =>
          normalizeAttributionResultRecordV1({
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
          })
        ),
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
        credits: creditResult.rows.map((row) =>
          normalizeAttributionCreditRecordV1({
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
          })
        ),
        explainability: explainResult.rows.map((row) =>
          normalizeAttributionExplainRecordV1({
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
          })
        )
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
