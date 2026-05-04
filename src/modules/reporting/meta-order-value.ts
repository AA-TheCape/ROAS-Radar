import type { Request } from 'express';
import { Router } from 'express';
import { z } from 'zod';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { attachAuthContext, requireAuthenticated, type AuthContext } from '../auth/index.js';

class MetaOrderValueHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'MetaOrderValueHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

const stringListSchema = z
  .union([z.string(), z.array(z.string())])
  .optional()
  .transform((value) => {
    if (Array.isArray(value)) {
      return value.map((entry) => entry.trim()).filter(Boolean);
    }

    if (typeof value !== 'string') {
      return [];
    }

    return value
      .split(',')
      .map((entry) => entry.trim())
      .filter(Boolean);
  });

const metaOrderValueQuerySchema = z
  .object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    campaignIds: stringListSchema.default([]),
    campaignSearch: z.string().trim().min(1).max(200).optional(),
    actionType: z.string().trim().min(1).max(120).optional(),
    sortBy: z
      .enum(['reportDate', 'campaignName', 'attributedRevenue', 'purchaseCount', 'spend', 'roas', 'actionType'])
      .optional()
      .default('reportDate'),
    sortDirection: z.enum(['asc', 'desc']).optional().default('desc'),
    limit: z.coerce.number().int().positive().max(200).optional().default(50),
    offset: z.coerce.number().int().min(0).optional().default(0)
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

type MetaOrderValueAggregateTotalsRow = {
  total_rows: string | number;
  attributed_revenue: string | number;
  purchase_count: string | number;
  spend: string | number;
};

type MetaOrderValueAggregateRow = {
  report_date: string;
  campaign_id: string;
  campaign_name: string | null;
  attributed_revenue: string | number | null;
  purchase_count: string | number | null;
  spend: string | number;
  purchase_roas: string | number | null;
  canonical_action_type: string | null;
  canonical_selection_mode: 'priority' | 'fallback' | 'none';
  currency: string | null;
};

type MetaOrderValueQueryInput = z.infer<typeof metaOrderValueQuerySchema>;

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new MetaOrderValueHttpError(400, 'invalid_request', 'Invalid Meta order value query parameters', error.flatten());
    }

    throw error;
  }
}

function parseTenantHeader(rawHeader: string | undefined): number | null {
  if (typeof rawHeader !== 'string' || rawHeader.trim().length === 0) {
    return null;
  }

  const value = Number.parseInt(rawHeader.trim(), 10);

  if (!Number.isFinite(value) || value <= 0) {
    throw new MetaOrderValueHttpError(400, 'invalid_tenant', 'x-roas-radar-tenant-id must be a positive integer');
  }

  return Math.trunc(value);
}

function resolveOrganizationId(req: Request, auth: AuthContext): number {
  const tenantId = parseTenantHeader(req.header('x-roas-radar-tenant-id') ?? undefined);

  if (tenantId === null) {
    return env.DEFAULT_ORGANIZATION_ID;
  }

  if (auth.kind !== 'internal' && tenantId !== env.DEFAULT_ORGANIZATION_ID) {
    throw new MetaOrderValueHttpError(
      403,
      'tenant_override_forbidden',
      'Authenticated user sessions cannot override the reporting tenant'
    );
  }

  return tenantId;
}

function escapeLikePattern(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
}

function buildMetaOrderValueFilters(
  organizationId: number,
  input: MetaOrderValueQueryInput
): { sql: string; params: unknown[] } {
  const params: unknown[] = [organizationId, input.startDate, input.endDate];
  const filters = [
    'organization_id = $1',
    'report_date BETWEEN $2::date AND $3::date',
    "action_report_time = 'conversion'",
    'use_account_attribution_setting = true'
  ];

  if (input.campaignIds.length > 0) {
    params.push(input.campaignIds);
    filters.push(`campaign_id = ANY($${params.length}::text[])`);
  }

  if (input.campaignSearch) {
    params.push(`%${escapeLikePattern(input.campaignSearch)}%`);
    filters.push(`(campaign_id ILIKE $${params.length} ESCAPE '\\' OR campaign_name ILIKE $${params.length} ESCAPE '\\')`);
  }

  if (input.actionType) {
    params.push(input.actionType);
    filters.push(`canonical_action_type = $${params.length}`);
  }

  return {
    sql: filters.join('\n        AND '),
    params
  };
}

function buildOrderByClause(input: MetaOrderValueQueryInput): string {
  const direction = input.sortDirection.toUpperCase();

  switch (input.sortBy) {
    case 'campaignName':
      return `campaign_name ${direction} NULLS LAST, report_date DESC, campaign_id ASC`;
    case 'attributedRevenue':
      return `attributed_revenue ${direction} NULLS LAST, report_date DESC, campaign_id ASC`;
    case 'purchaseCount':
      return `purchase_count ${direction} NULLS LAST, report_date DESC, campaign_id ASC`;
    case 'spend':
      return `spend ${direction} NULLS LAST, report_date DESC, campaign_id ASC`;
    case 'roas':
      return `purchase_roas ${direction} NULLS LAST, report_date DESC, campaign_id ASC`;
    case 'actionType':
      return `canonical_action_type ${direction} NULLS LAST, report_date DESC, campaign_id ASC`;
    default:
      return `report_date ${direction}, attributed_revenue DESC NULLS LAST, campaign_id ASC`;
  }
}

function toNullableNumber(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  return Number(value);
}

export function createMetaOrderValueRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);

  router.get('/', async (req, res, next) => {
    try {
      const auth = res.locals.auth as AuthContext;
      const input = parseInput(metaOrderValueQuerySchema, req.query);
      const organizationId = resolveOrganizationId(req, auth);
      const filters = buildMetaOrderValueFilters(organizationId, input);

      const totalsResult = await query<MetaOrderValueAggregateTotalsRow>(
        `
          WITH filtered AS (
            SELECT
              attributed_revenue,
              purchase_count,
              spend
            FROM meta_ads_order_value_aggregates
            WHERE ${filters.sql}
          )
          SELECT
            COUNT(*) AS total_rows,
            COALESCE(SUM(COALESCE(attributed_revenue, 0)), 0) AS attributed_revenue,
            COALESCE(SUM(COALESCE(purchase_count, 0)), 0) AS purchase_count,
            COALESCE(SUM(spend), 0) AS spend
          FROM filtered
        `,
        filters.params
      );

      const rowsResult = await query<MetaOrderValueAggregateRow>(
        `
          SELECT
            report_date::text,
            campaign_id,
            campaign_name,
            attributed_revenue,
            purchase_count,
            spend,
            purchase_roas,
            canonical_action_type,
            canonical_selection_mode,
            currency
          FROM meta_ads_order_value_aggregates
          WHERE ${filters.sql}
          ORDER BY ${buildOrderByClause(input)}
          LIMIT $${filters.params.length + 1}
          OFFSET $${filters.params.length + 2}
        `,
        [...filters.params, input.limit, input.offset]
      );

      const totals = totalsResult.rows[0];
      const totalRows = Number(totals?.total_rows ?? 0);
      const totalAttributedRevenue = Number(totals?.attributed_revenue ?? 0);
      const totalSpend = Number(totals?.spend ?? 0);

      res.status(200).json({
        scope: {
          organizationId
        },
        range: {
          startDate: input.startDate,
          endDate: input.endDate
        },
        filters: {
          campaignIds: input.campaignIds,
          campaignSearch: input.campaignSearch ?? null,
          actionType: input.actionType ?? null
        },
        sort: {
          by: input.sortBy,
          direction: input.sortDirection
        },
        pagination: {
          limit: input.limit,
          offset: input.offset,
          returned: rowsResult.rows.length,
          totalRows,
          hasMore: input.offset + rowsResult.rows.length < totalRows
        },
        totals: {
          attributedRevenue: totalAttributedRevenue,
          purchaseCount: Number(totals?.purchase_count ?? 0),
          spend: totalSpend,
          roas: totalSpend > 0 ? totalAttributedRevenue / totalSpend : null
        },
        rows: rowsResult.rows.map((row) => {
          const attributedRevenue = toNullableNumber(row.attributed_revenue);
          const spend = Number(row.spend);

          return {
            date: row.report_date,
            campaignId: row.campaign_id,
            campaignName: row.campaign_name,
            attributedRevenue,
            purchaseCount: toNullableNumber(row.purchase_count),
            spend,
            roas: toNullableNumber(row.purchase_roas),
            calculatedRoas: attributedRevenue !== null && spend > 0 ? attributedRevenue / spend : null,
            canonicalActionType: row.canonical_action_type,
            canonicalSelectionMode: row.canonical_selection_mode,
            currency: row.currency
          };
        })
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
