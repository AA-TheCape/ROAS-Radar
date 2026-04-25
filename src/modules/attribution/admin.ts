import { randomUUID } from 'node:crypto';

import { type Router, Router as createRouter } from 'express';
import { z } from 'zod';

import {
  normalizeOrderAttributionBackfillRequest,
  orderAttributionBackfillEnqueueResponseSchema,
  orderAttributionBackfillJobResponseSchema,
  orderAttributionBackfillReportSchema,
  orderAttributionBackfillSubmittedOptionsSchema,
  type OrderAttributionBackfillEnqueueResponse,
  type OrderAttributionBackfillJobResponse,
  type OrderAttributionBackfillRequest
} from '../../../packages/attribution-schema/index.js';
import { query } from '../../db/pool.js';
import { attachAuthContext, requireAdmin, type AuthContext } from '../auth/index.js';

class AttributionAdminHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'AttributionAdminHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

type OrderAttributionBackfillRunRow = {
  id: string;
  status: 'queued' | 'processing' | 'completed' | 'failed';
  submitted_at: Date;
  submitted_by: string;
  started_at: Date | null;
  completed_at: Date | null;
  options: unknown;
  report: unknown;
  error_code: string | null;
  error_message: string | null;
};

function parseBackfillRequest(input: unknown): OrderAttributionBackfillRequest {
  try {
    return normalizeOrderAttributionBackfillRequest(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new AttributionAdminHttpError(
        400,
        'invalid_request',
        'Invalid order attribution backfill request',
        error.flatten()
      );
    }

    throw error;
  }
}

function getSubmittedBy(auth: AuthContext | null | undefined): string {
  if (!auth) {
    throw new AttributionAdminHttpError(401, 'unauthorized', 'Authentication required');
  }

  if (auth.kind === 'internal') {
    return 'internal';
  }

  return auth.user.email;
}

async function enqueueOrderAttributionBackfillRun(
  options: OrderAttributionBackfillRequest,
  submittedBy: string
): Promise<OrderAttributionBackfillEnqueueResponse> {
  const jobId = randomUUID();
  const submittedAt = new Date().toISOString();

  await query(
    `
      INSERT INTO order_attribution_backfill_runs (
        id,
        status,
        submitted_at,
        submitted_by,
        options
      )
      VALUES (
        $1,
        'queued',
        $2::timestamptz,
        $3,
        $4::jsonb
      )
    `,
    [jobId, submittedAt, submittedBy, JSON.stringify(options)]
  );

  return orderAttributionBackfillEnqueueResponseSchema.parse({
    ok: true,
    jobId,
    status: 'queued',
    submittedAt,
    submittedBy,
    options
  });
}

function mapBackfillRunRow(row: OrderAttributionBackfillRunRow): OrderAttributionBackfillJobResponse {
  return orderAttributionBackfillJobResponseSchema.parse({
    ok: true,
    jobId: row.id,
    status: row.status,
    submittedAt: row.submitted_at.toISOString(),
    submittedBy: row.submitted_by,
    startedAt: row.started_at?.toISOString() ?? null,
    completedAt: row.completed_at?.toISOString() ?? null,
    options: orderAttributionBackfillSubmittedOptionsSchema.parse(row.options),
    report: row.report == null ? null : orderAttributionBackfillReportSchema.parse(row.report),
    error:
      row.error_code && row.error_message
        ? {
            code: row.error_code,
            message: row.error_message
          }
        : null
  });
}

async function getOrderAttributionBackfillRun(jobId: string): Promise<OrderAttributionBackfillJobResponse> {
  const result = await query<OrderAttributionBackfillRunRow>(
    `
      SELECT
        id,
        status,
        submitted_at,
        submitted_by,
        started_at,
        completed_at,
        options,
        report,
        error_code,
        error_message
      FROM order_attribution_backfill_runs
      WHERE id = $1
      LIMIT 1
    `,
    [jobId]
  );

  const row = result.rows[0];

  if (!row) {
    throw new AttributionAdminHttpError(404, 'backfill_job_not_found', 'Order attribution backfill job was not found');
  }

  return mapBackfillRunRow(row);
}

export function createAttributionAdminRouter(): Router {
  const router = createRouter();

  router.use(attachAuthContext);
  router.use(requireAdmin);

  router.post('/orders/backfill', async (req, res, next) => {
    try {
      const auth = res.locals.auth as AuthContext | null | undefined;
      const options = parseBackfillRequest(req.body ?? {});
      const response = await enqueueOrderAttributionBackfillRun(options, getSubmittedBy(auth));

      res.status(202).json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders/backfill/:jobId', async (req, res, next) => {
    try {
      const response = await getOrderAttributionBackfillRun(req.params.jobId);
      res.status(200).json(response);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
