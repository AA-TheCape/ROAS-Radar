import { randomUUID } from 'node:crypto';
import { Router as createRouter } from 'express';
import { z } from 'zod';
import { normalizeOrderAttributionBackfillRequest, orderAttributionBackfillEnqueueResponseSchema, orderAttributionBackfillJobResponseSchema, orderAttributionBackfillReportSchema, orderAttributionBackfillSubmittedOptionsSchema } from '../../../packages/attribution-schema/index.js';
import { query } from '../../db/pool.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
class AttributionAdminHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'AttributionAdminHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
function parseBackfillRequest(input) {
    try {
        return normalizeOrderAttributionBackfillRequest(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new AttributionAdminHttpError(400, 'invalid_request', 'Invalid order attribution backfill request', error.flatten());
        }
        throw error;
    }
}
function getSubmittedBy(auth) {
    if (!auth) {
        throw new AttributionAdminHttpError(401, 'unauthorized', 'Authentication required');
    }
    if (auth.kind === 'internal') {
        return 'internal';
    }
    return auth.user.email;
}
async function enqueueOrderAttributionBackfillRun(options, submittedBy) {
    const jobId = randomUUID();
    const submittedAt = new Date().toISOString();
    await query(`
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
    `, [jobId, submittedAt, submittedBy, JSON.stringify(options)]);
    return orderAttributionBackfillEnqueueResponseSchema.parse({
        ok: true,
        jobId,
        status: 'queued',
        submittedAt,
        submittedBy,
        options
    });
}
function mapBackfillRunRow(row) {
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
        error: row.error_code && row.error_message
            ? {
                code: row.error_code,
                message: row.error_message
            }
            : null
    });
}
async function getOrderAttributionBackfillRun(jobId) {
    const result = await query(`
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
    `, [jobId]);
    const row = result.rows[0];
    if (!row) {
        throw new AttributionAdminHttpError(404, 'backfill_job_not_found', 'Order attribution backfill job was not found');
    }
    return mapBackfillRunRow(row);
}
export function createAttributionAdminRouter() {
    const router = createRouter();
    router.use(attachAuthContext);
    router.use(requireAdmin);
    router.post('/orders/backfill', async (req, res, next) => {
        try {
            const auth = res.locals.auth;
            const options = parseBackfillRequest(req.body ?? {});
            const response = await enqueueOrderAttributionBackfillRun(options, getSubmittedBy(auth));
            res.status(202).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/orders/backfill/:jobId', async (req, res, next) => {
        try {
            const response = await getOrderAttributionBackfillRun(req.params.jobId);
            res.status(200).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
