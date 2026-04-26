import { type Router, Router as createRouter } from 'express';
import { z } from 'zod';

import {
  normalizeOrderAttributionBackfillRequest,
  type OrderAttributionBackfillRequest
} from '../../../packages/attribution-schema/index.js';
import { emitOrderAttributionBackfillJobLifecycleLog } from '../../observability/index.js';
import { attachAuthContext, requireAdmin, type AuthContext } from '../auth/index.js';
import { enqueueOrderAttributionBackfillRun, getOrderAttributionBackfillRun } from './backfill-run-store.js';

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

async function loadOrderAttributionBackfillRun(jobId: string) {
  const row = await getOrderAttributionBackfillRun(jobId);

  if (!row) {
    throw new AttributionAdminHttpError(404, 'backfill_job_not_found', 'Order attribution backfill job was not found');
  }

  return row;
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

      emitOrderAttributionBackfillJobLifecycleLog({
        stage: 'enqueued',
        jobId: response.jobId,
        submittedAt: response.submittedAt,
        options: response.options
      });

      res.status(202).json(response);
    } catch (error) {
      next(error);
    }
  });

  router.get('/orders/backfill/:jobId', async (req, res, next) => {
    try {
      const response = await loadOrderAttributionBackfillRun(req.params.jobId);
      res.status(200).json(response);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
