import { type Router, Router as createRouter } from 'express';
import { z } from 'zod';

import {
  normalizeOrderAttributionBackfillRequest,
  type OrderAttributionBackfillRequest
} from '../../../packages/attribution-schema/index.js';
import { query } from '../../db/pool.js';
import { emitOrderAttributionBackfillJobLifecycleLog } from '../../observability/index.js';
import { attachAuthContext, requireAdmin, type AuthContext } from '../auth/index.js';
import { enqueueOrderAttributionBackfillRun, getOrderAttributionBackfillRun } from './backfill-run-store.js';
import { getAttributionQaPayloadForOrder } from './qa-payload-service.js';

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

const uuidSchema = z.string().uuid();
const orderQaDebugParamsSchema = z.object({
  orderId: z.string().trim().min(1)
});
const orderQaDebugQuerySchema = z.object({
  runId: uuidSchema.optional()
});

type EvidenceState = 'available' | 'missing' | 'expired_or_pruned';

type LatestRunRow = {
  run_id: string;
  normalized_at_utc: Date;
  retained_until: Date;
};

type RawEvidenceRow = {
  id: string;
  run_id: string;
  order_id: string;
  evidence_type: 'shopify_hint' | 'tracking_touchpoint';
  source_table: string;
  source_record_id: string;
  touchpoint_id: string | null;
  session_id: string | null;
  ingestion_source: string | null;
  event_type: string | null;
  occurred_at_utc: Date | null;
  captured_at_utc: Date | null;
  evidence_status: 'valid' | 'malformed';
  error_code: string | null;
  error_message: string | null;
  normalized_metadata: unknown;
  raw_payload: unknown;
  payload_size_bytes: number;
  payload_hash: string;
  created_at_utc: Date;
  retained_until: Date;
};

type Ga4FallbackDebugCandidateRow = {
  candidate_key: string;
  occurred_at: Date;
  ga4_user_key: string;
  ga4_client_id: string | null;
  ga4_session_id: string | null;
  transaction_id: string | null;
  email_hash: string | null;
  customer_identity_id: string | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
  session_has_required_fields: boolean;
  source_export_hour: Date;
  source_dataset: string;
  source_table_type: string;
  retained_until: Date;
  created_at: Date;
  updated_at: Date;
  matched_on: string;
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

function parseInput<TSchema extends z.ZodTypeAny>(schema: TSchema, input: unknown, message: string): z.infer<TSchema> {
  try {
    return schema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new AttributionAdminHttpError(400, 'invalid_request', message, error.flatten());
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

function requireInternalAdminUser(auth: AuthContext | null | undefined): void {
  if (!auth) {
    throw new AttributionAdminHttpError(401, 'unauthorized', 'Authentication required');
  }

  if (auth.kind !== 'user' || !auth.user.isAdmin) {
    throw new AttributionAdminHttpError(403, 'forbidden', 'Internal admin user access required');
  }
}

function asObjectRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

async function resolveQaDebugRun(orderId: string, requestedRunId?: string): Promise<LatestRunRow | null> {
  const result = await query<LatestRunRow>(
    `
      SELECT
        run_id::text AS run_id,
        normalized_at_utc,
        retained_until
      FROM attribution_order_inputs
      WHERE order_id = $1
        ${requestedRunId ? 'AND run_id = $2::uuid' : ''}
      ORDER BY normalized_at_utc DESC, run_id DESC
      LIMIT 1
    `,
    requestedRunId ? [orderId, requestedRunId] : [orderId]
  );

  return result.rows[0] ?? null;
}

async function loadRawEvidence(runId: string, orderId: string): Promise<RawEvidenceRow[]> {
  const result = await query<RawEvidenceRow>(
    `
      SELECT
        id::text,
        run_id::text AS run_id,
        order_id,
        evidence_type,
        source_table,
        source_record_id,
        touchpoint_id,
        session_id::text AS session_id,
        ingestion_source,
        event_type,
        occurred_at_utc,
        captured_at_utc,
        evidence_status,
        error_code,
        error_message,
        normalized_metadata,
        raw_payload,
        payload_size_bytes,
        payload_hash,
        created_at_utc,
        retained_until
      FROM attribution_raw_evidence
      WHERE run_id = $1::uuid
        AND order_id = $2
      ORDER BY evidence_type ASC, occurred_at_utc ASC NULLS LAST, source_record_id ASC, id ASC
    `,
    [runId, orderId]
  );

  return result.rows;
}

async function loadGa4FallbackDebugCandidate(input: {
  orderId: string;
  candidateKeys: string[];
  emailHash: string | null;
  customerIdentityId: string | null;
}): Promise<Ga4FallbackDebugCandidateRow | null> {
  const result = await query<Ga4FallbackDebugCandidateRow>(
    `
      WITH candidates AS (
        SELECT
          candidate_key,
          occurred_at,
          ga4_user_key,
          ga4_client_id,
          ga4_session_id,
          transaction_id,
          email_hash,
          customer_identity_id::text AS customer_identity_id,
          source,
          medium,
          campaign,
          content,
          term,
          click_id_type,
          click_id_value,
          session_has_required_fields,
          source_export_hour,
          source_dataset,
          source_table_type,
          retained_until,
          created_at,
          updated_at,
          CASE
            WHEN candidate_key = ANY($1::text[]) THEN 'qa_candidate_key'
            WHEN transaction_id = $2 THEN 'transaction_id'
            WHEN $3::text IS NOT NULL AND email_hash = $3 THEN 'email_hash'
            WHEN $4::uuid IS NOT NULL AND customer_identity_id = $4::uuid THEN 'customer_identity_id'
            ELSE 'unknown'
          END AS matched_on,
          CASE WHEN candidate_key = ANY($1::text[]) THEN 0 ELSE 1 END AS match_rank
        FROM ga4_fallback_candidates
        WHERE candidate_key = ANY($1::text[])
          OR transaction_id = $2
          OR ($3::text IS NOT NULL AND email_hash = $3)
          OR ($4::uuid IS NOT NULL AND customer_identity_id = $4::uuid)
      )
      SELECT *
      FROM candidates
      ORDER BY match_rank ASC, retained_until DESC, occurred_at DESC, candidate_key ASC
      LIMIT 1
    `,
    [input.candidateKeys, input.orderId, input.emailHash, input.customerIdentityId]
  );

  return result.rows[0] ?? null;
}

function mapRawEvidence(row: RawEvidenceRow) {
  return {
    id: row.id,
    runId: row.run_id,
    orderId: row.order_id,
    evidenceType: row.evidence_type,
    sourceTable: row.source_table,
    sourceRecordId: row.source_record_id,
    touchpointId: row.touchpoint_id,
    sessionId: row.session_id,
    ingestionSource: row.ingestion_source,
    eventType: row.event_type,
    occurredAtUtc: row.occurred_at_utc?.toISOString() ?? null,
    capturedAtUtc: row.captured_at_utc?.toISOString() ?? null,
    evidenceStatus: row.evidence_status,
    errorCode: row.error_code,
    errorMessage: row.error_message,
    normalizedMetadata: asObjectRecord(row.normalized_metadata),
    rawPayload: row.raw_payload,
    payloadSizeBytes: row.payload_size_bytes,
    payloadHash: row.payload_hash,
    createdAtUtc: row.created_at_utc.toISOString(),
    retainedUntil: row.retained_until.toISOString()
  };
}

function mapGa4FallbackDebugCandidate(row: Ga4FallbackDebugCandidateRow | null) {
  if (!row) {
    return null;
  }

  return {
    candidateKey: row.candidate_key,
    occurredAt: row.occurred_at.toISOString(),
    ga4UserKey: row.ga4_user_key,
    ga4ClientId: row.ga4_client_id,
    ga4SessionId: row.ga4_session_id,
    transactionId: row.transaction_id,
    emailHash: row.email_hash,
    customerIdentityId: row.customer_identity_id,
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    content: row.content,
    term: row.term,
    clickIdType: row.click_id_type,
    clickIdValue: row.click_id_value,
    sessionHasRequiredFields: row.session_has_required_fields,
    sourceExportHour: row.source_export_hour.toISOString(),
    sourceDataset: row.source_dataset,
    sourceTableType: row.source_table_type,
    retainedUntil: row.retained_until.toISOString(),
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
    matchedOn: row.matched_on
  };
}

function rawEvidenceState(run: LatestRunRow | null, rawEvidenceCount: number): EvidenceState {
  if (!run) {
    return 'missing';
  }

  return rawEvidenceCount > 0 ? 'available' : 'expired_or_pruned';
}

function ga4EvidenceState(hasQaCandidate: boolean, candidate: Ga4FallbackDebugCandidateRow | null): EvidenceState {
  if (candidate) {
    return candidate.retained_until.getTime() < Date.now() ? 'expired_or_pruned' : 'available';
  }

  return hasQaCandidate ? 'expired_or_pruned' : 'missing';
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

  router.get('/orders/:orderId/qa-debug', async (req, res, next) => {
    try {
      const auth = res.locals.auth as AuthContext | null | undefined;
      requireInternalAdminUser(auth);

      const { orderId } = parseInput(orderQaDebugParamsSchema, req.params, 'Invalid attribution QA debug request');
      const input = parseInput(orderQaDebugQuerySchema, req.query, 'Invalid attribution QA debug request');
      const result = await getAttributionQaPayloadForOrder(orderId, { sanitize: false });

      if (!result) {
        throw new AttributionAdminHttpError(404, 'shopify_order_not_found', `No Shopify order was found for ${orderId}`);
      }

      const run = await resolveQaDebugRun(orderId, input.runId);
      if (input.runId && !run) {
        throw new AttributionAdminHttpError(
          404,
          'attribution_run_not_found',
          `No attribution run ${input.runId} was found for order ${orderId}`
        );
      }

      const rawEvidence = run ? await loadRawEvidence(run.run_id, orderId) : [];
      const ga4CandidateKeys = result.payload.candidates.ga4_fallback.map((candidate) => candidate.source_key);
      const ga4FallbackCandidate = await loadGa4FallbackDebugCandidate({
        orderId,
        candidateKeys: ga4CandidateKeys,
        emailHash: result.payload.order.identifiers.email_hash,
        customerIdentityId: null
      });
      const rawShopifyHints = rawEvidence.filter((record) => record.evidence_type === 'shopify_hint');
      const rawTouchpoints = rawEvidence.filter((record) => record.evidence_type === 'tracking_touchpoint');

      res.json({
        orderId,
        source: result.source,
        selectedRunId: run?.run_id ?? null,
        selectedRunReason: input.runId ? 'explicit_run_id' : run ? 'latest_run_for_order' : 'no_persisted_attribution_run',
        generatedAtUtc: new Date().toISOString(),
        evidenceState: {
          attributionRun: run ? 'available' : 'missing',
          rawEvidence: rawEvidenceState(run, rawEvidence.length),
          rawShopifyHints: rawEvidenceState(run, rawShopifyHints.length),
          rawTouchpoints: rawEvidenceState(run, rawTouchpoints.length),
          ga4FallbackCandidate: ga4EvidenceState(ga4CandidateKeys.length > 0, ga4FallbackCandidate)
        },
        payload: result.payload,
        rawShopifyHints: rawShopifyHints.map(mapRawEvidence),
        rawTouchpoints: rawTouchpoints.map(mapRawEvidence),
        ga4FallbackCandidate: mapGa4FallbackDebugCandidate(ga4FallbackCandidate)
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
