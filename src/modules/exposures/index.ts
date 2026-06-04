import { createHash } from 'node:crypto';

import { type Router, Router as createRouter } from 'express';
import type { PoolClient } from 'pg';
import { z } from 'zod';

import { withTransaction } from '../../db/pool.js';
import { attachAuthContext, requireAdmin } from '../auth/index.js';
import { ingestIdentityEdges } from '../identity/index.js';
import { buildCanonicalSpendDimensions } from '../marketing-dimensions/index.js';

const MAX_BULK_EXPOSURE_EVENTS = 500;
const MAX_TEXT_LENGTH = 255;
const MAX_RAW_PAYLOAD_BYTES = 64 * 1024;
const MAX_FUTURE_SKEW_MS = 60 * 60 * 1000;

const sourcePlatformSchema = z.enum(['meta_ads', 'google_ads', 'tiktok_ads', 'pinterest_ads', 'snapchat_ads', 'unknown']);
const exposureTypeSchema = z.enum(['impression', 'view']);
const optionalTextSchema = z.union([z.string(), z.null(), z.undefined()]);

const exposureEventSchema = z.object({
  idempotencyKey: z.string().trim().min(1).max(500).optional(),
  tenantId: optionalTextSchema,
  workspaceId: optionalTextSchema,
  sourcePlatform: sourcePlatformSchema,
  exposureType: exposureTypeSchema,
  occurredAt: z.string().datetime({ offset: true }),
  receivedAt: z.string().datetime({ offset: true }).optional(),
  sessionId: optionalTextSchema,
  shopifyCustomerId: optionalTextSchema,
  email: optionalTextSchema,
  hashedEmail: optionalTextSchema,
  phone: optionalTextSchema,
  phoneHash: optionalTextSchema,
  checkoutToken: optionalTextSchema,
  cartToken: optionalTextSchema,
  accountId: optionalTextSchema,
  accountName: optionalTextSchema,
  campaignId: optionalTextSchema,
  campaignName: optionalTextSchema,
  adsetId: optionalTextSchema,
  adsetName: optionalTextSchema,
  adId: optionalTextSchema,
  adName: optionalTextSchema,
  creativeId: optionalTextSchema,
  creativeName: optionalTextSchema,
  source: optionalTextSchema,
  medium: optionalTextSchema,
  campaign: optionalTextSchema,
  content: optionalTextSchema,
  term: optionalTextSchema,
  rawPayload: z.record(z.unknown()).optional()
});

const bulkExposureRequestSchema = z.object({
  events: z.array(exposureEventSchema).min(1).max(MAX_BULK_EXPOSURE_EVENTS)
});

type ExposureEventInput = z.infer<typeof exposureEventSchema>;
type BulkExposureRequest = z.infer<typeof bulkExposureRequestSchema>;
type IdentityResolutionStatus = 'resolved' | 'unresolved' | 'skipped' | 'conflict';

type StoredExposureRow = {
  id: string;
  idempotency_key: string;
  identity_journey_id: string | null;
  identity_resolution_status: IdentityResolutionStatus;
  validity_status: 'valid' | 'invalid';
  invalid_reason: string | null;
};

export type ExposureIngestResult = {
  exposureEventId: string;
  idempotencyKey: string;
  deduplicated: boolean;
  identityJourneyId: string | null;
  identityResolutionStatus: IdentityResolutionStatus;
  validityStatus: 'valid' | 'invalid';
  invalidReason: string | null;
};

class ExposureHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'ExposureHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function parseBulkExposureRequest(input: unknown): BulkExposureRequest {
  try {
    return bulkExposureRequestSchema.parse(input);
  } catch (error) {
    if (error instanceof z.ZodError) {
      throw new ExposureHttpError(400, 'invalid_request', 'Invalid exposure ingestion request', error.flatten());
    }

    throw error;
  }
}

function normalizeNullableString(value: string | null | undefined, maxLength = MAX_TEXT_LENGTH): string | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }

  return trimmed.slice(0, maxLength);
}

function normalizeHash(value: string | null | undefined): string | null {
  const normalized = normalizeNullableString(value, 64)?.toLowerCase() ?? null;
  return normalized && /^[a-f0-9]{64}$/.test(normalized) ? normalized : null;
}

function normalizeSessionId(value: string | null | undefined): string | null {
  const normalized = normalizeNullableString(value);
  return normalized && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(normalized)
    ? normalized
    : null;
}

function deriveSourcePlatformDefaults(platform: ExposureEventInput['sourcePlatform']): { source: string; medium: string } {
  if (platform === 'meta_ads') {
    return { source: 'meta', medium: 'paid_social' };
  }

  if (platform === 'google_ads') {
    return { source: 'google', medium: 'cpc' };
  }

  if (platform === 'tiktok_ads') {
    return { source: 'tiktok', medium: 'paid_social' };
  }

  return { source: 'unknown', medium: 'unknown' };
}

function buildIdempotencyKey(input: ExposureEventInput): string {
  if (input.idempotencyKey?.trim()) {
    return input.idempotencyKey.trim();
  }

  const hash = createHash('sha256');
  hash.update(
    JSON.stringify({
      sourcePlatform: input.sourcePlatform,
      exposureType: input.exposureType,
      occurredAt: input.occurredAt,
      sessionId: normalizeNullableString(input.sessionId),
      hashedEmail: normalizeHash(input.hashedEmail),
      shopifyCustomerId: normalizeNullableString(input.shopifyCustomerId),
      accountId: normalizeNullableString(input.accountId),
      campaignId: normalizeNullableString(input.campaignId),
      adsetId: normalizeNullableString(input.adsetId),
      adId: normalizeNullableString(input.adId),
      creativeId: normalizeNullableString(input.creativeId)
    })
  );
  return `exposure:${hash.digest('hex')}`;
}

function buildIdentitySourceRecordId(idempotencyKey: string): string {
  if (idempotencyKey.length <= MAX_TEXT_LENGTH) {
    return idempotencyKey;
  }

  return `sha256:${createHash('sha256').update(idempotencyKey).digest('hex')}`;
}

function validateExposure(input: ExposureEventInput): { validityStatus: 'valid' | 'invalid'; invalidReason: string | null } {
  const occurredAt = new Date(input.occurredAt);
  if (occurredAt.getTime() > Date.now() + MAX_FUTURE_SKEW_MS) {
    return { validityStatus: 'invalid', invalidReason: 'occurred_at_future_skew' };
  }

  if (!normalizeNullableString(input.campaignId) && !normalizeNullableString(input.campaign)) {
    return { validityStatus: 'invalid', invalidReason: 'missing_campaign_reference' };
  }

  const rawPayload = input.rawPayload ?? input;
  if (Buffer.byteLength(JSON.stringify(rawPayload), 'utf8') > MAX_RAW_PAYLOAD_BYTES) {
    return { validityStatus: 'invalid', invalidReason: 'raw_payload_too_large' };
  }

  return { validityStatus: 'valid', invalidReason: null };
}

function mapStoredRow(row: StoredExposureRow, deduplicated: boolean): ExposureIngestResult {
  return {
    exposureEventId: row.id,
    idempotencyKey: row.idempotency_key,
    deduplicated,
    identityJourneyId: row.identity_journey_id,
    identityResolutionStatus: row.identity_resolution_status,
    validityStatus: row.validity_status,
    invalidReason: row.invalid_reason
  };
}

async function loadExistingExposure(client: PoolClient, idempotencyKey: string): Promise<StoredExposureRow | null> {
  const result = await client.query<StoredExposureRow>(
    `
      SELECT
        id::text,
        idempotency_key,
        identity_journey_id::text,
        identity_resolution_status,
        validity_status,
        invalid_reason
      FROM ad_exposure_events
      WHERE idempotency_key = $1
      FOR UPDATE
      LIMIT 1
    `,
    [idempotencyKey]
  );

  return result.rows[0] ?? null;
}

export async function ingestExposureEvent(input: ExposureEventInput): Promise<ExposureIngestResult> {
  return withTransaction(async (client) => {
    const idempotencyKey = buildIdempotencyKey(input);
    const existing = await loadExistingExposure(client, idempotencyKey);
    if (existing) {
      return mapStoredRow(existing, true);
    }

    const occurredAt = new Date(input.occurredAt);
    const validity = validateExposure(input);
    const hashedEmail = normalizeHash(input.hashedEmail);
    const phoneHash = normalizeHash(input.phoneHash);

    const identityResult =
      validity.validityStatus === 'valid'
        ? await ingestIdentityEdges(client, {
            sourceTimestamp: occurredAt,
            evidenceSource: 'ad_exposure',
            sourceTable: 'ad_exposure_events',
            sourceRecordId: buildIdentitySourceRecordId(idempotencyKey),
            idempotencyKey: `ad_exposure:${idempotencyKey}`,
            sessionId: normalizeSessionId(input.sessionId),
            checkoutToken: normalizeNullableString(input.checkoutToken),
            cartToken: normalizeNullableString(input.cartToken),
            shopifyCustomerId: normalizeNullableString(input.shopifyCustomerId),
            email: normalizeNullableString(input.email),
            hashedEmail,
            phone: normalizeNullableString(input.phone),
            phoneHash
          })
        : null;

    const identityResolutionStatus: IdentityResolutionStatus =
      identityResult?.outcome === 'linked'
        ? identityResult.journeyId
          ? 'resolved'
          : 'unresolved'
        : identityResult?.outcome === 'conflict'
          ? 'conflict'
          : identityResult
            ? 'skipped'
            : 'skipped';
    const platformDefaults = deriveSourcePlatformDefaults(input.sourcePlatform);
    const dimensions = buildCanonicalSpendDimensions({
      source: normalizeNullableString(input.source) ?? platformDefaults.source,
      medium: normalizeNullableString(input.medium) ?? platformDefaults.medium,
      campaign: normalizeNullableString(input.campaign) ?? normalizeNullableString(input.campaignName) ?? normalizeNullableString(input.campaignId),
      content: normalizeNullableString(input.content) ?? normalizeNullableString(input.creativeName) ?? normalizeNullableString(input.creativeId),
      term: normalizeNullableString(input.term)
    });
    const rawPayload = input.rawPayload ?? input;

    const result = await client.query<StoredExposureRow>(
      `
        INSERT INTO ad_exposure_events (
          idempotency_key,
          tenant_id,
          workspace_id,
          source_platform,
          exposure_type,
          occurred_at,
          received_at,
          identity_journey_id,
          identity_resolution_status,
          identity_resolution_reason,
          roas_radar_session_id,
          shopify_customer_id,
          hashed_email,
          phone_hash,
          checkout_token,
          cart_token,
          account_id,
          account_name,
          campaign_id,
          campaign_name,
          adset_id,
          adset_name,
          ad_id,
          ad_name,
          creative_id,
          creative_name,
          source,
          medium,
          campaign,
          content,
          term,
          validity_status,
          invalid_reason,
          raw_payload
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, COALESCE($7::timestamptz, now()), $8::uuid, $9, $10,
          $11::uuid, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
          $25, $26, $27, $28, $29, $30, $31, $32, $33, $34::jsonb
        )
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING
          id::text,
          idempotency_key,
          identity_journey_id::text,
          identity_resolution_status,
          validity_status,
          invalid_reason
      `,
      [
        idempotencyKey,
        normalizeNullableString(input.tenantId),
        normalizeNullableString(input.workspaceId),
        input.sourcePlatform,
        input.exposureType,
        occurredAt,
        input.receivedAt ? new Date(input.receivedAt) : null,
        identityResult?.journeyId ?? null,
        identityResolutionStatus,
        identityResult?.reason ?? null,
        normalizeSessionId(input.sessionId),
        normalizeNullableString(input.shopifyCustomerId),
        hashedEmail,
        phoneHash,
        normalizeNullableString(input.checkoutToken),
        normalizeNullableString(input.cartToken),
        normalizeNullableString(input.accountId),
        normalizeNullableString(input.accountName),
        normalizeNullableString(input.campaignId),
        normalizeNullableString(input.campaignName),
        normalizeNullableString(input.adsetId),
        normalizeNullableString(input.adsetName),
        normalizeNullableString(input.adId),
        normalizeNullableString(input.adName),
        normalizeNullableString(input.creativeId),
        normalizeNullableString(input.creativeName),
        dimensions.source,
        dimensions.medium,
        dimensions.campaign,
        dimensions.content,
        dimensions.term,
        validity.validityStatus,
        validity.invalidReason,
        JSON.stringify(rawPayload)
      ]
    );

    if (!result.rows[0]) {
      const racedExisting = await loadExistingExposure(client, idempotencyKey);
      if (racedExisting) {
        return mapStoredRow(racedExisting, true);
      }

      throw new Error(`ad exposure insert conflicted but no row was found for idempotency key ${idempotencyKey}`);
    }

    return mapStoredRow(result.rows[0], false);
  });
}

export async function ingestExposureEvents(input: BulkExposureRequest): Promise<{
  accepted: number;
  deduplicated: number;
  invalid: number;
  resolvedIdentity: number;
  results: ExposureIngestResult[];
}> {
  const results: ExposureIngestResult[] = [];

  for (const event of input.events) {
    results.push(await ingestExposureEvent(event));
  }

  return {
    accepted: results.length,
    deduplicated: results.filter((result) => result.deduplicated).length,
    invalid: results.filter((result) => result.validityStatus === 'invalid').length,
    resolvedIdentity: results.filter((result) => result.identityResolutionStatus === 'resolved').length,
    results
  };
}

export function createExposureAdminRouter(): Router {
  const router = createRouter();

  router.use(attachAuthContext);
  router.use(requireAdmin);

  router.post('/', async (req, res, next) => {
    try {
      const input = parseBulkExposureRequest(req.body);
      const summary = await ingestExposureEvents(input);
      res.status(202).json({
        schemaVersion: 'ad_exposure_events_v1',
        ...summary
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
