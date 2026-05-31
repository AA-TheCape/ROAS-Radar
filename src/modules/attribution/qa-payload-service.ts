import type { PoolClient } from 'pg';

import {
  normalizeAttributionQaPayloadV1,
  type AttributionQaPayloadV1
} from '../../../packages/attribution-schema/index.js';
import { query } from '../../db/pool.js';
import {
  extractAttributionCandidatesForOrder,
  type AttributionCandidateOrder,
  type Ga4AttributionCandidateInput
} from './candidate-extraction.js';
import { ATTRIBUTION_MODELS, executeAttributionModels } from './engine.js';
import { lookupGa4FallbackCandidates } from './ga4-fallback-candidates.js';
import { buildAttributionQaSnapshot, type AttributionQaSnapshotOrder } from './qa-snapshot.js';
import { resolveAttributionTier } from './resolver.js';

type AttributionQaPayloadSource = 'persisted_snapshot' | 'generated_on_read';

export type AttributionQaPayloadResult = {
  orderId: string;
  source: AttributionQaPayloadSource;
  payload: AttributionQaPayloadV1;
};

type AttributionQaPayloadReadOptions = {
  sanitize?: boolean;
};

type QaOrderRow = AttributionQaSnapshotOrder & {
  customer_identity_id: string | null;
  attribution_snapshot: unknown;
};

const SENSITIVE_URL_QUERY_KEYS = new Set([
  'access_token',
  'auth_token',
  'cart_token',
  'checkout_token',
  'client_id',
  'client_secret',
  'code',
  'email',
  'email_hash',
  'fbclid',
  'gclid',
  'gbraid',
  'id_token',
  'msclkid',
  'password',
  'refresh_token',
  'token',
  'ttclid',
  'wbraid'
]);

const SENSITIVE_OBJECT_KEY_EXACT_MATCHES = new Set([
  'fbclid',
  'gclid',
  'gbraid',
  'msclkid',
  'ttclid',
  'wbraid'
]);

function asObjectRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function redactedHint(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }

  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  if (normalized.length <= 8) {
    return '[REDACTED]';
  }

  return `${normalized.slice(0, 4)}...[REDACTED]...${normalized.slice(-4)}`;
}

function isSensitiveObjectKey(key: string): boolean {
  const normalized = key.toLowerCase();
  return (
    normalized.includes('token') ||
    normalized.includes('email') ||
    normalized.includes('password') ||
    normalized.includes('secret') ||
    normalized.includes('click_id') ||
    normalized.includes('client_id') ||
    normalized.includes('session_id') ||
    normalized.includes('user_key') ||
    normalized.startsWith('ga4_') ||
    SENSITIVE_OBJECT_KEY_EXACT_MATCHES.has(normalized)
  );
}

function redactSensitiveUrlQueryValues(value: string): string {
  try {
    const url = new URL(value);
    let changed = false;
    for (const key of Array.from(url.searchParams.keys())) {
      if (SENSITIVE_URL_QUERY_KEYS.has(key.toLowerCase())) {
        url.searchParams.set(key, '[REDACTED]');
        changed = true;
      }
    }
    return changed ? url.toString() : value;
  } catch {
    return value.replace(
      /([?&](?:access_token|auth_token|cart_token|checkout_token|client_id|client_secret|code|email|email_hash|fbclid|gclid|gbraid|id_token|msclkid|password|refresh_token|token|ttclid|wbraid)=)[^&#\s]+/gi,
      '$1[REDACTED]'
    );
  }
}

export function redactSensitiveQaValue(value: unknown): unknown {
  if (typeof value === 'string') {
    return redactSensitiveUrlQueryValues(value);
  }

  if (Array.isArray(value)) {
    return value.map((item) => redactSensitiveQaValue(item));
  }

  const record = asObjectRecord(value);
  if (!record) {
    return value;
  }

  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [
      key,
      isSensitiveObjectKey(key) ? redactedHint(String(item ?? '')) : redactSensitiveQaValue(item)
    ])
  );
}

function redactCandidateSourceKey(value: string): string {
  const separatorIndex = value.indexOf(':');
  if (separatorIndex === -1) {
    return value;
  }

  const prefix = value.slice(0, separatorIndex);
  if (!SENSITIVE_URL_QUERY_KEYS.has(prefix.toLowerCase()) && prefix.toLowerCase() !== 'landing_session_id') {
    return value;
  }

  return `${prefix}:${redactedHint(value.slice(separatorIndex + 1)) ?? '[REDACTED]'}`;
}

function sanitizeNonGa4Candidate(candidate: AttributionQaPayloadV1['candidates']['deterministic_first_party'][number]) {
  return {
    ...candidate,
    source_key: redactCandidateSourceKey(candidate.source_key),
    session_id: null,
    source_touch_event_id: null,
    click_id_value: null
  };
}

export function sanitizeAttributionQaPayload(payload: AttributionQaPayloadV1): AttributionQaPayloadV1 {
  const ga4TouchpointIds = new Map<string, string>();
  const sanitizedGa4Candidates = payload.candidates.ga4_fallback.map((candidate, index) => {
    const redactedId = `ga4_fallback_candidate_${index + 1}`;
    ga4TouchpointIds.set(candidate.source_key, redactedId);
    if (candidate.touchpoint_id) {
      ga4TouchpointIds.set(candidate.touchpoint_id, redactedId);
    }

    return {
      ...candidate,
      source_key: redactedId,
      touchpoint_id: redactedId,
      session_id: null,
      source_touch_event_id: null,
      click_id_value: null
    };
  });

  return normalizeAttributionQaPayloadV1({
    ...payload,
    order: {
      ...payload.order,
      identifiers: {
        ...payload.order.identifiers,
        checkout_token: null,
        cart_token: null,
        email_hash: null
      }
    },
    outcome: {
      ...payload.outcome,
      winner_session_id: null
    },
    candidates: {
      deterministic_first_party: payload.candidates.deterministic_first_party.map(sanitizeNonGa4Candidate),
      shopify_hint: payload.candidates.shopify_hint.map(sanitizeNonGa4Candidate),
      ga4_fallback: sanitizedGa4Candidates
    },
    model_summaries: payload.model_summaries.map((summary) => ({
      ...summary,
      winner_session_id: null
    })),
    credits: payload.credits.map((credit) => ({
      ...credit,
      session_id: null,
      click_id_value: null
    })),
    explainability: payload.explainability.map((record) => ({
      ...record,
      touchpoint_id: record.touchpoint_id ? ga4TouchpointIds.get(record.touchpoint_id) ?? record.touchpoint_id : null,
      details_json: redactSensitiveQaValue(record.details_json) as Record<string, unknown>
    })),
    diagnostics: {
      normalization_failures: payload.diagnostics.normalization_failures.map((failure) => ({
        ...failure,
        source_key: failure.source_key ? ga4TouchpointIds.get(failure.source_key) ?? failure.source_key : null
      })),
      notes: payload.diagnostics.notes.map((note) => redactSensitiveUrlQueryValues(note))
    }
  });
}

function readPersistedQaSnapshot(row: QaOrderRow): AttributionQaPayloadV1 | null {
  const snapshot = asObjectRecord(row.attribution_snapshot);
  if (!snapshot) {
    return null;
  }

  const qaSnapshot = snapshot.qaSnapshot;
  if (!qaSnapshot) {
    return null;
  }

  return normalizeAttributionQaPayloadV1(qaSnapshot);
}

function toCandidateOrder(row: QaOrderRow): AttributionCandidateOrder {
  return {
    shopifyOrderId: row.shopify_order_id,
    processedAt: row.processed_at,
    createdAtShopify: row.created_at_shopify,
    ingestedAt: row.ingested_at,
    landingSessionId: row.landing_session_id,
    checkoutToken: row.checkout_token,
    cartToken: row.cart_token,
    emailHash: row.email_hash,
    customerIdentityId: row.customer_identity_id,
    identityJourneyId: row.identity_journey_id,
    sourceName: row.source_name,
    rawPayload: row.raw_payload
  };
}

async function loadGa4Candidates(
  client: PoolClient,
  input: { order: AttributionCandidateOrder; orderOccurredAtUtc: Date }
): Promise<Ga4AttributionCandidateInput[]> {
  const candidates = await lookupGa4FallbackCandidates(
    {
      orderOccurredAt: input.orderOccurredAtUtc.toISOString(),
      customerIdentityId: input.order.customerIdentityId,
      emailHash: input.order.emailHash,
      transactionId: input.order.shopifyOrderId
    },
    client
  );

  return candidates.map((candidate) => ({
    stableIdentifier: candidate.candidateKey,
    occurredAt: candidate.occurredAt,
    sessionId: null,
    sourceTouchEventId: null,
    source: candidate.source,
    medium: candidate.medium,
    campaign: candidate.campaign,
    content: candidate.content,
    term: candidate.term,
    clickIdType: candidate.clickIdType,
    clickIdValue: candidate.clickIdValue,
    attributionReason: 'ga4_fallback_match'
  }));
}

async function buildQaPayloadFromOrder(row: QaOrderRow): Promise<AttributionQaPayloadV1> {
  const client = { query } as unknown as PoolClient;
  const candidateOrder = toCandidateOrder(row);
  const candidates = await extractAttributionCandidatesForOrder(client, candidateOrder, {
    loadGa4Candidates
  });
  const journey = resolveAttributionTier(candidates);
  const orderOccurredAt = journey.orderOccurredAtUtc ?? row.processed_at ?? row.created_at_shopify ?? row.ingested_at;
  const execution = executeAttributionModels(journey.touchpoints, {
    orderOccurredAt,
    orderRevenue: row.total_price,
    attributionModels: ATTRIBUTION_MODELS,
    normalizationFailuresCount: journey.normalizationFailures.length
  });

  return buildAttributionQaSnapshot({
    order: row,
    candidates,
    journey,
    execution,
    generatedAt: new Date()
  });
}

export async function getAttributionQaPayloadForOrder(
  orderId: string,
  options: AttributionQaPayloadReadOptions = {}
): Promise<AttributionQaPayloadResult | null> {
  const sanitize = options.sanitize ?? true;
  const result = await query<QaOrderRow>(
    `
      SELECT
        shopify_order_id,
        name,
        currency_code,
        subtotal_price,
        total_price,
        processed_at,
        created_at_shopify,
        ingested_at,
        landing_session_id::text AS landing_session_id,
        checkout_token,
        cart_token,
        shopify_customer_id,
        email_hash,
        customer_identity_id::text AS customer_identity_id,
        identity_journey_id::text AS identity_journey_id,
        source_name,
        raw_payload,
        attribution_snapshot
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `,
    [orderId]
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  const persistedPayload = readPersistedQaSnapshot(row);
  if (persistedPayload) {
    return {
      orderId,
      source: 'persisted_snapshot',
      payload: sanitize ? sanitizeAttributionQaPayload(persistedPayload) : persistedPayload
    };
  }

  const generatedPayload = await buildQaPayloadFromOrder(row);

  return {
    orderId,
    source: 'generated_on_read',
    payload: sanitize ? sanitizeAttributionQaPayload(generatedPayload) : generatedPayload
  };
}
