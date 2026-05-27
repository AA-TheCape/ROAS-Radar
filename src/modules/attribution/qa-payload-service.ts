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

type QaOrderRow = AttributionQaSnapshotOrder & {
  customer_identity_id: string | null;
  attribution_snapshot: unknown;
};

function asObjectRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
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

export async function getAttributionQaPayloadForOrder(orderId: string): Promise<AttributionQaPayloadResult | null> {
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
      payload: persistedPayload
    };
  }

  return {
    orderId,
    source: 'generated_on_read',
    payload: await buildQaPayloadFromOrder(row)
  };
}
