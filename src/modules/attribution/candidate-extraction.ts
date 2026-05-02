import type { PoolClient } from 'pg';

import { buildCanonicalTouchpointDimensions } from '../marketing-dimensions/index.js';
import {
  extractShopifyHintAttribution,
  type ShopifyAttributionHintPayload
} from '../shopify/attribution-hints.js';
import {
  confidenceScoreForWinner,
  dedupeDeterministicCandidates,
  isDirectTouchpoint,
  type DeterministicIngestionSource,
  type ResolvedAttributionTouchpoint
} from './resolver.js';

const ATTRIBUTION_WINDOW_DAYS = 7;

type OrderTimestampSource = 'processed_at' | 'created_at_shopify' | 'ingested_at';

type SessionCandidateRow = {
  session_id: string;
  source_touch_event_id: string | null;
  occurred_at: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
};

export type AttributionCandidateOrder = {
  shopifyOrderId: string;
  processedAt: Date | string | null;
  createdAtShopify: Date | string | null;
  ingestedAt: Date | string | null;
  landingSessionId: string | null;
  checkoutToken: string | null;
  cartToken: string | null;
  emailHash?: string | null;
  customerIdentityId?: string | null;
  sourceName?: string | null;
  rawPayload?: unknown;
};

export type AttributionCandidateNormalizationFailure = {
  scope: 'order' | 'shopify_hint' | 'ga4_fallback';
  reason: string;
  sourceKey: string | null;
};

export type AttributionCandidate = {
  sourceClass: 'deterministic_first_party' | 'deterministic_shopify_hint' | 'ga4_fallback';
  sourceKey: string;
  sessionId: string | null;
  sourceTouchEventId: string | null;
  ingestionSource: DeterministicIngestionSource | 'shopify_marketing_hint' | 'ga4_fallback';
  occurredAtUtc: Date;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  attributionReason: string;
  confidenceScore: number;
  isDirect: boolean;
  isSynthetic: boolean;
};

export type Ga4AttributionCandidateInput = {
  stableIdentifier: string;
  occurredAt: Date | string | null;
  sessionId?: string | null;
  sourceTouchEventId?: string | null;
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  clickIdType?: string | null;
  clickIdValue?: string | null;
  gclid?: string | null;
  gbraid?: string | null;
  wbraid?: string | null;
  fbclid?: string | null;
  ttclid?: string | null;
  msclkid?: string | null;
  attributionReason?: string | null;
  confidenceScore?: number | null;
};

export type AttributionCandidateExtractionOptions = {
  loadDeterministicFirstPartyCandidates?: (
    client: PoolClient,
    order: AttributionCandidateOrder
  ) => Promise<AttributionCandidate[]>;
  loadGa4Candidates?: (
    client: PoolClient,
    input: { order: AttributionCandidateOrder; orderOccurredAtUtc: Date }
  ) => Promise<Ga4AttributionCandidateInput[]>;
};

export type AttributionCandidateExtractionResult = {
  orderOccurredAtUtc: Date | null;
  orderTimestampSource: OrderTimestampSource | null;
  deterministicFirstParty: AttributionCandidate[];
  shopifyHint: AttributionCandidate[];
  ga4Fallback: AttributionCandidate[];
  normalizationFailures: AttributionCandidateNormalizationFailure[];
};

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function clampConfidenceScore(value: number | null | undefined, fallback: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return fallback;
  }

  return Math.min(Math.max(value, 0), 1);
}

export function normalizeTimestampToUtc(value: Date | string | null | undefined): Date | null {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : new Date(value.toISOString());
  }

  const normalized = value.trim();
  if (!normalized || !/(Z|[+-]\d{2}:\d{2})$/i.test(normalized)) {
    return null;
  }

  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function resolveOrderOccurredAtUtc(
  order: AttributionCandidateOrder
): { orderOccurredAtUtc: Date | null; source: OrderTimestampSource | null } {
  const candidates: Array<{ source: OrderTimestampSource; value: Date | string | null }> = [
    { source: 'processed_at', value: order.processedAt },
    { source: 'created_at_shopify', value: order.createdAtShopify },
    { source: 'ingested_at', value: order.ingestedAt }
  ];

  for (const candidate of candidates) {
    const normalized = normalizeTimestampToUtc(candidate.value);
    if (normalized) {
      return {
        orderOccurredAtUtc: normalized,
        source: candidate.source
      };
    }
  }

  return {
    orderOccurredAtUtc: null,
    source: null
  };
}

function buildResolvedTouchpoint(
  row: SessionCandidateRow,
  ingestionSource: DeterministicIngestionSource,
  attributionReason: string,
  isForced: boolean
): ResolvedAttributionTouchpoint {
  return {
    sessionId: row.session_id,
    sourceTouchEventId: row.source_touch_event_id,
    occurredAt: row.occurred_at,
    source: row.source,
    medium: row.medium,
    campaign: row.campaign,
    content: row.content,
    term: row.term,
    clickIdType: row.click_id_type,
    clickIdValue: row.click_id_value,
    attributionReason,
    ingestionSource,
    isDirect: isDirectTouchpoint({
      source: row.source,
      medium: row.medium,
      campaign: row.campaign,
      content: row.content,
      term: row.term,
      clickIdValue: row.click_id_value
    }),
    isForced
  };
}

async function fetchLandingSessionCandidate(
  client: PoolClient,
  sessionId: string
): Promise<ResolvedAttributionTouchpoint | null> {
  const result = await client.query<SessionCandidateRow>(
    `
      SELECT
        s.id::text AS session_id,
        first_event.id::text AS source_touch_event_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT e.id
        FROM tracking_events e
        WHERE e.session_id = s.id
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT 1
      ) AS first_event ON true
      WHERE s.id = $1::uuid
      LIMIT 1
    `,
    [sessionId]
  );

  const row = result.rows[0];
  return row ? buildResolvedTouchpoint(row, 'landing_session_id', 'matched_by_landing_session', true) : null;
}

async function fetchLatestTokenCandidate(
  client: PoolClient,
  tokenColumn: 'shopify_checkout_token' | 'shopify_cart_token',
  token: string,
  orderOccurredAtUtc: Date,
  ingestionSource: DeterministicIngestionSource,
  attributionReason: string
): Promise<ResolvedAttributionTouchpoint | null> {
  const result = await client.query<SessionCandidateRow>(
    `
      SELECT
        e.session_id::text AS session_id,
        e.id::text AS source_touch_event_id,
        e.occurred_at,
        COALESCE(e.utm_source, s.initial_utm_source) AS source,
        COALESCE(e.utm_medium, s.initial_utm_medium) AS medium,
        COALESCE(e.utm_campaign, s.initial_utm_campaign) AS campaign,
        COALESCE(e.utm_content, s.initial_utm_content) AS content,
        COALESCE(e.utm_term, s.initial_utm_term) AS term,
        CASE
          WHEN e.gclid IS NOT NULL THEN 'gclid'
          WHEN e.gbraid IS NOT NULL THEN 'gbraid'
          WHEN e.wbraid IS NOT NULL THEN 'wbraid'
          WHEN e.fbclid IS NOT NULL THEN 'fbclid'
          WHEN e.ttclid IS NOT NULL THEN 'ttclid'
          WHEN e.msclkid IS NOT NULL THEN 'msclkid'
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          e.gclid,
          e.gbraid,
          e.wbraid,
          e.fbclid,
          e.ttclid,
          e.msclkid,
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_events e
      INNER JOIN tracking_sessions s
        ON s.id = e.session_id
      WHERE ${tokenColumn} = $1
        AND e.occurred_at <= $2
        AND e.occurred_at >= $2 - ($3::int * interval '1 day')
      ORDER BY e.occurred_at DESC, e.id DESC
      LIMIT 1
    `,
    [token, orderOccurredAtUtc, ATTRIBUTION_WINDOW_DAYS]
  );

  const row = result.rows[0];
  return row ? buildResolvedTouchpoint(row, ingestionSource, attributionReason, true) : null;
}

async function fetchIdentityCandidates(
  client: PoolClient,
  order: AttributionCandidateOrder,
  orderOccurredAtUtc: Date
): Promise<ResolvedAttributionTouchpoint[]> {
  if (!order.customerIdentityId && !order.emailHash) {
    return [];
  }

  const result = await client.query<SessionCandidateRow>(
    `
      SELECT
        s.id::text AS session_id,
        first_event.id::text AS source_touch_event_id,
        s.first_seen_at AS occurred_at,
        s.initial_utm_source AS source,
        s.initial_utm_medium AS medium,
        s.initial_utm_campaign AS campaign,
        s.initial_utm_content AS content,
        s.initial_utm_term AS term,
        CASE
          WHEN s.initial_gclid IS NOT NULL THEN 'gclid'
          WHEN s.initial_gbraid IS NOT NULL THEN 'gbraid'
          WHEN s.initial_wbraid IS NOT NULL THEN 'wbraid'
          WHEN s.initial_fbclid IS NOT NULL THEN 'fbclid'
          WHEN s.initial_ttclid IS NOT NULL THEN 'ttclid'
          WHEN s.initial_msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          s.initial_gclid,
          s.initial_gbraid,
          s.initial_wbraid,
          s.initial_fbclid,
          s.initial_ttclid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT e.id
        FROM tracking_events e
        WHERE e.session_id = s.id
        ORDER BY e.occurred_at ASC, e.id ASC
        LIMIT 1
      ) AS first_event ON true
      WHERE (
        ($1::uuid IS NOT NULL AND s.customer_identity_id = $1::uuid)
        OR EXISTS (
          SELECT 1
          FROM shopify_orders o
          WHERE o.shopify_order_id = $2
            AND o.customer_identity_id IS NOT NULL
            AND o.customer_identity_id = s.customer_identity_id
        )
      )
        AND s.first_seen_at <= $3
        AND s.first_seen_at >= $3 - ($4::int * interval '1 day')
      ORDER BY s.first_seen_at ASC, s.id ASC
    `,
    [order.customerIdentityId ?? null, order.shopifyOrderId, orderOccurredAtUtc, ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows.map((row) =>
    buildResolvedTouchpoint(row, 'customer_identity', 'matched_by_customer_identity', false)
  );
}

function mapDeterministicCandidate(candidate: ResolvedAttributionTouchpoint): AttributionCandidate {
  return {
    sourceClass: 'deterministic_first_party',
    sourceKey: candidate.sessionId ?? candidate.sourceTouchEventId ?? `${candidate.ingestionSource}:${candidate.occurredAt.toISOString()}`,
    sessionId: candidate.sessionId,
    sourceTouchEventId: candidate.sourceTouchEventId,
    ingestionSource: candidate.ingestionSource,
    occurredAtUtc: candidate.occurredAt,
    source: candidate.source,
    medium: candidate.medium,
    campaign: candidate.campaign,
    content: candidate.content,
    term: candidate.term,
    clickIdType: candidate.clickIdType,
    clickIdValue: candidate.clickIdValue,
    attributionReason: candidate.attributionReason,
    confidenceScore: confidenceScoreForWinner(candidate),
    isDirect: candidate.isDirect,
    isSynthetic: false
  };
}

function buildShopifyHintCandidate(orderOccurredAtUtc: Date, hint: ReturnType<typeof extractShopifyHintAttribution>): AttributionCandidate | null {
  if (!hint) {
    return null;
  }

  return {
    sourceClass: 'deterministic_shopify_hint',
    sourceKey: `shopify:${orderOccurredAtUtc.toISOString()}:${hint.clickIdType ?? 'utm'}`,
    sessionId: null,
    sourceTouchEventId: null,
    ingestionSource: 'shopify_marketing_hint',
    occurredAtUtc: orderOccurredAtUtc,
    source: hint.source,
    medium: hint.medium,
    campaign: hint.campaign,
    content: hint.content,
    term: hint.term,
    clickIdType: hint.clickIdType,
    clickIdValue: hint.clickIdValue,
    attributionReason: 'shopify_hint_derived',
    confidenceScore: hint.confidenceScore,
    isDirect: isDirectTouchpoint({
      source: hint.source,
      medium: hint.medium,
      campaign: hint.campaign,
      content: hint.content,
      term: hint.term,
      clickIdValue: hint.clickIdValue
    }),
    isSynthetic: true
  };
}

function compareGa4Candidates(left: AttributionCandidate, right: AttributionCandidate): number {
  if (right.occurredAtUtc.getTime() !== left.occurredAtUtc.getTime()) {
    return right.occurredAtUtc.getTime() - left.occurredAtUtc.getTime();
  }

  if (Boolean(right.clickIdValue) !== Boolean(left.clickIdValue)) {
    return Number(Boolean(right.clickIdValue)) - Number(Boolean(left.clickIdValue));
  }

  return left.sourceKey.localeCompare(right.sourceKey);
}

function mapGa4Candidate(
  rawCandidate: Ga4AttributionCandidateInput,
  orderOccurredAtUtc: Date
): { candidate: AttributionCandidate | null; failure: AttributionCandidateNormalizationFailure | null } {
  const sourceKey = normalizeNullableString(rawCandidate.stableIdentifier);
  if (!sourceKey) {
    return {
      candidate: null,
      failure: {
        scope: 'ga4_fallback',
        reason: 'missing_stable_identifier',
        sourceKey: null
      }
    };
  }

  const occurredAtUtc = normalizeTimestampToUtc(rawCandidate.occurredAt);
  if (!occurredAtUtc) {
    return {
      candidate: null,
      failure: {
        scope: 'ga4_fallback',
        reason: 'invalid_candidate_timestamp',
        sourceKey
      }
    };
  }

  if (occurredAtUtc.getTime() > orderOccurredAtUtc.getTime()) {
    return {
      candidate: null,
      failure: {
        scope: 'ga4_fallback',
        reason: 'future_dated_candidate',
        sourceKey
      }
    };
  }

  const canonicalDimensions = buildCanonicalTouchpointDimensions({
    source: rawCandidate.source,
    medium: rawCandidate.medium,
    campaign: rawCandidate.campaign,
    content: rawCandidate.content,
    term: rawCandidate.term,
    clickIdType: rawCandidate.clickIdType,
    clickIdValue: rawCandidate.clickIdValue,
    gclid: rawCandidate.gclid,
    gbraid: rawCandidate.gbraid,
    wbraid: rawCandidate.wbraid,
    fbclid: rawCandidate.fbclid,
    ttclid: rawCandidate.ttclid,
    msclkid: rawCandidate.msclkid
  });

  return {
    candidate: {
      sourceClass: 'ga4_fallback',
      sourceKey,
      sessionId: normalizeNullableString(rawCandidate.sessionId),
      sourceTouchEventId: normalizeNullableString(rawCandidate.sourceTouchEventId),
      ingestionSource: 'ga4_fallback',
      occurredAtUtc,
      source: canonicalDimensions.source,
      medium: canonicalDimensions.medium,
      campaign: canonicalDimensions.campaign,
      content: canonicalDimensions.content,
      term: canonicalDimensions.term,
      clickIdType: canonicalDimensions.clickIdType,
      clickIdValue: canonicalDimensions.clickIdValue,
      attributionReason: normalizeNullableString(rawCandidate.attributionReason) ?? 'ga4_fallback_match',
      confidenceScore: clampConfidenceScore(rawCandidate.confidenceScore, canonicalDimensions.clickIdValue ? 0.35 : 0.25),
      isDirect: isDirectTouchpoint({
        source: canonicalDimensions.source,
        medium: canonicalDimensions.medium,
        campaign: canonicalDimensions.campaign,
        content: canonicalDimensions.content,
        term: canonicalDimensions.term,
        clickIdValue: canonicalDimensions.clickIdValue
      }),
      isSynthetic: true
    },
    failure: null
  };
}

function normalizeShopifyHintPayload(rawPayload: unknown): ShopifyAttributionHintPayload | null {
  if (!rawPayload || typeof rawPayload !== 'object' || Array.isArray(rawPayload)) {
    return null;
  }

  const payload = rawPayload as Record<string, unknown>;
  return {
    landing_site: typeof payload.landing_site === 'string' ? payload.landing_site : null,
    note_attributes: Array.isArray(payload.note_attributes) ? (payload.note_attributes as ShopifyAttributionHintPayload['note_attributes']) : undefined,
    attributes: Array.isArray(payload.attributes) ? (payload.attributes as ShopifyAttributionHintPayload['attributes']) : undefined
  };
}

export async function collectDeterministicFirstPartyCandidates(
  client: PoolClient,
  order: AttributionCandidateOrder
): Promise<AttributionCandidate[]> {
  const { orderOccurredAtUtc } = resolveOrderOccurredAtUtc(order);
  if (!orderOccurredAtUtc) {
    return [];
  }

  const candidates: ResolvedAttributionTouchpoint[] = [];

  if (order.landingSessionId) {
    const landingCandidate = await fetchLandingSessionCandidate(client, order.landingSessionId);
    if (landingCandidate) {
      candidates.push(landingCandidate);
    }
  }

  if (order.checkoutToken) {
    const checkoutCandidate = await fetchLatestTokenCandidate(
      client,
      'shopify_checkout_token',
      order.checkoutToken,
      orderOccurredAtUtc,
      'checkout_token',
      'matched_by_checkout_token'
    );

    if (checkoutCandidate) {
      candidates.push(checkoutCandidate);
    }
  }

  if (order.cartToken) {
    const cartCandidate = await fetchLatestTokenCandidate(
      client,
      'shopify_cart_token',
      order.cartToken,
      orderOccurredAtUtc,
      'cart_token',
      'matched_by_cart_token'
    );

    if (cartCandidate) {
      candidates.push(cartCandidate);
    }
  }

  candidates.push(...(await fetchIdentityCandidates(client, order, orderOccurredAtUtc)));

  return dedupeDeterministicCandidates(candidates).map(mapDeterministicCandidate);
}

export async function extractAttributionCandidatesForOrder(
  client: PoolClient,
  order: AttributionCandidateOrder,
  options: AttributionCandidateExtractionOptions = {}
): Promise<AttributionCandidateExtractionResult> {
  const normalizationFailures: AttributionCandidateNormalizationFailure[] = [];
  const { orderOccurredAtUtc, source } = resolveOrderOccurredAtUtc(order);

  if (!orderOccurredAtUtc) {
    normalizationFailures.push({
      scope: 'order',
      reason: 'missing_order_timestamp',
      sourceKey: order.shopifyOrderId
    });

    return {
      orderOccurredAtUtc: null,
      orderTimestampSource: null,
      deterministicFirstParty: [],
      shopifyHint: [],
      ga4Fallback: [],
      normalizationFailures
    };
  }

  const deterministicFirstParty = options.loadDeterministicFirstPartyCandidates
    ? await options.loadDeterministicFirstPartyCandidates(client, order)
    : await collectDeterministicFirstPartyCandidates(client, order);

  const shopifyHintPayload = normalizeShopifyHintPayload(order.rawPayload);
  const shopifyHintCandidate = buildShopifyHintCandidate(
    orderOccurredAtUtc,
    shopifyHintPayload ? extractShopifyHintAttribution(shopifyHintPayload) : null
  );

  if (order.rawPayload && !shopifyHintPayload) {
    normalizationFailures.push({
      scope: 'shopify_hint',
      reason: 'invalid_shopify_payload_shape',
      sourceKey: order.shopifyOrderId
    });
  }

  const rawGa4Candidates = options.loadGa4Candidates
    ? await options.loadGa4Candidates(client, { order, orderOccurredAtUtc })
    : [];
  const ga4BySourceKey = new Map<string, AttributionCandidate>();

  for (const rawCandidate of rawGa4Candidates) {
    const mapped = mapGa4Candidate(rawCandidate, orderOccurredAtUtc);
    if (mapped.failure) {
      normalizationFailures.push(mapped.failure);
      continue;
    }

    const candidate = mapped.candidate;
    if (!candidate) {
      continue;
    }

    const existing = ga4BySourceKey.get(candidate.sourceKey);
    if (!existing || compareGa4Candidates(candidate, existing) < 0) {
      ga4BySourceKey.set(candidate.sourceKey, candidate);
    }
  }

  return {
    orderOccurredAtUtc,
    orderTimestampSource: source,
    deterministicFirstParty,
    shopifyHint: shopifyHintCandidate ? [shopifyHintCandidate] : [],
    ga4Fallback: Array.from(ga4BySourceKey.values()).sort(compareGa4Candidates),
    normalizationFailures
  };
}
