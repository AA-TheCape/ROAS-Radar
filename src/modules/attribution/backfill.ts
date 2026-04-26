import type { PoolClient } from 'pg';

import type { OrderAttributionBackfillReport as OrderAttributionBackfillJobReport } from '../../../packages/attribution-schema/index.js';

import { withTransaction } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';
import { refreshDailyReportingMetrics } from '../reporting/aggregates.js';
import { formatDateInTimezone, getReportingTimezone } from '../settings/index.js';
import { applyShopifyOrderWriteback } from '../shopify/writeback.js';
import {
  ATTRIBUTION_MODELS,
  computeAttributionOutputs,
  computeSingleWinnerCredits,
  type AttributionCredit
} from './engine.js';
import {
  confidenceScoreForWinner,
  dedupeDeterministicCandidates,
  isDirectTouchpoint,
  selectLastNonDirectWinner,
  type DeterministicIngestionSource,
  type ResolvedAttributionTouchpoint,
  type ResolvedJourney
} from './resolver.js';

const ATTRIBUTION_MODEL_VERSION = 1;
const ATTRIBUTION_WINDOW_DAYS = 7;
const MAX_PREVIEW_ORDERS = 25;
const MAX_REPORTED_FAILURES = 100;
const MISSING_ATTRIBUTION_SQL = `
  attribution.shopify_order_id IS NULL
  OR (
    attribution.session_id IS NULL
    AND attribution.attributed_source IS NULL
    AND attribution.attributed_medium IS NULL
    AND attribution.attributed_campaign IS NULL
    AND attribution.attributed_content IS NULL
    AND attribution.attributed_term IS NULL
    AND attribution.attributed_click_id_value IS NULL
  )
`;

type OrderRow = {
  shopify_order_id: string;
  total_price: string;
  processed_at: Date | null;
  created_at_shopify: Date | null;
  ingested_at: Date;
  landing_session_id: string | null;
  checkout_token: string | null;
  cart_token: string | null;
  email_hash: string | null;
  customer_identity_id: string | null;
};

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

type BackfillCandidateRow = {
  shopify_order_id: string;
  order_occurred_at: Date;
};

type BackfillScopeMetrics = {
  totalOrdersInScope: number;
  ordersMissingAttribution: number;
  ordersWithAttribution: number;
  completenessRate: number;
};

type BackfillPreviewRow = {
  shopifyOrderId: string;
  orderOccurredAt: string;
  recoverable: boolean;
  touchpointCount: number;
  winnerSessionId: string | null;
  attributionReason: string;
};

export type OrderAttributionBackfillOptions = {
  windowStart: Date;
  windowEnd: Date;
  requestedBy: string;
  workerId: string;
  limit?: number;
  dryRun?: boolean;
  onlyWebOrders?: boolean;
  writeToShopifyWhenAvailable?: boolean;
  applyWriteback?: typeof applyShopifyOrderWriteback;
};

export type OrderAttributionBackfillFailure = {
  orderId: string | null;
  code: string;
  message: string;
};

export type OrderAttributionBackfillReport = {
  requestedBy: string;
  workerId: string;
  dryRun: boolean;
  scope: {
    windowStart: string;
    windowEnd: string;
    onlyWebOrders: boolean;
    limit: number;
  };
  beforeMetrics: BackfillScopeMetrics;
  afterMetrics: BackfillScopeMetrics;
  scannedOrders: number;
  recoverableOrders: number;
  recoveredOrders: number;
  unrecoverableOrders: number;
  failedOrders: number;
  shopifyWritebackCompleted: number;
  shopifyWritebackSkipped: number;
  shopifyWritebackFailed: number;
  failures: OrderAttributionBackfillFailure[];
  preview: BackfillPreviewRow[];
};

export class OrderAttributionBackfillRunError extends Error {
  code: string;
  report: OrderAttributionBackfillJobReport;

  constructor(message: string, options: { code: string; report: OrderAttributionBackfillJobReport; cause?: unknown }) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = 'OrderAttributionBackfillRunError';
    this.code = options.code;
    this.report = options.report;
  }
}

function normalizeFailureCode(error: unknown, fallback: string): string {
  if (typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'string' && error.code.trim()) {
    return error.code.trim();
  }

  if (error instanceof Error && error.name.trim()) {
    return error.name.trim();
  }

  return fallback;
}

function normalizeFailureMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }

  if (typeof error === 'string' && error.trim()) {
    return error.trim();
  }

  return fallback;
}

function buildEmptyScopeMetrics(): BackfillScopeMetrics {
  return {
    totalOrdersInScope: 0,
    ordersMissingAttribution: 0,
    ordersWithAttribution: 0,
    completenessRate: 1
  };
}

function buildOrderAttributionBackfillReport(input: {
  requestedBy: string;
  workerId: string;
  dryRun: boolean;
  windowStart: Date;
  windowEnd: Date;
  onlyWebOrders: boolean;
  limit: number;
  beforeMetrics: BackfillScopeMetrics;
  afterMetrics: BackfillScopeMetrics;
  scannedOrders: number;
  recoverableOrders: number;
  recoveredOrders: number;
  unrecoverableOrders: number;
  failedOrders: number;
  shopifyWritebackCompleted: number;
  shopifyWritebackSkipped: number;
  shopifyWritebackFailed: number;
  failures: OrderAttributionBackfillFailure[];
  preview: BackfillPreviewRow[];
}): OrderAttributionBackfillReport {
  return {
    requestedBy: input.requestedBy,
    workerId: input.workerId,
    dryRun: input.dryRun,
    scope: {
      windowStart: input.windowStart.toISOString(),
      windowEnd: input.windowEnd.toISOString(),
      onlyWebOrders: input.onlyWebOrders,
      limit: input.limit
    },
    beforeMetrics: input.beforeMetrics,
    afterMetrics: input.afterMetrics,
    scannedOrders: input.scannedOrders,
    recoverableOrders: input.recoverableOrders,
    recoveredOrders: input.recoveredOrders,
    unrecoverableOrders: input.unrecoverableOrders,
    failedOrders: input.failedOrders,
    shopifyWritebackCompleted: input.shopifyWritebackCompleted,
    shopifyWritebackSkipped: input.shopifyWritebackSkipped,
    shopifyWritebackFailed: input.shopifyWritebackFailed,
    failures: input.failures,
    preview: input.preview
  };
}

function recordFailure(
  failures: OrderAttributionBackfillFailure[],
  failure: OrderAttributionBackfillFailure
): void {
  if (failures.length >= MAX_REPORTED_FAILURES) {
    return;
  }

  failures.push(failure);
}

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function resolveOrderOccurredAt(order: OrderRow): Date {
  return order.processed_at ?? order.created_at_shopify ?? order.ingested_at;
}

function buildResolvedTouchpoint(
  row: SessionCandidateRow,
  ingestionSource: DeterministicIngestionSource,
  attributionReason: string
): ResolvedAttributionTouchpoint {
  const touchpoint: ResolvedAttributionTouchpoint = {
    sessionId: row.session_id,
    sourceTouchEventId: row.source_touch_event_id,
    occurredAt: row.occurred_at,
    source: normalizeNullableString(row.source),
    medium: normalizeNullableString(row.medium),
    campaign: normalizeNullableString(row.campaign),
    content: normalizeNullableString(row.content),
    term: normalizeNullableString(row.term),
    clickIdType: normalizeNullableString(row.click_id_type),
    clickIdValue: normalizeNullableString(row.click_id_value),
    attributionReason,
    ingestionSource,
    isDirect: true,
    isForced: false
  };

  touchpoint.isDirect = isDirectTouchpoint(touchpoint);
  return touchpoint;
}

async function fetchOrder(client: PoolClient, shopifyOrderId: string): Promise<OrderRow | null> {
  const result = await client.query<OrderRow>(
    `
      SELECT
        shopify_order_id,
        total_price,
        processed_at,
        created_at_shopify,
        ingested_at,
        landing_session_id::text AS landing_session_id,
        checkout_token,
        cart_token,
        email_hash,
        customer_identity_id::text AS customer_identity_id
      FROM shopify_orders
      WHERE shopify_order_id = $1
      LIMIT 1
    `,
    [shopifyOrderId]
  );

  return result.rows[0] ?? null;
}

async function fetchLandingSessionCandidate(
  client: PoolClient,
  landingSessionId: string
): Promise<ResolvedAttributionTouchpoint | null> {
  const result = await client.query<SessionCandidateRow>(
    `
      SELECT
        s.id::text AS session_id,
        event.id::text AS source_touch_event_id,
        COALESCE(event.occurred_at, s.first_seen_at) AS occurred_at,
        COALESCE(event.utm_source, s.initial_utm_source) AS source,
        COALESCE(event.utm_medium, s.initial_utm_medium) AS medium,
        COALESCE(event.utm_campaign, s.initial_utm_campaign) AS campaign,
        COALESCE(event.utm_content, s.initial_utm_content) AS content,
        COALESCE(event.utm_term, s.initial_utm_term) AS term,
        CASE
          WHEN COALESCE(event.gclid, s.initial_gclid) IS NOT NULL THEN 'gclid'
          WHEN COALESCE(event.gbraid, s.initial_gbraid) IS NOT NULL THEN 'gbraid'
          WHEN COALESCE(event.wbraid, s.initial_wbraid) IS NOT NULL THEN 'wbraid'
          WHEN COALESCE(event.fbclid, s.initial_fbclid) IS NOT NULL THEN 'fbclid'
          WHEN COALESCE(event.ttclid, s.initial_ttclid) IS NOT NULL THEN 'ttclid'
          WHEN COALESCE(event.msclkid, s.initial_msclkid) IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          event.gclid,
          s.initial_gclid,
          event.gbraid,
          s.initial_gbraid,
          event.wbraid,
          s.initial_wbraid,
          event.fbclid,
          s.initial_fbclid,
          event.ttclid,
          s.initial_ttclid,
          event.msclkid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT
          te.id,
          te.occurred_at,
          te.utm_source,
          te.utm_medium,
          te.utm_campaign,
          te.utm_content,
          te.utm_term,
          te.gclid,
          te.gbraid,
          te.wbraid,
          te.fbclid,
          te.ttclid,
          te.msclkid
        FROM tracking_events te
        WHERE te.session_id = s.id
        ORDER BY te.occurred_at ASC, te.id ASC
        LIMIT 1
      ) AS event ON TRUE
      WHERE s.id = $1::uuid
      LIMIT 1
    `,
    [landingSessionId]
  );

  const row = result.rows[0];
  return row ? buildResolvedTouchpoint(row, 'landing_session_id', 'matched_by_landing_session') : null;
}

async function fetchLatestTokenCandidate(
  client: PoolClient,
  tokenColumn: 'shopify_checkout_token' | 'shopify_cart_token',
  tokenValue: string,
  orderOccurredAt: Date,
  ingestionSource: DeterministicIngestionSource,
  attributionReason: string
): Promise<ResolvedAttributionTouchpoint | null> {
  const result = await client.query<SessionCandidateRow>(
    `
      SELECT
        te.session_id::text AS session_id,
        te.id::text AS source_touch_event_id,
        te.occurred_at,
        te.utm_source AS source,
        te.utm_medium AS medium,
        te.utm_campaign AS campaign,
        te.utm_content AS content,
        te.utm_term AS term,
        CASE
          WHEN te.gclid IS NOT NULL THEN 'gclid'
          WHEN te.gbraid IS NOT NULL THEN 'gbraid'
          WHEN te.wbraid IS NOT NULL THEN 'wbraid'
          WHEN te.fbclid IS NOT NULL THEN 'fbclid'
          WHEN te.ttclid IS NOT NULL THEN 'ttclid'
          WHEN te.msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(te.gclid, te.gbraid, te.wbraid, te.fbclid, te.ttclid, te.msclkid) AS click_id_value
      FROM tracking_events te
      WHERE te.${tokenColumn} = $1
        AND te.occurred_at <= $2
        AND te.occurred_at >= $2 - ($3::int * interval '1 day')
      ORDER BY te.occurred_at DESC, te.id DESC
      LIMIT 1
    `,
    [tokenValue, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]
  );

  const row = result.rows[0];
  return row ? buildResolvedTouchpoint(row, ingestionSource, attributionReason) : null;
}

async function fetchIdentityCandidates(
  client: PoolClient,
  order: OrderRow,
  orderOccurredAt: Date
): Promise<ResolvedAttributionTouchpoint[]> {
  if (!order.customer_identity_id) {
    return [];
  }

  const result = await client.query<SessionCandidateRow>(
    `
      SELECT
        s.id::text AS session_id,
        event.id::text AS source_touch_event_id,
        COALESCE(event.occurred_at, s.first_seen_at) AS occurred_at,
        COALESCE(event.utm_source, s.initial_utm_source) AS source,
        COALESCE(event.utm_medium, s.initial_utm_medium) AS medium,
        COALESCE(event.utm_campaign, s.initial_utm_campaign) AS campaign,
        COALESCE(event.utm_content, s.initial_utm_content) AS content,
        COALESCE(event.utm_term, s.initial_utm_term) AS term,
        CASE
          WHEN COALESCE(event.gclid, s.initial_gclid) IS NOT NULL THEN 'gclid'
          WHEN COALESCE(event.gbraid, s.initial_gbraid) IS NOT NULL THEN 'gbraid'
          WHEN COALESCE(event.wbraid, s.initial_wbraid) IS NOT NULL THEN 'wbraid'
          WHEN COALESCE(event.fbclid, s.initial_fbclid) IS NOT NULL THEN 'fbclid'
          WHEN COALESCE(event.ttclid, s.initial_ttclid) IS NOT NULL THEN 'ttclid'
          WHEN COALESCE(event.msclkid, s.initial_msclkid) IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(
          event.gclid,
          s.initial_gclid,
          event.gbraid,
          s.initial_gbraid,
          event.wbraid,
          s.initial_wbraid,
          event.fbclid,
          s.initial_fbclid,
          event.ttclid,
          s.initial_ttclid,
          event.msclkid,
          s.initial_msclkid
        ) AS click_id_value
      FROM tracking_sessions s
      LEFT JOIN LATERAL (
        SELECT
          te.id,
          te.occurred_at,
          te.utm_source,
          te.utm_medium,
          te.utm_campaign,
          te.utm_content,
          te.utm_term,
          te.gclid,
          te.gbraid,
          te.wbraid,
          te.fbclid,
          te.ttclid,
          te.msclkid
        FROM tracking_events te
        WHERE te.session_id = s.id
        ORDER BY te.occurred_at ASC, te.id ASC
        LIMIT 1
      ) AS event ON TRUE
      WHERE s.customer_identity_id = $1::uuid
        AND s.first_seen_at <= $2
        AND s.first_seen_at >= $2 - ($3::int * interval '1 day')
      ORDER BY s.first_seen_at ASC, s.id ASC
    `,
    [order.customer_identity_id, orderOccurredAt, ATTRIBUTION_WINDOW_DAYS]
  );

  return result.rows.map((row) => buildResolvedTouchpoint(row, 'customer_identity', 'matched_by_customer_identity'));
}

async function collectDeterministicCandidates(client: PoolClient, order: OrderRow): Promise<ResolvedAttributionTouchpoint[]> {
  const orderOccurredAt = resolveOrderOccurredAt(order);
  const candidates: ResolvedAttributionTouchpoint[] = [];

  if (order.landing_session_id) {
    const landingCandidate = await fetchLandingSessionCandidate(client, order.landing_session_id);
    if (landingCandidate) {
      candidates.push(landingCandidate);
    }
  }

  if (order.checkout_token) {
    const checkoutCandidate = await fetchLatestTokenCandidate(
      client,
      'shopify_checkout_token',
      order.checkout_token,
      orderOccurredAt,
      'checkout_token',
      'matched_by_checkout_token'
    );

    if (checkoutCandidate) {
      candidates.push(checkoutCandidate);
    }
  }

  if (order.cart_token) {
    const cartCandidate = await fetchLatestTokenCandidate(
      client,
      'shopify_cart_token',
      order.cart_token,
      orderOccurredAt,
      'cart_token',
      'matched_by_cart_token'
    );

    if (cartCandidate) {
      candidates.push(cartCandidate);
    }
  }

  candidates.push(...(await fetchIdentityCandidates(client, order, orderOccurredAt)));
  return candidates;
}

async function resolveAttributionJourney(client: PoolClient, order: OrderRow): Promise<ResolvedJourney> {
  const candidates = await collectDeterministicCandidates(client, order);
  const touchpoints = dedupeDeterministicCandidates(candidates);
  const winner = selectLastNonDirectWinner(touchpoints);

  return {
    touchpoints,
    winner,
    confidenceScore: confidenceScoreForWinner(winner)
  };
}

function selectPrimaryCredit(credits: AttributionCredit[]): AttributionCredit | undefined {
  return credits.find((credit) => credit.isPrimary) ?? credits[credits.length - 1];
}

async function persistAttribution(client: PoolClient, order: OrderRow, journey: ResolvedJourney): Promise<void> {
  const orderOccurredAt = resolveOrderOccurredAt(order);
  const outputs = computeAttributionOutputs(journey.touchpoints, {
    orderOccurredAt,
    orderRevenue: order.total_price
  });

  if (journey.winner) {
    const winnerIndex = journey.touchpoints.findIndex((touchpoint) => touchpoint.sessionId === journey.winner?.sessionId);
    if (winnerIndex >= 0) {
      outputs.last_touch = computeSingleWinnerCredits('last_touch', journey.touchpoints, winnerIndex, order.total_price);
    }
  }

  const primaryCredit = selectPrimaryCredit(outputs.last_touch);
  if (!primaryCredit) {
    throw new Error(`Failed to compute attribution credits for Shopify order ${order.shopify_order_id}`);
  }

  await client.query('DELETE FROM attribution_order_credits WHERE shopify_order_id = $1', [order.shopify_order_id]);

  for (const model of ATTRIBUTION_MODELS) {
    for (const credit of outputs[model]) {
      await client.query(
        `
          INSERT INTO attribution_order_credits (
            shopify_order_id,
            attribution_model,
            touchpoint_position,
            session_id,
            touchpoint_occurred_at,
            attributed_source,
            attributed_medium,
            attributed_campaign,
            attributed_content,
            attributed_term,
            attributed_click_id_type,
            attributed_click_id_value,
            credit_weight,
            revenue_credit,
            is_primary,
            attribution_reason,
            model_version
          )
          VALUES (
            $1,
            $2,
            $3,
            $4::uuid,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16,
            $17
          )
        `,
        [
          order.shopify_order_id,
          credit.attributionModel,
          credit.touchpointPosition,
          credit.sessionId,
          credit.touchpointOccurredAt,
          normalizeNullableString(credit.source),
          normalizeNullableString(credit.medium),
          normalizeNullableString(credit.campaign),
          normalizeNullableString(credit.content),
          normalizeNullableString(credit.term),
          normalizeNullableString(credit.clickIdType),
          normalizeNullableString(credit.clickIdValue),
          credit.creditWeight,
          credit.revenueCredit,
          credit.isPrimary,
          credit.attributionReason,
          ATTRIBUTION_MODEL_VERSION
        ]
      );
    }
  }

  await client.query(
    `
      INSERT INTO attribution_results (
        shopify_order_id,
        session_id,
        attribution_model,
        attributed_source,
        attributed_medium,
        attributed_campaign,
        attributed_content,
        attributed_term,
        attributed_click_id_type,
        attributed_click_id_value,
        confidence_score,
        attribution_reason,
        attributed_at,
        reprocess_version,
        model_version
      )
      VALUES (
        $1,
        $2::uuid,
        'last_touch',
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        now(),
        1,
        $12
      )
      ON CONFLICT (shopify_order_id)
      DO UPDATE SET
        session_id = EXCLUDED.session_id,
        attribution_model = EXCLUDED.attribution_model,
        attributed_source = EXCLUDED.attributed_source,
        attributed_medium = EXCLUDED.attributed_medium,
        attributed_campaign = EXCLUDED.attributed_campaign,
        attributed_content = EXCLUDED.attributed_content,
        attributed_term = EXCLUDED.attributed_term,
        attributed_click_id_type = EXCLUDED.attributed_click_id_type,
        attributed_click_id_value = EXCLUDED.attributed_click_id_value,
        confidence_score = EXCLUDED.confidence_score,
        attribution_reason = EXCLUDED.attribution_reason,
        attributed_at = now(),
        model_version = EXCLUDED.model_version
    `,
    [
      order.shopify_order_id,
      primaryCredit.sessionId,
      normalizeNullableString(primaryCredit.source),
      normalizeNullableString(primaryCredit.medium),
      normalizeNullableString(primaryCredit.campaign),
      normalizeNullableString(primaryCredit.content),
      normalizeNullableString(primaryCredit.term),
      normalizeNullableString(primaryCredit.clickIdType),
      normalizeNullableString(primaryCredit.clickIdValue),
      journey.confidenceScore,
      primaryCredit.attributionReason,
      ATTRIBUTION_MODEL_VERSION
    ]
  );

  await client.query(
    `
      UPDATE shopify_orders
      SET
        attribution_snapshot = $2::jsonb,
        attribution_snapshot_updated_at = now()
      WHERE shopify_order_id = $1
    `,
    [
      order.shopify_order_id,
      JSON.stringify({
        confidenceScore: journey.confidenceScore,
        winner: journey.winner
          ? {
              sessionId: journey.winner.sessionId,
              sourceTouchEventId: journey.winner.sourceTouchEventId,
              occurredAt: journey.winner.occurredAt.toISOString(),
              source: journey.winner.source,
              medium: journey.winner.medium,
              campaign: journey.winner.campaign,
              content: journey.winner.content,
              term: journey.winner.term,
              clickIdType: journey.winner.clickIdType,
              clickIdValue: journey.winner.clickIdValue,
              attributionReason: journey.winner.attributionReason,
              ingestionSource: journey.winner.ingestionSource,
              isDirect: journey.winner.isDirect
            }
          : null,
        timeline: journey.touchpoints.map((touchpoint) => ({
          sessionId: touchpoint.sessionId,
          sourceTouchEventId: touchpoint.sourceTouchEventId,
          occurredAt: touchpoint.occurredAt.toISOString(),
          source: touchpoint.source,
          medium: touchpoint.medium,
          campaign: touchpoint.campaign,
          content: touchpoint.content,
          term: touchpoint.term,
          clickIdType: touchpoint.clickIdType,
          clickIdValue: touchpoint.clickIdValue,
          attributionReason: touchpoint.attributionReason,
          ingestionSource: touchpoint.ingestionSource,
          isDirect: touchpoint.isDirect
        }))
      })
    ]
  );
}

async function fetchScopeMetrics(client: PoolClient, options: {
  windowStart: Date;
  windowEnd: Date;
  onlyWebOrders: boolean;
}): Promise<BackfillScopeMetrics> {
  const result = await client.query<{
    total_orders_in_scope: string;
    orders_missing_attribution: string;
    orders_with_attribution: string;
  }>(
    `
      WITH scoped_orders AS (
        SELECT o.shopify_order_id
        FROM shopify_orders o
        WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1
          AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= $2
          AND ($3::boolean = false OR COALESCE(o.source_name, '') = 'web')
      )
      SELECT
        COUNT(*)::text AS total_orders_in_scope,
        COUNT(*) FILTER (WHERE ${MISSING_ATTRIBUTION_SQL})::text AS orders_missing_attribution,
        COUNT(*) FILTER (WHERE NOT (${MISSING_ATTRIBUTION_SQL}))::text AS orders_with_attribution
      FROM scoped_orders scoped
      LEFT JOIN attribution_results attribution
        ON attribution.shopify_order_id = scoped.shopify_order_id
    `,
    [options.windowStart, options.windowEnd, options.onlyWebOrders]
  );

  const row = result.rows[0];
  const totalOrdersInScope = Number(row?.total_orders_in_scope ?? '0');
  const ordersMissingAttribution = Number(row?.orders_missing_attribution ?? '0');
  const ordersWithAttribution = Number(row?.orders_with_attribution ?? '0');

  return {
    totalOrdersInScope,
    ordersMissingAttribution,
    ordersWithAttribution,
    completenessRate: totalOrdersInScope > 0 ? ordersWithAttribution / totalOrdersInScope : 1
  };
}

async function fetchBackfillCandidates(
  client: PoolClient,
  options: {
    windowStart: Date;
    windowEnd: Date;
    onlyWebOrders: boolean;
    limit: number;
  }
): Promise<BackfillCandidateRow[]> {
  const result = await client.query<BackfillCandidateRow>(
    `
      SELECT
        o.shopify_order_id,
        COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS order_occurred_at
      FROM shopify_orders o
      LEFT JOIN attribution_results attribution
        ON attribution.shopify_order_id = o.shopify_order_id
      WHERE COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) >= $1
        AND COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) <= $2
        AND ($3::boolean = false OR COALESCE(o.source_name, '') = 'web')
        AND (${MISSING_ATTRIBUTION_SQL})
      ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC, o.id DESC
      LIMIT $4
    `,
    [options.windowStart, options.windowEnd, options.onlyWebOrders, options.limit]
  );

  return result.rows;
}

function previewRowForOrder(order: OrderRow, journey: ResolvedJourney): BackfillPreviewRow {
  return {
    shopifyOrderId: order.shopify_order_id,
    orderOccurredAt: resolveOrderOccurredAt(order).toISOString(),
    recoverable: Boolean(journey.winner),
    touchpointCount: journey.touchpoints.length,
    winnerSessionId: journey.winner?.sessionId ?? null,
    attributionReason: journey.winner?.attributionReason ?? 'unrecoverable'
  };
}

export async function backfillRecentOrdersWithRecoveredAttribution(
  options: OrderAttributionBackfillOptions
): Promise<OrderAttributionBackfillReport> {
  if (!(options.windowStart instanceof Date) || Number.isNaN(options.windowStart.getTime())) {
    throw new Error('windowStart must be a valid Date');
  }

  if (!(options.windowEnd instanceof Date) || Number.isNaN(options.windowEnd.getTime())) {
    throw new Error('windowEnd must be a valid Date');
  }

  if (options.windowStart > options.windowEnd) {
    throw new Error('windowStart must be on or before windowEnd');
  }

  const limit = Math.max(1, options.limit ?? 500);
  const dryRun = options.dryRun ?? false;
  const onlyWebOrders = options.onlyWebOrders ?? true;
  const writeToShopifyWhenAvailable = options.writeToShopifyWhenAvailable ?? true;
  const applyWriteback = options.applyWriteback ?? applyShopifyOrderWriteback;
  let beforeMetrics = buildEmptyScopeMetrics();
  let afterMetrics = buildEmptyScopeMetrics();
  let scannedOrders = 0;
  let recoverableOrders = 0;
  let recoveredOrders = 0;
  let unrecoverableOrders = 0;
  let failedOrders = 0;
  let shopifyWritebackCompleted = 0;
  let shopifyWritebackSkipped = 0;
  let shopifyWritebackFailed = 0;
  const failures: OrderAttributionBackfillFailure[] = [];
  const preview: BackfillPreviewRow[] = [];

  try {
    logInfo('order_attribution_backfill_started', {
      requestedBy: options.requestedBy,
      workerId: options.workerId,
      dryRun,
      onlyWebOrders,
      limit,
      windowStart: options.windowStart.toISOString(),
      windowEnd: options.windowEnd.toISOString()
    });

    beforeMetrics = await withTransaction((client) =>
      fetchScopeMetrics(client, {
        windowStart: options.windowStart,
        windowEnd: options.windowEnd,
        onlyWebOrders
      })
    );

    const candidateRows = await withTransaction((client) =>
      fetchBackfillCandidates(client, {
        windowStart: options.windowStart,
        windowEnd: options.windowEnd,
        onlyWebOrders,
        limit
      })
    );

    scannedOrders = candidateRows.length;
    const reportingDates = new Set<string>();
    const reportingTimezone = dryRun ? null : await withTransaction((client) => getReportingTimezone(client));

    for (const candidate of candidateRows) {
      try {
        const resolved = await withTransaction(async (client) => {
          const order = await fetchOrder(client, candidate.shopify_order_id);
          if (!order) {
            return null;
          }

          const journey = await resolveAttributionJourney(client, order);
          return { order, journey };
        });

        if (!resolved) {
          failedOrders += 1;
          recordFailure(failures, {
            orderId: candidate.shopify_order_id,
            code: 'order_not_found',
            message: `Shopify order ${candidate.shopify_order_id} was not found during backfill processing`
          });
          continue;
        }

        if (preview.length < MAX_PREVIEW_ORDERS) {
          preview.push(previewRowForOrder(resolved.order, resolved.journey));
        }

        if (!resolved.journey.winner) {
          unrecoverableOrders += 1;
          continue;
        }

        recoverableOrders += 1;

        if (dryRun) {
          continue;
        }

        await withTransaction(async (client) => {
          await persistAttribution(client, resolved.order, resolved.journey);

          if (reportingTimezone) {
            reportingDates.add(formatDateInTimezone(resolveOrderOccurredAt(resolved.order), reportingTimezone));
          }
        });

        recoveredOrders += 1;

        if (writeToShopifyWhenAvailable) {
          try {
            const writeback = await applyWriteback({
              workerId: options.workerId,
              shopifyOrderId: resolved.order.shopify_order_id,
              requestedReason: 'recent_order_attribution_backfill'
            });

            if (writeback.status === 'completed') {
              shopifyWritebackCompleted += 1;
            } else {
              shopifyWritebackSkipped += 1;
            }
          } catch (error) {
            shopifyWritebackFailed += 1;
            recordFailure(failures, {
              orderId: resolved.order.shopify_order_id,
              code: normalizeFailureCode(error, 'shopify_writeback_failed'),
              message: normalizeFailureMessage(
                error,
                `Shopify writeback failed for Shopify order ${resolved.order.shopify_order_id}`
              )
            });
            logError('order_attribution_backfill_shopify_writeback_failed', error, {
              requestedBy: options.requestedBy,
              workerId: options.workerId,
              shopifyOrderId: resolved.order.shopify_order_id
            });
          }
        }
      } catch (error) {
        failedOrders += 1;
        recordFailure(failures, {
          orderId: candidate.shopify_order_id,
          code: normalizeFailureCode(error, 'order_attribution_backfill_failed'),
          message: normalizeFailureMessage(error, `Failed to backfill Shopify order ${candidate.shopify_order_id}`)
        });
        logError('order_attribution_backfill_order_failed', error, {
          requestedBy: options.requestedBy,
          workerId: options.workerId,
          shopifyOrderId: candidate.shopify_order_id
        });
      }
    }

    if (!dryRun && reportingDates.size > 0) {
      await withTransaction((client) => refreshDailyReportingMetrics(client, [...reportingDates]));
    }

    afterMetrics = dryRun
      ? beforeMetrics
      : await withTransaction((client) =>
          fetchScopeMetrics(client, {
            windowStart: options.windowStart,
            windowEnd: options.windowEnd,
            onlyWebOrders
          })
        );

    const report = buildOrderAttributionBackfillReport({
      requestedBy: options.requestedBy,
      workerId: options.workerId,
      dryRun,
      windowStart: options.windowStart,
      windowEnd: options.windowEnd,
      onlyWebOrders,
      limit,
      beforeMetrics,
      afterMetrics,
      scannedOrders,
      recoverableOrders,
      recoveredOrders,
      unrecoverableOrders,
      failedOrders,
      shopifyWritebackCompleted,
      shopifyWritebackSkipped,
      shopifyWritebackFailed,
      failures,
      preview
    });

    logInfo(dryRun ? 'order_attribution_backfill_dry_run_completed' : 'order_attribution_backfill_completed', report);
    return report;
  } catch (error) {
    const partialReport = buildOrderAttributionBackfillReport({
      requestedBy: options.requestedBy,
      workerId: options.workerId,
      dryRun,
      windowStart: options.windowStart,
      windowEnd: options.windowEnd,
      onlyWebOrders,
      limit,
      beforeMetrics,
      afterMetrics,
      scannedOrders,
      recoverableOrders,
      recoveredOrders,
      unrecoverableOrders,
      failedOrders,
      shopifyWritebackCompleted,
      shopifyWritebackSkipped,
      shopifyWritebackFailed,
      failures,
      preview
    });

    logError('order_attribution_backfill_run_failed', error, {
      requestedBy: options.requestedBy,
      workerId: options.workerId,
      scannedOrders,
      recoveredOrders,
      unrecoverableOrders,
      writebackCompleted: shopifyWritebackCompleted,
      failedOrders
    });

    throw new OrderAttributionBackfillRunError(normalizeFailureMessage(error, 'Order attribution backfill job failed'), {
      code: normalizeFailureCode(error, 'order_attribution_backfill_run_failed'),
      report: toOrderAttributionBackfillJobReport(partialReport),
      cause: error
    });
  }
}

export function toOrderAttributionBackfillJobReport(report: OrderAttributionBackfillReport): OrderAttributionBackfillJobReport {
  return {
    scanned: report.scannedOrders,
    recovered: report.recoveredOrders,
    unrecoverable: report.unrecoverableOrders,
    writebackCompleted: report.shopifyWritebackCompleted,
    failures: report.failures
  };
}
