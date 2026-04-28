import type { PoolClient, QueryResult, QueryResultRow } from 'pg';

import type {
  AttributionConfidenceLabel,
  AttributionMatchSource,
  ResolvedAttributionTouchpoint,
  ResolvedJourney
} from './resolver.js';

export const GA4_FALLBACK_ROLLOUT_MODES = ['off', 'shadow', 'on'] as const;
export type Ga4FallbackRolloutMode = (typeof GA4_FALLBACK_ROLLOUT_MODES)[number];

type ShadowComparisonInput = {
  shopifyOrderId: string;
  orderOccurredAt: Date;
  orderRevenue: string | number;
  rolloutMode: Ga4FallbackRolloutMode;
  currentJourney: ResolvedJourney;
  shadowJourney: ResolvedJourney;
};

type ShadowReportFilters = {
  startDate: string;
  endDate: string;
};

type QueryExecutor =
  | PoolClient
  | {
      query: <T extends QueryResultRow = QueryResultRow>(
        text: string,
        params?: unknown[]
      ) => Promise<QueryResult<T>>;
    };

export type Ga4FallbackShadowReport = {
  range: ShadowReportFilters;
  rolloutMode: Ga4FallbackRolloutMode;
  summary: {
    evaluatedOrders: number;
    shadowGa4FallbackOrders: number;
    changedOrders: number;
    currentAttributedOrders: number;
    shadowAttributedOrders: number;
    attributedOrderDelta: number;
    currentAttributedRevenue: number;
    shadowAttributedRevenue: number;
    attributedRevenueDelta: number;
  };
  productionEnablement: {
    requiresExplicitApproval: true;
    meetsAcceptanceThresholds: boolean;
    approvalStatus: 'blocked' | 'pending_explicit_approval';
    thresholds: {
      minEvaluatedOrders: number;
      maxAttributedOrderDeltaRate: number;
      maxAttributedRevenueDeltaRate: number;
    };
  };
};

type ShadowReportRow = {
  evaluated_orders: string;
  shadow_ga4_fallback_orders: string;
  changed_orders: string;
  current_attributed_orders: string;
  shadow_attributed_orders: string;
  current_attributed_revenue: string;
  shadow_attributed_revenue: string;
};

function normalizeMode(value: string | undefined): Ga4FallbackRolloutMode {
  const normalized = value?.trim().toLowerCase();
  if (normalized === 'shadow' || normalized === 'on') {
    return normalized;
  }
  return 'off';
}

function toNumeric(value: string | number): number {
  const numeric = typeof value === 'number' ? value : Number.parseFloat(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function roundMetric(value: number): number {
  return Math.round(value * 100) / 100;
}

function confidenceScoreFromJourneyWinner(
  matchSource: AttributionMatchSource,
  clickIdValue: string | null
): number {
  switch (matchSource) {
    case 'landing_session_id':
    case 'checkout_token':
      return 1;
    case 'cart_token':
      return 0.9;
    case 'customer_identity':
      return 0.6;
    case 'shopify_hint_fallback':
      return clickIdValue ? 0.55 : 0.4;
    case 'ga4_fallback':
      return clickIdValue ? 0.35 : 0.25;
    case 'unattributed':
      return 0;
  }
}

function summarizeTouchpoint(touchpoint: ResolvedAttributionTouchpoint | null) {
  return {
    matchSource: touchpoint?.matchSource ?? 'unattributed',
    confidenceScore: touchpoint
      ? confidenceScoreFromJourneyWinner(touchpoint.matchSource, touchpoint.clickIdValue)
      : 0,
    confidenceLabel: touchpoint?.confidenceLabel ?? 'none',
    attributionReason: touchpoint?.attributionReason ?? 'unattributed',
    source: touchpoint?.source ?? null,
    medium: touchpoint?.medium ?? null,
    campaign: touchpoint?.campaign ?? null,
    ga4ClientId: touchpoint?.ga4ClientId ?? null,
    ga4SessionId: touchpoint?.ga4SessionId ?? null
  };
}

export function getGa4FallbackRolloutMode(): Ga4FallbackRolloutMode {
  return normalizeMode(process.env.GA4_FALLBACK_ROLLOUT_MODE);
}

export function getGa4FallbackShadowThresholds() {
  const minEvaluatedOrders =
    Math.max(
      1,
      Math.trunc(Number(process.env.GA4_FALLBACK_SHADOW_MIN_EVALUATED_ORDERS ?? '100'))
    ) || 100;
  const maxAttributedOrderDeltaRate =
    Number(process.env.GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_ORDER_DELTA_RATE ?? '0.02') || 0.02;
  const maxAttributedRevenueDeltaRate =
    Number(process.env.GA4_FALLBACK_SHADOW_MAX_ATTRIBUTED_REVENUE_DELTA_RATE ?? '0.02') || 0.02;

  return {
    minEvaluatedOrders,
    maxAttributedOrderDeltaRate,
    maxAttributedRevenueDeltaRate
  };
}

export async function persistGa4FallbackShadowComparison(
  client: PoolClient,
  input: ShadowComparisonInput
): Promise<void> {
  if (input.rolloutMode !== 'shadow') {
    return;
  }

  const currentWinner = summarizeTouchpoint(input.currentJourney.winner);
  const shadowWinner = summarizeTouchpoint(input.shadowJourney.winner);
  const shadowWouldChangeWinner = currentWinner.matchSource !== shadowWinner.matchSource;

  await client.query(
    `
      INSERT INTO ga4_fallback_shadow_comparisons (
        shopify_order_id,
        order_occurred_at,
        order_revenue,
        rollout_mode,
        current_match_source,
        current_confidence_score,
        current_confidence_label,
        current_attribution_reason,
        current_source,
        current_medium,
        current_campaign,
        shadow_match_source,
        shadow_confidence_score,
        shadow_confidence_label,
        shadow_attribution_reason,
        shadow_source,
        shadow_medium,
        shadow_campaign,
        shadow_ga4_client_id,
        shadow_ga4_session_id,
        shadow_would_change_winner,
        evaluated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, now()
      )
      ON CONFLICT (shopify_order_id)
      DO UPDATE SET
        order_occurred_at = EXCLUDED.order_occurred_at,
        order_revenue = EXCLUDED.order_revenue,
        rollout_mode = EXCLUDED.rollout_mode,
        current_match_source = EXCLUDED.current_match_source,
        current_confidence_score = EXCLUDED.current_confidence_score,
        current_confidence_label = EXCLUDED.current_confidence_label,
        current_attribution_reason = EXCLUDED.current_attribution_reason,
        current_source = EXCLUDED.current_source,
        current_medium = EXCLUDED.current_medium,
        current_campaign = EXCLUDED.current_campaign,
        shadow_match_source = EXCLUDED.shadow_match_source,
        shadow_confidence_score = EXCLUDED.shadow_confidence_score,
        shadow_confidence_label = EXCLUDED.shadow_confidence_label,
        shadow_attribution_reason = EXCLUDED.shadow_attribution_reason,
        shadow_source = EXCLUDED.shadow_source,
        shadow_medium = EXCLUDED.shadow_medium,
        shadow_campaign = EXCLUDED.shadow_campaign,
        shadow_ga4_client_id = EXCLUDED.shadow_ga4_client_id,
        shadow_ga4_session_id = EXCLUDED.shadow_ga4_session_id,
        shadow_would_change_winner = EXCLUDED.shadow_would_change_winner,
        evaluated_at = now()
    `,
    [
      input.shopifyOrderId,
      input.orderOccurredAt,
      toNumeric(input.orderRevenue).toFixed(2),
      input.rolloutMode,
      currentWinner.matchSource,
      input.currentJourney.confidenceScore,
      input.currentJourney.confidenceLabel,
      currentWinner.attributionReason,
      currentWinner.source,
      currentWinner.medium,
      currentWinner.campaign,
      shadowWinner.matchSource,
      input.shadowJourney.confidenceScore,
      input.shadowJourney.confidenceLabel,
      shadowWinner.attributionReason,
      shadowWinner.source,
      shadowWinner.medium,
      shadowWinner.campaign,
      shadowWinner.ga4ClientId,
      shadowWinner.ga4SessionId,
      shadowWouldChangeWinner
    ]
  );
}

export async function fetchGa4FallbackShadowReport(
  client: QueryExecutor,
  filters: ShadowReportFilters
): Promise<Ga4FallbackShadowReport> {
  const result = await client.query<ShadowReportRow>(
    `
      SELECT
        COUNT(*)::text AS evaluated_orders,
        COUNT(*) FILTER (WHERE shadow_match_source = 'ga4_fallback')::text AS shadow_ga4_fallback_orders,
        COUNT(*) FILTER (WHERE shadow_would_change_winner)::text AS changed_orders,
        COUNT(*) FILTER (WHERE current_match_source <> 'unattributed')::text AS current_attributed_orders,
        COUNT(*) FILTER (WHERE shadow_match_source <> 'unattributed')::text AS shadow_attributed_orders,
        COALESCE(SUM(order_revenue) FILTER (WHERE current_match_source <> 'unattributed'), 0)::text AS current_attributed_revenue,
        COALESCE(SUM(order_revenue) FILTER (WHERE shadow_match_source <> 'unattributed'), 0)::text AS shadow_attributed_revenue
      FROM ga4_fallback_shadow_comparisons
      WHERE order_occurred_at >= $1::date
        AND order_occurred_at < ($2::date + interval '1 day')
    `,
    [filters.startDate, filters.endDate]
  );

  const row = result.rows[0];
  const evaluatedOrders = Number.parseInt(row?.evaluated_orders ?? '0', 10) || 0;
  const shadowGa4FallbackOrders = Number.parseInt(row?.shadow_ga4_fallback_orders ?? '0', 10) || 0;
  const changedOrders = Number.parseInt(row?.changed_orders ?? '0', 10) || 0;
  const currentAttributedOrders = Number.parseInt(row?.current_attributed_orders ?? '0', 10) || 0;
  const shadowAttributedOrders = Number.parseInt(row?.shadow_attributed_orders ?? '0', 10) || 0;
  const currentAttributedRevenue = roundMetric(Number.parseFloat(row?.current_attributed_revenue ?? '0') || 0);
  const shadowAttributedRevenue = roundMetric(Number.parseFloat(row?.shadow_attributed_revenue ?? '0') || 0);
  const attributedOrderDelta = shadowAttributedOrders - currentAttributedOrders;
  const attributedRevenueDelta = roundMetric(shadowAttributedRevenue - currentAttributedRevenue);
  const thresholds = getGa4FallbackShadowThresholds();

  const meetsAcceptanceThresholds =
    evaluatedOrders >= thresholds.minEvaluatedOrders &&
    Math.abs(attributedOrderDelta) / Math.max(currentAttributedOrders, 1) <= thresholds.maxAttributedOrderDeltaRate &&
    Math.abs(attributedRevenueDelta) / Math.max(currentAttributedRevenue, 0.01) <= thresholds.maxAttributedRevenueDeltaRate;

  return {
    range: filters,
    rolloutMode: getGa4FallbackRolloutMode(),
    summary: {
      evaluatedOrders,
      shadowGa4FallbackOrders,
      changedOrders,
      currentAttributedOrders,
      shadowAttributedOrders,
      attributedOrderDelta,
      currentAttributedRevenue,
      shadowAttributedRevenue,
      attributedRevenueDelta
    },
    productionEnablement: {
      requiresExplicitApproval: true,
      meetsAcceptanceThresholds,
      approvalStatus: meetsAcceptanceThresholds ? 'pending_explicit_approval' : 'blocked',
      thresholds
    }
  };
}

export function buildJourney(
  touchpoints: ResolvedAttributionTouchpoint[],
  winner: ResolvedAttributionTouchpoint | null,
  confidenceScore: number,
  confidenceLabel: AttributionConfidenceLabel
): ResolvedJourney {
  return {
    touchpoints,
    winner,
    confidenceScore,
    confidenceLabel
  };
}
