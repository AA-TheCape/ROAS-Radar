import { Router } from 'express';
import { z } from 'zod';
import { query } from '../../db/pool.js';
import { calculatePerformanceMetrics } from '../../shared/metrics.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';
import { attachAuthContext, requireAuthenticated } from '../auth/index.js';
import { fetchDataQualityReport, resolveRunDate } from '../data-quality/index.js';
import { getReportingTimezone } from '../settings/index.js';
class ReportingHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'ReportingHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const dateStringSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const attributionTierSchema = z.enum([
    'deterministic_first_party',
    'deterministic_shopify_hint',
    'ga4_fallback',
    'unattributed'
]);
const ATTRIBUTION_TIER_LABELS = {
    deterministic_first_party: 'Deterministic first-party',
    deterministic_shopify_hint: 'Deterministic Shopify hint',
    ga4_fallback: 'GA4 fallback',
    unattributed: 'Unattributed'
};
const ATTRIBUTION_TIER_DESCRIPTIONS = {
    deterministic_first_party: 'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
    deterministic_shopify_hint: 'Recovered synthetically from Shopify marketing hints after first-party resolution failed.',
    ga4_fallback: 'Recovered from the GA4 fallback contract only after first-party and Shopify-hint matches were unavailable.',
    unattributed: 'No eligible first-party, Shopify hint, or GA4 fallback match qualified, or the required timing data could not be normalized.'
};
const baseFiltersObjectSchema = z.object({
    startDate: dateStringSchema,
    endDate: dateStringSchema,
    attributionModel: z.enum(ATTRIBUTION_MODELS).optional().default('last_touch'),
    attributionTier: attributionTierSchema.optional(),
    source: z.string().trim().min(1).optional(),
    campaign: z.string().trim().min(1).optional()
});
function withValidDateRange(schema) {
    return schema.superRefine((value, ctx) => {
        if (value.startDate > value.endDate) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'startDate must be on or before endDate',
                path: ['startDate']
            });
        }
    });
}
const baseFiltersSchema = withValidDateRange(baseFiltersObjectSchema);
const campaignsQuerySchema = withValidDateRange(baseFiltersObjectSchema.extend({
    limit: z.coerce.number().int().positive().max(200).optional().default(50)
}));
const timeseriesQuerySchema = withValidDateRange(baseFiltersObjectSchema.extend({
    groupBy: z.enum(['day', 'source', 'campaign']).optional().default('day')
}));
const ordersQuerySchema = withValidDateRange(baseFiltersObjectSchema.extend({
    limit: z.coerce.number().int().positive().max(200).optional().default(50)
}));
const orderDetailsParamsSchema = z.object({
    shopifyOrderId: z.string().trim().min(1)
});
const reconciliationQuerySchema = z.object({
    runDate: dateStringSchema.optional()
});
function parseInput(schema, input) {
    try {
        return schema.parse(input);
    }
    catch (error) {
        if (error instanceof z.ZodError) {
            throw new ReportingHttpError(400, 'invalid_request', 'Invalid reporting query parameters', error.flatten());
        }
        throw error;
    }
}
function buildMetricDimensionFilters(attributionModel, source, campaign, alias = '') {
    const params = [attributionModel];
    const qualifiedAlias = alias ? `${alias}.` : '';
    const filters = [`${qualifiedAlias}attribution_model = $3`];
    if (source) {
        params.push(source);
        filters.push(`${qualifiedAlias}source = $${params.length + 2}`);
    }
    if (campaign) {
        params.push(campaign);
        filters.push(`${qualifiedAlias}campaign = $${params.length + 2}`);
    }
    return {
        sql: filters.length > 0 ? ` AND ${filters.join(' AND ')}` : '',
        params
    };
}
function buildOrderAttributionFilters(attributionModel, source, campaign, attributionTier) {
    const params = [attributionModel];
    const filters = [];
    if (source) {
        params.push(source);
        filters.push(`c.attributed_source = $${params.length + 2}`);
    }
    if (campaign) {
        params.push(campaign);
        filters.push(`c.attributed_campaign = $${params.length + 2}`);
    }
    if (attributionTier) {
        params.push(attributionTier);
        filters.push(`COALESCE(o.attribution_tier, 'unattributed') = $${params.length + 2}`);
    }
    return {
        sql: filters.length > 0 ? ` AND ${filters.join(' AND ')}` : '',
        params
    };
}
function normalizeContent(value) {
    if (typeof value !== 'string') {
        return null;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
}
function asObjectRecord(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
}
function readNullableString(value) {
    return typeof value === 'string' && value.trim() ? value : null;
}
function readNullableNumber(value) {
    return typeof value === 'number' && Number.isFinite(value) ? value : null;
}
function extractOrderAttributionMetadata(snapshot) {
    const snapshotRecord = asObjectRecord(snapshot);
    const winnerRecord = asObjectRecord(snapshotRecord?.winner);
    return {
        confidenceScore: readNullableNumber(snapshotRecord?.confidenceScore),
        winner: {
            sessionId: readNullableString(winnerRecord?.sessionId),
            source: readNullableString(winnerRecord?.source),
            medium: readNullableString(winnerRecord?.medium),
            campaign: readNullableString(winnerRecord?.campaign),
            content: readNullableString(winnerRecord?.content),
            term: readNullableString(winnerRecord?.term),
            clickIdType: readNullableString(winnerRecord?.clickIdType),
            clickIdValue: readNullableString(winnerRecord?.clickIdValue)
        }
    };
}
function normalizeAttributionTier(value) {
    return attributionTierSchema.safeParse(value).success ? value : 'unattributed';
}
function countDaysInRange(startDate, endDate) {
    const start = Date.parse(`${startDate}T00:00:00.000Z`);
    const end = Date.parse(`${endDate}T00:00:00.000Z`);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
        return 0;
    }
    return Math.floor((end - start) / 86_400_000) + 1;
}
export function createReportingRouter() {
    const router = Router();
    router.use(attachAuthContext);
    router.use(requireAuthenticated);
    router.get('/summary', async (req, res, next) => {
        try {
            const input = parseInput(baseFiltersSchema, req.query);
            const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
            const result = await query(`
          SELECT
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
        `, [input.startDate, input.endDate, ...filters.params]);
            const row = result.rows[0];
            const metrics = calculatePerformanceMetrics({
                visits: row?.visits ?? 0,
                orders: row?.orders ?? 0,
                attributedRevenue: row?.revenue ?? 0,
                spend: row?.spend ?? 0
            });
            res.json({
                range: {
                    startDate: input.startDate,
                    endDate: input.endDate
                },
                totals: {
                    visits: metrics.visits,
                    orders: metrics.orders,
                    revenue: metrics.attributedRevenue,
                    spend: metrics.spend,
                    conversionRate: metrics.conversionRate,
                    roas: metrics.roas
                }
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/campaigns', async (req, res, next) => {
        try {
            const input = parseInput(campaignsQuerySchema, req.query);
            const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
            const result = await query(`
          SELECT
            source,
            medium,
            campaign,
            NULLIF(content, '') AS content,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
          GROUP BY source, medium, campaign, content
          ORDER BY revenue DESC, orders DESC, visits DESC, source ASC, campaign ASC
          LIMIT $${filters.params.length + 3}
        `, [input.startDate, input.endDate, ...filters.params, input.limit]);
            res.json({
                rows: result.rows.map((row) => {
                    const visits = Number(row.visits);
                    const orders = Number(row.orders);
                    const revenue = Number(row.revenue);
                    return {
                        source: row.source,
                        medium: row.medium,
                        campaign: row.campaign,
                        content: normalizeContent(row.content),
                        visits,
                        orders,
                        revenue,
                        conversionRate: visits > 0 ? orders / visits : 0
                    };
                }),
                nextCursor: null
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/spend-details', async (req, res, next) => {
        try {
            const input = parseInput(baseFiltersSchema, req.query);
            const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
            const result = await query(`
          SELECT
            source,
            medium,
            campaign,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
            AND spend > 0
            ${filters.sql}
          GROUP BY source, medium, campaign
          ORDER BY spend DESC, source ASC, medium ASC, campaign ASC
        `, [input.startDate, input.endDate, ...filters.params]);
            const groupMap = new Map();
            for (const row of result.rows) {
                const spend = Number(row.spend);
                const source = row.source;
                const medium = row.medium;
                const channel = `${source} / ${medium}`;
                const groupKey = `${source}\u0000${medium}`;
                const existingGroup = groupMap.get(groupKey);
                if (existingGroup) {
                    existingGroup.subtotal += spend;
                    existingGroup.campaigns.push({
                        campaign: row.campaign,
                        spend
                    });
                    continue;
                }
                groupMap.set(groupKey, {
                    source,
                    medium,
                    channel,
                    subtotal: spend,
                    campaigns: [
                        {
                            campaign: row.campaign,
                            spend
                        }
                    ]
                });
            }
            const groups = [...groupMap.values()].sort((left, right) => right.subtotal - left.subtotal || left.channel.localeCompare(right.channel));
            const totalSpend = groups.reduce((sum, group) => sum + group.subtotal, 0);
            const rangeDays = countDaysInRange(input.startDate, input.endDate);
            const topChannel = groups[0]
                ? {
                    source: groups[0].source,
                    medium: groups[0].medium,
                    channel: groups[0].channel,
                    spend: groups[0].subtotal
                }
                : null;
            res.json({
                summary: {
                    totalSpend,
                    activeChannels: groups.length,
                    activeCampaigns: result.rows.length,
                    averageDailySpend: rangeDays > 0 ? totalSpend / rangeDays : 0,
                    topChannel
                },
                groups,
                totalSpend
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/timeseries', async (req, res, next) => {
        try {
            const input = parseInput(timeseriesQuerySchema, req.query);
            const filters = buildMetricDimensionFilters(input.attributionModel, input.source, input.campaign);
            const groupExpr = input.groupBy === 'source' ? 'source' : input.groupBy === 'campaign' ? 'campaign' : 'metric_date::text';
            const result = await query(`
          SELECT
            ${groupExpr} AS bucket,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
          GROUP BY bucket
          ORDER BY bucket ASC
        `, [input.startDate, input.endDate, ...filters.params]);
            const bucketMetrics = result.rows.map((row) => {
                const metrics = calculatePerformanceMetrics({
                    visits: row.visits,
                    orders: row.orders,
                    attributedRevenue: row.revenue,
                    spend: row.spend
                });
                return {
                    bucket: row.bucket,
                    visits: metrics.visits,
                    orders: metrics.orders,
                    revenue: metrics.attributedRevenue,
                    spend: metrics.spend,
                    conversionRate: metrics.conversionRate,
                    roas: metrics.roas
                };
            });
            res.json({
                points: bucketMetrics.map((row) => ({
                    date: row.bucket,
                    visits: row.visits,
                    orders: row.orders,
                    revenue: row.revenue
                })),
                lowestBuckets: [...bucketMetrics]
                    .sort((left, right) => left.revenue - right.revenue ||
                    left.orders - right.orders ||
                    left.visits - right.visits ||
                    left.bucket.localeCompare(right.bucket))
                    .slice(0, 3)
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/orders', async (req, res, next) => {
        try {
            const input = parseInput(ordersQuerySchema, req.query);
            const filters = buildOrderAttributionFilters(input.attributionModel, input.source, input.campaign, input.attributionTier);
            const reportingTimezone = await getReportingTimezone();
            const result = await query(`
          SELECT
            o.shopify_order_id,
            COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS processed_at,
            o.total_price,
            o.attribution_tier,
            o.attribution_source,
            o.attribution_reason AS order_attribution_reason,
            o.attribution_matched_at,
            o.attribution_snapshot,
            c.attributed_source,
            c.attributed_medium,
            c.attributed_campaign,
            c.attribution_reason AS primary_credit_attribution_reason
          FROM shopify_orders o
          LEFT JOIN LATERAL (
            SELECT
              attributed_source,
              attributed_medium,
              attributed_campaign,
              attribution_reason
            FROM attribution_order_credits
            WHERE shopify_order_id = o.shopify_order_id
              AND attribution_model = $3
            ORDER BY is_primary DESC, touchpoint_position ASC
            LIMIT 1
          ) c
            ON TRUE
          WHERE COALESCE(o.source_name, '') = 'web'
            AND timezone($${filters.params.length + 3}::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) >= $1::date
            AND timezone($${filters.params.length + 3}::text, COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at)) < ($2::date + interval '1 day')
            ${filters.sql}
          ORDER BY COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) DESC, o.shopify_order_id DESC
          LIMIT $${filters.params.length + 4}
        `, [input.startDate, input.endDate, ...filters.params, reportingTimezone, input.limit]);
            res.json({
                rows: result.rows.map((row) => {
                    const metadata = extractOrderAttributionMetadata(row.attribution_snapshot);
                    const attributionTier = normalizeAttributionTier(row.attribution_tier);
                    const orderAttributionReason = row.order_attribution_reason ?? 'unattributed';
                    return {
                        shopifyOrderId: row.shopify_order_id,
                        processedAt: row.processed_at?.toISOString() ?? null,
                        orderOccurredAtUtc: row.processed_at?.toISOString() ?? null,
                        totalPrice: Number(row.total_price),
                        source: row.attributed_source ?? metadata.winner.source,
                        medium: row.attributed_medium ?? metadata.winner.medium,
                        campaign: row.attributed_campaign ?? metadata.winner.campaign,
                        attributionReason: orderAttributionReason,
                        primaryCreditAttributionReason: row.primary_credit_attribution_reason ?? orderAttributionReason,
                        attributionTier,
                        attributionTierLabel: ATTRIBUTION_TIER_LABELS[attributionTier],
                        attributionTierDescription: ATTRIBUTION_TIER_DESCRIPTIONS[attributionTier],
                        attributionSource: row.attribution_source,
                        attributionMatchedAt: row.attribution_matched_at?.toISOString() ?? null,
                        confidenceScore: metadata.confidenceScore,
                        sessionId: metadata.winner.sessionId
                    };
                })
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/orders/:shopifyOrderId', async (req, res, next) => {
        try {
            const { shopifyOrderId } = parseInput(orderDetailsParamsSchema, req.params);
            const orderResult = await query(`
          SELECT
            o.shopify_order_id,
            o.shopify_order_number,
            o.shopify_customer_id,
            o.customer_identity_id::text AS customer_identity_id,
            o.email_hash,
            o.currency_code,
            o.subtotal_price,
            o.total_price,
            o.financial_status,
            o.fulfillment_status,
            o.processed_at,
            o.created_at_shopify,
            o.updated_at_shopify,
            o.landing_session_id::text AS landing_session_id,
            o.checkout_token,
            o.cart_token,
            o.source_name,
            o.attribution_tier,
            o.attribution_source,
            o.attribution_matched_at,
            o.attribution_reason,
            o.attribution_snapshot,
            o.attribution_snapshot_updated_at,
            o.ingested_at,
            o.raw_payload
          FROM shopify_orders o
          WHERE o.shopify_order_id = $1
          LIMIT 1
        `, [shopifyOrderId]);
            if (!orderResult.rowCount) {
                throw new ReportingHttpError(404, 'order_not_found', `Shopify order ${shopifyOrderId} was not found`);
            }
            const lineItemsResult = await query(`
          SELECT
            li.shopify_line_item_id,
            li.shopify_product_id,
            li.shopify_variant_id,
            li.sku,
            li.title,
            li.variant_title,
            li.vendor,
            li.quantity,
            li.price,
            li.total_discount,
            li.fulfillment_status,
            li.requires_shipping,
            li.taxable,
            li.ingested_at,
            li.raw_payload
          FROM shopify_order_line_items li
          WHERE li.shopify_order_id = $1
          ORDER BY li.id ASC
        `, [shopifyOrderId]);
            const creditsResult = await query(`
          SELECT
            c.attribution_model,
            c.touchpoint_position,
            c.session_id::text AS session_id,
            c.touchpoint_occurred_at,
            c.attributed_source,
            c.attributed_medium,
            c.attributed_campaign,
            c.attributed_content,
            c.attributed_term,
            c.attributed_click_id_type,
            c.attributed_click_id_value,
            c.credit_weight,
            c.revenue_credit,
            c.is_primary,
            c.attribution_reason,
            c.created_at,
            c.model_version
          FROM attribution_order_credits c
          WHERE c.shopify_order_id = $1
          ORDER BY c.attribution_model ASC, c.touchpoint_position ASC
        `, [shopifyOrderId]);
            const order = orderResult.rows[0];
            const metadata = extractOrderAttributionMetadata(order.attribution_snapshot);
            res.json({
                order: {
                    shopifyOrderId: order.shopify_order_id,
                    shopifyOrderNumber: order.shopify_order_number,
                    shopifyCustomerId: order.shopify_customer_id,
                    customerIdentityId: order.customer_identity_id,
                    emailHash: order.email_hash,
                    currencyCode: order.currency_code,
                    subtotalPrice: Number(order.subtotal_price),
                    totalPrice: Number(order.total_price),
                    financialStatus: order.financial_status,
                    fulfillmentStatus: order.fulfillment_status,
                    processedAt: order.processed_at?.toISOString() ?? null,
                    createdAtShopify: order.created_at_shopify?.toISOString() ?? null,
                    updatedAtShopify: order.updated_at_shopify?.toISOString() ?? null,
                    landingSessionId: order.landing_session_id,
                    checkoutToken: order.checkout_token,
                    cartToken: order.cart_token,
                    sourceName: order.source_name,
                    orderOccurredAtUtc: order.processed_at?.toISOString() ??
                        order.created_at_shopify?.toISOString() ??
                        order.ingested_at.toISOString(),
                    attributionTier: normalizeAttributionTier(order.attribution_tier),
                    attributionTierLabel: ATTRIBUTION_TIER_LABELS[normalizeAttributionTier(order.attribution_tier)],
                    attributionTierDescription: ATTRIBUTION_TIER_DESCRIPTIONS[normalizeAttributionTier(order.attribution_tier)],
                    attributionSource: order.attribution_source,
                    attributionMatchedAt: order.attribution_matched_at?.toISOString() ?? null,
                    attributionReason: order.attribution_reason ?? 'unattributed',
                    confidenceScore: metadata.confidenceScore,
                    sessionId: metadata.winner.sessionId,
                    attributedSource: metadata.winner.source,
                    attributedMedium: metadata.winner.medium,
                    attributedCampaign: metadata.winner.campaign,
                    attributedContent: metadata.winner.content,
                    attributedTerm: metadata.winner.term,
                    attributedClickIdType: metadata.winner.clickIdType,
                    attributedClickIdValue: metadata.winner.clickIdValue,
                    attributionSnapshot: order.attribution_snapshot,
                    attributionSnapshotUpdatedAt: order.attribution_snapshot_updated_at?.toISOString() ?? null,
                    ingestedAt: order.ingested_at.toISOString(),
                    rawPayload: order.raw_payload
                },
                lineItems: lineItemsResult.rows.map((row) => ({
                    shopifyLineItemId: row.shopify_line_item_id,
                    shopifyProductId: row.shopify_product_id,
                    shopifyVariantId: row.shopify_variant_id,
                    sku: row.sku,
                    title: row.title,
                    variantTitle: row.variant_title,
                    vendor: row.vendor,
                    quantity: row.quantity,
                    price: Number(row.price),
                    totalDiscount: Number(row.total_discount),
                    fulfillmentStatus: row.fulfillment_status,
                    requiresShipping: row.requires_shipping,
                    taxable: row.taxable,
                    ingestedAt: row.ingested_at.toISOString(),
                    rawPayload: row.raw_payload
                })),
                attributionCredits: creditsResult.rows.map((row) => ({
                    attributionModel: row.attribution_model,
                    touchpointPosition: row.touchpoint_position,
                    sessionId: row.session_id,
                    touchpointOccurredAt: row.touchpoint_occurred_at?.toISOString() ?? null,
                    source: row.attributed_source,
                    medium: row.attributed_medium,
                    campaign: row.attributed_campaign,
                    content: row.attributed_content,
                    term: row.attributed_term,
                    clickIdType: row.attributed_click_id_type,
                    clickIdValue: row.attributed_click_id_value,
                    creditWeight: Number(row.credit_weight),
                    revenueCredit: Number(row.revenue_credit),
                    isPrimary: row.is_primary,
                    attributionReason: row.attribution_reason,
                    createdAt: row.created_at.toISOString(),
                    modelVersion: row.model_version
                }))
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/reconciliation', async (req, res, next) => {
        try {
            const input = parseInput(reconciliationQuerySchema, req.query);
            const runDate = input.runDate ?? resolveRunDate();
            const report = await fetchDataQualityReport(runDate);
            res.json({
                version: '2026-04-11',
                tenantId: 'roas-radar',
                data: report
            });
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
