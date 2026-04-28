"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createInternalIdentityRouter = createInternalIdentityRouter;
const express_1 = require("express");
const zod_1 = require("zod");
const pool_js_1 = require("../../db/pool.js");
const privacy_js_1 = require("../../shared/privacy.js");
const index_js_1 = require("../auth/index.js");
class IdentityReadHttpError extends Error {
    statusCode;
    code;
    details;
    constructor(statusCode, code, message, details) {
        super(message);
        this.name = 'IdentityReadHttpError';
        this.statusCode = statusCode;
        this.code = code;
        this.details = details;
    }
}
const identityNodeTypes = [
    'session_id',
    'checkout_token',
    'cart_token',
    'shopify_customer_id',
    'hashed_email',
    'phone_hash'
];
const lookupQuerySchema = zod_1.z
    .object({
    nodeType: zod_1.z.enum(identityNodeTypes),
    nodeKey: zod_1.z.string().trim().min(1)
})
    .superRefine((value, ctx) => {
    if ((value.nodeType === 'hashed_email' || value.nodeType === 'phone_hash') && !(0, privacy_js_1.isSha256Hex)(value.nodeKey)) {
        ctx.addIssue({
            code: zod_1.z.ZodIssueCode.custom,
            path: ['nodeKey'],
            message: `${value.nodeType} lookups require a sha256 hex digest`
        });
    }
    if (value.nodeType === 'session_id') {
        const parsed = zod_1.z.string().uuid().safeParse(value.nodeKey);
        if (!parsed.success) {
            ctx.addIssue({
                code: zod_1.z.ZodIssueCode.custom,
                path: ['nodeKey'],
                message: 'session_id lookups require a valid UUID'
            });
        }
    }
});
const journeyParamsSchema = zod_1.z.object({
    journeyId: zod_1.z.string().uuid()
});
function parseInput(schema, input) {
    try {
        return schema.parse(input);
    }
    catch (error) {
        if (error instanceof zod_1.z.ZodError) {
            throw new IdentityReadHttpError(400, 'invalid_request', 'Invalid identity lookup request', error.flatten());
        }
        throw error;
    }
}
function formatIsoString(value) {
    return value?.toISOString() ?? null;
}
function mapJourneySummary(row) {
    return {
        journeyId: row.id,
        status: row.status,
        authoritativeShopifyCustomerId: row.authoritative_shopify_customer_id,
        primaryIdentifiers: {
            hashedEmail: row.primary_email_hash,
            phoneHash: row.primary_phone_hash
        },
        mergeVersion: row.merge_version,
        mergedIntoJourneyId: row.merged_into_journey_id,
        lookbackWindow: {
            startedAt: row.lookback_window_started_at.toISOString(),
            expiresAt: row.lookback_window_expires_at.toISOString(),
            lastTouchEligibleAt: row.last_touch_eligible_at.toISOString()
        },
        createdAt: row.created_at.toISOString(),
        updatedAt: row.updated_at.toISOString(),
        lastResolvedAt: row.last_resolved_at.toISOString()
    };
}
function mapEdgeRow(row) {
    return {
        edgeId: row.edge_id,
        nodeId: row.node_id,
        nodeType: row.node_type,
        nodeKey: row.node_key,
        isAuthoritative: row.is_authoritative,
        isAmbiguous: row.is_ambiguous,
        edgeType: row.edge_type,
        precedenceRank: row.precedence_rank,
        evidenceSource: row.evidence_source,
        sourceTable: row.source_table,
        sourceRecordId: row.source_record_id,
        isActive: row.is_active,
        conflictCode: row.conflict_code,
        firstObservedAt: row.first_observed_at.toISOString(),
        lastObservedAt: row.last_observed_at.toISOString(),
        createdAt: row.edge_created_at.toISOString(),
        updatedAt: row.edge_updated_at.toISOString()
    };
}
function mapSessionRow(row) {
    return {
        sessionId: row.session_id,
        startedAt: row.session_started_at.toISOString(),
        endedAt: row.session_ended_at.toISOString(),
        journeySessionNumber: row.journey_session_number,
        reverseJourneySessionNumber: row.reverse_journey_session_number,
        metrics: {
            eventCount: row.session_event_count,
            pageViewCount: row.page_view_count,
            productViewCount: row.product_view_count,
            addToCartCount: row.add_to_cart_count,
            checkoutStartedCount: row.checkout_started_count,
            orderCount: row.session_order_count,
            orderRevenue: Number(row.session_order_revenue)
        },
        flags: {
            isFirstSession: row.is_first_session,
            isLastSession: row.is_last_session,
            isConvertingSession: row.is_converting_session
        },
        acquisition: {
            anonymousUserId: row.anonymous_user_id,
            landingPage: row.landing_page,
            referrerUrl: row.referrer_url,
            utmSource: row.utm_source,
            utmMedium: row.utm_medium,
            utmCampaign: row.utm_campaign,
            utmContent: row.utm_content,
            utmTerm: row.utm_term,
            gclid: row.gclid,
            gbraid: row.gbraid,
            wbraid: row.wbraid,
            fbclid: row.fbclid,
            ttclid: row.ttclid,
            msclkid: row.msclkid
        }
    };
}
function mapOrderRow(row) {
    return {
        shopifyOrderId: row.shopify_order_id,
        shopifyOrderNumber: row.shopify_order_number,
        shopifyCustomerId: row.shopify_customer_id,
        emailHash: row.email_hash,
        currencyCode: row.currency_code,
        totalPrice: Number(row.total_price),
        financialStatus: row.financial_status,
        fulfillmentStatus: row.fulfillment_status,
        processedAt: formatIsoString(row.processed_at),
        createdAtShopify: formatIsoString(row.created_at_shopify),
        updatedAtShopify: formatIsoString(row.updated_at_shopify),
        landingSessionId: row.landing_session_id,
        checkoutToken: row.checkout_token,
        cartToken: row.cart_token,
        sourceName: row.source_name,
        ingestedAt: row.ingested_at.toISOString()
    };
}
async function fetchJourneySummaryById(journeyId) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        id::text AS id,
        status,
        authoritative_shopify_customer_id,
        primary_email_hash,
        primary_phone_hash,
        merge_version,
        merged_into_journey_id::text AS merged_into_journey_id,
        lookback_window_started_at,
        lookback_window_expires_at,
        last_touch_eligible_at,
        created_at,
        updated_at,
        last_resolved_at
      FROM identity_journeys
      WHERE id = $1::uuid
      LIMIT 1
    `, [journeyId]);
    return result.rows[0] ?? null;
}
async function fetchJourneySummaryByLookup(nodeType, nodeKey) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        journey.id::text AS id,
        journey.status,
        journey.authoritative_shopify_customer_id,
        journey.primary_email_hash,
        journey.primary_phone_hash,
        journey.merge_version,
        journey.merged_into_journey_id::text AS merged_into_journey_id,
        journey.lookback_window_started_at,
        journey.lookback_window_expires_at,
        journey.last_touch_eligible_at,
        journey.created_at,
        journey.updated_at,
        journey.last_resolved_at
      FROM identity_nodes node
      INNER JOIN identity_edges edge
        ON edge.node_id = node.id
       AND edge.is_active = true
      INNER JOIN identity_journeys journey
        ON journey.id = edge.journey_id
      WHERE node.node_type = $1
        AND node.node_key = $2
      LIMIT 1
    `, [nodeType, nodeKey]);
    return result.rows[0] ?? null;
}
async function fetchJourneyEdges(journeyId) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        edge.id::text AS edge_id,
        node.id::text AS node_id,
        node.node_type,
        node.node_key,
        node.is_authoritative,
        node.is_ambiguous,
        edge.edge_type,
        edge.precedence_rank,
        edge.evidence_source,
        edge.source_table,
        edge.source_record_id,
        edge.is_active,
        edge.conflict_code,
        edge.first_observed_at,
        edge.last_observed_at,
        edge.created_at AS edge_created_at,
        edge.updated_at AS edge_updated_at
      FROM identity_edges edge
      INNER JOIN identity_nodes node
        ON node.id = edge.node_id
      WHERE edge.journey_id = $1::uuid
      ORDER BY edge.is_active DESC, edge.precedence_rank DESC, node.node_type ASC, node.node_key ASC, edge.created_at ASC
    `, [journeyId]);
    return result.rows;
}
async function fetchJourneySessions(journeyId) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        session_id::text AS session_id,
        session_started_at,
        session_ended_at,
        journey_session_number,
        reverse_journey_session_number,
        session_event_count,
        page_view_count,
        product_view_count,
        add_to_cart_count,
        checkout_started_count,
        session_order_count,
        session_order_revenue,
        is_first_session,
        is_last_session,
        is_converting_session,
        anonymous_user_id,
        landing_page,
        referrer_url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        gclid,
        gbraid,
        wbraid,
        fbclid,
        ttclid,
        msclkid
      FROM customer_journey
      WHERE identity_journey_id = $1::uuid
      ORDER BY session_started_at ASC, session_id ASC
    `, [journeyId]);
    return result.rows;
}
async function fetchJourneyOrders(journeyId) {
    const result = await (0, pool_js_1.query)(`
      SELECT
        shopify_order_id,
        shopify_order_number,
        shopify_customer_id,
        email_hash,
        currency_code,
        total_price,
        financial_status,
        fulfillment_status,
        processed_at,
        created_at_shopify,
        updated_at_shopify,
        landing_session_id::text AS landing_session_id,
        checkout_token,
        cart_token,
        source_name,
        ingested_at
      FROM shopify_orders
      WHERE identity_journey_id = $1::uuid
      ORDER BY COALESCE(processed_at, created_at_shopify, ingested_at) ASC, shopify_order_id ASC
    `, [journeyId]);
    return result.rows;
}
async function buildJourneyResponse(journeyId) {
    const journey = await fetchJourneySummaryById(journeyId);
    if (!journey) {
        throw new IdentityReadHttpError(404, 'journey_not_found', 'Identity journey was not found');
    }
    const [edges, sessions, orders] = await Promise.all([
        fetchJourneyEdges(journeyId),
        fetchJourneySessions(journeyId),
        fetchJourneyOrders(journeyId)
    ]);
    return {
        journey: mapJourneySummary(journey),
        identifiers: {
            total: edges.length,
            activeCount: edges.filter((edge) => edge.is_active).length,
            ambiguousCount: edges.filter((edge) => edge.is_ambiguous).length,
            nodes: edges.map(mapEdgeRow)
        },
        timeline: {
            sessions: sessions.map(mapSessionRow),
            orders: orders.map(mapOrderRow)
        }
    };
}
function createInternalIdentityRouter() {
    const router = (0, express_1.Router)();
    router.use(index_js_1.attachAuthContext);
    router.use(index_js_1.requireInternalService);
    router.get('/lookup', async (req, res, next) => {
        try {
            const { nodeType, nodeKey } = parseInput(lookupQuerySchema, req.query);
            const journey = await fetchJourneySummaryByLookup(nodeType, nodeKey);
            if (!journey) {
                throw new IdentityReadHttpError(404, 'identity_lookup_not_found', 'No active identity journey matched the supplied identifier');
            }
            const response = await buildJourneyResponse(journey.id);
            res.status(200).json({
                lookup: {
                    nodeType,
                    nodeKey
                },
                ...response
            });
        }
        catch (error) {
            next(error);
        }
    });
    router.get('/journeys/:journeyId', async (req, res, next) => {
        try {
            const { journeyId } = parseInput(journeyParamsSchema, req.params);
            const response = await buildJourneyResponse(journeyId);
            res.status(200).json(response);
        }
        catch (error) {
            next(error);
        }
    });
    return router;
}
