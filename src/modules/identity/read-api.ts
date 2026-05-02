import { Router } from "express";
import { z } from "zod";

import { query } from "../../db/pool.js";
import { isSha256Hex } from "../../shared/privacy.js";
import { attachAuthContext, requireInternalService } from "../auth/index.js";

class IdentityReadHttpError extends Error {
	statusCode: number;
	code: string;
	details?: unknown;

	constructor(
		statusCode: number,
		code: string,
		message: string,
		details?: unknown,
	) {
		super(message);
		this.name = "IdentityReadHttpError";
		this.statusCode = statusCode;
		this.code = code;
		this.details = details;
	}
}

const identityNodeTypes = [
	"session_id",
	"checkout_token",
	"cart_token",
	"shopify_customer_id",
	"hashed_email",
	"phone_hash",
] as const;

const lookupQuerySchema = z
	.object({
		nodeType: z.enum(identityNodeTypes),
		nodeKey: z.string().trim().min(1),
	})
	.superRefine((value, ctx) => {
		if (
			(value.nodeType === "hashed_email" || value.nodeType === "phone_hash") &&
			!isSha256Hex(value.nodeKey)
		) {
			ctx.addIssue({
				code: z.ZodIssueCode.custom,
				path: ["nodeKey"],
				message: `${value.nodeType} lookups require a sha256 hex digest`,
			});
		}

		if (value.nodeType === "session_id") {
			const parsed = z.string().uuid().safeParse(value.nodeKey);
			if (!parsed.success) {
				ctx.addIssue({
					code: z.ZodIssueCode.custom,
					path: ["nodeKey"],
					message: "session_id lookups require a valid UUID",
				});
			}
		}
	});

const journeyParamsSchema = z.object({
	journeyId: z.string().uuid(),
});

type IdentityJourneySummaryRow = {
	id: string;
	status: "active" | "quarantined" | "merged" | "conflicted";
	authoritative_shopify_customer_id: string | null;
	primary_email_hash: string | null;
	primary_phone_hash: string | null;
	merge_version: number;
	merged_into_journey_id: string | null;
	lookback_window_started_at: Date;
	lookback_window_expires_at: Date;
	last_touch_eligible_at: Date;
	created_at: Date;
	updated_at: Date;
	last_resolved_at: Date;
};

type IdentityEdgeRow = {
	edge_id: string;
	node_id: string;
	node_type: (typeof identityNodeTypes)[number];
	node_key: string;
	is_authoritative: boolean;
	is_ambiguous: boolean;
	edge_type: "authoritative" | "deterministic" | "promoted" | "quarantined";
	precedence_rank: number;
	evidence_source: string;
	source_table: string | null;
	source_record_id: string | null;
	is_active: boolean;
	conflict_code: string | null;
	first_observed_at: Date;
	last_observed_at: Date;
	edge_created_at: Date;
	edge_updated_at: Date;
};

type JourneySessionRow = {
	session_id: string;
	session_started_at: Date;
	session_ended_at: Date;
	journey_session_number: number;
	reverse_journey_session_number: number;
	session_event_count: number;
	page_view_count: number;
	product_view_count: number;
	add_to_cart_count: number;
	checkout_started_count: number;
	session_order_count: number;
	session_order_revenue: string | number;
	is_first_session: boolean;
	is_last_session: boolean;
	is_converting_session: boolean;
	anonymous_user_id: string | null;
	landing_page: string | null;
	referrer_url: string | null;
	utm_source: string | null;
	utm_medium: string | null;
	utm_campaign: string | null;
	utm_content: string | null;
	utm_term: string | null;
	gclid: string | null;
	gbraid: string | null;
	wbraid: string | null;
	fbclid: string | null;
	ttclid: string | null;
	msclkid: string | null;
};

type JourneyOrderRow = {
	shopify_order_id: string;
	shopify_order_number: string | null;
	shopify_customer_id: string | null;
	email_hash: string | null;
	currency_code: string;
	total_price: string | number;
	financial_status: string | null;
	fulfillment_status: string | null;
	processed_at: Date | null;
	created_at_shopify: Date | null;
	updated_at_shopify: Date | null;
	landing_session_id: string | null;
	checkout_token: string | null;
	cart_token: string | null;
	source_name: string | null;
	ingested_at: Date;
};

function parseInput<TSchema extends z.ZodTypeAny>(
	schema: TSchema,
	input: unknown,
): z.infer<TSchema> {
	try {
		return schema.parse(input);
	} catch (error) {
		if (error instanceof z.ZodError) {
			throw new IdentityReadHttpError(
				400,
				"invalid_request",
				"Invalid identity lookup request",
				error.flatten(),
			);
		}

		throw error;
	}
}

function formatIsoString(value: Date | null): string | null {
	return value?.toISOString() ?? null;
}

function mapJourneySummary(row: IdentityJourneySummaryRow) {
	return {
		journeyId: row.id,
		status: row.status,
		authoritativeShopifyCustomerId: row.authoritative_shopify_customer_id,
		primaryIdentifiers: {
			hashedEmail: row.primary_email_hash,
			phoneHash: row.primary_phone_hash,
		},
		mergeVersion: row.merge_version,
		mergedIntoJourneyId: row.merged_into_journey_id,
		lookbackWindow: {
			startedAt: row.lookback_window_started_at.toISOString(),
			expiresAt: row.lookback_window_expires_at.toISOString(),
			lastTouchEligibleAt: row.last_touch_eligible_at.toISOString(),
		},
		createdAt: row.created_at.toISOString(),
		updatedAt: row.updated_at.toISOString(),
		lastResolvedAt: row.last_resolved_at.toISOString(),
	};
}

function mapEdgeRow(row: IdentityEdgeRow) {
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
		updatedAt: row.edge_updated_at.toISOString(),
	};
}

function mapSessionRow(row: JourneySessionRow) {
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
			orderRevenue: Number(row.session_order_revenue),
		},
		flags: {
			isFirstSession: row.is_first_session,
			isLastSession: row.is_last_session,
			isConvertingSession: row.is_converting_session,
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
			msclkid: row.msclkid,
		},
	};
}

function mapOrderRow(row: JourneyOrderRow) {
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
		ingestedAt: row.ingested_at.toISOString(),
	};
}

async function fetchJourneySummaryById(
	journeyId: string,
): Promise<IdentityJourneySummaryRow | null> {
	const result = await query<IdentityJourneySummaryRow>(
		`
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
    `,
		[journeyId],
	);

	return result.rows[0] ?? null;
}

async function fetchJourneySummaryByLookup(
	nodeType: (typeof identityNodeTypes)[number],
	nodeKey: string,
): Promise<IdentityJourneySummaryRow | null> {
	const result = await query<IdentityJourneySummaryRow>(
		`
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
    `,
		[nodeType, nodeKey],
	);

	return result.rows[0] ?? null;
}

async function fetchJourneyEdges(
	journeyId: string,
): Promise<IdentityEdgeRow[]> {
	const result = await query<IdentityEdgeRow>(
		`
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
    `,
		[journeyId],
	);

	return result.rows;
}

async function fetchJourneySessions(
	journeyId: string,
): Promise<JourneySessionRow[]> {
	const result = await query<JourneySessionRow>(
		`
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
    `,
		[journeyId],
	);

	return result.rows;
}

async function fetchJourneyOrders(
	journeyId: string,
): Promise<JourneyOrderRow[]> {
	const result = await query<JourneyOrderRow>(
		`
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
    `,
		[journeyId],
	);

	return result.rows;
}

async function buildJourneyResponse(journeyId: string) {
	const journey = await fetchJourneySummaryById(journeyId);

	if (!journey) {
		throw new IdentityReadHttpError(
			404,
			"journey_not_found",
			"Identity journey was not found",
		);
	}

	const [edges, sessions, orders] = await Promise.all([
		fetchJourneyEdges(journeyId),
		fetchJourneySessions(journeyId),
		fetchJourneyOrders(journeyId),
	]);

	return {
		journey: mapJourneySummary(journey),
		identifiers: {
			total: edges.length,
			activeCount: edges.filter((edge) => edge.is_active).length,
			ambiguousCount: edges.filter((edge) => edge.is_ambiguous).length,
			nodes: edges.map(mapEdgeRow),
		},
		timeline: {
			sessions: sessions.map(mapSessionRow),
			orders: orders.map(mapOrderRow),
		},
	};
}

export function createInternalIdentityRouter(): Router {
	const router = Router();

	router.use(attachAuthContext);
	router.use(requireInternalService);

	router.get("/lookup", async (req, res, next) => {
		try {
			const { nodeType, nodeKey } = parseInput(lookupQuerySchema, req.query);
			const journey = await fetchJourneySummaryByLookup(nodeType, nodeKey);

			if (!journey) {
				throw new IdentityReadHttpError(
					404,
					"identity_lookup_not_found",
					"No active identity journey matched the supplied identifier",
				);
			}

			const response = await buildJourneyResponse(journey.id);
			res.status(200).json({
				lookup: {
					nodeType,
					nodeKey,
				},
				...response,
			});
		} catch (error) {
			next(error);
		}
	});

	router.get("/journeys/:journeyId", async (req, res, next) => {
		try {
			const { journeyId } = parseInput(journeyParamsSchema, req.params);
			const response = await buildJourneyResponse(journeyId);
			res.status(200).json(response);
		} catch (error) {
			next(error);
		}
	});

	return router;
}
