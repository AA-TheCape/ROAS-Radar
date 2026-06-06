import { Router } from "express";
import { z } from "zod";

import { env } from "../../config/env.js";
import { query } from "../../db/pool.js";
import {
	emitGoogleCpcMissingCampaignNameLog,
	logError,
	logInfo,
} from "../../observability/index.js";
import { calculatePerformanceMetrics } from "../../shared/metrics.js";
import { ATTRIBUTION_MODELS } from "../attribution/engine.js";
import { attachAuthContext, requireAuthenticated } from "../auth/index.js";
import {
	fetchDataQualityReport,
	resolveRunDate,
} from "../data-quality/index.js";
import { getReportingTimezone } from "../settings/index.js";
import {
	buildCampaignResolutionGroupKey,
	resolveCampaignDisplayMetadata,
	type CampaignDisplayResolution,
} from "./metadata-resolution.js";

class ReportingHttpError extends Error {
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
		this.name = "ReportingHttpError";
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
type ReportingAttributionTier = z.infer<typeof attributionTierSchema>;

const ATTRIBUTION_TIER_LABELS: Record<ReportingAttributionTier, string> = {
  deterministic_first_party: 'Deterministic first-party',
  deterministic_shopify_hint: 'Deterministic Shopify hint',
  ga4_fallback: 'GA4 fallback',
  unattributed: 'Unattributed'
};

const ATTRIBUTION_TIER_DESCRIPTIONS: Record<ReportingAttributionTier, string> = {
  deterministic_first_party:
    'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
  deterministic_shopify_hint:
    'Recovered synthetically from Shopify marketing hints after first-party resolution failed.',
  ga4_fallback:
    'Recovered from the GA4 fallback contract only after first-party and Shopify-hint matches were unavailable.',
  unattributed:
    'No eligible first-party, Shopify hint, or GA4 fallback match qualified, or the required timing data could not be normalized.'
};

const baseFiltersObjectSchema = z.object({
  startDate: dateStringSchema,
  endDate: dateStringSchema,
  attributionModel: z.enum(ATTRIBUTION_MODELS).optional().default('last_touch'),
  reportingMode: z.enum(['combined', 'clicks', 'deterministic_views', 'meta_view_through']).optional().default('clicks'),
  attributionTier: attributionTierSchema.optional(),
  source: z.string().trim().min(1).optional(),
  campaign: z.string().trim().min(1).optional()
});

const optionalModelFiltersObjectSchema = z.object({
	startDate: dateStringSchema,
	endDate: dateStringSchema,
	attributionModel: z.enum(ATTRIBUTION_MODELS).optional(),
	attributionTier: attributionTierSchema.optional(),
	source: z.string().trim().min(1).optional(),
	campaign: z.string().trim().min(1).optional(),
});

function withValidDateRange<T extends z.ZodRawShape>(schema: z.ZodObject<T>) {
	return schema.superRefine((value, ctx) => {
		if (value.startDate > value.endDate) {
			ctx.addIssue({
				code: z.ZodIssueCode.custom,
				message: "startDate must be on or before endDate",
				path: ["startDate"],
			});
		}
	});
}

const baseFiltersSchema = withValidDateRange(baseFiltersObjectSchema);

const campaignsQuerySchema = withValidDateRange(
	baseFiltersObjectSchema.extend({
		limit: z.coerce.number().int().positive().max(200).optional().default(50),
	}),
);

const timeseriesQuerySchema = withValidDateRange(
	baseFiltersObjectSchema.extend({
		groupBy: z.enum(["day", "source", "campaign"]).optional().default("day"),
	}),
);

const modelComparisonQuerySchema = withValidDateRange(
	optionalModelFiltersObjectSchema.extend({
		dateGrain: z.enum(["day", "week"]).optional().default("week"),
		sourceOfTruth: z.enum(["deterministic", "mmm"]).optional().default("deterministic"),
	}),
);

const ordersQuerySchema = withValidDateRange(
	baseFiltersObjectSchema.extend({
		limit: z.coerce.number().int().positive().max(200).optional().default(50),
	}),
);

const orderDetailsParamsSchema = z.object({
	shopifyOrderId: z.string().trim().min(1),
});

const reconciliationQuerySchema = z.object({
	runDate: dateStringSchema.optional(),
});

type SummaryRow = {
	visits: string | number;
	orders: string | number;
	revenue: string | number;
	spend: string | number;
};

type ModelFreshnessRow = {
	latest_model_output_at: Date | string | null;
	model_output_count: string | number;
};

type ReportingMetricTotals = {
  visits: number;
  orders: number;
  revenue: number;
  spend: number;
  conversionRate: number;
  roas: number | null;
};

const REPORTING_MODE_METADATA = {
  clicks: {
    label: 'Click attribution',
    canonical: true,
    description: 'Canonical reporting totals from click-attributed order credits.'
  },
  deterministic_views: {
    label: 'Deterministic view layer',
    canonical: false,
    description: 'Layer-only Meta API-verified deterministic view/impression attribution.'
  },
  meta_view_through: {
    label: 'Meta API view-through',
    canonical: false,
    description: 'Meta API-reported view-through purchase revenue, purchases, and ROAS from impression-time reporting.'
  },
  combined: {
    label: 'Non-canonical comparison total',
    canonical: false,
    description: 'Comparison-only sum of click attribution and deterministic view attribution; do not treat as canonical revenue.'
  }
} as const;

type CampaignRow = {
	source: string;
	medium: string;
	campaign: string;
	content: string | null;
	visits: string | number;
	orders: string | number;
	revenue: string | number;
};

type SpendDetailRow = {
	source: string;
	medium: string;
	campaign: string;
	spend: string | number;
};

type CampaignLabelResponseFields = {
	campaignDisplayName?: string;
	campaignEntityId?: string | null;
	campaignEntityType?: "campaign" | "adset";
	parentCampaignEntityId?: string | null;
	parentCampaignDisplayName?: string | null;
	campaignPlatform?: "google_ads" | "meta_ads" | null;
	campaignNameResolutionStatus?: "resolved" | "fallback_name" | "unresolved";
	campaignLabel?: {
		displayName: string;
		source: string;
		rawId: string;
		entityId: string | null;
		objectType: "campaign" | "adset" | null;
		entityType?: "campaign" | "adset";
		parentCampaignEntityId?: string | null;
		parentCampaignDisplayName?: string | null;
		parentCampaign?: {
			entityId: string | null;
			displayName: string | null;
		} | null;
		platform: "google_ads" | "meta_ads" | null;
		resolutionStatus: "resolved" | "fallback_name" | "unresolved";
		lastSeenAt: string | null;
		updatedAt: string | null;
	};
};

type TimeseriesRow = {
	bucket: string;
	visits: string | number;
	orders: string | number;
	revenue: string | number;
	spend: string | number;
};

type ModelComparisonRow = {
	bucket: string;
	attribution_model: string;
	reporting_view: string;
	source: string;
	medium: string;
	campaign: string;
	visits: string | number;
	attributed_orders: string | number;
	attributed_revenue: string | number;
	spend: string | number;
	strict_deterministic_orders: string | number;
	fallback_included_orders: string | number;
	blended_deterministic_orders: string | number;
};

type MmmContributionSummary = {
	mean?: number;
	credibleInterval80?: {
		lower?: number;
		upper?: number;
	};
	credibleInterval95?: {
		lower?: number;
		upper?: number;
	};
};

type MmmModelArtifact = {
	contributionOutputs?: {
		channels?: Array<{
			key?: string;
			source?: string;
			medium?: string;
			campaign?: string;
			contribution?: MmmContributionSummary;
			contributionShare?: MmmContributionSummary;
			posteriorProbabilityPositive?: number;
		}>;
	};
};

type MmmModelRunForReportingRow = {
	id: string;
	model_type: string;
	model_version: string;
	mart_version: string;
	attribution_model: string;
	training_start_date: string;
	training_end_date: string;
	input_summary: unknown;
	model_artifact: MmmModelArtifact;
	calibration_report: unknown;
	validation_report: unknown;
	completed_at: Date | string | null;
};

type MmmWeeklyChannelReportingRow = {
	bucket: string;
	week_end_date: string;
	attribution_model: string;
	channel_key: string;
	source: string;
	medium: string;
	campaign: string;
	channel: string;
	channel_group: string;
	spend: string | number;
	impressions: string | number;
	clicks: string | number;
	shopify_orders: string | number;
	shopify_revenue: string | number;
	attribution_credit_orders: string | number;
	attribution_credit_revenue: string | number;
	dq_status: string;
	source_row_count: string | number;
	generated_at: Date | string;
};

type OrderAttributionRow = {
  shopify_order_id: string;
  processed_at: Date | null;
  total_price: string | number;
  attribution_tier: string | null;
  attribution_source: string | null;
  attribution_source_code: string | null;
  matching_method_code: string | null;
  order_attribution_reason: string | null;
  attribution_matched_at: Date | null;
  attribution_confidence_score: string | number | null;
  last_attribution_run_at: Date | null;
  attribution_snapshot: unknown;
  attributed_source: string | null;
  attributed_medium: string | null;
  attributed_campaign: string | null;
  primary_credit_attribution_reason: string | null;
};

type OrderDetailsRow = {
  shopify_order_id: string;
  shopify_order_number: string | null;
  shopify_customer_id: string | null;
  customer_identity_id: string | null;
  email_hash: string | null;
  currency_code: string;
  subtotal_price: string | number;
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
  attribution_tier: string | null;
  attribution_source: string | null;
  attribution_source_code: string | null;
  matching_method_code: string | null;
  attribution_matched_at: Date | null;
  attribution_reason: string | null;
  attribution_confidence_score: string | number | null;
  last_attribution_run_at: Date | null;
  attribution_snapshot: unknown;
  attribution_snapshot_updated_at: Date | null;
  result_attribution_model: string | null;
  result_session_id: string | null;
  result_attributed_source: string | null;
  result_attributed_medium: string | null;
  result_attributed_campaign: string | null;
  result_attributed_content: string | null;
  result_attributed_term: string | null;
  result_attributed_click_id_type: string | null;
  result_attributed_click_id_value: string | null;
  ingested_at: Date;
  raw_payload: unknown;
};

type OrderLineItemRow = {
	shopify_line_item_id: string;
	shopify_product_id: string | null;
	shopify_variant_id: string | null;
	sku: string | null;
	title: string | null;
	variant_title: string | null;
	vendor: string | null;
	quantity: number;
	price: string | number;
	total_discount: string | number;
	fulfillment_status: string | null;
	requires_shipping: boolean | null;
	taxable: boolean | null;
	ingested_at: Date;
	raw_payload: unknown;
};

type AttributionCreditRow = {
	attribution_model: string;
	touchpoint_position: number;
	session_id: string | null;
	touchpoint_occurred_at: Date | null;
	attributed_source: string | null;
	attributed_medium: string | null;
	attributed_campaign: string | null;
	attributed_content: string | null;
	attributed_term: string | null;
	attributed_click_id_type: string | null;
	attributed_click_id_value: string | null;
	credit_weight: string | number;
	revenue_credit: string | number;
	is_primary: boolean;
	attribution_reason: string;
	match_source: string;
	confidence_label: string;
	created_at: Date;
	model_version: number;
};

function parseInput<TSchema extends z.ZodTypeAny>(
	schema: TSchema,
	input: unknown,
): z.infer<TSchema> {
	try {
		return schema.parse(input);
	} catch (error) {
		if (error instanceof z.ZodError) {
			throw new ReportingHttpError(
				400,
				"invalid_request",
				"Invalid reporting query parameters",
				error.flatten(),
			);
		}

		throw error;
	}
}

function buildMetricDimensionFilters(
	attributionModel: string,
	source: string | undefined,
	campaign: string | undefined,
	alias = "",
): { sql: string; params: string[] } {
	const params: string[] = [attributionModel];
	const qualifiedAlias = alias ? `${alias}.` : "";
	const filters: string[] = [`${qualifiedAlias}attribution_model = $3`];

	if (source) {
		params.push(source);
		filters.push(`${qualifiedAlias}source = $${params.length + 2}`);
	}

	if (campaign) {
		params.push(campaign);
		filters.push(`${qualifiedAlias}campaign = $${params.length + 2}`);
	}

	return {
		sql: filters.length > 0 ? ` AND ${filters.join(" AND ")}` : "",
		params,
	};
}

function buildDeterministicViewFilters(
	source: string | undefined,
	campaign: string | undefined,
): { sql: string; params: string[] } {
	const params: string[] = [];
	const filters = [
		"dmo.model_key = 'deterministic_views'",
		"dmo.output_type = 'credited_input'",
		"dmo.platform_verified = true",
	];

	if (source) {
		params.push(source);
		filters.push(`
      CASE dmo.platform
        WHEN 'google_ads' THEN 'google'
        WHEN 'meta_ads' THEN 'meta'
        ELSE dmo.platform
      END = $${params.length + 2}
    `);
	}

	if (campaign) {
		params.push(campaign);
		filters.push(`
      COALESCE(NULLIF(dmo.campaign_id, ''), NULLIF(dmo.adset_id, ''), NULLIF(dmo.ad_id, ''), 'unknown') = $${params.length + 2}
    `);
	}

	return {
		sql: ` AND ${filters.join(" AND ")}`,
		params,
	};
}

function buildMetaViewThroughFilters(
	source: string | undefined,
	campaign: string | undefined,
): { sql: string; params: string[] } {
	const params: string[] = [];
	const filters = [
		"organization_id = $3",
		"action_report_time = 'impression'",
		"use_account_attribution_setting = true",
	];

	if (source) {
		params.push(source);
		filters.push(`'meta' = $${params.length + 3}`);
	}

	if (campaign) {
		params.push(campaign);
		filters.push(`(
      campaign_id = $${params.length + 3}
      OR campaign_name = $${params.length + 3}
    )`);
	}

	return {
		sql: ` AND ${filters.join(" AND ")}`,
		params,
	};
}

function toReportingTotals(row: SummaryRow | undefined): ReportingMetricTotals {
	const metrics = calculatePerformanceMetrics({
		visits: row?.visits ?? 0,
		orders: row?.orders ?? 0,
		attributedRevenue: row?.revenue ?? 0,
		spend: row?.spend ?? 0,
	});

	return {
		visits: metrics.visits,
		orders: metrics.orders,
		revenue: metrics.attributedRevenue,
		spend: metrics.spend,
		conversionRate: metrics.conversionRate,
		roas: metrics.roas,
	};
}

function combineReportingTotals(
	clicks: ReportingMetricTotals,
	deterministicViews: ReportingMetricTotals,
): ReportingMetricTotals {
	return toReportingTotals({
		visits: clicks.visits,
		orders: clicks.orders + deterministicViews.orders,
		revenue: clicks.revenue + deterministicViews.revenue,
		spend: clicks.spend,
	});
}

function buildOrderAttributionFilters(
  attributionModel: string,
  source: string | undefined,
  campaign: string | undefined,
  attributionTier?: z.infer<typeof attributionTierSchema>
): { sql: string; params: string[] } {
	const params: string[] = [attributionModel];
	const filters: string[] = [];

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

function buildModelComparisonFilters(input: {
	attributionModel?: string;
	source?: string;
	campaign?: string;
	attributionTier?: ReportingAttributionTier;
}): { sql: string; params: string[] } {
	const params: string[] = [];
	const filters: string[] = [];

	if (input.attributionModel) {
		params.push(input.attributionModel);
		filters.push(`attribution_model = $${params.length + 2}`);
	}

	if (input.source) {
		params.push(input.source);
		filters.push(`source = $${params.length + 2}`);
	}

	if (input.campaign) {
		params.push(input.campaign);
		filters.push(`campaign = $${params.length + 2}`);
	}

	if (input.attributionTier) {
		const reportingView =
			input.attributionTier === "deterministic_first_party"
				? "strict_deterministic"
				: input.attributionTier === "unattributed"
					? null
					: "fallback_included";

		if (reportingView) {
			params.push(reportingView);
			filters.push(`reporting_view = $${params.length + 2}`);
		} else {
			filters.push("attributed_orders = 0");
		}
	}

	return {
		sql: filters.length > 0 ? ` AND ${filters.join(" AND ")}` : "",
		params,
	};
}

function buildMmmWeeklyChannelFilters(input: {
	attributionModel?: string;
	source?: string;
	campaign?: string;
}): { sql: string; params: string[] } {
	const params: string[] = [];
	const filters: string[] = [];

	if (input.attributionModel) {
		params.push(input.attributionModel);
		filters.push(`attribution_model = $${params.length + 2}`);
	}

	if (input.source) {
		params.push(input.source);
		filters.push(`source = $${params.length + 2}`);
	}

	if (input.campaign) {
		params.push(input.campaign);
		filters.push(`campaign = $${params.length + 2}`);
	}

	return {
		sql: filters.length > 0 ? ` AND ${filters.join(" AND ")}` : "",
		params,
	};
}

function toIsoString(value: Date | string | null | undefined): string | null {
	if (value instanceof Date) {
		return value.toISOString();
	}

	return typeof value === "string" && value.length > 0
		? new Date(value).toISOString()
		: null;
}

function normalizeContent(value: string | null): string | null {
	if (typeof value !== "string") {
		return null;
	}

	const trimmed = value.trim();
	return trimmed.length > 0 && trimmed.toLowerCase() !== "unknown" ? trimmed : null;
}

type AttributionWinnerMetadata = {
  sessionId: string | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
};

type OrderAttributionMetadata = {
  confidenceScore: number | null;
  winner: AttributionWinnerMetadata;
};

function asObjectRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function readNullableString(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

function readNullableNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function extractOrderAttributionMetadata(snapshot: unknown): OrderAttributionMetadata {
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

function selectPrimaryAttributionCredit(
  rows: AttributionCreditRow[],
  attributionModel: string | null
): AttributionCreditRow | undefined {
  if (attributionModel) {
    const modelPrimary = rows.find((row) => row.attribution_model === attributionModel && row.is_primary);

    if (modelPrimary) {
      return modelPrimary;
    }
  }

  return rows.find((row) => row.is_primary) ?? rows[0];
}

function coalesceNullableString(...values: Array<string | null | undefined>): string | null {
  return values.find((value): value is string => typeof value === 'string' && value.trim().length > 0) ?? null;
}

function buildPersistedOrderAttribution(
  order: OrderDetailsRow,
  primaryCredit: AttributionCreditRow | undefined,
  metadata: OrderAttributionMetadata
): AttributionWinnerMetadata {
  return {
    sessionId: coalesceNullableString(order.result_session_id, primaryCredit?.session_id, metadata.winner.sessionId),
    source: coalesceNullableString(order.result_attributed_source, primaryCredit?.attributed_source, metadata.winner.source),
    medium: coalesceNullableString(order.result_attributed_medium, primaryCredit?.attributed_medium, metadata.winner.medium),
    campaign: coalesceNullableString(order.result_attributed_campaign, primaryCredit?.attributed_campaign, metadata.winner.campaign),
    content: coalesceNullableString(order.result_attributed_content, primaryCredit?.attributed_content, metadata.winner.content),
    term: coalesceNullableString(order.result_attributed_term, primaryCredit?.attributed_term, metadata.winner.term),
    clickIdType: coalesceNullableString(
      order.result_attributed_click_id_type,
      primaryCredit?.attributed_click_id_type,
      metadata.winner.clickIdType
    ),
    clickIdValue: coalesceNullableString(
      order.result_attributed_click_id_value,
      primaryCredit?.attributed_click_id_value,
      metadata.winner.clickIdValue
    )
  };
}

function readOrderConfidenceScore(
  persistedScore: string | number | null,
  lastAttributionRunAt: Date | null,
  snapshotScore: number | null
): number | null {
  if (persistedScore === null || persistedScore === undefined) {
    return snapshotScore;
  }

  const parsed = Number(persistedScore);
  if (!Number.isFinite(parsed)) {
    return snapshotScore;
  }

  return lastAttributionRunAt || snapshotScore !== null ? parsed : null;
}

function readAttributionLookupCode(
  legacyCode: string | null,
  lookupCode: string | null,
  lastAttributionRunAt: Date | null
): string | null {
  return legacyCode ?? (lastAttributionRunAt ? lookupCode : null);
}

function normalizeAttributionTier(value: string | null | undefined): ReportingAttributionTier {
  return attributionTierSchema.safeParse(value).success ? (value as ReportingAttributionTier) : 'unattributed';
}

function buildCampaignLabelFields(
	resolution: CampaignDisplayResolution | undefined,
	input: { source?: string; rawId?: string } = {},
): CampaignLabelResponseFields {
	if (!resolution?.campaignDisplayName) {
		return {};
	}

	const source = input.source ?? resolution.source;
	const rawId = input.rawId ?? resolution.campaign;
	const objectType = resolution.campaignEntityType ?? null;
	const parentCampaign =
		resolution.parentCampaignEntityId || resolution.parentCampaignDisplayName
			? {
					entityId: resolution.parentCampaignEntityId ?? null,
					displayName: resolution.parentCampaignDisplayName ?? null,
				}
			: null;

	return {
		campaignDisplayName: resolution.campaignDisplayName,
		campaignEntityId: resolution.campaignEntityId,
		campaignEntityType: resolution.campaignEntityType,
		parentCampaignEntityId: resolution.parentCampaignEntityId,
		parentCampaignDisplayName: resolution.parentCampaignDisplayName,
		campaignPlatform: resolution.campaignPlatform,
		campaignNameResolutionStatus: resolution.campaignNameResolutionStatus,
		campaignLabel: {
			displayName: resolution.campaignDisplayName,
			source,
			rawId,
			entityId: resolution.campaignEntityId,
			objectType,
			entityType: resolution.campaignEntityType,
			parentCampaignEntityId: resolution.parentCampaignEntityId,
			parentCampaignDisplayName: resolution.parentCampaignDisplayName,
			parentCampaign,
			platform: resolution.campaignPlatform,
			resolutionStatus: resolution.campaignNameResolutionStatus,
			lastSeenAt: resolution.lastSeenAt,
			updatedAt: resolution.updatedAt,
		},
	};
}

function isMetaLikeAttributionSource(source: string, medium: string): boolean {
	const normalizedSource = source.trim().toLowerCase();
	const normalizedMedium = medium.trim().toLowerCase();

	return (
		['meta', 'facebook', 'fb', 'instagram', 'ig'].includes(normalizedSource) ||
		(normalizedMedium.includes('social') && ['paid_social', 'paidsocial', 'cpc', 'paid'].includes(normalizedMedium))
	);
}

const PLATFORM_BACKED_META_CAMPAIGN_METADATA_SOURCES: ReadonlySet<
	NonNullable<CampaignDisplayResolution['campaignMetadataSource']>
> = new Set([
	'ad_platform_entity_metadata',
	'cache',
	'meta_api',
]);

function isPlatformBackedMetaCampaignResolution(resolution: CampaignDisplayResolution | undefined): boolean {
	return (
		resolution?.campaignPlatform === 'meta_ads' &&
		resolution.campaignNameResolutionStatus === 'resolved' &&
		resolution.campaignMetadataSource !== undefined &&
		PLATFORM_BACKED_META_CAMPAIGN_METADATA_SOURCES.has(resolution.campaignMetadataSource)
	);
}

function selectCampaignResolution(
	metadata: Awaited<ReturnType<typeof resolveCampaignDisplayMetadata>>,
	row: { source: string; medium: string; campaign: string }
): CampaignDisplayResolution | undefined {
	const groupResolution = metadata.byGroup.get(
		buildCampaignResolutionGroupKey(row.source, row.medium, row.campaign),
	);

	if (groupResolution) {
		return groupResolution;
	}

	const campaignResolution = metadata.byCampaign.get(row.campaign);

	if (isPlatformBackedMetaCampaignResolution(campaignResolution)) {
		return campaignResolution;
	}

	return isMetaLikeAttributionSource(row.source, row.medium) ? campaignResolution : undefined;
}

function selectNullableCampaignResolution(
	metadata: Awaited<ReturnType<typeof resolveCampaignDisplayMetadata>>,
	row: { source: string | null; medium: string | null; campaign: string | null }
): CampaignDisplayResolution | undefined {
	const { source, medium, campaign } = row;

	if (!source || !medium || !campaign) {
		return undefined;
	}

	return selectCampaignResolution(metadata, { source, medium, campaign });
}

function isGoogleCpcAttributedCampaign(row: {
	source: string | null;
	medium: string | null;
	campaign: string | null;
}): row is { source: string; medium: string; campaign: string } {
	return (
		row.source?.trim().toLowerCase() === 'google' &&
		row.medium?.trim().toLowerCase() === 'cpc' &&
		Boolean(row.campaign?.trim())
	);
}

function toDateString(value: Date | null | undefined): string | null {
	return value?.toISOString().slice(0, 10) ?? null;
}

function resolveReportRowSource(row: { source: string }, resolution: CampaignDisplayResolution | undefined): string {
	return resolution?.campaignPlatform === 'meta_ads' ? 'meta' : row.source;
}

function resolveReportRowContent(row: { content: string | null }, resolution: CampaignDisplayResolution | undefined): string | null {
	if (resolution?.campaignPlatform === 'meta_ads' && resolution.campaignNameResolutionStatus === 'resolved') {
		return null;
	}

	return normalizeContent(row.content);
}

function countDaysInRange(startDate: string, endDate: string): number {
	const start = Date.parse(`${startDate}T00:00:00.000Z`);
	const end = Date.parse(`${endDate}T00:00:00.000Z`);

	if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
		return 0;
	}

	return Math.floor((end - start) / 86_400_000) + 1;
}

const REPORTING_SCHEMA_VERSION = "2026-05-27";

export function createReportingRouter(): Router {
	const router = Router();

	router.use(attachAuthContext);
	router.use(requireAuthenticated);
	router.use((_req, res, next) => {
		res.setHeader("X-ROAS-Radar-Reporting-Schema", REPORTING_SCHEMA_VERSION);
		next();
	});

	router.get("/summary", async (req, res, next) => {
		const requestStartedAt = Date.now();
		try {
			const input = parseInput(baseFiltersSchema, req.query);
			const clickFilters = buildMetricDimensionFilters(
				input.attributionModel,
				input.source,
				input.campaign,
			);
			const clickResult = await query<SummaryRow>(
				`
          SELECT
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS orders,
            COALESCE(SUM(attributed_revenue), 0) AS revenue,
            COALESCE(SUM(spend), 0) AS spend
          FROM daily_reporting_metrics
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${clickFilters.sql}
        `,
				[input.startDate, input.endDate, ...clickFilters.params],
			);
			const deterministicViewFilters = buildDeterministicViewFilters(
				input.source,
				input.campaign,
			);
			const deterministicViewResult = await query<SummaryRow>(
				`
          SELECT
            0 AS visits,
            COALESCE(SUM(dmo.contribution_weight), 0) AS orders,
            COALESCE(SUM(inputs.total_amount * dmo.contribution_weight), 0) AS revenue,
            0 AS spend
          FROM deterministic_model_outputs dmo
          INNER JOIN attribution_order_inputs inputs
            ON inputs.run_id = dmo.run_id
           AND inputs.order_id = dmo.order_id
          WHERE inputs.order_occurred_at_utc >= $1::date
            AND inputs.order_occurred_at_utc < ($2::date + interval '1 day')
            ${deterministicViewFilters.sql}
        `,
				[input.startDate, input.endDate, ...deterministicViewFilters.params],
			);
			const metaViewThroughFilters = buildMetaViewThroughFilters(
				input.source,
				input.campaign,
			);
			const metaViewThroughResult = await query<SummaryRow>(
				`
          SELECT
            0 AS visits,
            COALESCE(SUM(COALESCE(purchase_count, 0)), 0) AS orders,
            COALESCE(SUM(COALESCE(attributed_revenue, 0)), 0) AS revenue,
            COALESCE(SUM(spend), 0) AS spend
          FROM meta_ads_order_value_aggregates
          WHERE report_date BETWEEN $1::date AND $2::date
            ${metaViewThroughFilters.sql}
        `,
				[
					input.startDate,
					input.endDate,
					env.DEFAULT_ORGANIZATION_ID,
					...metaViewThroughFilters.params,
				],
			);
			const modelFreshnessResult = await query<ModelFreshnessRow>(
				`
          SELECT
            MAX(dmo.generated_at_utc) AS latest_model_output_at,
            COUNT(*) AS model_output_count
          FROM deterministic_model_outputs dmo
          INNER JOIN attribution_order_inputs inputs
            ON inputs.run_id = dmo.run_id
           AND inputs.order_id = dmo.order_id
          WHERE inputs.order_occurred_at_utc >= $1::date
            AND inputs.order_occurred_at_utc < ($2::date + interval '1 day')
            ${deterministicViewFilters.sql}
        `,
				[input.startDate, input.endDate, ...deterministicViewFilters.params],
			);

			const clickTotals = toReportingTotals(clickResult.rows[0]);
			const deterministicViewTotals = toReportingTotals(
				deterministicViewResult.rows[0],
			);
			const metaViewThroughTotals = toReportingTotals(
				metaViewThroughResult.rows[0],
			);
			const combinedTotals = combineReportingTotals(
				clickTotals,
				deterministicViewTotals,
			);
			const selectedTotals =
				input.reportingMode === "clicks"
					? clickTotals
					: input.reportingMode === "deterministic_views"
						? deterministicViewTotals
						: input.reportingMode === "meta_view_through"
							? metaViewThroughTotals
						: combinedTotals;
      const modeMetadata = REPORTING_MODE_METADATA[input.reportingMode];
			const modelFreshness = modelFreshnessResult.rows[0];
			const latestModelOutputAt =
				modelFreshness?.latest_model_output_at instanceof Date
					? modelFreshness.latest_model_output_at.toISOString()
					: modelFreshness?.latest_model_output_at ?? null;
			const modelOutputFreshnessHours = latestModelOutputAt
				? Number(
						((Date.now() - new Date(latestModelOutputAt).getTime()) / 3_600_000).toFixed(2),
					)
				: 9999;

			logInfo("combined_report_api_health", {
				service: process.env.K_SERVICE ?? "roas-radar",
				path: "/api/reporting/summary",
				reportingMode: input.reportingMode,
				startDate: input.startDate,
				endDate: input.endDate,
				source: input.source ?? null,
				campaign: input.campaign ?? null,
				durationMs: Date.now() - requestStartedAt,
				status: "success",
				statusClass: "2xx",
				combinedOrders: combinedTotals.orders,
				combinedRevenue: combinedTotals.revenue,
				deterministicViewOrders: deterministicViewTotals.orders,
				modelOutputCount: Number(modelFreshness?.model_output_count ?? 0),
				latestModelOutputAt,
				modelOutputFreshnessHours,
				modelOutputFreshnessStatus:
					modelOutputFreshnessHours >= 24 ? "stale" : "healthy",
			});

			res.json({
				range: {
					startDate: input.startDate,
					endDate: input.endDate,
				},
				reportingMode: input.reportingMode,
        reportingModeLabel: modeMetadata.label,
        totalsLabel: modeMetadata.label,
        totalsCanonical: modeMetadata.canonical,
        totalsDescription: modeMetadata.description,
				totals: selectedTotals,
        comparisonTotals: {
          combined: {
            ...REPORTING_MODE_METADATA.combined,
            totals: combinedTotals
          }
        },
				layers: {
          clicks: {
            ...REPORTING_MODE_METADATA.clicks,
            totals: clickTotals
          },
          deterministicViews: {
            ...REPORTING_MODE_METADATA.deterministic_views,
            totals: deterministicViewTotals
          },
          metaViewThrough: {
            ...REPORTING_MODE_METADATA.meta_view_through,
            totals: metaViewThroughTotals
          },
				},
			});
		} catch (error) {
			const statusCode =
				error instanceof ReportingHttpError ? error.statusCode : 500;
			logError("combined_report_api_health", error, {
				service: process.env.K_SERVICE ?? "roas-radar",
				path: "/api/reporting/summary",
				durationMs: Date.now() - requestStartedAt,
				status: statusCode >= 500 ? "error" : "client_error",
				statusCode,
				statusClass: `${Math.floor(statusCode / 100)}xx`,
			});
			next(error);
		}
	});

	router.get("/campaigns", async (req, res, next) => {
		try {
			const input = parseInput(campaignsQuerySchema, req.query);
			const filters = buildMetricDimensionFilters(
				input.attributionModel,
				input.source,
				input.campaign,
			);
				const result = await query<CampaignRow>(
				`
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
        `,
					[input.startDate, input.endDate, ...filters.params, input.limit],
				);
				const campaignMetadata = await resolveCampaignDisplayMetadata(
					input.startDate,
					input.endDate,
					result.rows.map((row) => row.campaign),
					input.source,
				);

				res.json({
					rows: result.rows.map((row) => {
						const visits = Number(row.visits);
						const orders = Number(row.orders);
						const revenue = Number(row.revenue);
						const resolution = selectCampaignResolution(campaignMetadata, row);

						return {
							source: resolveReportRowSource(row, resolution),
							medium: row.medium,
							campaign: row.campaign,
							content: resolveReportRowContent(row, resolution),
							visits,
							orders,
							revenue,
							conversionRate: visits > 0 ? orders / visits : 0,
							...buildCampaignLabelFields(resolution, {
								source: resolveReportRowSource(row, resolution),
								rawId: row.campaign,
							}),
						};
					}),
					nextCursor: null,
				});
		} catch (error) {
			next(error);
		}
	});

	router.get("/spend-details", async (req, res, next) => {
		try {
			const input = parseInput(baseFiltersSchema, req.query);
			const filters = buildMetricDimensionFilters(
				input.attributionModel,
				input.source,
				input.campaign,
			);
			const result = await query<SpendDetailRow>(
				`
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
        `,
				[input.startDate, input.endDate, ...filters.params],
			);

			const groupMap = new Map<
				string,
					{
						source: string;
						medium: string;
						channel: string;
						subtotal: number;
						campaigns: Array<
							{
								campaign: string;
								spend: number;
							} & CampaignLabelResponseFields
						>;
					}
				>();
				const campaignMetadata = await resolveCampaignDisplayMetadata(
					input.startDate,
					input.endDate,
					result.rows.map((row) => row.campaign),
					input.source,
				);

				for (const row of result.rows) {
					const spend = Number(row.spend);
					const source = row.source;
					const medium = row.medium;
					const groupKey = `${source}\u0000${medium}`;
					const existingGroup = groupMap.get(groupKey);
					const resolution = selectCampaignResolution(campaignMetadata, row);
					const displaySource = resolveReportRowSource(row, resolution);
					const labelFields = buildCampaignLabelFields(resolution, {
						source: displaySource,
						rawId: row.campaign,
					});

					if (existingGroup) {
						existingGroup.subtotal += spend;
						existingGroup.campaigns.push({
							campaign: row.campaign,
							spend,
							...labelFields,
						});
						continue;
					}

				groupMap.set(groupKey, {
					source: displaySource,
					medium,
					channel: `${displaySource} / ${medium}`,
					subtotal: spend,
						campaigns: [
							{
								campaign: row.campaign,
								spend,
								...labelFields,
							},
						],
					});
				}

			const groups = [...groupMap.values()].sort(
				(left, right) =>
					right.subtotal - left.subtotal ||
					left.channel.localeCompare(right.channel),
			);
			const totalSpend = groups.reduce((sum, group) => sum + group.subtotal, 0);
			const rangeDays = countDaysInRange(input.startDate, input.endDate);
			const topChannel = groups[0]
				? {
						source: groups[0].source,
						medium: groups[0].medium,
						channel: groups[0].channel,
						spend: groups[0].subtotal,
					}
				: null;

			res.json({
				summary: {
					totalSpend,
					activeChannels: groups.length,
					activeCampaigns: result.rows.length,
					averageDailySpend: rangeDays > 0 ? totalSpend / rangeDays : 0,
					topChannel,
				},
				groups,
				totalSpend,
			});
		} catch (error) {
			next(error);
		}
	});

	router.get("/timeseries", async (req, res, next) => {
		try {
			const input = parseInput(timeseriesQuerySchema, req.query);
			const filters = buildMetricDimensionFilters(
				input.attributionModel,
				input.source,
				input.campaign,
			);
			const groupExpr =
				input.groupBy === "source"
					? "source"
					: input.groupBy === "campaign"
						? "campaign"
						: "metric_date::text";
				const result = await query<TimeseriesRow>(
				`
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
        `,
					[input.startDate, input.endDate, ...filters.params],
				);
				const campaignMetadata =
					input.groupBy === "campaign"
						? await resolveCampaignDisplayMetadata(
								input.startDate,
								input.endDate,
								result.rows.map((row) => row.bucket),
								input.source,
							)
						: null;

				const bucketMetrics = result.rows.map((row) => {
					const metrics = calculatePerformanceMetrics({
						visits: row.visits,
					orders: row.orders,
					attributedRevenue: row.revenue,
					spend: row.spend,
				});

				return {
					bucket: row.bucket,
					visits: metrics.visits,
					orders: metrics.orders,
					revenue: metrics.attributedRevenue,
						spend: metrics.spend,
						conversionRate: metrics.conversionRate,
						roas: metrics.roas,
						...(input.groupBy === "campaign"
							? buildCampaignLabelFields(campaignMetadata?.byCampaign.get(row.bucket), {
									rawId: row.bucket,
								})
							: {}),
					};
				});

			res.json({
				points: bucketMetrics.map((row) => ({
					date: row.bucket,
						visits: row.visits,
						orders: row.orders,
						revenue: row.revenue,
						...(input.groupBy === "campaign"
							? buildCampaignLabelFields(campaignMetadata?.byCampaign.get(row.bucket), {
									rawId: row.bucket,
								})
							: {}),
					})),
				lowestBuckets: [...bucketMetrics]
					.sort(
						(left, right) =>
							left.revenue - right.revenue ||
							left.orders - right.orders ||
							left.visits - right.visits ||
							left.bucket.localeCompare(right.bucket),
					)
					.slice(0, 3),
			});
		} catch (error) {
			next(error);
		}
	});

	router.get("/model-comparison", async (req, res, next) => {
		try {
			const input = parseInput(modelComparisonQuerySchema, req.query);
			if (input.sourceOfTruth === "mmm") {
				const filters = buildMmmWeeklyChannelFilters(input);
				const result = await query<MmmWeeklyChannelReportingRow>(
					`
          SELECT
            week_start_date::text AS bucket,
            week_end_date::text,
            attribution_model,
            channel_key,
            source,
            medium,
            campaign,
            channel,
            channel_group,
            COALESCE(SUM(spend), 0) AS spend,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(shopify_orders), 0) AS shopify_orders,
            COALESCE(SUM(shopify_revenue), 0) AS shopify_revenue,
            COALESCE(SUM(attribution_credit_orders), 0) AS attribution_credit_orders,
            COALESCE(SUM(attribution_credit_revenue), 0) AS attribution_credit_revenue,
            CASE
              WHEN COUNT(*) FILTER (WHERE dq_status = 'fail') > 0 THEN 'fail'
              WHEN COUNT(*) FILTER (WHERE dq_status = 'warn') > 0 THEN 'warn'
              ELSE 'pass'
            END AS dq_status,
            COALESCE(SUM(source_row_count), 0) AS source_row_count,
            MAX(generated_at) AS generated_at
          FROM mmm_weekly_channel_input_mart_v1
          WHERE week_start_date <= date_trunc('week', $2::date)::date
            AND week_end_date >= date_trunc('week', $1::date)::date
          ${filters.sql}
          GROUP BY bucket, week_end_date, attribution_model, channel_key, source, medium, campaign, channel, channel_group
          ORDER BY bucket ASC, source ASC, medium ASC, campaign ASC, attribution_model ASC
        `,
					[input.startDate, input.endDate, ...filters.params],
				);
				const runResult = await query<MmmModelRunForReportingRow>(
					`
          SELECT DISTINCT ON (attribution_model)
            id::text,
            model_type,
            model_version,
            mart_version,
            attribution_model,
            training_start_date::text,
            training_end_date::text,
            input_summary,
            model_artifact,
            calibration_report,
            validation_report,
            completed_at
          FROM mmm_model_runs
          WHERE run_status = 'completed'
            AND mart_version = 'mmm_weekly_channel_input_mart_v1'
            AND training_end_date >= $1::date
            AND training_start_date <= $2::date
            ${input.attributionModel ? "AND attribution_model = $3" : ""}
          ORDER BY attribution_model, completed_at DESC NULLS LAST, created_at DESC
        `,
					input.attributionModel
						? [input.startDate, input.endDate, input.attributionModel]
						: [input.startDate, input.endDate],
				);
				const runsByAttributionModel = new Map(
					runResult.rows.map((row) => [row.attribution_model, row]),
				);
				const contributionByModelAndChannel = new Map<
					string,
					NonNullable<
						NonNullable<
							MmmModelArtifact["contributionOutputs"]
						>["channels"]
					>[number]
				>();

				for (const run of runResult.rows) {
					for (const channel of run.model_artifact?.contributionOutputs?.channels ??
						[]) {
						const key = [
							run.attribution_model,
							channel.source ?? "unknown",
							channel.medium ?? "unknown",
							channel.campaign ?? "unknown",
						].join("\u0000");
						contributionByModelAndChannel.set(key, channel);
					}
				}

				const rows = result.rows.map((row) => {
					const metrics = calculatePerformanceMetrics({
						visits: row.clicks,
						orders: row.attribution_credit_orders,
						attributedRevenue: row.attribution_credit_revenue,
						spend: row.spend,
					});
					const run = runsByAttributionModel.get(row.attribution_model);
					const contribution = contributionByModelAndChannel.get(
						[
							row.attribution_model,
							row.source || "unknown",
							row.medium || "unknown",
							row.campaign || "unknown",
						].join("\u0000"),
					);

					return {
						bucket: row.bucket,
						dateGrain: "week",
						sourceOfTruth: "mmm",
						attributionModel: row.attribution_model,
						reportingView: "mmm_weekly_channel",
						source: row.source,
						medium: row.medium,
						campaign: row.campaign,
						channel: row.channel,
						channelGroup: row.channel_group,
						visits: metrics.visits,
						orders: metrics.orders,
						revenue: metrics.attributedRevenue,
						spend: metrics.spend,
						conversionRate: metrics.conversionRate,
						roas: metrics.roas,
						impressions: Number(row.impressions),
						clicks: Number(row.clicks),
						shopifyOrders: Number(row.shopify_orders),
						shopifyRevenue: Number(row.shopify_revenue),
						mmmContribution: contribution?.contribution ?? null,
						mmmContributionShare: contribution?.contributionShare ?? null,
						posteriorProbabilityPositive:
							contribution?.posteriorProbabilityPositive ?? null,
						tierBreakdown: {
							strictDeterministicOrders: 0,
							fallbackIncludedOrders: 0,
							blendedDeterministicOrders: metrics.orders,
						},
						provenance: {
							sourceOfTruth: "mmm",
							martVersion: "mmm_weekly_channel_input_mart_v1",
							sourceMartVersion: "mmm_daily_input_mart_v1",
							modelRunId: run?.id ?? null,
							modelType: run?.model_type ?? null,
							modelVersion: run?.model_version ?? null,
							trainingStartDate: run?.training_start_date ?? null,
							trainingEndDate: run?.training_end_date ?? null,
							completedAt: toIsoString(run?.completed_at),
							generatedAt: toIsoString(row.generated_at),
							dqStatus: row.dq_status,
							sourceRowCount: Number(row.source_row_count),
							inputSummary: run?.input_summary ?? null,
							calibrationReport: run?.calibration_report ?? null,
							validationReport: run?.validation_report ?? null,
						},
					};
				});

				res.json({
					range: {
						startDate: input.startDate,
						endDate: input.endDate,
					},
					dateGrain: "week",
					sourceOfTruth: "mmm",
					provenance: {
						sourceOfTruth: "mmm",
						martVersion: "mmm_weekly_channel_input_mart_v1",
						sourceMartVersion: "mmm_daily_input_mart_v1",
						modelRunIds: runResult.rows.map((row) => row.id),
						generatedAt:
							rows.reduce<string | null>((latest, row) => {
								const generatedAt = row.provenance.generatedAt;
								return generatedAt && (!latest || generatedAt > latest)
									? generatedAt
									: latest;
							}, null) ?? null,
					},
					rows,
				});
				return;
			}

			const filters = buildModelComparisonFilters(input);
			const bucketExpr =
				input.dateGrain === "week"
					? "date_trunc('week', metric_date::timestamp)::date::text"
					: "metric_date::text";
			const result = await query<ModelComparisonRow>(
				`
          SELECT
            ${bucketExpr} AS bucket,
            attribution_model,
            reporting_view,
            source,
            medium,
            campaign,
            COALESCE(SUM(visits), 0) AS visits,
            COALESCE(SUM(attributed_orders), 0) AS attributed_orders,
            COALESCE(SUM(attributed_revenue), 0) AS attributed_revenue,
            COALESCE(SUM(spend), 0) AS spend,
            COALESCE(SUM(strict_deterministic_orders), 0) AS strict_deterministic_orders,
            COALESCE(SUM(fallback_included_orders), 0) AS fallback_included_orders,
            COALESCE(SUM(blended_deterministic_orders), 0) AS blended_deterministic_orders
          FROM reporting_model_comparison_daily
          WHERE metric_date BETWEEN $1::date AND $2::date
          ${filters.sql}
          GROUP BY bucket, attribution_model, reporting_view, source, medium, campaign
          ORDER BY bucket ASC, source ASC, medium ASC, campaign ASC, attribution_model ASC, reporting_view ASC
        `,
				[input.startDate, input.endDate, ...filters.params],
			);

			const rows = result.rows.map((row) => {
				const metrics = calculatePerformanceMetrics({
					visits: row.visits,
					orders: row.attributed_orders,
					attributedRevenue: row.attributed_revenue,
					spend: row.spend,
				});

				return {
					bucket: row.bucket,
					dateGrain: input.dateGrain,
					sourceOfTruth: "deterministic",
					attributionModel: row.attribution_model,
					reportingView: row.reporting_view,
					source: row.source,
					medium: row.medium,
					campaign: row.campaign,
					visits: metrics.visits,
					orders: metrics.orders,
					revenue: metrics.attributedRevenue,
					spend: metrics.spend,
					conversionRate: metrics.conversionRate,
					roas: metrics.roas,
					tierBreakdown: {
						strictDeterministicOrders: Number(row.strict_deterministic_orders),
						fallbackIncludedOrders: Number(row.fallback_included_orders),
						blendedDeterministicOrders: Number(row.blended_deterministic_orders),
					},
					provenance: {
						sourceOfTruth: "deterministic",
						martVersion: "reporting_model_comparison_daily_v1",
						sourceMartVersion: "daily_reporting_metrics_v1",
						modelRunId: null,
						modelType: null,
						modelVersion: null,
						trainingStartDate: null,
						trainingEndDate: null,
						completedAt: null,
						generatedAt: null,
						dqStatus: null,
						sourceRowCount: null,
						inputSummary: null,
						calibrationReport: null,
						validationReport: null,
					},
				};
			});

			res.json({
				range: {
					startDate: input.startDate,
					endDate: input.endDate,
				},
				dateGrain: input.dateGrain,
				sourceOfTruth: "deterministic",
				provenance: {
					sourceOfTruth: "deterministic",
					martVersion: "reporting_model_comparison_daily_v1",
					sourceMartVersion: "daily_reporting_metrics_v1",
					modelRunIds: [],
					generatedAt: null,
				},
				rows,
			});
		} catch (error) {
			next(error);
		}
	});

  router.get('/orders', async (req, res, next) => {
    try {
      const input = parseInput(ordersQuerySchema, req.query);
      const filters = buildOrderAttributionFilters(input.attributionModel, input.source, input.campaign, input.attributionTier);
      const reportingTimezone = await getReportingTimezone();
      const result = await query<OrderAttributionRow>(
        `
          SELECT
            o.shopify_order_id,
            COALESCE(o.processed_at, o.created_at_shopify, o.ingested_at) AS processed_at,
            o.total_price,
            o.attribution_tier,
            o.attribution_source,
            sources.code AS attribution_source_code,
            methods.code AS matching_method_code,
            o.attribution_reason AS order_attribution_reason,
            o.attribution_matched_at,
            o.attribution_confidence_score::text AS attribution_confidence_score,
            o.last_attribution_run_at,
            o.attribution_snapshot,
            c.attributed_source,
            c.attributed_medium,
            c.attributed_campaign,
            c.attribution_reason AS primary_credit_attribution_reason
          FROM shopify_orders o
          LEFT JOIN attribution_sources sources
            ON sources.id = o.attribution_source_id
          LEFT JOIN matching_methods methods
            ON methods.id = o.matching_method_id
          LEFT JOIN LATERAL (
            SELECT
              attributed_source,
              attributed_medium,
              attributed_campaign,
              match_source,
              confidence_label,
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
        `,
				[
					input.startDate,
					input.endDate,
					...filters.params,
					reportingTimezone,
					input.limit,
				],
			);
      const orderRowsWithMetadata = result.rows.map((row) => {
        const metadata = extractOrderAttributionMetadata(row.attribution_snapshot);

        return {
          row,
          metadata,
          attribution: {
            source: row.attributed_source ?? metadata.winner.source,
            medium: row.attributed_medium ?? metadata.winner.medium,
            campaign: row.attributed_campaign ?? metadata.winner.campaign
          }
        };
      });
      const campaignMetadata = await resolveCampaignDisplayMetadata(
        input.startDate,
        input.endDate,
        orderRowsWithMetadata
          .map((entry) => entry.attribution.campaign)
          .filter((campaign): campaign is string => Boolean(campaign)),
        input.source
      );

      const googleCpcMissingCampaignIds: string[] = [];
      let googleCpcAttributionRowCount = 0;
      const rows = orderRowsWithMetadata.map(({ row, metadata, attribution }) => {
          const attributionTier = normalizeAttributionTier(row.attribution_tier);
          const orderAttributionReason = row.order_attribution_reason ?? 'unattributed';
          const campaignResolution = selectNullableCampaignResolution(campaignMetadata, attribution);
          const creditAttribution = {
            source: row.attributed_source,
            medium: row.attributed_medium,
            campaign: row.attributed_campaign
          };

          if (isGoogleCpcAttributedCampaign(creditAttribution)) {
            googleCpcAttributionRowCount += 1;

            if (!campaignResolution?.campaignDisplayName) {
              googleCpcMissingCampaignIds.push(creditAttribution.campaign);
            }
          }

          return {
            shopifyOrderId: row.shopify_order_id,
            processedAt: row.processed_at?.toISOString() ?? null,
            orderOccurredAtUtc: row.processed_at?.toISOString() ?? null,
            totalPrice: Number(row.total_price),
            source: attribution.source,
            medium: attribution.medium,
            campaign: attribution.campaign,
            campaignName: campaignResolution?.campaignDisplayName ?? null,
            ...buildCampaignLabelFields(campaignResolution, {
              source: attribution.source ?? undefined,
              rawId: attribution.campaign ?? undefined
            }),
            attributionReason: orderAttributionReason,
            primaryCreditAttributionReason: row.primary_credit_attribution_reason ?? orderAttributionReason,
            attributionTier,
            attributionTierLabel: ATTRIBUTION_TIER_LABELS[attributionTier],
            attributionTierDescription: ATTRIBUTION_TIER_DESCRIPTIONS[attributionTier],
            attributionSource: readAttributionLookupCode(
              row.attribution_source,
              row.attribution_source_code,
              row.last_attribution_run_at
            ),
            matchingMethod: readAttributionLookupCode(null, row.matching_method_code, row.last_attribution_run_at),
            attributionMatchedAt: row.attribution_matched_at?.toISOString() ?? null,
            confidenceScore: readOrderConfidenceScore(
              row.attribution_confidence_score,
              row.last_attribution_run_at,
              metadata.confidenceScore
            ),
            lastAttributionRunAt: row.last_attribution_run_at?.toISOString() ?? null,
            sessionId: metadata.winner.sessionId
          };
        });

      emitGoogleCpcMissingCampaignNameLog({
        endpoint: 'reporting_orders_list',
        startDate: input.startDate,
        endDate: input.endDate,
        attributionRowCount: googleCpcAttributionRowCount,
        missingCampaignNameCount: googleCpcMissingCampaignIds.length,
        campaignIds: googleCpcMissingCampaignIds
      });

      res.json({ rows });
    } catch (error) {
      next(error);
    }
  });

	router.get("/orders/:shopifyOrderId", async (req, res, next) => {
		try {
			const { shopifyOrderId } = parseInput(
				orderDetailsParamsSchema,
				req.params,
			);

			const orderResult = await query<OrderDetailsRow>(
				`
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
            sources.code AS attribution_source_code,
            methods.code AS matching_method_code,
            o.attribution_matched_at,
            o.attribution_reason,
            o.attribution_confidence_score::text AS attribution_confidence_score,
            o.last_attribution_run_at,
            o.attribution_snapshot,
            o.attribution_snapshot_updated_at,
            results.attribution_model AS result_attribution_model,
            results.session_id::text AS result_session_id,
            results.attributed_source AS result_attributed_source,
            results.attributed_medium AS result_attributed_medium,
            results.attributed_campaign AS result_attributed_campaign,
            results.attributed_content AS result_attributed_content,
            results.attributed_term AS result_attributed_term,
            results.attributed_click_id_type AS result_attributed_click_id_type,
            results.attributed_click_id_value AS result_attributed_click_id_value,
            o.ingested_at,
            o.attribution_snapshot,
            o.raw_payload
          FROM shopify_orders o
          LEFT JOIN attribution_sources sources
            ON sources.id = o.attribution_source_id
          LEFT JOIN matching_methods methods
            ON methods.id = o.matching_method_id
          LEFT JOIN attribution_results results
            ON results.shopify_order_id = o.shopify_order_id
          WHERE o.shopify_order_id = $1
          LIMIT 1
        `,
				[shopifyOrderId],
			);

			if (!orderResult.rowCount) {
				throw new ReportingHttpError(
					404,
					"order_not_found",
					`Shopify order ${shopifyOrderId} was not found`,
				);
			}

			const lineItemsResult = await query<OrderLineItemRow>(
				`
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
        `,
				[shopifyOrderId],
			);

			const creditsResult = await query<AttributionCreditRow>(
				`
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
            c.match_source,
            c.confidence_label,
            c.created_at,
            c.model_version
          FROM attribution_order_credits c
          WHERE c.shopify_order_id = $1
          ORDER BY c.attribution_model ASC, c.touchpoint_position ASC
        `,
				[shopifyOrderId],
			);

      const order = orderResult.rows[0];
      const metadata = extractOrderAttributionMetadata(order.attribution_snapshot);
      const primaryCredit = selectPrimaryAttributionCredit(creditsResult.rows, order.result_attribution_model);
      const orderAttribution = buildPersistedOrderAttribution(order, primaryCredit, metadata);
      const attributionSources = [
        orderAttribution.source,
        ...creditsResult.rows.map((row) => row.attributed_source)
      ].filter((source): source is string => Boolean(source));
      const scopedAttributionSources = [...new Set(attributionSources)];
      const campaignMetadata = await resolveCampaignDisplayMetadata(
        order.processed_at?.toISOString().slice(0, 10) ??
          order.created_at_shopify?.toISOString().slice(0, 10) ??
          order.ingested_at.toISOString().slice(0, 10),
        order.processed_at?.toISOString().slice(0, 10) ??
          order.created_at_shopify?.toISOString().slice(0, 10) ??
          order.ingested_at.toISOString().slice(0, 10),
        [
          orderAttribution.campaign,
          ...creditsResult.rows.map((row) => row.attributed_campaign)
        ].filter((campaign): campaign is string => Boolean(campaign)),
        scopedAttributionSources.length === 1 ? scopedAttributionSources[0] : undefined
      );
      const orderCampaignResolution = selectNullableCampaignResolution(campaignMetadata, orderAttribution);
      const orderDate =
        toDateString(order.processed_at) ??
        toDateString(order.created_at_shopify) ??
        toDateString(order.ingested_at);
      const googleCpcMissingCampaignIds: string[] = [];
      let googleCpcAttributionRowCount = 0;
      const attributionCredits = creditsResult.rows.map((row) => {
        const creditAttribution = {
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign
        };
        const campaignResolution = selectNullableCampaignResolution(campaignMetadata, creditAttribution);

        if (isGoogleCpcAttributedCampaign(creditAttribution)) {
          googleCpcAttributionRowCount += 1;

          if (!campaignResolution?.campaignDisplayName) {
            googleCpcMissingCampaignIds.push(creditAttribution.campaign);
          }
        }

        return {
          attributionModel: row.attribution_model,
          touchpointPosition: row.touchpoint_position,
          sessionId: row.session_id,
          touchpointOccurredAt: row.touchpoint_occurred_at?.toISOString() ?? null,
          source: row.attributed_source,
          medium: row.attributed_medium,
          campaign: row.attributed_campaign,
          campaignName: campaignResolution?.campaignDisplayName ?? null,
          ...buildCampaignLabelFields(campaignResolution, {
            source: row.attributed_source ?? undefined,
            rawId: row.attributed_campaign ?? undefined
          }),
          content: row.attributed_content,
          term: row.attributed_term,
          clickIdType: row.attributed_click_id_type,
          clickIdValue: row.attributed_click_id_value,
          creditWeight: Number(row.credit_weight),
          revenueCredit: Number(row.revenue_credit),
          isPrimary: row.is_primary,
          attributionReason: row.attribution_reason,
          matchSource: row.match_source,
          confidenceLabel: row.confidence_label,
          createdAt: row.created_at.toISOString(),
          modelVersion: row.model_version
        };
      });

      emitGoogleCpcMissingCampaignNameLog({
        endpoint: 'reporting_order_details',
        startDate: orderDate,
        endDate: orderDate,
        attributionRowCount: googleCpcAttributionRowCount,
        missingCampaignNameCount: googleCpcMissingCampaignIds.length,
        campaignIds: googleCpcMissingCampaignIds
      });

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
          orderOccurredAtUtc:
            order.processed_at?.toISOString() ??
            order.created_at_shopify?.toISOString() ??
            order.ingested_at.toISOString(),
          attributionTier: normalizeAttributionTier(order.attribution_tier),
          attributionTierLabel: ATTRIBUTION_TIER_LABELS[normalizeAttributionTier(order.attribution_tier)],
          attributionTierDescription: ATTRIBUTION_TIER_DESCRIPTIONS[normalizeAttributionTier(order.attribution_tier)],
          attributionSource: readAttributionLookupCode(
            order.attribution_source,
            order.attribution_source_code,
            order.last_attribution_run_at
          ),
          matchingMethod: readAttributionLookupCode(null, order.matching_method_code, order.last_attribution_run_at),
          attributionMatchedAt: order.attribution_matched_at?.toISOString() ?? null,
          attributionReason: order.attribution_reason ?? 'unattributed',
          confidenceScore: readOrderConfidenceScore(
            order.attribution_confidence_score,
            order.last_attribution_run_at,
            metadata.confidenceScore
          ),
          lastAttributionRunAt: order.last_attribution_run_at?.toISOString() ?? null,
          sessionId: orderAttribution.sessionId,
          attributedSource: orderAttribution.source,
          attributedMedium: orderAttribution.medium,
          attributedCampaign: orderAttribution.campaign,
          attributedCampaignName: orderCampaignResolution?.campaignDisplayName ?? null,
          ...buildCampaignLabelFields(orderCampaignResolution, {
            source: orderAttribution.source ?? undefined,
            rawId: orderAttribution.campaign ?? undefined
          }),
          attributedContent: orderAttribution.content,
          attributedTerm: orderAttribution.term,
          attributedClickIdType: orderAttribution.clickIdType,
          attributedClickIdValue: orderAttribution.clickIdValue,
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
        attributionCredits
      });
    } catch (error) {
      next(error);
    }
  });

	router.get("/reconciliation", async (req, res, next) => {
		try {
			const input = parseInput(reconciliationQuerySchema, req.query);
			const runDate = input.runDate ?? resolveRunDate();
			const report = await fetchDataQualityReport(runDate);

			res.json({
				version: "2026-04-11",
				tenantId: "roas-radar",
				data: report,
			});
		} catch (error) {
			next(error);
		}
	});

	return router;
}
