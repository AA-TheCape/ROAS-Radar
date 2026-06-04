import type {
	AttributionCreditRecordV1,
	AttributionExplainRecordV1,
	AttributionLookbackRule,
	AttributionModelKey,
	AttributionResultRecordV1,
	AttributionTouchpointInputV1,
	OrderAttributionBackfillEnqueueResponse,
	OrderAttributionBackfillJobResponse,
	OrderAttributionBackfillRequest,
	OrderAttributionBackfillSubmittedOptions,
} from "../../../packages/attribution-schema/index.js";
import {
	orderAttributionBackfillEnqueueResponseSchema,
	orderAttributionBackfillJobResponseSchema,
	orderAttributionBackfillRequestSchema,
} from "../../../packages/attribution-schema/index.js";

export type {
	OrderAttributionBackfillEnqueueResponse,
	OrderAttributionBackfillJobResponse,
	OrderAttributionBackfillRequest,
	OrderAttributionBackfillSubmittedOptions,
};

export type AttributionTier =
  | 'deterministic_first_party'
  | 'deterministic_shopify_hint'
  | 'ga4_fallback'
  | 'unattributed';

export type ReportingFilters = {
  startDate: string;
  endDate: string;
  sourceOfTruth?: 'deterministic' | 'mmm';
  attributionTier?: AttributionTier | '';
  attributionModel?:
    | 'first_touch'
    | 'last_touch'
    | 'linear'
    | 'time_decay'
    | 'position_based'
    | 'rule_based_weighted';
  source?: string;
  campaign?: string;
};

export type AttributionFilters = {
  startDate: string;
  endDate: string;
  source?: string;
  medium?: string;
  campaign?: string;
  orderId?: string;
};

export type SummaryTotals = {
	visits: number;
	orders: number;
	revenue: number;
	spend: number;
	conversionRate: number;
	roas: number | null;
};

export type SummaryResponse = {
	range: {
		startDate: string;
		endDate: string;
	};
	totals: SummaryTotals;
};

export type CampaignRow = {
	source: string;
	medium: string;
	campaign: string;
	content: string | null;
	visits: number;
	orders: number;
	revenue: number;
	conversionRate: number;
	campaignDisplayName?: string;
	campaignEntityId?: string | null;
	campaignPlatform?: "google_ads" | "meta_ads" | null;
	campaignNameResolutionStatus?: "resolved" | "fallback_name" | "unresolved";
	campaignLabel?: CampaignLabel;
};

export type CampaignsResponse = {
	rows: CampaignRow[];
	nextCursor: string | null;
};

export type SpendDetailCampaignRow = {
	campaign: string;
	spend: number;
	campaignDisplayName?: string;
	campaignEntityId?: string | null;
	campaignPlatform?: "google_ads" | "meta_ads" | null;
	campaignNameResolutionStatus?: "resolved" | "fallback_name" | "unresolved";
	campaignLabel?: CampaignLabel;
};

export type SpendDetailChannelGroup = {
	source: string;
	medium: string;
	channel: string;
	subtotal: number;
	campaigns: SpendDetailCampaignRow[];
};

export type SpendDetailsSummary = {
	totalSpend: number;
	activeChannels: number;
	activeCampaigns: number;
	averageDailySpend: number;
	topChannel: {
		source: string;
		medium: string;
		channel: string;
		spend: number;
	} | null;
};

export type SpendDetailsResponse = {
	summary: SpendDetailsSummary;
	groups: SpendDetailChannelGroup[];
	totalSpend: number;
};

export type TimeseriesGroupBy = "day" | "source" | "campaign";

export type TimeseriesPoint = {
	date: string;
	visits: number;
	orders: number;
	revenue: number;
	campaignDisplayName?: string;
	campaignEntityId?: string | null;
	campaignPlatform?: "google_ads" | "meta_ads" | null;
	campaignNameResolutionStatus?: "resolved" | "fallback_name" | "unresolved";
	campaignLabel?: CampaignLabel;
};

export type TimeseriesResponse = {
	points: TimeseriesPoint[];
	lowestBuckets: Array<{
		bucket: string;
		visits: number;
		orders: number;
		revenue: number;
		spend: number;
		conversionRate: number;
		roas: number | null;
		campaignDisplayName?: string;
		campaignEntityId?: string | null;
		campaignPlatform?: "google_ads" | "meta_ads" | null;
		campaignNameResolutionStatus?: "resolved" | "fallback_name" | "unresolved";
		campaignLabel?: CampaignLabel;
	}>;
};

export type ReportingModelComparisonRow = {
	bucket: string;
	dateGrain: 'day' | 'week';
	sourceOfTruth: 'deterministic' | 'mmm';
	attributionModel: string;
	reportingView: 'strict_deterministic' | 'fallback_included' | 'blended_deterministic' | 'mmm_weekly_channel';
	source: string;
	medium: string;
	campaign: string;
	channel?: string;
	channelGroup?: string;
	visits: number;
	orders: number;
	revenue: number;
	spend: number;
	conversionRate: number;
	roas: number | null;
	impressions?: number;
	clicks?: number;
	shopifyOrders?: number;
	shopifyRevenue?: number;
	mmmContribution?: {
		mean: number;
		median: number;
		credibleInterval80: {
			lower: number;
			upper: number;
		};
		credibleInterval95: {
			lower: number;
			upper: number;
		};
	} | null;
	mmmContributionShare?: {
		mean: number;
		median: number;
		credibleInterval80: {
			lower: number;
			upper: number;
		};
		credibleInterval95: {
			lower: number;
			upper: number;
		};
	} | null;
	posteriorProbabilityPositive?: number | null;
	tierBreakdown: {
		strictDeterministicOrders: number;
		fallbackIncludedOrders: number;
		blendedDeterministicOrders: number;
	};
	provenance: {
		sourceOfTruth: 'deterministic' | 'mmm';
		martVersion: string;
		sourceMartVersion: string;
		modelRunId: string | null;
		modelType: string | null;
		modelVersion: string | null;
		trainingStartDate: string | null;
		trainingEndDate: string | null;
		completedAt: string | null;
		generatedAt: string | null;
		dqStatus: string | null;
		sourceRowCount: number | null;
		inputSummary: unknown;
		calibrationReport: unknown;
		validationReport: unknown;
	};
};

export type ReportingModelComparisonResponse = {
	range: {
		startDate: string;
		endDate: string;
	};
	dateGrain: 'day' | 'week';
	sourceOfTruth: 'deterministic' | 'mmm';
	provenance: {
		sourceOfTruth: 'deterministic' | 'mmm';
		martVersion: string;
		sourceMartVersion: string;
		modelRunIds: string[];
		generatedAt: string | null;
	};
	rows: ReportingModelComparisonRow[];
};

export type CampaignLabel = {
	displayName: string;
	entityId: string | null;
	platform: "google_ads" | "meta_ads" | null;
	resolutionStatus: "resolved" | "fallback_name" | "unresolved";
	lastSeenAt: string | null;
	updatedAt: string | null;
};

export type OrderRow = {
  shopifyOrderId: string;
  processedAt: string | null;
  orderOccurredAtUtc: string | null;
  totalPrice: number;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  attributionReason: string;
  primaryCreditAttributionReason: string;
  attributionTier: AttributionTier;
  attributionTierLabel: string;
  attributionTierDescription: string;
  attributionSource: string | null;
  attributionMatchedAt: string | null;
  confidenceScore: number | null;
  sessionId: string | null;
};

export type OrdersResponse = {
	rows: OrderRow[];
};

export type MetaOrderValueSortBy =
  | 'reportDate'
  | 'campaignName'
  | 'attributedRevenue'
  | 'purchaseCount'
  | 'spend'
  | 'roas'
  | 'actionType';

export type MetaOrderValueSortDirection = 'asc' | 'desc';

export type MetaOrderValueRow = {
  date: string;
  campaignId: string;
  campaignName: string | null;
  attributedRevenue: number | null;
  purchaseCount: number | null;
  spend: number;
  roas: number | null;
  calculatedRoas: number | null;
  canonicalActionType: string | null;
  canonicalSelectionMode: 'priority' | 'fallback' | 'none';
  currency: string | null;
};

export type MetaOrderValueResponse = {
  scope: {
    organizationId: number;
  };
  range: {
    startDate: string;
    endDate: string;
  };
  filters: {
    campaignIds: string[];
    campaignSearch: string | null;
    actionType: string | null;
  };
  sort: {
    by: MetaOrderValueSortBy;
    direction: MetaOrderValueSortDirection;
  };
  pagination: {
    limit: number;
    offset: number;
    returned: number;
    totalRows: number;
    hasMore: boolean;
  };
  totals: {
    attributedRevenue: number;
    purchaseCount: number;
    spend: number;
    roas: number | null;
  };
  rows: MetaOrderValueRow[];
};

export type MetaOrderValueQuery = {
  startDate: string;
  endDate: string;
  campaignSearch?: string;
  actionType?: string;
  sortBy?: MetaOrderValueSortBy;
  sortDirection?: MetaOrderValueSortDirection;
  limit?: number;
  offset?: number;
};

export type MmmReadinessStatus = 'ready' | 'partial' | 'not_ready';

export type MmmExcludedDateWindow = {
  startDate: string;
  endDate: string;
  reason: 'no_mmm_mart_rows' | 'no_rows_matching_filters';
};

export type MmmExportRow = {
  date: string;
  martVersion: string;
  martRowType: 'paid_media' | 'attribution';
  attributionModel: string;
  platform: 'meta' | 'google' | 'taxonomy';
  platformConnectionId: number | null;
  granularity: string;
  entityKey: string;
  accountId: string | null;
  accountName: string | null;
  campaignId: string | null;
  campaignName: string | null;
  adsetId: string | null;
  adsetName: string | null;
  adId: string | null;
  adName: string | null;
  creativeId: string | null;
  creativeName: string | null;
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
  currency: string | null;
  spend: number;
  impressions: number;
  clicks: number;
  shopifyOrders: number;
  shopifyRevenue: number;
  attributionCreditOrders: number;
  attributionCreditRevenue: number;
  newCustomerCreditOrders: number;
  returningCustomerCreditOrders: number;
  newCustomerCreditRevenue: number;
  returningCustomerCreditRevenue: number;
  matchSourceCoverage: unknown;
  confidenceLabelCoverage: unknown;
  spendLastSyncedAt: string | null;
  shopifyLastIngestedAt: string | null;
  attributionLastComputedAt: string | null;
  lastComputedAt: string | null;
};

export type MmmExportResponse = {
  schemaVersion: 'mmm_daily_input_mart_v1';
  range: {
    startDate: string;
    endDate: string;
  };
  filters: {
    martRowType: 'paid_media' | 'attribution' | null;
    attributionModel: string | null;
    platform: 'meta' | 'google' | 'taxonomy' | null;
    source: string | null;
    campaign: string | null;
  };
  readiness: {
    status: MmmReadinessStatus;
    generationTimestamp: string | null;
    includedDateCount: number;
    excludedDateWindows: MmmExcludedDateWindow[];
  };
  pagination: {
    limit: number;
    offset: number;
    returned: number;
    totalRows: number;
    hasMore: boolean;
  };
  rows: MmmExportRow[];
};

export type MmmExportQuery = {
  startDate: string;
  endDate: string;
  martRowType?: 'paid_media' | 'attribution';
  attributionModel?: string;
  platform?: 'meta' | 'google' | 'taxonomy';
  source?: string;
  campaign?: string;
  limit?: number;
  offset?: number;
};

export type MmmGateChecklistStatus = 'pass' | 'warn' | 'fail' | 'pending' | 'waived';

export type MmmReadinessGateChecklistItem = {
  key: string;
  label: string;
  owner: string;
  status: MmmGateChecklistStatus;
  detail: string;
  waiverReason?: string;
};

export type MmmReadinessGateApproval = {
  owner: string;
  status: MmmGateChecklistStatus;
  approvedBy: string | null;
  approvedAt: string | null;
  detail: string;
};

export type MmmReadinessGateWaiver = {
  checklistKey: string;
  reason: string;
  expiresAt?: string;
  waivedBy: string;
  waivedAt: string;
  evidenceHash: string;
};

export type MmmReadinessGate = {
  id: string;
  gateVersion: 'mmm_readiness_gate_v1';
  range: {
    startDate: string;
    endDate: string;
  };
  filters: {
    martRowType: 'paid_media' | 'attribution' | null;
    attributionModel: string | null;
    platform: 'meta' | 'google' | 'taxonomy' | null;
    source: string | null;
    campaign: string | null;
  };
  evidencePayload: {
    exportReadiness?: MmmExportResponse['readiness'];
    exportSummary?: {
      totalRows: number;
      paidMediaRows: number;
      attributionRows: number;
      totalSpend: number;
      totalImpressions: number;
      totalClicks: number;
      totalShopifyOrders: number;
      totalShopifyRevenue: number;
      totalAttributionCreditOrders: number;
      totalAttributionCreditRevenue: number;
      latestSpendLastSyncedAt: string | null;
      latestShopifyLastIngestedAt: string | null;
      latestAttributionLastComputedAt: string | null;
      latestLastComputedAt: string | null;
      unresolvedMetadataRows: number;
    };
    exposureCoverage?: ExposureCoverageResponse['totals'] & { latestExposureAt: string | null };
    taxonomyDrift?: Record<string, unknown> | null;
    dataQualityBlockers?: Array<{
      checkKey: string;
      status: string;
      severity: string;
      discrepancyCount: number;
      summary: string;
      checkedAt: string | null;
    }>;
    latestModelRun?: MmmModelRun | null;
  };
  checklistStatuses: MmmReadinessGateChecklistItem[];
  ownerApprovals: MmmReadinessGateApproval[];
  waivers: MmmReadinessGateWaiver[];
  unresolvedCriticalIssueCount: number;
  evidenceHash: string;
  gateStatus: 'pending' | 'approved' | 'blocked';
  finalState: 'approved' | 'blocked';
  decisionReason: string | null;
  decidedBy: string | null;
  decidedAt: string | null;
  createdBy: string;
  updatedBy: string;
  createdAt: string;
  updatedAt: string;
};

export type MmmReadinessGateResponse = {
  schemaVersion: 'mmm_readiness_gate_v1';
  gate: MmmReadinessGate;
};

export type MmmReadinessGateDecisionPayload = MmmExportQuery & {
  owner?: string;
  reason?: string;
  waiver?: {
    checklistKey: string;
    reason: string;
    expiresAt?: string;
  };
};

export type MmmModelRun = {
  id: string;
  modelType: 'baseline_linear_mmm' | 'bayesian_hierarchical_mmm';
  modelVersion: 'baseline_linear_mmm_v1' | 'bayesian_hierarchical_mmm_v1';
  martVersion: 'mmm_daily_input_mart_v1' | 'mmm_weekly_channel_input_mart_v1';
  attributionModel: string;
  runStatus: 'completed' | 'failed';
  trainingStartDate: string;
  trainingEndDate: string;
  holdoutStartDate: string | null;
  holdoutEndDate: string | null;
  runConfig: Record<string, unknown>;
  inputSummary: Record<string, unknown>;
  modelArtifact: Record<string, unknown>;
  calibrationReport: Record<string, unknown>;
  validationReport: Record<string, unknown>;
  errorCode: string | null;
  errorMessage: string | null;
  createdAt: string;
  startedAt: string;
  completedAt: string | null;
};

export type MmmModelRunsResponse = {
  schemaVersion: 'mmm_model_runs_v1';
  rows: MmmModelRun[];
};

export type ExposureCoverageRow = {
  date: string;
  sourcePlatform: 'meta_ads' | 'google_ads' | 'tiktok_ads' | 'pinterest_ads' | 'snapchat_ads' | 'unknown';
  exposureType: 'impression' | 'view';
  totalExposures: number;
  validExposures: number;
  invalidExposures: number;
  identityResolvedExposures: number;
  identityUnresolvedExposures: number;
  identityResolutionRate: number | null;
  campaignJoinableExposures: number;
  campaignMetadataResolvedExposures: number;
  campaignMetadataResolutionRate: number | null;
  latestExposureAt: string | null;
};

export type ExposureCoverageResponse = {
  schemaVersion: 'ad_exposure_coverage_v1';
  range: {
    startDate: string;
    endDate: string;
  };
  filters: {
    sourcePlatform: ExposureCoverageRow['sourcePlatform'] | null;
    exposureType: ExposureCoverageRow['exposureType'] | null;
  };
  totals: Omit<ExposureCoverageRow, 'date' | 'sourcePlatform' | 'exposureType' | 'latestExposureAt'>;
  rows: ExposureCoverageRow[];
};

export type OrderDetailLineItem = {
	shopifyLineItemId: string;
	shopifyProductId: string | null;
	shopifyVariantId: string | null;
	sku: string | null;
	title: string | null;
	variantTitle: string | null;
	vendor: string | null;
	quantity: number;
	price: number;
	totalDiscount: number;
	fulfillmentStatus: string | null;
	requiresShipping: boolean | null;
	taxable: boolean | null;
	ingestedAt: string;
	rawPayload: unknown;
};

export type OrderDetailAttributionCredit = {
	attributionModel: string;
	touchpointPosition: number;
	sessionId: string | null;
	touchpointOccurredAt: string | null;
	source: string | null;
	medium: string | null;
	campaign: string | null;
	content: string | null;
	term: string | null;
	clickIdType: string | null;
	clickIdValue: string | null;
	creditWeight: number;
	revenueCredit: number;
	isPrimary: boolean;
	attributionReason: string;
	matchSource: string;
	confidenceLabel: string;
	createdAt: string;
	modelVersion: number;
};

export type OrderDetail = {
  shopifyOrderId: string;
  shopifyOrderNumber: string | null;
  shopifyCustomerId: string | null;
  customerIdentityId: string | null;
  email: string | null;
  emailHash: string | null;
  currencyCode: string;
  subtotalPrice: number;
  totalPrice: number;
  financialStatus: string | null;
  fulfillmentStatus: string | null;
  processedAt: string | null;
  createdAtShopify: string | null;
  updatedAtShopify: string | null;
  landingSessionId: string | null;
  checkoutToken: string | null;
  cartToken: string | null;
  sourceName: string | null;
  orderOccurredAtUtc: string;
  attributionTier: AttributionTier;
  attributionTierLabel: string;
  attributionTierDescription: string;
  attributionSource: string | null;
  attributionMatchedAt: string | null;
  attributionReason: string;
  confidenceScore: number | null;
  sessionId: string | null;
  attributedSource: string | null;
  attributedMedium: string | null;
  attributedCampaign: string | null;
  attributedContent: string | null;
  attributedTerm: string | null;
  attributedClickIdType: string | null;
  attributedClickIdValue: string | null;
  attributionSnapshot: unknown;
  attributionSnapshotUpdatedAt: string | null;
  ingestedAt: string;
  rawPayload: unknown;
};

export type OrderDetailsResponse = {
	order: OrderDetail;
	lineItems: OrderDetailLineItem[];
	attributionCredits: OrderDetailAttributionCredit[];
};

export type AttributionResultRow = {
  record: AttributionResultRecordV1;
  orderOccurredAtUtc: string;
  run: {
    id: string;
    status: string;
    triggerSource: string;
    submittedBy: string;
    windowStartUtc: string | null;
    windowEndUtc: string | null;
    lookbackClickWindowDays: number;
    lookbackViewWindowDays: number;
    createdAtUtc: string;
    completedAtUtc: string | null;
  };
  model: {
    key: AttributionModelKey;
    winnerSelectionRule: AttributionModelKey;
    lookbackRuleApplied: AttributionLookbackRule;
  };
  primaryTouchpoint: AttributionCreditRecordV1 | null;
};

export type AttributionResultsResponse = {
  rows: AttributionResultRow[];
  nextCursor: string | null;
};

export type AttributionChannelTotalRow = {
  modelKey: AttributionModelKey;
  source: string | null;
  medium: string | null;
  orderCount: number;
  revenueCredited: string;
  creditWeightTotal: string;
};

export type AttributionChannelTotalsResponse = {
  rows: AttributionChannelTotalRow[];
  lookbackClickWindowDays: number;
  lookbackViewWindowDays: number;
};

export type AttributionExplainabilityTouchpoint = Omit<AttributionTouchpointInputV1, 'schema_version'> & {
  runId: string;
  orderId: string;
  touchpointId: string;
  sessionId: string | null;
  identityJourneyId: string | null;
  touchpointOccurredAtUtc: string;
  touchpointCapturedAtUtc: string;
  touchpointSourceKind: AttributionTouchpointInputV1['touchpoint_source_kind'];
  ingestionSource: AttributionTouchpointInputV1['ingestion_source'];
  clickIdType: AttributionTouchpointInputV1['click_id_type'];
  clickIdValue: string | null;
  evidenceSource: AttributionTouchpointInputV1['evidence_source'];
  isDirect: boolean;
  engagementType: AttributionTouchpointInputV1['engagement_type'];
  isSynthetic: boolean;
  isEligible: boolean;
  ineligibilityReason: string | null;
  attributionReason: string | null;
  attributionHint: Record<string, unknown>;
};

export type AttributionExplainabilityResponse = {
  orderId: string;
  selectedRunReason: 'explicit_run_id' | 'latest_run_for_order';
  run: {
    id: string;
    attributionSpecVersion: 'v1';
    status: string;
    triggerSource: string;
    submittedBy: string;
    windowStartUtc: string | null;
    windowEndUtc: string | null;
    lookbackClickWindowDays: number;
    lookbackViewWindowDays: number;
    createdAtUtc: string;
    completedAtUtc: string | null;
  };
  summaries: AttributionResultRecordV1[];
  touchpoints: AttributionExplainabilityTouchpoint[];
  credits: AttributionCreditRecordV1[];
  explainability: AttributionExplainRecordV1[];
};

export type AuthUser = {
	id: number;
	email: string;
	displayName: string;
	isAdmin: boolean;
	status: "active" | "disabled";
	lastLoginAt: string | null;
	createdAt: string;
};

export type AuthLoginResponse = {
	token: string;
	user: AuthUser;
};

export type AuthMeResponse = {
	user: AuthUser;
};

export type UsersResponse = {
	users: AuthUser[];
};

export type CreateUserPayload = {
	email: string;
	password: string;
	displayName: string;
	isAdmin?: boolean;
};

export type CreateUserResponse = {
	user: AuthUser;
};

export type AppSettings = {
	reportingTimezone: string;
	updatedAt: string;
};

export type UpdateAppSettingsPayload = {
	reportingTimezone: string;
};

export type UpdateAppSettingsResponse = {
	ok: true;
	settings: AppSettings;
};

export type MetaAdsConnection = {
	id: number;
	ad_account_id: string;
	granted_scopes: string[];
	token_expires_at: string | null;
	last_refreshed_at: string | null;
	last_sync_started_at: string | null;
	last_sync_completed_at: string | null;
	last_sync_status: string;
	last_sync_error: string | null;
	status: string;
	account_name: string | null;
	account_currency: string | null;
};

export type MetaAdsConfigSummary = {
	source: "database" | "environment";
	appId: string;
	appBaseUrl: string;
	appScopes: string[];
	adAccountId: string;
	appSecretConfigured: boolean;
	missingFields: string[];
};

export type MetaAdsStatusResponse = {
	config: MetaAdsConfigSummary;
	connection: MetaAdsConnection | null;
};

export type MetaAdsConfigPayload = {
	appId: string;
	appSecret?: string;
	appBaseUrl: string;
	appScopes: string | string[];
	adAccountId: string;
};

export type MetaAdsConfigResponse = {
	ok: true;
	config: MetaAdsConfigSummary;
};

export type MetaAdsOAuthStartResponse = {
	authorizationUrl: string;
	redirectUri: string;
	state: string;
};

export type MetaAdsSyncResponse = {
	ok: true;
	enqueuedJobs: number;
	dates: string[];
};

export type GoogleAdsConnection = {
	id: number;
	customer_id: string;
	login_customer_id: string | null;
	token_scopes: string[];
	last_refreshed_at: string | null;
	last_sync_started_at: string | null;
	last_sync_completed_at: string | null;
	last_sync_status: string;
	last_sync_error: string | null;
	status: string;
	customer_descriptive_name: string | null;
	currency_code: string | null;
};

export type GoogleAdsReconciliation = {
	checked_range_start: string;
	checked_range_end: string;
	missing_dates: string[];
	enqueued_jobs: number;
	status: string;
	checked_at: string;
};

export type GoogleAdsStatusResponse = {
	config: GoogleAdsConfigSummary;
	connection: GoogleAdsConnection | null;
	reconciliation: GoogleAdsReconciliation | null;
};

export type GoogleAdsConfigSummary = {
	source: "database" | "environment";
	clientId: string;
	appBaseUrl: string;
	appScopes: string[];
	clientSecretConfigured: boolean;
	developerTokenConfigured: boolean;
	missingFields: string[];
};

export type GoogleAdsConfigPayload = {
	clientId: string;
	clientSecret?: string;
	developerToken?: string;
	appBaseUrl: string;
	appScopes: string | string[];
};

export type GoogleAdsConfigResponse = {
	ok: true;
	config: GoogleAdsConfigSummary;
};

export type GoogleAdsOAuthStartPayload = {
	customerId: string;
	loginCustomerId?: string;
};

export type GoogleAdsOAuthStartResponse = {
	authorizationUrl: string;
	redirectUri: string;
	state: string;
};

export type GoogleAdsConnectResponse = {
	ok: true;
	customerId: string;
	customerName: string | null;
	currencyCode: string | null;
	plannedDates: string[];
};

export type ShopifyConnectionResponse = {
	connected: boolean;
	shopDomain: string | null;
	installUrl?: string | null;
	reconnectUrl?: string | null;
	status?: string;
	installedAt?: string | null;
	reconnectedAt?: string | null;
	uninstalledAt?: string | null;
	scopes?: string[];
	webhookBaseUrl?: string | null;
	webhookSubscriptions?: unknown;
	shop?: {
		name: string | null;
		email: string | null;
		currency: string | null;
	} | null;
};

export type ShopifyWebhookSyncResponse = {
	ok: true;
	shopDomain: string;
	webhookSubscriptions: unknown;
};

export type ShopifyBackfillResponse = {
	ok: true;
	shopDomain: string;
	startDate: string;
	endDate: string;
	importedOrders: number;
	processedOrders: number;
	duplicatedOrders: number;
};

export type ShopifyAttributionRecoveryResponse = {
	ok: true;
	startDate: string;
	endDate: string;
	rescannedOrders: number;
	relinkedOrders: number;
	requeuedOrders: number;
	shopifyHintAttributedOrders: number;
};

export type IdentityHealthFilters = {
	startDate: string;
	endDate: string;
	source?: string;
};

export type IdentityHealthSeriesPoint = {
	date: string;
	linked: number;
	skipped: number;
	conflicts: number;
	mergeRuns: number;
	rehomedNodes: number;
	quarantinedNodes: number;
};

export type IdentityBackfillLatestRun = {
	runId: string;
	status: "processing" | "completed" | "failed";
	requestedBy: string;
	workerId: string;
	sources: string[];
	startedAt: string;
	completedAt: string | null;
	updatedAt: string;
	errorCode: string | null;
	errorMessage: string | null;
};

export type IdentityHealthOverviewResponse = {
	range: {
		startDate: string;
		endDate: string;
	};
	source: string | null;
	summary: {
		totalIngestions: number;
		linkedIngestions: number;
		skippedIngestions: number;
		conflictIngestions: number;
		mergeRuns: number;
		rehomedNodes: number;
		quarantinedNodes: number;
		unresolvedConflicts: number;
		unlinkedSessions: number;
		linkedSessions: number;
	};
	series: IdentityHealthSeriesPoint[];
	backfill: {
		activeRuns: number;
		failedRuns: number;
		completedRuns: number;
		latestRun: IdentityBackfillLatestRun | null;
	};
};

export type IdentityConflictRow = {
	edgeId: string;
	journeyId: string;
	journeyStatus: "active" | "quarantined" | "merged" | "conflicted";
	authoritativeShopifyCustomerId: string | null;
	nodeType: string;
	nodeKey: string;
	evidenceSource: string;
	sourceTable: string | null;
	sourceRecordId: string | null;
	conflictCode: string;
	firstObservedAt: string;
	lastObservedAt: string;
	updatedAt: string;
};

export type IdentityConflictsResponse = {
	range: {
		startDate: string;
		endDate: string;
	};
	source: string | null;
	conflicts: IdentityConflictRow[];
};

export type AdminDebugJourneyResponse = {
  order: {
    shopifyOrderId: string;
    shopifyOrderNumber: string | null;
    shopifyCustomerId: string | null;
    currencyCode: string;
    subtotalPrice: number;
    totalPrice: number;
    processedAt: string | null;
    createdAtShopify: string | null;
    landingSessionId: string | null;
    checkoutToken: string | null;
    cartToken: string | null;
    identityJourneyId: string | null;
    currentAttribution: {
      attributionModel: string | null;
      source: string | null;
      medium: string | null;
      campaign: string | null;
      content: string | null;
      term: string | null;
      confidenceScore: number | null;
      attributionReason: string | null;
      attributedAt: string | null;
    };
  };
  run: {
    runId: string;
    status: string;
    triggerSource: string;
    createdAt: string;
    completedAt: string | null;
    orderOccurredAt: string;
  } | null;
  events: Array<{
    sourceTable: string;
    id: string;
    sessionId: string;
    eventType: string;
    occurredAt: string;
    pageUrl: string | null;
    referrerUrl: string | null;
    utmSource: string | null;
    utmMedium: string | null;
    utmCampaign: string | null;
    gclid: string | null;
    fbclid: string | null;
    shopifyCartToken: string | null;
    shopifyCheckoutToken: string | null;
    ingestionSource: string | null;
  }>;
  identity: {
    journey: {
      id: string;
      authoritativeShopifyCustomerId: string | null;
      status: string;
      mergeVersion: number;
      mergedIntoJourneyId: string | null;
      primaryEmailHash: string | null;
      primaryPhoneHash: string | null;
      lookbackWindowStartedAt: string;
      lookbackWindowExpiresAt: string;
      lastTouchEligibleAt: string;
      firstSourceSystem: string | null;
      lastSourceSystem: string | null;
      createdAt: string;
      updatedAt: string;
    } | null;
    edges: Array<{
      edgeId: string;
      nodeType: string;
      nodeKey: string;
      edgeType: string;
      precedenceRank: number;
      evidenceSource: string;
      sourceTable: string | null;
      sourceRecordId: string | null;
      isActive: boolean;
      conflictCode: string | null;
      firstObservedAt: string;
      lastObservedAt: string;
    }>;
    mergeAudits: Array<{
      id: string;
      winnerJourneyId: string;
      loserJourneyId: string;
      mergeReasonCode: string;
      evidenceSource: string;
      sourceTable: string | null;
      sourceRecordId: string | null;
      sourceTimestamp: string;
      winnerScore: unknown;
      loserScore: unknown;
      candidateScores: unknown;
      rehomedNodes: number;
      quarantinedNodes: number;
      createdAt: string;
    }>;
  };
  attribution: {
    touchpoints: Array<{
      touchpointId: string;
      sessionId: string | null;
      identityJourneyId: string | null;
      occurredAt: string;
      capturedAt: string;
      sourceKind: string;
      ingestionSource: string;
      source: string | null;
      medium: string | null;
      campaign: string | null;
      evidenceSource: string;
      isDirect: boolean;
      engagementType: string;
      isSynthetic: boolean;
      isEligible: boolean;
      ineligibilityReason: string | null;
      attributionReason: string | null;
      attributionHint: unknown;
    }>;
    modelSummaries: Array<{
      modelKey: string;
      allocationStatus: string;
      winnerTouchpointId: string | null;
      winnerEvidenceSource: string | null;
      winnerAttributionReason: string | null;
      totalCreditWeight: number;
      totalRevenueCredited: number;
      touchpointCountConsidered: number;
      eligibleClickCount: number;
      eligibleViewCount: number;
      winnerSelectionRule: string;
      directSuppressionApplied: boolean;
      deterministicBlockApplied: boolean;
      normalizationFailuresCount: number;
    }>;
    credits: Array<{
      modelKey: string;
      touchpointId: string;
      touchpointPosition: number;
      occurredAt: string;
      source: string | null;
      medium: string | null;
      campaign: string | null;
      evidenceSource: string;
      attributionReason: string;
      creditWeight: number;
      revenueCredit: number;
      isPrimary: boolean;
      confidenceLabel: string;
    }>;
    explainRecords: Array<{
      touchpointId: string | null;
      modelKey: string | null;
      explainStage: string;
      decision: string;
      decisionReason: string;
      details: unknown;
      createdAt: string;
    }>;
  };
};

export type CampaignResolverDebugPayload = {
  resolverVersion?: string;
  platform?: string | null;
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  accountId?: string | null;
  campaignId?: string | null;
  adsetId?: string | null;
  adId?: string | null;
  occurredAt?: string | null;
  enqueueUnmapped?: boolean;
};

export type CampaignResolverDebugResponse = {
  resolution: {
    status: 'resolved' | 'fallback' | 'unmapped';
    resolverVersion: string;
    source: 'override' | 'rule' | 'heuristic' | 'unmapped';
    confidence: number;
    ruleId: string | null;
    canonical: {
      campaignId: string | null;
      campaignName: string | null;
      source: string | null;
      medium: string | null;
      channel: string | null;
      channelGroup: string | null;
      hierarchy: Record<string, unknown>;
    };
    qaQueueId: string | null;
  };
};

export type AdminDebugReplayPayload = {
  eventType?: string;
  sourceTable?: string;
  fromTime?: string;
  toTime?: string;
  limit?: number;
  dryRun?: boolean;
};

export type AdminDebugReplayResponse = {
  replay: {
    replayRunId: number;
    candidateCount: number;
    replayedCount: number;
    skippedCount: number;
    failedCount: number;
    dryRunCount: number;
  };
};

export type AdminDebugAuditRow = {
  id: string;
  actorKind: 'internal' | 'user';
  actorUserId: number | null;
  actorEmail: string;
  action: string;
  targetType: string;
  targetId: string | null;
  requestPayload: unknown;
  resultSummary: unknown;
  createdAt: string;
};

export type AdminDebugAuditResponse = {
  rows: AdminDebugAuditRow[];
};

declare global {
	interface Window {
		__ROAS_RADAR_RUNTIME_CONFIG__?: {
			apiBaseUrl?: string;
			reportingToken?: string;
			reportingTenantId?: string;
		};
	}
}

const runtimeConfig = window.__ROAS_RADAR_RUNTIME_CONFIG__;
const viteEnv = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env ?? {};
const API_BASE_URL = (runtimeConfig?.apiBaseUrl ?? viteEnv.VITE_API_BASE_URL ?? '').replace(
  /\/$/,
  ''
);
const REPORTING_TOKEN = runtimeConfig?.reportingToken ?? viteEnv.VITE_REPORTING_API_TOKEN ?? '';
const TENANT_ID = runtimeConfig?.reportingTenantId ?? viteEnv.VITE_REPORTING_TENANT_ID ?? '1';
const AUTH_TOKEN_STORAGE_KEY = 'roas_radar_auth_token';

export function getStoredAuthToken(): string {
	try {
		return window.localStorage.getItem(AUTH_TOKEN_STORAGE_KEY) ?? "";
	} catch {
		return "";
	}
}

export function storeAuthToken(token: string): void {
	try {
		window.localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, token);
	} catch {
		// Ignore storage errors and rely on in-memory login state for the current tab.
	}
}

export function clearStoredAuthToken(): void {
	try {
		window.localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
	} catch {
		// Ignore storage errors.
	}
}

function buildSearchParams(
	filters: ReportingFilters,
	extras: Record<string, string> = {},
): URLSearchParams {
	const params = new URLSearchParams({
		startDate: filters.startDate,
		endDate: filters.endDate,
	});

	if (filters.source?.trim()) {
		params.set("source", filters.source.trim());
	}

	if (filters.campaign?.trim()) {
		params.set("campaign", filters.campaign.trim());
	}

	if (filters.attributionModel?.trim()) {
		params.set("attributionModel", filters.attributionModel.trim());
	}

  if (filters.attributionTier?.trim()) {
    params.set('attributionTier', filters.attributionTier.trim());
  }

  for (const [key, value] of Object.entries(extras)) {
    params.set(key, value);
  }

	return params;
}

function buildAttributionSearchParams(filters: AttributionFilters, extras: Record<string, string> = {}): URLSearchParams {
  const params = new URLSearchParams({
    startDate: filters.startDate,
    endDate: filters.endDate
  });

  if (filters.source?.trim()) {
    params.set('source', filters.source.trim());
  }

  if (filters.medium?.trim()) {
    params.set('medium', filters.medium.trim());
  }

  if (filters.campaign?.trim()) {
    params.set('campaign', filters.campaign.trim());
  }

  if (filters.orderId?.trim()) {
    params.set('orderId', filters.orderId.trim());
  }

  for (const [key, value] of Object.entries(extras)) {
    params.set(key, value);
  }

  return params;
}

function buildIdentityHealthSearchParams(
	filters: IdentityHealthFilters,
	extras: Record<string, string> = {},
): URLSearchParams {
	const params = new URLSearchParams({
		startDate: filters.startDate,
		endDate: filters.endDate,
	});

	if (filters.source?.trim()) {
		params.set("source", filters.source.trim());
	}

	for (const [key, value] of Object.entries(extras)) {
		params.set(key, value);
	}

	return params;
}

function buildMetaOrderValueSearchParams(query: MetaOrderValueQuery): URLSearchParams {
  const params = new URLSearchParams({
    startDate: query.startDate,
    endDate: query.endDate
  });

  if (query.campaignSearch?.trim()) {
    params.set('campaignSearch', query.campaignSearch.trim());
  }

  if (query.actionType?.trim()) {
    params.set('actionType', query.actionType.trim());
  }

  if (query.sortBy) {
    params.set('sortBy', query.sortBy);
  }

  if (query.sortDirection) {
    params.set('sortDirection', query.sortDirection);
  }

  if (typeof query.limit === 'number') {
    params.set('limit', `${query.limit}`);
  }

  if (typeof query.offset === 'number') {
    params.set('offset', `${query.offset}`);
  }

  return params;
}

function buildMmmExportSearchParams(query: MmmExportQuery): URLSearchParams {
  const params = new URLSearchParams({
    startDate: query.startDate,
    endDate: query.endDate
  });

  if (query.martRowType) {
    params.set('martRowType', query.martRowType);
  }

  if (query.attributionModel?.trim()) {
    params.set('attributionModel', query.attributionModel.trim());
  }

  if (query.platform) {
    params.set('platform', query.platform);
  }

  if (query.source?.trim()) {
    params.set('source', query.source.trim());
  }

  if (query.campaign?.trim()) {
    params.set('campaign', query.campaign.trim());
  }

  if (typeof query.limit === 'number') {
    params.set('limit', `${query.limit}`);
  }

  if (typeof query.offset === 'number') {
    params.set('offset', `${query.offset}`);
  }

  return params;
}

function buildHeaders(includeJsonBody: boolean): Record<string, string> {
	const headers: Record<string, string> = {
		"x-roas-radar-tenant-id": TENANT_ID,
	};
	const authToken = getStoredAuthToken() || REPORTING_TOKEN;

	if (authToken) {
		headers.authorization = `Bearer ${authToken}`;
	}

	if (includeJsonBody) {
		headers["content-type"] = "application/json";
	}

	return headers;
}

async function requestJson<T>(
	path: string,
	options: {
		searchParams?: URLSearchParams;
		method?: "GET" | "POST" | "PUT";
		body?: unknown;
		parse?: (payload: unknown) => T;
	} = {},
): Promise<T> {
	const { searchParams, method = "GET", body, parse } = options;
	const query = searchParams ? `?${searchParams.toString()}` : "";
	const includeJsonBody = body !== undefined;

	const response = await fetch(`${API_BASE_URL}${path}${query}`, {
		method,
		headers: buildHeaders(includeJsonBody),
		body: includeJsonBody ? JSON.stringify(body) : undefined,
	});

	if (!response.ok) {
		let message = `Request failed with status ${response.status}`;

		try {
			const errorBody = (await response.json()) as { message?: string };
			if (errorBody.message) {
				message = errorBody.message;
			}
		} catch {
			// Ignore malformed error payloads and keep the status message.
		}

		throw new Error(message);
	}

	const payload = (await response.json()) as unknown;
	return parse ? parse(payload) : (payload as T);
}

export function fetchSummary(filters: ReportingFilters) {
	return requestJson<SummaryResponse>("/api/reporting/summary", {
		searchParams: buildSearchParams(filters),
	});
}

export function login(email: string, password: string) {
	return requestJson<AuthLoginResponse>("/api/auth/login", {
		method: "POST",
		body: {
			email,
			password,
		},
	});
}

export function fetchCurrentUser() {
	return requestJson<AuthMeResponse>("/api/auth/me");
}

export function fetchAppSettings() {
	return requestJson<AppSettings>("/api/settings");
}

export function updateAppSettings(payload: UpdateAppSettingsPayload) {
	return requestJson<UpdateAppSettingsResponse>("/api/settings", {
		method: "PUT",
		body: payload,
	});
}

export function logout() {
	return requestJson<{ ok: true }>("/api/auth/logout", {
		method: "POST",
	});
}

export function fetchCampaigns(filters: ReportingFilters, limit = 12) {
	return requestJson<CampaignsResponse>("/api/reporting/campaigns", {
		searchParams: buildSearchParams(filters, { limit: `${limit}` }),
	});
}

export function fetchSpendDetails(filters: ReportingFilters) {
	return requestJson<SpendDetailsResponse>("/api/reporting/spend-details", {
		searchParams: buildSearchParams(filters),
	});
}

export function fetchTimeseries(
	filters: ReportingFilters,
	groupBy: TimeseriesGroupBy,
) {
	return requestJson<TimeseriesResponse>("/api/reporting/timeseries", {
		searchParams: buildSearchParams(filters, { groupBy }),
	});
}

export function fetchReportingModelComparison(
	filters: ReportingFilters,
	dateGrain: "day" | "week" = "week",
) {
	return requestJson<ReportingModelComparisonResponse>(
		"/api/reporting/model-comparison",
		{
			searchParams: buildSearchParams(filters, {
				dateGrain,
				sourceOfTruth: filters.sourceOfTruth ?? "deterministic",
			}),
		},
	);
}

export function fetchOrders(filters: ReportingFilters, limit = 10) {
	return requestJson<OrdersResponse>("/api/reporting/orders", {
		searchParams: buildSearchParams(filters, { limit: `${limit}` }),
	});
}

export function fetchMetaOrderValue(query: MetaOrderValueQuery) {
  return requestJson<MetaOrderValueResponse>('/api/reporting/meta-order-value', {
    searchParams: buildMetaOrderValueSearchParams(query)
  });
}

export function fetchMmmExport(query: MmmExportQuery) {
  return requestJson<MmmExportResponse>('/api/reporting/mmm', {
    searchParams: buildMmmExportSearchParams(query)
  });
}

export function fetchMmmReadinessGate(query: MmmExportQuery) {
  return requestJson<MmmReadinessGateResponse>('/api/reporting/mmm/readiness-gate', {
    searchParams: buildMmmExportSearchParams(query)
  });
}

export function refreshMmmReadinessGate(query: MmmExportQuery) {
  return requestJson<MmmReadinessGateResponse>('/api/reporting/mmm/readiness-gate/refresh', {
    method: 'POST',
    body: query
  });
}

export function approveMmmReadinessGate(payload: MmmReadinessGateDecisionPayload) {
  return requestJson<MmmReadinessGateResponse>('/api/reporting/mmm/readiness-gate/approve', {
    method: 'POST',
    body: payload
  });
}

export function waiveMmmReadinessGate(payload: MmmReadinessGateDecisionPayload & { waiver: NonNullable<MmmReadinessGateDecisionPayload['waiver']> }) {
  return requestJson<MmmReadinessGateResponse>('/api/reporting/mmm/readiness-gate/waive', {
    method: 'POST',
    body: payload
  });
}

export function blockMmmReadinessGate(payload: MmmReadinessGateDecisionPayload) {
  return requestJson<MmmReadinessGateResponse>('/api/reporting/mmm/readiness-gate/block', {
    method: 'POST',
    body: payload
  });
}

export function fetchMmmModelRuns(query: {
  startDate?: string;
  endDate?: string;
  attributionModel?: string;
  limit?: number;
} = {}) {
  const searchParams = new URLSearchParams();

  if (query.startDate) {
    searchParams.set('startDate', query.startDate);
  }

  if (query.endDate) {
    searchParams.set('endDate', query.endDate);
  }

  if (query.attributionModel?.trim()) {
    searchParams.set('attributionModel', query.attributionModel.trim());
  }

  if (typeof query.limit === 'number') {
    searchParams.set('limit', `${query.limit}`);
  }

  return requestJson<MmmModelRunsResponse>('/api/reporting/mmm/model-runs', {
    searchParams
  });
}

export function fetchExposureCoverage(query: {
  startDate: string;
  endDate: string;
  sourcePlatform?: ExposureCoverageRow['sourcePlatform'];
  exposureType?: ExposureCoverageRow['exposureType'];
}) {
  const searchParams = new URLSearchParams({
    startDate: query.startDate,
    endDate: query.endDate
  });

  if (query.sourcePlatform) {
    searchParams.set('sourcePlatform', query.sourcePlatform);
  }

  if (query.exposureType) {
    searchParams.set('exposureType', query.exposureType);
  }

  return requestJson<ExposureCoverageResponse>('/api/reporting/mmm/exposure-coverage', {
    searchParams
  });
}

export function fetchOrderDetails(shopifyOrderId: string) {
	return requestJson<OrderDetailsResponse>(
		`/api/reporting/orders/${encodeURIComponent(shopifyOrderId)}`,
	);
}

export function fetchAttributionResults(
  filters: AttributionFilters,
  modelKey: AttributionModelKey,
  options: {
    runId?: string;
    cursor?: string;
    limit?: number;
  } = {}
) {
  const searchParams = buildAttributionSearchParams(filters, {
    modelKey,
    ...(options.runId ? { runId: options.runId } : {}),
    ...(options.cursor ? { cursor: options.cursor } : {}),
    ...(options.limit ? { limit: String(options.limit) } : {})
  });

  return requestJson<AttributionResultsResponse>('/api/attribution/results', { searchParams });
}

export async function fetchAllAttributionResults(
  filters: AttributionFilters,
  modelKey: AttributionModelKey,
  options: {
    runId?: string;
    limitPerPage?: number;
  } = {}
) {
  const rows: AttributionResultRow[] = [];
  const limit = options.limitPerPage ?? 200;
  let cursor: string | undefined;

  do {
    const response = await fetchAttributionResults(filters, modelKey, {
      runId: options.runId,
      cursor,
      limit
    });

    rows.push(...response.rows);
    cursor = response.nextCursor ?? undefined;
  } while (cursor);

  return rows;
}

export function fetchAttributionChannelTotals(filters: AttributionFilters, runId?: string) {
  return requestJson<AttributionChannelTotalsResponse>('/api/attribution/channel-totals', {
    searchParams: buildAttributionSearchParams(filters, runId ? { runId } : {})
  });
}

export function fetchAttributionExplainability(
  orderId: string,
  options: {
    runId?: string;
    modelKey?: AttributionModelKey;
  } = {}
) {
  const searchParams = new URLSearchParams();

  if (options.runId) {
    searchParams.set('runId', options.runId);
  }

  if (options.modelKey) {
    searchParams.set('modelKey', options.modelKey);
  }

  return requestJson<AttributionExplainabilityResponse>(
    `/api/attribution/orders/${encodeURIComponent(orderId)}/explainability`,
    { searchParams }
  );
}

export function fetchMetaAdsStatus() {
	return requestJson<MetaAdsStatusResponse>("/api/admin/meta-ads/status");
}

export function startMetaAdsOauth(redirectPath?: string) {
	const searchParams = new URLSearchParams();

	if (redirectPath?.trim()) {
		searchParams.set("redirectPath", redirectPath.trim());
	}

	return requestJson<MetaAdsOAuthStartResponse>(
		"/api/admin/meta-ads/oauth/start",
		{ searchParams },
	);
}

export function updateMetaAdsConfig(payload: MetaAdsConfigPayload) {
	return requestJson<MetaAdsConfigResponse>("/api/admin/meta-ads/config", {
		method: "PUT",
		body: payload,
	});
}

export function syncMetaAds(startDate: string, endDate: string) {
	return requestJson<MetaAdsSyncResponse>("/api/admin/meta-ads/sync", {
		method: "POST",
		body: { startDate, endDate },
	});
}

export function fetchGoogleAdsStatus() {
	return requestJson<GoogleAdsStatusResponse>("/api/admin/google-ads/status");
}

export function updateGoogleAdsConfig(payload: GoogleAdsConfigPayload) {
	return requestJson<GoogleAdsConfigResponse>("/api/admin/google-ads/config", {
		method: "PUT",
		body: payload,
	});
}

export function startGoogleAdsOauth(
	payload: GoogleAdsOAuthStartPayload,
	redirectPath?: string,
) {
	const searchParams = new URLSearchParams();
	searchParams.set("customerId", payload.customerId);

	if (payload.loginCustomerId) {
		searchParams.set("loginCustomerId", payload.loginCustomerId);
	}

	if (redirectPath) {
		searchParams.set("redirectPath", redirectPath);
	}

	return requestJson<GoogleAdsOAuthStartResponse>(
		"/api/admin/google-ads/oauth/start",
		{ searchParams },
	);
}

export function syncGoogleAds(startDate: string, endDate: string) {
	return requestJson<MetaAdsSyncResponse>("/api/admin/google-ads/sync", {
		method: "POST",
		body: { startDate, endDate },
	});
}

export function reconcileGoogleAds() {
	return requestJson<{ ok: true; enqueuedJobs: number }>(
		"/api/admin/google-ads/reconcile",
		{
			method: "POST",
		},
	);
}

export function fetchShopifyConnection() {
	return requestJson<ShopifyConnectionResponse>(
		"/api/admin/shopify/connection",
	);
}

export function syncShopifyWebhooks() {
	return requestJson<ShopifyWebhookSyncResponse>(
		"/api/admin/shopify/webhooks/sync",
		{
			method: "POST",
		},
	);
}

export function backfillShopifyOrders(startDate: string, endDate: string) {
	return requestJson<ShopifyBackfillResponse>(
		"/api/admin/shopify/orders/backfill",
		{
			method: "POST",
			body: { startDate, endDate },
		},
	);
}

export function recoverShopifyAttributionHints(
	startDate: string,
	endDate: string,
) {
	return requestJson<ShopifyAttributionRecoveryResponse>(
		"/api/admin/shopify/orders/recover-attribution",
		{
			method: "POST",
			body: { startDate, endDate },
		},
	);
}

export function enqueueOrderAttributionBackfill(
	payload: OrderAttributionBackfillRequest,
) {
	const request = orderAttributionBackfillRequestSchema.parse(payload);

	return requestJson<OrderAttributionBackfillEnqueueResponse>(
		"/api/admin/attribution/orders/backfill",
		{
			method: "POST",
			body: request,
			parse: (response) =>
				orderAttributionBackfillEnqueueResponseSchema.parse(response),
		},
	);
}

export function fetchOrderAttributionBackfillJob(jobId: string) {
	return requestJson<OrderAttributionBackfillJobResponse>(
		`/api/admin/attribution/orders/backfill/${jobId}`,
		{
			parse: (response) =>
				orderAttributionBackfillJobResponseSchema.parse(response),
		},
	);
}

export function fetchUsers() {
	return requestJson<UsersResponse>("/api/admin/users");
}

export function createUser(payload: CreateUserPayload) {
	return requestJson<CreateUserResponse>("/api/admin/users", {
		method: "POST",
		body: payload,
	});
}

export function fetchIdentityHealthOverview(filters: IdentityHealthFilters) {
	return requestJson<IdentityHealthOverviewResponse>(
		"/api/admin/identity/health",
		{
			searchParams: buildIdentityHealthSearchParams(filters),
		},
	);
}

export function fetchIdentityHealthConflicts(
	filters: IdentityHealthFilters,
	limit = 25,
) {
	return requestJson<IdentityConflictsResponse>(
		"/api/admin/identity/health/conflicts",
		{
			searchParams: buildIdentityHealthSearchParams(filters, {
				limit: `${limit}`,
			}),
		},
	);
}

export function fetchAdminDebugJourney(shopifyOrderId: string) {
  return requestJson<AdminDebugJourneyResponse>(
    `/api/admin/attribution/debug/journeys/${encodeURIComponent(shopifyOrderId)}`
  );
}

export function debugCampaignResolver(payload: CampaignResolverDebugPayload) {
  return requestJson<CampaignResolverDebugResponse>('/api/admin/attribution/debug/campaign-resolver', {
    method: 'POST',
    body: payload
  });
}

export function triggerAdminDebugReplay(payload: AdminDebugReplayPayload) {
  return requestJson<AdminDebugReplayResponse>('/api/admin/attribution/debug/replay', {
    method: 'POST',
    body: payload
  });
}

export function triggerAdminDebugRecompute(payload: OrderAttributionBackfillRequest) {
  const request = orderAttributionBackfillRequestSchema.parse(payload);

  return requestJson<OrderAttributionBackfillEnqueueResponse>('/api/admin/attribution/debug/recompute', {
    method: 'POST',
    body: request,
    parse: (response) => orderAttributionBackfillEnqueueResponseSchema.parse(response)
  });
}

export function fetchAdminDebugAudit(limit = 25) {
  const searchParams = new URLSearchParams({ limit: `${limit}` });
  return requestJson<AdminDebugAuditResponse>('/api/admin/attribution/debug/audit', {
    searchParams
  });
}
