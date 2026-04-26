import type {
  OrderAttributionBackfillEnqueueResponse,
  OrderAttributionBackfillJobResponse,
  OrderAttributionBackfillRequest,
  OrderAttributionBackfillSubmittedOptions
} from '../../../packages/attribution-schema/index.js';
import {
  orderAttributionBackfillEnqueueResponseSchema,
  orderAttributionBackfillJobResponseSchema,
  orderAttributionBackfillRequestSchema
} from '../../../packages/attribution-schema/index.js';

export type {
  OrderAttributionBackfillEnqueueResponse,
  OrderAttributionBackfillJobResponse,
  OrderAttributionBackfillRequest,
  OrderAttributionBackfillSubmittedOptions
};

export type ReportingFilters = {
  startDate: string;
  endDate: string;
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
};

export type CampaignsResponse = {
  rows: CampaignRow[];
  nextCursor: string | null;
};

export type SpendDetailCampaignRow = {
  campaign: string;
  spend: number;
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

export type TimeseriesGroupBy = 'day' | 'source' | 'campaign';

export type TimeseriesPoint = {
  date: string;
  visits: number;
  orders: number;
  revenue: number;
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
  }>;
};

export type OrderRow = {
  shopifyOrderId: string;
  processedAt: string | null;
  totalPrice: number;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  attributionReason: string;
};

export type OrdersResponse = {
  rows: OrderRow[];
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
  ingestedAt: string;
  rawPayload: unknown;
};

export type OrderDetailsResponse = {
  order: OrderDetail;
  lineItems: OrderDetailLineItem[];
  attributionCredits: OrderDetailAttributionCredit[];
};

export type AuthUser = {
  id: number;
  email: string;
  displayName: string;
  isAdmin: boolean;
  status: 'active' | 'disabled';
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
  source: 'database' | 'environment';
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
  source: 'database' | 'environment';
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
  status: 'processing' | 'completed' | 'failed';
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
  journeyStatus: 'active' | 'quarantined' | 'merged' | 'conflicted';
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
const TENANT_ID = runtimeConfig?.reportingTenantId ?? viteEnv.VITE_REPORTING_TENANT_ID ?? 'roas-radar';
const AUTH_TOKEN_STORAGE_KEY = 'roas_radar_auth_token';

export function getStoredAuthToken(): string {
  try {
    return window.localStorage.getItem(AUTH_TOKEN_STORAGE_KEY) ?? '';
  } catch {
    return '';
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

function buildSearchParams(filters: ReportingFilters, extras: Record<string, string> = {}): URLSearchParams {
  const params = new URLSearchParams({
    startDate: filters.startDate,
    endDate: filters.endDate
  });

  if (filters.source?.trim()) {
    params.set('source', filters.source.trim());
  }

  if (filters.campaign?.trim()) {
    params.set('campaign', filters.campaign.trim());
  }

  if (filters.attributionModel?.trim()) {
    params.set('attributionModel', filters.attributionModel.trim());
  }

  for (const [key, value] of Object.entries(extras)) {
    params.set(key, value);
  }

  return params;
}

function buildIdentityHealthSearchParams(filters: IdentityHealthFilters, extras: Record<string, string> = {}): URLSearchParams {
  const params = new URLSearchParams({
    startDate: filters.startDate,
    endDate: filters.endDate
  });

  if (filters.source?.trim()) {
    params.set('source', filters.source.trim());
  }

  for (const [key, value] of Object.entries(extras)) {
    params.set(key, value);
  }

  return params;
}

function buildHeaders(includeJsonBody: boolean): Record<string, string> {
  const headers: Record<string, string> = {
    'x-roas-radar-tenant-id': TENANT_ID
  };
  const authToken = getStoredAuthToken() || REPORTING_TOKEN;

  if (authToken) {
    headers.authorization = `Bearer ${authToken}`;
  }

  if (includeJsonBody) {
    headers['content-type'] = 'application/json';
  }

  return headers;
}

async function requestJson<T>(
  path: string,
  options: {
    searchParams?: URLSearchParams;
    method?: 'GET' | 'POST' | 'PUT';
    body?: unknown;
    parse?: (payload: unknown) => T;
  } = {}
): Promise<T> {
  const { searchParams, method = 'GET', body, parse } = options;
  const query = searchParams ? `?${searchParams.toString()}` : '';
  const includeJsonBody = body !== undefined;

  const response = await fetch(`${API_BASE_URL}${path}${query}`, {
    method,
    headers: buildHeaders(includeJsonBody),
    body: includeJsonBody ? JSON.stringify(body) : undefined
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
  return requestJson<SummaryResponse>('/api/reporting/summary', { searchParams: buildSearchParams(filters) });
}

export function login(email: string, password: string) {
  return requestJson<AuthLoginResponse>('/api/auth/login', {
    method: 'POST',
    body: {
      email,
      password
    }
  });
}

export function fetchCurrentUser() {
  return requestJson<AuthMeResponse>('/api/auth/me');
}

export function fetchAppSettings() {
  return requestJson<AppSettings>('/api/settings');
}

export function updateAppSettings(payload: UpdateAppSettingsPayload) {
  return requestJson<UpdateAppSettingsResponse>('/api/settings', {
    method: 'PUT',
    body: payload
  });
}

export function logout() {
  return requestJson<{ ok: true }>('/api/auth/logout', {
    method: 'POST'
  });
}

export function fetchCampaigns(filters: ReportingFilters, limit = 12) {
  return requestJson<CampaignsResponse>('/api/reporting/campaigns', {
    searchParams: buildSearchParams(filters, { limit: `${limit}` })
  });
}

export function fetchSpendDetails(filters: ReportingFilters) {
  return requestJson<SpendDetailsResponse>('/api/reporting/spend-details', {
    searchParams: buildSearchParams(filters)
  });
}

export function fetchTimeseries(filters: ReportingFilters, groupBy: TimeseriesGroupBy) {
  return requestJson<TimeseriesResponse>('/api/reporting/timeseries', {
    searchParams: buildSearchParams(filters, { groupBy })
  });
}

export function fetchOrders(filters: ReportingFilters, limit = 10) {
  return requestJson<OrdersResponse>('/api/reporting/orders', {
    searchParams: buildSearchParams(filters, { limit: `${limit}` })
  });
}

export function fetchOrderDetails(shopifyOrderId: string) {
  return requestJson<OrderDetailsResponse>(`/api/reporting/orders/${encodeURIComponent(shopifyOrderId)}`);
}

export function fetchMetaAdsStatus() {
  return requestJson<MetaAdsStatusResponse>('/api/admin/meta-ads/status');
}

export function startMetaAdsOauth(redirectPath?: string) {
  const searchParams = new URLSearchParams();

  if (redirectPath?.trim()) {
    searchParams.set('redirectPath', redirectPath.trim());
  }

  return requestJson<MetaAdsOAuthStartResponse>('/api/admin/meta-ads/oauth/start', { searchParams });
}

export function updateMetaAdsConfig(payload: MetaAdsConfigPayload) {
  return requestJson<MetaAdsConfigResponse>('/api/admin/meta-ads/config', {
    method: 'PUT',
    body: payload
  });
}

export function syncMetaAds(startDate: string, endDate: string) {
  return requestJson<MetaAdsSyncResponse>('/api/admin/meta-ads/sync', {
    method: 'POST',
    body: { startDate, endDate }
  });
}

export function fetchGoogleAdsStatus() {
  return requestJson<GoogleAdsStatusResponse>('/api/admin/google-ads/status');
}

export function updateGoogleAdsConfig(payload: GoogleAdsConfigPayload) {
  return requestJson<GoogleAdsConfigResponse>('/api/admin/google-ads/config', {
    method: 'PUT',
    body: payload
  });
}

export function startGoogleAdsOauth(payload: GoogleAdsOAuthStartPayload, redirectPath?: string) {
  const searchParams = new URLSearchParams();
  searchParams.set('customerId', payload.customerId);

  if (payload.loginCustomerId) {
    searchParams.set('loginCustomerId', payload.loginCustomerId);
  }

  if (redirectPath) {
    searchParams.set('redirectPath', redirectPath);
  }

  return requestJson<GoogleAdsOAuthStartResponse>('/api/admin/google-ads/oauth/start', { searchParams });
}

export function syncGoogleAds(startDate: string, endDate: string) {
  return requestJson<MetaAdsSyncResponse>('/api/admin/google-ads/sync', {
    method: 'POST',
    body: { startDate, endDate }
  });
}

export function reconcileGoogleAds() {
  return requestJson<{ ok: true; enqueuedJobs: number }>('/api/admin/google-ads/reconcile', {
    method: 'POST'
  });
}

export function fetchShopifyConnection() {
  return requestJson<ShopifyConnectionResponse>('/api/admin/shopify/connection');
}

export function syncShopifyWebhooks() {
  return requestJson<ShopifyWebhookSyncResponse>('/api/admin/shopify/webhooks/sync', {
    method: 'POST'
  });
}

export function backfillShopifyOrders(startDate: string, endDate: string) {
  return requestJson<ShopifyBackfillResponse>('/api/admin/shopify/orders/backfill', {
    method: 'POST',
    body: { startDate, endDate }
  });
}

export function recoverShopifyAttributionHints(startDate: string, endDate: string) {
  return requestJson<ShopifyAttributionRecoveryResponse>('/api/admin/shopify/orders/recover-attribution', {
    method: 'POST',
    body: { startDate, endDate }
  });
}

export function enqueueOrderAttributionBackfill(payload: OrderAttributionBackfillRequest) {
  const request = orderAttributionBackfillRequestSchema.parse(payload);

  return requestJson<OrderAttributionBackfillEnqueueResponse>('/api/admin/attribution/orders/backfill', {
    method: 'POST',
    body: request,
    parse: (response) => orderAttributionBackfillEnqueueResponseSchema.parse(response)
  });
}

export function fetchOrderAttributionBackfillJob(jobId: string) {
  return requestJson<OrderAttributionBackfillJobResponse>(`/api/admin/attribution/orders/backfill/${jobId}`, {
    parse: (response) => orderAttributionBackfillJobResponseSchema.parse(response)
  });
}

export function fetchUsers() {
  return requestJson<UsersResponse>('/api/admin/users');
}

export function createUser(payload: CreateUserPayload) {
  return requestJson<CreateUserResponse>('/api/admin/users', {
    method: 'POST',
    body: payload
  });
}

export function fetchIdentityHealthOverview(filters: IdentityHealthFilters) {
  return requestJson<IdentityHealthOverviewResponse>('/api/admin/identity/health', {
    searchParams: buildIdentityHealthSearchParams(filters)
  });
}

export function fetchIdentityHealthConflicts(filters: IdentityHealthFilters, limit = 25) {
  return requestJson<IdentityConflictsResponse>('/api/admin/identity/health/conflicts', {
    searchParams: buildIdentityHealthSearchParams(filters, { limit: `${limit}` })
  });
}
