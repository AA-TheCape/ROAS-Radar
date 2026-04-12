export type ReportingFilters = {
  startDate: string;
  endDate: string;
  source?: string;
  campaign?: string;
};

export type SummaryTotals = {
  visits: number;
  orders: number;
  revenue: number;
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

export type TimeseriesGroupBy = 'day' | 'source' | 'campaign';

export type TimeseriesPoint = {
  date: string;
  visits: number;
  orders: number;
  revenue: number;
};

export type TimeseriesResponse = {
  points: TimeseriesPoint[];
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
const API_BASE_URL = (runtimeConfig?.apiBaseUrl ?? import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:3000').replace(
  /\/$/,
  ''
);
const REPORTING_TOKEN = runtimeConfig?.reportingToken ?? import.meta.env.VITE_REPORTING_API_TOKEN ?? '';
const TENANT_ID = runtimeConfig?.reportingTenantId ?? import.meta.env.VITE_REPORTING_TENANT_ID ?? 'roas-radar';

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

  for (const [key, value] of Object.entries(extras)) {
    params.set(key, value);
  }

  return params;
}

async function requestJson<T>(path: string, searchParams: URLSearchParams): Promise<T> {
  const headers: Record<string, string> = {
    'x-roas-radar-tenant-id': TENANT_ID
  };

  if (REPORTING_TOKEN) {
    headers.authorization = `Bearer ${REPORTING_TOKEN}`;
  }

  const response = await fetch(`${API_BASE_URL}${path}?${searchParams.toString()}`, { headers });

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

  return (await response.json()) as T;
}

export function fetchSummary(filters: ReportingFilters) {
  return requestJson<SummaryResponse>('/api/reporting/summary', buildSearchParams(filters));
}

export function fetchCampaigns(filters: ReportingFilters, limit = 12) {
  return requestJson<CampaignsResponse>('/api/reporting/campaigns', buildSearchParams(filters, { limit: `${limit}` }));
}

export function fetchTimeseries(filters: ReportingFilters, groupBy: TimeseriesGroupBy) {
  return requestJson<TimeseriesResponse>('/api/reporting/timeseries', buildSearchParams(filters, { groupBy }));
}

export function fetchOrders(filters: ReportingFilters, limit = 10) {
  return requestJson<OrdersResponse>('/api/reporting/orders', buildSearchParams(filters, { limit: `${limit}` }));
}
