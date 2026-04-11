import type { PerformanceMetrics } from '../../../src/shared/metrics';

export type AttributionModel = 'last_touch' | 'first_touch' | 'linear' | 'position_based' | 'time_decay';

export type Filters = {
  startDate: string;
  endDate: string;
  attributionModel: AttributionModel;
  source?: string;
  medium?: string;
  campaign?: string;
  content?: string;
  search?: string;
};

export type ModelsResponse = {
  data: {
    defaultModel: AttributionModel;
    supportedModels: AttributionModel[];
    requiredScope: string;
  };
};

export type OverviewResponse = {
  data: {
    totals: PerformanceMetrics;
  };
};

export type TimeseriesPoint = PerformanceMetrics & {
  date: string;
};

export type TimeseriesResponse = {
  data: {
    points: TimeseriesPoint[];
  };
};

export type ChannelRow = PerformanceMetrics & {
  source: string;
  medium: string;
  shareOfRevenue: number;
};

export type ChannelsResponse = {
  data: {
    rows: ChannelRow[];
    pagination: {
      limit: number;
      nextCursor: string | null;
    };
  };
};

export type CampaignRow = PerformanceMetrics & {
  source: string;
  medium: string;
  campaign: string;
};

export type CreativeRow = PerformanceMetrics & {
  source: string;
  medium: string;
  campaign: string;
  campaignId: string | null;
  campaignName: string | null;
  adId: string | null;
  adName: string | null;
  creativeId: string | null;
  creativeName: string;
  content: string;
  costPerClick: number | null;
};

export type CreativesResponse = {
  data: {
    rows: CreativeRow[];
    pagination: {
      limit: number;
      nextCursor: string | null;
    };
  };
};

export type CampaignsResponse = {
  data: {
    rows: CampaignRow[];
    pagination: {
      limit: number;
      nextCursor: string | null;
    };
  };
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:3000').replace(/\/$/, '');
const REPORTING_TOKEN = import.meta.env.VITE_REPORTING_API_TOKEN ?? '';
const TENANT_ID = import.meta.env.VITE_REPORTING_TENANT_ID ?? 'roas-radar';

function buildSearchParams(filters: Filters, extras: Record<string, string> = {}): URLSearchParams {
  const params = new URLSearchParams({
    startDate: filters.startDate,
    endDate: filters.endDate,
    attributionModel: filters.attributionModel
  });

  const optionalFilters = ['source', 'medium', 'campaign', 'content', 'search'] as const;

  for (const key of optionalFilters) {
    const value = filters[key];
    if (value) {
      params.set(key, value);
    }
  }

  for (const [key, value] of Object.entries(extras)) {
    params.set(key, value);
  }

  return params;
}

async function requestJson<T>(path: string, searchParams: URLSearchParams): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}?${searchParams.toString()}`, {
    headers: {
      authorization: REPORTING_TOKEN ? `Bearer ${REPORTING_TOKEN}` : '',
      'x-roas-radar-tenant-id': TENANT_ID
    }
  });

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;

    try {
      const errorBody = (await response.json()) as { error?: { message?: string } };
      if (errorBody.error?.message) {
        message = errorBody.error.message;
      }
    } catch {
      // Ignore JSON parsing errors and keep the status-based message.
    }

    throw new Error(message);
  }

  return (await response.json()) as T;
}

export async function fetchModels(): Promise<ModelsResponse> {
  return requestJson<ModelsResponse>('/api/reporting/models', new URLSearchParams());
}

export async function fetchOverview(filters: Filters): Promise<OverviewResponse> {
  return requestJson<OverviewResponse>('/api/reporting/overview', buildSearchParams(filters));
}

export async function fetchTimeseries(filters: Filters): Promise<TimeseriesResponse> {
  return requestJson<TimeseriesResponse>('/api/reporting/timeseries', buildSearchParams(filters));
}

export async function fetchChannels(filters: Filters, limit = 8): Promise<ChannelsResponse> {
  return requestJson<ChannelsResponse>('/api/reporting/channels', buildSearchParams(filters, { limit: `${limit}` }));
}

export async function fetchCampaigns(filters: Filters, limit = 6): Promise<CampaignsResponse> {
  return requestJson<CampaignsResponse>('/api/reporting/campaigns', buildSearchParams(filters, { limit: `${limit}` }));
}

export async function fetchCreatives(filters: Filters, limit = 50): Promise<CreativesResponse> {
  return requestJson<CreativesResponse>('/api/reporting/creatives', buildSearchParams(filters, { limit: `${limit}` }));
}
