import React, {
  Suspense,
  lazy,
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useState,
  type FormEvent
} from 'react';
import {
  ATTRIBUTION_MODEL_KEYS,
  ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT,
  type OrderAttributionBackfillSubmittedOptions,
  orderAttributionBackfillRequestSchema
} from '../../packages/attribution-schema/index.js';

import {
  backfillShopifyOrders,
  clearStoredAuthToken,
  createUser,
  enqueueOrderAttributionBackfill,
  fetchAllAttributionResults,
  fetchAttributionChannelTotals,
  fetchAttributionExplainability,
  fetchAppSettings,
  fetchCampaigns,
  fetchCurrentUser,
  fetchIdentityHealthConflicts,
  fetchIdentityHealthOverview,
  fetchGoogleAdsStatus,
  fetchMetaAdsStatus,
  fetchOrderDetails,
  fetchOrderAttributionBackfillJob,
  fetchOrders,
  fetchSpendDetails,
  fetchShopifyConnection,
  fetchSummary,
  fetchTimeseries,
  fetchUsers,
  getStoredAuthToken,
  login,
  logout,
  reconcileGoogleAds,
  recoverShopifyAttributionHints,
  storeAuthToken,
  syncShopifyWebhooks,
  startGoogleAdsOauth,
  startMetaAdsOauth,
  syncGoogleAds,
  syncMetaAds,
  updateAppSettings,
  updateGoogleAdsConfig,
  updateMetaAdsConfig,
  type AppSettings,
  type AttributionChannelTotalsResponse,
  type AttributionExplainabilityResponse,
  type AttributionFilters,
  type AttributionResultRow,
  type AuthUser,
  type CampaignRow,
  type CreateUserPayload,
  type GoogleAdsConfigSummary,
  type GoogleAdsStatusResponse,
  type IdentityConflictsResponse,
  type IdentityHealthFilters,
  type IdentityHealthOverviewResponse,
  type MetaAdsConnection,
  type MetaAdsConfigSummary,
  type OrderAttributionBackfillEnqueueResponse,
  type OrderAttributionBackfillJobResponse,
  type OrderDetailsResponse,
  type OrderRow,
  type ReportingFilters,
  type ShopifyConnectionResponse,
  type ShopifyBackfillResponse,
  type ShopifyAttributionRecoveryResponse,
  type SpendDetailChannelGroup,
  type SummaryTotals,
  type TimeseriesGroupBy,
  type TimeseriesPoint
} from './lib/api';
import { isAttributionTier } from './lib/attributionTier';
import {
  formatCurrency,
  formatDateLabel,
  formatDateTimeLabel,
  formatNumber,
  formatPercent
} from './lib/format';
import AuthenticatedAppShell, {
  type AppShellBreadcrumb,
  type AppShellNavItem
} from './components/AuthenticatedAppShell';
import TitleBarTimestamp from './components/TitleBarTimestamp';
import {
  AuthGate,
  Banner,
  Button,
  ButtonRow,
  Field,
  FieldGrid,
  Form,
  Input,
  Panel,
  SectionState,
  Select
} from './components/AuthenticatedUi';

const ReportingDashboard = lazy(() => import('./components/ReportingDashboard'));
const AttributionDashboard = lazy(() => import('./components/AttributionDashboard'));
const OrderDetailsView = lazy(() => import('./components/OrderDetailsView'));
const SettingsAdminView = lazy(() => import('./components/SettingsAdminView'));
const IdentityGraphHealthView = lazy(() => import('./components/IdentityGraphHealthView'));

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type DashboardState = {
  summary: AsyncSection<SummaryTotals>;
  campaigns: AsyncSection<CampaignRow[]>;
  timeseries: AsyncSection<TimeseriesPoint[]>;
  orders: AsyncSection<OrderRow[]>;
  spendDetails: AsyncSection<SpendDetailChannelGroup[]>;
};

type AttributionState = {
  results: AsyncSection<AttributionResultRow[]>;
  channelTotals: AsyncSection<AttributionChannelTotalsResponse>;
  explainability: AsyncSection<AttributionExplainabilityResponse>;
};

type ActionFeedback = {
  context: string | null;
  loading: string | null;
  error: string | null;
  message: string | null;
};

type AuthState = {
  checking: boolean;
  user: AuthUser | null;
  error: string | null;
};

type MetaConnectionState = {
  config: MetaAdsConfigSummary;
  connection: MetaAdsConnection | null;
};

type MetaConfigForm = {
  appId: string;
  appSecret: string;
  appBaseUrl: string;
  appScopes: string;
  adAccountId: string;
};

type GoogleConfigForm = {
  clientId: string;
  clientSecret: string;
  developerToken: string;
  appBaseUrl: string;
  appScopes: string;
};

type GoogleConnectForm = {
  customerId: string;
  loginCustomerId: string;
};

type GoogleConnectionState = {
  config: GoogleAdsConfigSummary;
  connection: GoogleAdsStatusResponse['connection'];
  reconciliation: GoogleAdsStatusResponse['reconciliation'];
};

type SettingsForm = {
  reportingTimezone: string;
};

type AppPage = 'dashboard' | 'attribution' | 'identity-health' | 'settings' | 'order-details';

const AUTHENTICATED_NAV_ITEMS: AppShellNavItem[] = [
  {
    key: 'dashboard',
    label: 'Dashboard',
    description: 'Summary metrics, campaign performance, time-based revenue trends, and attributed order rows.'
  },
  {
    key: 'attribution',
    label: 'Attribution',
    description: 'Compare the six attribution-engine model views side-by-side and inspect per-order rationale.'
  },
  {
    key: 'identity-health',
    label: 'Identity health',
    description: 'Merge activity, conflict drill-down, unlinked session pressure, and identity graph backfill status.'
  },
  {
    key: 'settings',
    label: 'Settings',
    description: 'Reporting timezone, platform connections, sync actions, and dashboard user access.'
  }
];

const DEFAULT_REPORTING_TIMEZONE = 'America/Los_Angeles';
const DEFAULT_GROUP_BY: TimeseriesGroupBy = 'day';
const DEFAULT_ATTRIBUTION_MODEL = ATTRIBUTION_MODEL_KEYS[2];
const ORDER_ATTRIBUTION_BACKFILL_JOB_STORAGE_KEY = 'roas-radar.order-attribution-backfill.latest-job-id';
const ORDER_ATTRIBUTION_BACKFILL_POLL_INTERVAL_MS = 5000;
const REPORTING_TIMEZONE_OPTIONS = [
  'America/Los_Angeles',
  'America/Denver',
  'America/Chicago',
  'America/New_York',
  'America/Phoenix',
  'America/Anchorage',
  'Pacific/Honolulu',
  'UTC',
  'PST',
  'PT'
] as const;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

const PRESETS = [
  { label: 'Today', value: (reportingTimezone: string) => buildRange(1, reportingTimezone) },
  { label: 'Yesterday', value: (reportingTimezone: string) => buildSingleDayRange(-1, reportingTimezone) },
  { label: 'Last 7D', value: (reportingTimezone: string) => buildRange(7, reportingTimezone) },
  { label: 'Last 30D', value: (reportingTimezone: string) => buildRange(30, reportingTimezone) },
  { label: 'Last 90D', value: (reportingTimezone: string) => buildRange(90, reportingTimezone) }
] as const;

function formatDateInput(date: Date, reportingTimezone = DEFAULT_REPORTING_TIMEZONE): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: reportingTimezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);

  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    return date.toISOString().slice(0, 10);
  }

  return `${year}-${month}-${day}`;
}

function buildRange(
  days: number,
  reportingTimezone = DEFAULT_REPORTING_TIMEZONE
): Pick<ReportingFilters, 'startDate' | 'endDate'> {
  const end = new Date();
  const start = new Date(end.getTime() - (days - 1) * MS_PER_DAY);

  return {
    startDate: formatDateInput(start, reportingTimezone),
    endDate: formatDateInput(end, reportingTimezone)
  };
}

function buildSingleDayRange(
  offsetDays: number,
  reportingTimezone = DEFAULT_REPORTING_TIMEZONE
): Pick<ReportingFilters, 'startDate' | 'endDate'> {
  const date = new Date(Date.now() + offsetDays * MS_PER_DAY);
  const value = formatDateInput(date, reportingTimezone);

  return {
    startDate: value,
    endDate: value
  };
}

function buildYesterdayDateInput(reportingTimezone = DEFAULT_REPORTING_TIMEZONE): string {
  return buildSingleDayRange(-1, reportingTimezone).startDate;
}

function buildAprilFirstDateInput(reportingTimezone = DEFAULT_REPORTING_TIMEZONE): string {
  const currentYear = formatDateInput(new Date(), reportingTimezone).slice(0, 4);
  return `${currentYear}-04-01`;
}

function normalizeReportingFilters(filters: ReportingFilters): ReportingFilters {
  if (filters.startDate && filters.endDate && filters.startDate > filters.endDate) {
    return {
      ...filters,
      endDate: filters.startDate
    };
  }

  return filters;
}

const DASHBOARD_QUERY_PARAM_KEYS = ['startDate', 'endDate', 'source', 'campaign', 'attributionModel', 'attributionTier', 'groupBy'] as const;
const REPORTING_FILTER_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const ATTRIBUTION_MODELS = new Set<NonNullable<ReportingFilters['attributionModel']>>([
  'first_touch',
  'last_touch',
  'linear',
  'time_decay',
  'position_based',
  'rule_based_weighted'
]);

export function createDefaultReportingFilters(reportingTimezone = DEFAULT_REPORTING_TIMEZONE): ReportingFilters {
  return {
    ...buildRange(30, reportingTimezone),
    source: '',
    campaign: '',
    attributionTier: ''
  };
}

function createDefaultAttributionFilters(reportingTimezone = DEFAULT_REPORTING_TIMEZONE): AttributionFilters {
  return {
    ...buildRange(30, reportingTimezone),
    source: '',
    medium: '',
    campaign: '',
    orderId: ''
  };
}

function isValidDateInput(value: string | null): value is string {
  return Boolean(value && REPORTING_FILTER_DATE_PATTERN.test(value));
}

function isTimeseriesGroupBy(value: string | null): value is TimeseriesGroupBy {
  return value === 'day' || value === 'source' || value === 'campaign';
}

function isAttributionModel(value: string | null): value is NonNullable<ReportingFilters['attributionModel']> {
  return Boolean(value && ATTRIBUTION_MODELS.has(value as NonNullable<ReportingFilters['attributionModel']>));
}

export function readDashboardStateFromSearch(
  search: string,
  reportingTimezone = DEFAULT_REPORTING_TIMEZONE
): {
  filters: ReportingFilters;
  groupBy: TimeseriesGroupBy;
} {
  const params = new URLSearchParams(search);
  const defaults = createDefaultReportingFilters(reportingTimezone);
  const startDate = params.get('startDate');
  const endDate = params.get('endDate');
  const source = params.get('source');
  const campaign = params.get('campaign');
  const attributionModel = params.get('attributionModel');
  const attributionTier = params.get('attributionTier');
  const groupBy = params.get('groupBy');

  return {
    filters: normalizeReportingFilters({
      startDate: isValidDateInput(startDate) ? startDate : defaults.startDate,
      endDate: isValidDateInput(endDate) ? endDate : defaults.endDate,
      source: source ?? '',
      campaign: campaign ?? '',
      attributionModel: isAttributionModel(attributionModel) ? attributionModel : undefined,
      attributionTier: isAttributionTier(attributionTier) ? attributionTier : ''
    }),
    groupBy: isTimeseriesGroupBy(groupBy) ? groupBy : DEFAULT_GROUP_BY
  };
}

export function applyDashboardStateToSearch(
  currentSearch: string,
  filters: ReportingFilters,
  groupBy: TimeseriesGroupBy
): string {
  const params = new URLSearchParams(currentSearch);

  for (const key of DASHBOARD_QUERY_PARAM_KEYS) {
    params.delete(key);
  }

  params.set('startDate', filters.startDate);
  params.set('endDate', filters.endDate);

  if (filters.source?.trim()) {
    params.set('source', filters.source.trim());
  }

  if (filters.campaign?.trim()) {
    params.set('campaign', filters.campaign.trim());
  }

  if (filters.attributionModel?.trim()) {
    params.set('attributionModel', filters.attributionModel.trim());
  }

  if (filters.attributionTier?.trim()) {
    params.set('attributionTier', filters.attributionTier.trim());
  }

  params.set('groupBy', groupBy);

  return params.toString();
}

function readInitialDashboardState() {
  if (typeof window === 'undefined') {
    return {
      filters: createDefaultReportingFilters(DEFAULT_REPORTING_TIMEZONE),
      groupBy: DEFAULT_GROUP_BY
    };
  }

  return readDashboardStateFromSearch(window.location.search, DEFAULT_REPORTING_TIMEZONE);
}

function createLoadingSection<T>(): AsyncSection<T> {
  return {
    data: null,
    loading: true,
    error: null
  };
}

function createResolvedSection<T>(data: T): AsyncSection<T> {
  return {
    data,
    loading: false,
    error: null
  };
}

function createErroredSection<T>(message: string): AsyncSection<T> {
  return {
    data: null,
    loading: false,
    error: message
  };
}

function readStoredOrderAttributionBackfillJobId(): string | null {
  if (typeof window === 'undefined') {
    return null;
  }

  const value = window.localStorage.getItem(ORDER_ATTRIBUTION_BACKFILL_JOB_STORAGE_KEY)?.trim() ?? '';
  return value || null;
}

function storeOrderAttributionBackfillJobId(jobId: string) {
  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(ORDER_ATTRIBUTION_BACKFILL_JOB_STORAGE_KEY, jobId);
}

function useDashboardData(
  filters: ReportingFilters,
  groupBy: TimeseriesGroupBy,
  enabled: boolean,
  refreshKey: number
) {
  const [state, setState] = useState<DashboardState>({
    summary: createLoadingSection(),
    campaigns: createLoadingSection(),
    timeseries: createLoadingSection(),
    orders: createLoadingSection(),
    spendDetails: createLoadingSection()
  });

  useEffect(() => {
    void refreshKey;

    if (!enabled) {
      setState({
        summary: {
          data: null,
          loading: false,
          error: null
        },
        campaigns: createResolvedSection<CampaignRow[]>([]),
        timeseries: createResolvedSection<TimeseriesPoint[]>([]),
        orders: createResolvedSection<OrderRow[]>([]),
        spendDetails: createResolvedSection<SpendDetailChannelGroup[]>([])
      });
      return;
    }

    let cancelled = false;

    setState({
      summary: createLoadingSection(),
      campaigns: createLoadingSection(),
      timeseries: createLoadingSection(),
      orders: createLoadingSection(),
      spendDetails: createLoadingSection()
    });

    fetchSummary(filters)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            summary: createResolvedSection(response.totals)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            summary: createErroredSection(error.message)
          }));
        }
      });

    fetchCampaigns(filters, 12)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            campaigns: createResolvedSection(response.rows)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            campaigns: createErroredSection(error.message)
          }));
        }
      });

    fetchTimeseries(filters, groupBy)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            timeseries: createResolvedSection(response.points)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            timeseries: createErroredSection(error.message)
          }));
        }
      });

    fetchOrders(filters, 10)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            orders: createResolvedSection(response.rows)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            orders: createErroredSection(error.message)
          }));
        }
      });

    fetchSpendDetails(filters)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            spendDetails: createResolvedSection(response.groups)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            spendDetails: createErroredSection(error.message)
          }));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [enabled, filters, groupBy, refreshKey]);

  return state;
}

function formatOptionalDateTime(value: string | null | undefined, reportingTimezone: string): string {
  return value ? formatDateTimeLabel(value, reportingTimezone) : 'Not available';
}

function toBackfillOptionState(options: OrderAttributionBackfillSubmittedOptions) {
  return {
    dryRun: options.dryRun,
    limit: String(options.limit),
    webOrdersOnly: options.webOrdersOnly,
    skipShopifyWriteback: options.skipShopifyWriteback
  };
}

function AuthenticatedViewFallback({ title, description }: { title: string; description: string }) {
  return (
    <Panel title={title} description={description} wide>
      <SectionState loading empty={false} error={null} emptyLabel="">
        <div />
      </SectionState>
    </Panel>
  );
}

function App() {
  const initialDashboardState = readInitialDashboardState();
  const [authState, setAuthState] = useState<AuthState>({
    checking: true,
    user: null,
    error: null
  });
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [loginSubmitting, setLoginSubmitting] = useState(false);
  const [filters, setFilters] = useState<ReportingFilters>(initialDashboardState.filters);
  const [attributionFilters, setAttributionFilters] = useState<AttributionFilters>(
    createDefaultAttributionFilters(DEFAULT_REPORTING_TIMEZONE)
  );
  const [activeAttributionModel, setActiveAttributionModel] = useState<(typeof ATTRIBUTION_MODEL_KEYS)[number]>(
    DEFAULT_ATTRIBUTION_MODEL
  );
  const [appSettings, setAppSettings] = useState<AsyncSection<AppSettings>>(createLoadingSection());
  const [settingsForm, setSettingsForm] = useState<SettingsForm>({
    reportingTimezone: DEFAULT_REPORTING_TIMEZONE
  });
  const [usersSection, setUsersSection] = useState<AsyncSection<AuthUser[]>>(createLoadingSection());
  const [orderDetailsSection, setOrderDetailsSection] = useState<AsyncSection<OrderDetailsResponse>>({
    data: null,
    loading: false,
    error: null
  });
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const [newUserForm, setNewUserForm] = useState<CreateUserPayload>({
    email: '',
    password: '',
    displayName: '',
    isAdmin: false
  });
  const [groupBy, setGroupBy] = useState<TimeseriesGroupBy>(initialDashboardState.groupBy);
  const [currentPage, setCurrentPage] = useState<AppPage>('dashboard');
  const [dashboardRefreshKey, setDashboardRefreshKey] = useState(0);
  const [attributionState, setAttributionState] = useState<AttributionState>({
    results: {
      data: null,
      loading: false,
      error: null
    },
    channelTotals: {
      data: null,
      loading: false,
      error: null
    },
    explainability: {
      data: null,
      loading: false,
      error: null
    }
  });
  const [selectedAttributionOrderId, setSelectedAttributionOrderId] = useState<string | null>(null);
  const [identityHealthFilters, setIdentityHealthFilters] = useState<IdentityHealthFilters>({
    ...buildRange(30, DEFAULT_REPORTING_TIMEZONE),
    source: ''
  });
  const [identityHealthOverview, setIdentityHealthOverview] = useState<AsyncSection<IdentityHealthOverviewResponse>>({
    data: null,
    loading: false,
    error: null
  });
  const [identityHealthConflicts, setIdentityHealthConflicts] = useState<AsyncSection<IdentityConflictsResponse>>({
    data: null,
    loading: false,
    error: null
  });
  const [shopifyConnection, setShopifyConnection] = useState<AsyncSection<ShopifyConnectionResponse>>(createLoadingSection());
  const [shopifyBackfillRange, setShopifyBackfillRange] = useState({
    startDate: buildAprilFirstDateInput(DEFAULT_REPORTING_TIMEZONE),
    endDate: buildYesterdayDateInput(DEFAULT_REPORTING_TIMEZONE)
  });
  const [shopifyOrderAttributionBackfillOptions, setShopifyOrderAttributionBackfillOptions] = useState({
    dryRun: true,
    limit: String(ORDER_ATTRIBUTION_BACKFILL_DEFAULT_LIMIT),
    webOrdersOnly: true,
    skipShopifyWriteback: false
  });
  const [orderAttributionBackfillJob, setOrderAttributionBackfillJob] = useState<AsyncSection<OrderAttributionBackfillJobResponse>>({
    data: null,
    loading: false,
    error: null
  });
  const [metaConnection, setMetaConnection] = useState<AsyncSection<MetaConnectionState>>(createLoadingSection());
  const [metaConfigForm, setMetaConfigForm] = useState<MetaConfigForm>({
    appId: '',
    appSecret: '',
    appBaseUrl: '',
    appScopes: 'ads_read',
    adAccountId: ''
  });
  const [googleConnection, setGoogleConnection] = useState<AsyncSection<GoogleConnectionState>>(createLoadingSection());
  const [googleConfigForm, setGoogleConfigForm] = useState<GoogleConfigForm>({
    clientId: '',
    clientSecret: '',
    developerToken: '',
    appBaseUrl: '',
    appScopes: 'https://www.googleapis.com/auth/adwords'
  });
  const [googleForm, setGoogleForm] = useState<GoogleConnectForm>({
    customerId: '',
    loginCustomerId: ''
  });
  const [actionFeedback, setActionFeedback] = useState<ActionFeedback>({
    context: null,
    loading: null,
    error: null,
    message: null
  });

  const deferredSource = useDeferredValue(filters.source);
  const deferredCampaign = useDeferredValue(filters.campaign);

  const appliedFilters = useMemo<ReportingFilters>(
    () => ({
      startDate: filters.startDate,
      endDate: filters.endDate,
      source: (deferredSource ?? '').trim(),
      campaign: (deferredCampaign ?? '').trim(),
      attributionModel: filters.attributionModel,
      attributionTier: filters.attributionTier ?? ''
    }),
    [deferredCampaign, deferredSource, filters.attributionModel, filters.attributionTier, filters.endDate, filters.startDate]
  );

  const dashboard = useDashboardData(appliedFilters, groupBy, authState.user !== null, dashboardRefreshKey);
  const reportingTimezone = appSettings.data?.reportingTimezone ?? settingsForm.reportingTimezone ?? DEFAULT_REPORTING_TIMEZONE;

  const loadIdentityHealth = useCallback(async () => {
    if (!authState.user?.isAdmin) {
      setIdentityHealthOverview({
        data: null,
        loading: false,
        error: null
      });
      setIdentityHealthConflicts({
        data: null,
        loading: false,
        error: null
      });
      return;
    }

    setIdentityHealthOverview({
      data: null,
      loading: true,
      error: null
    });
    setIdentityHealthConflicts({
      data: null,
      loading: true,
      error: null
    });

    try {
      const [overview, conflicts] = await Promise.all([
        fetchIdentityHealthOverview(identityHealthFilters),
        fetchIdentityHealthConflicts(identityHealthFilters, 25)
      ]);
      setIdentityHealthOverview(createResolvedSection(overview));
      setIdentityHealthConflicts(createResolvedSection(conflicts));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load identity health metrics';
      setIdentityHealthOverview(createErroredSection(message));
      setIdentityHealthConflicts(createErroredSection(message));
    }
  }, [authState.user?.isAdmin, identityHealthFilters]);

  const loadAppSettings = useCallback(async () => {
    setAppSettings(createLoadingSection());

    try {
      const settings = await fetchAppSettings();
      setAppSettings(createResolvedSection(settings));
      setSettingsForm({
        reportingTimezone: settings.reportingTimezone
      });
      setShopifyBackfillRange({
        startDate: buildAprilFirstDateInput(settings.reportingTimezone),
        endDate: buildYesterdayDateInput(settings.reportingTimezone)
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load dashboard settings';
      setAppSettings(createErroredSection(message));
    }
  }, []);

  const loadConnections = useCallback(async () => {
    setShopifyConnection(createLoadingSection());
    setMetaConnection(createLoadingSection());
    setGoogleConnection(createLoadingSection());

    try {
      const [shopifyStatus, metaStatus, googleStatus] = await Promise.all([
        fetchShopifyConnection(),
        fetchMetaAdsStatus(),
        fetchGoogleAdsStatus()
      ]);
      setShopifyConnection(createResolvedSection(shopifyStatus));
      setMetaConnection(createResolvedSection(metaStatus));
      setMetaConfigForm((current) => ({
        appId: metaStatus.config.appId || current.appId,
        appSecret: '',
        appBaseUrl: metaStatus.config.appBaseUrl || current.appBaseUrl,
        appScopes: metaStatus.config.appScopes.length ? metaStatus.config.appScopes.join(', ') : current.appScopes,
        adAccountId: metaStatus.config.adAccountId || current.adAccountId
      }));
      setGoogleConnection(createResolvedSection(googleStatus));
      setGoogleConfigForm((current) => ({
        clientId: googleStatus.config.clientId || current.clientId,
        clientSecret: '',
        developerToken: '',
        appBaseUrl: googleStatus.config.appBaseUrl || current.appBaseUrl,
        appScopes: googleStatus.config.appScopes.length ? googleStatus.config.appScopes.join(', ') : current.appScopes
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load ad connection state';
      setShopifyConnection(createErroredSection(message));
      setMetaConnection(createErroredSection(message));
      setGoogleConnection(createErroredSection(message));
    }
  }, []);

  const loadOrderAttributionBackfillJob = useCallback(
    async (jobId: string, options?: { preserveData?: boolean }) => {
      setOrderAttributionBackfillJob((current) => ({
        data: options?.preserveData ? current.data : null,
        loading: true,
        error: null
      }));

      try {
        const response = await fetchOrderAttributionBackfillJob(jobId);
        setShopifyBackfillRange({
          startDate: response.options.startDate,
          endDate: response.options.endDate
        });
        setShopifyOrderAttributionBackfillOptions(toBackfillOptionState(response.options));
        setOrderAttributionBackfillJob({
          data: response,
          loading: false,
          error: null
        });
      } catch (error) {
        setOrderAttributionBackfillJob((current) => ({
          data: current.data,
          loading: false,
          error: error instanceof Error ? error.message : 'Failed to load order attribution backfill job'
        }));
      }
    },
    []
  );

  useEffect(() => {
    if (authState.user) {
      void loadAppSettings();
      void loadConnections();
      return;
    }

    setAppSettings(createLoadingSection());
    setShopifyConnection(createLoadingSection());
    setOrderAttributionBackfillJob({
      data: null,
      loading: false,
      error: null
    });
    setIdentityHealthOverview({
      data: null,
      loading: false,
      error: null
    });
    setIdentityHealthConflicts({
      data: null,
      loading: false,
      error: null
    });
    setMetaConnection(createLoadingSection());
    setGoogleConnection(createLoadingSection());
  }, [authState.user, loadAppSettings, loadConnections]);

  useEffect(() => {
    if (authState.user?.isAdmin && currentPage === 'identity-health') {
      void loadIdentityHealth();
      return;
    }

    if (!authState.user?.isAdmin) {
      setIdentityHealthOverview({
        data: null,
        loading: false,
        error: null
      });
      setIdentityHealthConflicts({
        data: null,
        loading: false,
        error: null
      });
    }
  }, [authState.user?.isAdmin, currentPage, loadIdentityHealth]);

  useEffect(() => {
    if (!authState.user?.isAdmin) {
      setOrderAttributionBackfillJob({
        data: null,
        loading: false,
        error: null
      });
      return;
    }

    const storedJobId = readStoredOrderAttributionBackfillJobId();
    if (!storedJobId) {
      return;
    }

    void loadOrderAttributionBackfillJob(storedJobId);
  }, [authState.user?.isAdmin, loadOrderAttributionBackfillJob]);

  useEffect(() => {
    if (!authState.user?.isAdmin || typeof window === 'undefined') {
      return;
    }

    const job = orderAttributionBackfillJob.data;
    if (!job || (job.status !== 'queued' && job.status !== 'processing')) {
      return;
    }

    const timer = window.setTimeout(() => {
      void loadOrderAttributionBackfillJob(job.jobId, { preserveData: true });
    }, ORDER_ATTRIBUTION_BACKFILL_POLL_INTERVAL_MS);

    return () => {
      window.clearTimeout(timer);
    };
  }, [authState.user?.isAdmin, loadOrderAttributionBackfillJob, orderAttributionBackfillJob.data]);

  useEffect(() => {
    if (!authState.user || currentPage !== 'attribution') {
      return;
    }

    let cancelled = false;

    setAttributionState((current) => ({
      ...current,
      results: createLoadingSection(),
      channelTotals: createLoadingSection()
    }));

    fetchAllAttributionResults(attributionFilters, activeAttributionModel)
      .then((rows) => {
        if (!cancelled) {
          setAttributionState((current) => ({
            ...current,
            results: createResolvedSection(rows)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setAttributionState((current) => ({
            ...current,
            results: createErroredSection(error.message)
          }));
        }
      });

    fetchAttributionChannelTotals(attributionFilters)
      .then((response) => {
        if (!cancelled) {
          setAttributionState((current) => ({
            ...current,
            channelTotals: createResolvedSection(response)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setAttributionState((current) => ({
            ...current,
            channelTotals: createErroredSection(error.message)
          }));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [activeAttributionModel, attributionFilters, authState.user, currentPage]);

  useEffect(() => {
    setSelectedAttributionOrderId(null);
    setAttributionState((current) => ({
      ...current,
      explainability: {
        data: null,
        loading: false,
        error: null
      }
    }));
  }, [activeAttributionModel, attributionFilters]);

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    const nextSearch = applyDashboardStateToSearch(window.location.search, filters, groupBy);
    const nextUrl = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ''}${window.location.hash}`;
    window.history.replaceState(window.history.state, '', nextUrl);
  }, [filters, groupBy]);

  useEffect(() => {
    const token = getStoredAuthToken();

    if (!token) {
      setAuthState({
        checking: false,
        user: null,
        error: null
      });
      setUsersSection({
        data: null,
        loading: false,
        error: null
      });
      setOrderDetailsSection({
        data: null,
        loading: false,
        error: null
      });
      setSelectedOrderId(null);
      setAttributionState({
        results: {
          data: null,
          loading: false,
          error: null
        },
        channelTotals: {
          data: null,
          loading: false,
          error: null
        },
        explainability: {
          data: null,
          loading: false,
          error: null
        }
      });
      setSelectedAttributionOrderId(null);
      return;
    }

    fetchCurrentUser()
      .then((response) => {
        setAuthState({
          checking: false,
          user: response.user,
          error: null
        });
      })
      .catch((error: Error) => {
        clearStoredAuthToken();
        setAuthState({
          checking: false,
          user: null,
          error: error.message
        });
        setOrderDetailsSection({
          data: null,
          loading: false,
          error: null
        });
        setSelectedOrderId(null);
        setAttributionState({
          results: {
            data: null,
            loading: false,
            error: null
          },
          channelTotals: {
            data: null,
            loading: false,
            error: null
          },
          explainability: {
            data: null,
            loading: false,
            error: null
          }
        });
        setSelectedAttributionOrderId(null);
      });
  }, []);

  const openOrderDetails = useCallback(async (shopifyOrderId: string) => {
    setCurrentPage('order-details');
    setSelectedOrderId(shopifyOrderId);
    setOrderDetailsSection(createLoadingSection());

    try {
      const response = await fetchOrderDetails(shopifyOrderId);
      setOrderDetailsSection(createResolvedSection(response));
    } catch (error) {
      setOrderDetailsSection(
        createErroredSection(error instanceof Error ? error.message : 'Failed to load order details')
      );
    }
  }, []);

  const closeOrderDetails = useCallback(() => {
    setCurrentPage('dashboard');
    setSelectedOrderId(null);
    setOrderDetailsSection({
      data: null,
      loading: false,
      error: null
    });
  }, []);

  const openAttributionExplainability = useCallback(
    async (orderId: string, runId: string) => {
      setSelectedAttributionOrderId(orderId);
      setAttributionState((current) => ({
        ...current,
        explainability: createLoadingSection()
      }));

      try {
        const response = await fetchAttributionExplainability(orderId, {
          runId,
          modelKey: activeAttributionModel
        });
        setAttributionState((current) => ({
          ...current,
          explainability: createResolvedSection(response)
        }));
      } catch (error) {
        setAttributionState((current) => ({
          ...current,
          explainability: createErroredSection(
            error instanceof Error ? error.message : 'Failed to load attribution explainability'
          )
        }));
      }
    },
    [activeAttributionModel]
  );

  const loadUsers = useCallback(async () => {
    if (!authState.user?.isAdmin) {
      setUsersSection(createResolvedSection([]));
      return;
    }

    setUsersSection(createLoadingSection());

    try {
      const response = await fetchUsers();
      setUsersSection(createResolvedSection(response.users));
    } catch (error) {
      setUsersSection(createErroredSection(error instanceof Error ? error.message : 'Failed to load users'));
    }
  }, [authState.user?.isAdmin]);

  useEffect(() => {
    if (authState.user?.isAdmin) {
      void loadUsers();
      return;
    }

    setUsersSection({
      data: null,
      loading: false,
      error: null
    });
  }, [authState.user, loadUsers]);

  const summaryCards = useMemo(() => {
    const totals = dashboard.summary.data;
    const rangeLabel = `${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`;

    return [
      {
        label: 'Visits',
        value: formatNumber(totals?.visits),
        detail: rangeLabel
      },
      {
        label: 'Orders',
        value: formatNumber(totals?.orders),
        detail: `${formatPercent(totals?.conversionRate)} conversion`
      },
      {
        label: 'Revenue',
        value: formatCurrency(totals?.revenue),
        detail: totals?.roas == null ? 'ROAS pending spend data' : `${formatNumber(totals.roas)} ROAS`
      },
      {
        label: 'Spend',
        value: formatCurrency(totals?.spend),
        detail: rangeLabel
      },
      {
        label: 'AOV',
        value:
          totals && totals.orders > 0
            ? formatCurrency(totals.revenue / totals.orders)
            : formatCurrency(null),
        detail: `${formatNumber(totals?.orders)} attributed orders`
      }
    ];
  }, [dashboard.summary.data, filters.endDate, filters.startDate, reportingTimezone]);

  async function handleLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoginSubmitting(true);
    setAuthState((current) => ({
      ...current,
      error: null
    }));

    try {
      const response = await login(loginEmail.trim(), loginPassword);
      storeAuthToken(response.token);
      setAuthState({
        checking: false,
        user: response.user,
        error: null
      });
      setLoginPassword('');
    } catch (error) {
      clearStoredAuthToken();
      setAuthState({
        checking: false,
        user: null,
        error: error instanceof Error ? error.message : 'Login failed'
      });
    } finally {
      setLoginSubmitting(false);
    }
  }

  async function handleLogout() {
    try {
      await logout();
    } catch {
      // Ignore logout API errors and clear the local session either way.
    }

    clearStoredAuthToken();
    setAuthState({
      checking: false,
      user: null,
      error: null
    });
    setCurrentPage('dashboard');
    setAppSettings({
      data: null,
      loading: false,
      error: null
    });
    setSettingsForm({
      reportingTimezone: DEFAULT_REPORTING_TIMEZONE
    });
    setUsersSection({
      data: null,
      loading: false,
      error: null
    });
    setOrderDetailsSection({
      data: null,
      loading: false,
      error: null
    });
    setSelectedOrderId(null);
    setAttributionState({
      results: {
        data: null,
        loading: false,
        error: null
      },
      channelTotals: {
        data: null,
        loading: false,
        error: null
      },
      explainability: {
        data: null,
        loading: false,
        error: null
      }
    });
    setSelectedAttributionOrderId(null);
    setActionFeedback({
      context: null,
      loading: null,
      error: null,
      message: null
    });
    setOrderAttributionBackfillJob({
      data: null,
      loading: false,
      error: null
    });
    setIdentityHealthOverview({
      data: null,
      loading: false,
      error: null
    });
    setIdentityHealthConflicts({
      data: null,
      loading: false,
      error: null
    });
  }

  async function handleSettingsSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      context: 'settings-save',
      loading: 'settings-save',
      error: null,
      message: null
    });

    try {
      const response = await updateAppSettings({
        reportingTimezone: settingsForm.reportingTimezone.trim()
      });
      setAppSettings(createResolvedSection(response.settings));
      setSettingsForm({
        reportingTimezone: response.settings.reportingTimezone
      });
      setShopifyBackfillRange((current) => ({
        startDate:
          current.startDate === buildAprilFirstDateInput(reportingTimezone)
            ? buildAprilFirstDateInput(response.settings.reportingTimezone)
            : current.startDate,
        endDate:
          current.endDate === buildYesterdayDateInput(reportingTimezone)
            ? buildYesterdayDateInput(response.settings.reportingTimezone)
            : current.endDate
      }));
      startTransition(() => {
        setDashboardRefreshKey((current) => current + 1);
      });
      setActionFeedback({
        context: 'settings-save',
        loading: null,
        error: null,
        message: `Saved reporting timezone as ${response.settings.reportingTimezone}.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'settings-save',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to save dashboard settings',
        message: null
      });
    }
  }

  async function handleCreateUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      context: 'user-create',
      loading: 'user-create',
      error: null,
      message: null
    });

    try {
      const response = await createUser({
        ...newUserForm,
        email: newUserForm.email.trim().toLowerCase(),
        displayName: newUserForm.displayName.trim()
      });
      await loadUsers();
      setNewUserForm({
        email: '',
        password: '',
        displayName: '',
        isAdmin: false
      });
      setActionFeedback({
        context: 'user-create',
        loading: null,
        error: null,
        message: `Created user ${response.user.email}.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'user-create',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to create user',
        message: null
      });
    }
  }

  async function handleMetaConnect() {
    setActionFeedback({
      context: 'meta-connect',
      loading: 'meta-connect',
      error: null,
      message: null
    });

    try {
      if ((metaConnection.data?.config.missingFields.length ?? 0) > 0) {
        throw new Error('Save the Meta Ads configuration first. Some required fields are still missing.');
      }

      const response = await startMetaAdsOauth(window.location.pathname);
      setActionFeedback({
        context: 'meta-connect',
        loading: null,
        error: null,
        message: 'Redirecting to Meta Ads…'
      });
      window.location.assign(response.authorizationUrl);
    } catch (error) {
      setActionFeedback({
        context: 'meta-connect',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to start Meta Ads OAuth',
        message: null
      });
    }
  }

  async function handleMetaConfigSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      context: 'meta-config-save',
      loading: 'meta-config-save',
      error: null,
      message: null
    });

    try {
      const response = await updateMetaAdsConfig({
        appId: metaConfigForm.appId.trim(),
        appSecret: metaConfigForm.appSecret.trim() || undefined,
        appBaseUrl: metaConfigForm.appBaseUrl.trim(),
        appScopes: metaConfigForm.appScopes,
        adAccountId: metaConfigForm.adAccountId.trim()
      });
      await loadConnections();
      setMetaConfigForm((current) => ({
        ...current,
        appSecret: '',
        appScopes: response.config.appScopes.join(', ')
      }));
      setActionFeedback({
        context: 'meta-config-save',
        loading: null,
        error: null,
        message: 'Saved Meta Ads configuration.'
      });
    } catch (error) {
      setActionFeedback({
        context: 'meta-config-save',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to save Meta Ads configuration',
        message: null
      });
    }
  }

  async function handleShopifyTest() {
    setActionFeedback({
      context: 'shopify-test',
      loading: 'shopify-test',
      error: null,
      message: null
    });

    try {
      const response = await fetchShopifyConnection();
      setShopifyConnection(createResolvedSection(response));
      setActionFeedback({
        context: 'shopify-test',
        loading: null,
        error: null,
        message: response.connected
          ? `Shopify connection is active for ${response.shopDomain ?? 'the connected store'}.`
          : 'No active Shopify installation was found.'
      });
    } catch (error) {
      setActionFeedback({
        context: 'shopify-test',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to verify Shopify connection',
        message: null
      });
    }
  }

  async function handleShopifyWebhookSync() {
    setActionFeedback({
      context: 'shopify-webhooks',
      loading: 'shopify-webhooks',
      error: null,
      message: null
    });

    try {
      const response = await syncShopifyWebhooks();
      await loadConnections();
      setActionFeedback({
        context: 'shopify-webhooks',
        loading: null,
        error: null,
        message: `Re-provisioned Shopify webhooks for ${response.shopDomain}.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'shopify-webhooks',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to sync Shopify webhooks',
        message: null
      });
    }
  }

  async function handleShopifyBackfill(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      context: 'shopify-backfill',
      loading: 'shopify-backfill',
      error: null,
      message: null
    });

    try {
      const response: ShopifyBackfillResponse = await backfillShopifyOrders(
        shopifyBackfillRange.startDate,
        shopifyBackfillRange.endDate
      );
      await loadConnections();
      startTransition(() => {
        setDashboardRefreshKey((current) => current + 1);
      });
      setActionFeedback({
        context: 'shopify-backfill',
        loading: null,
        error: null,
        message: `Backfilled ${response.importedOrders} Shopify orders for ${response.startDate} to ${response.endDate} (${response.processedOrders} imported, ${response.duplicatedOrders} already present).`
      });
    } catch (error) {
      setActionFeedback({
        context: 'shopify-backfill',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to backfill Shopify orders',
        message: null
      });
    }
  }

  async function handleShopifyAttributionRecovery() {
    setActionFeedback({
      context: 'shopify-attribution-recovery',
      loading: 'shopify-attribution-recovery',
      error: null,
      message: null
    });

    try {
      const response: ShopifyAttributionRecoveryResponse = await recoverShopifyAttributionHints(
        shopifyBackfillRange.startDate,
        shopifyBackfillRange.endDate
      );
      await loadConnections();
      startTransition(() => {
        setDashboardRefreshKey((current) => current + 1);
      });
      setActionFeedback({
        context: 'shopify-attribution-recovery',
        loading: null,
        error: null,
        message: `Rescanned ${response.rescannedOrders} unknown Shopify web orders for ${response.startDate} to ${response.endDate}; relinked ${response.relinkedOrders}, attributed ${response.shopifyHintAttributedOrders} from Shopify hints, and requeued ${response.requeuedOrders} for standard attribution.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'shopify-attribution-recovery',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to recover Shopify attribution hints',
        message: null
      });
    }
  }

  async function handleShopifyOrderAttributionBackfill() {
    const parsedRequest = orderAttributionBackfillRequestSchema.safeParse({
      startDate: shopifyBackfillRange.startDate,
      endDate: shopifyBackfillRange.endDate,
      dryRun: shopifyOrderAttributionBackfillOptions.dryRun,
      limit: Number(shopifyOrderAttributionBackfillOptions.limit),
      webOrdersOnly: shopifyOrderAttributionBackfillOptions.webOrdersOnly,
      skipShopifyWriteback: shopifyOrderAttributionBackfillOptions.skipShopifyWriteback
    });

    if (!parsedRequest.success) {
      const [firstIssue] = parsedRequest.error.issues;
      setActionFeedback({
        context: 'shopify-order-attribution-backfill',
        loading: null,
        error: firstIssue?.message ?? 'Enter a valid date range and limit before queueing the order attribution backfill.',
        message: null
      });
      return;
    }

    setActionFeedback({
      context: 'shopify-order-attribution-backfill',
      loading: 'shopify-order-attribution-backfill',
      error: null,
      message: null
    });

    try {
      const response: OrderAttributionBackfillEnqueueResponse = await enqueueOrderAttributionBackfill(parsedRequest.data);
      const queuedJob: OrderAttributionBackfillJobResponse = {
        ...response,
        startedAt: null,
        completedAt: null,
        report: null,
        error: null
      };

      storeOrderAttributionBackfillJobId(response.jobId);
      setShopifyBackfillRange({
        startDate: response.options.startDate,
        endDate: response.options.endDate
      });
      setShopifyOrderAttributionBackfillOptions(toBackfillOptionState(response.options));
      setOrderAttributionBackfillJob({
        data: queuedJob,
        loading: false,
        error: null
      });
      await loadConnections();
      startTransition(() => {
        setDashboardRefreshKey((current) => current + 1);
      });
      setActionFeedback({
        context: 'shopify-order-attribution-backfill',
        loading: null,
        error: null,
        message: `Queued order attribution backfill job ${response.jobId} for ${response.options.startDate} to ${response.options.endDate}.`
      });
      void loadOrderAttributionBackfillJob(response.jobId, { preserveData: true });
    } catch (error) {
      setActionFeedback({
        context: 'shopify-order-attribution-backfill',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to queue order attribution backfill',
        message: null
      });
    }
  }

  async function handleMetaSync() {
    setActionFeedback({
      context: 'meta-sync',
      loading: 'meta-sync',
      error: null,
      message: null
    });

    try {
      const response = await syncMetaAds(filters.startDate, filters.endDate);
      await loadConnections();
      setActionFeedback({
        context: 'meta-sync',
        loading: null,
        error: null,
        message: `Queued ${response.enqueuedJobs} Meta Ads sync jobs for ${response.dates.length} dates.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'meta-sync',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to queue Meta Ads sync',
        message: null
      });
    }
  }

  async function handleGoogleConfigSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      context: 'google-config-save',
      loading: 'google-config-save',
      error: null,
      message: null
    });

    try {
      const response = await updateGoogleAdsConfig({
        clientId: googleConfigForm.clientId.trim(),
        clientSecret: googleConfigForm.clientSecret.trim() || undefined,
        developerToken: googleConfigForm.developerToken.trim() || undefined,
        appBaseUrl: googleConfigForm.appBaseUrl.trim(),
        appScopes: googleConfigForm.appScopes
      });
      await loadConnections();
      setGoogleConfigForm((current) => ({
        ...current,
        clientSecret: '',
        developerToken: '',
        appScopes: response.config.appScopes.join(', ')
      }));
      setActionFeedback({
        context: 'google-config-save',
        loading: null,
        error: null,
        message: 'Saved Google Ads config.'
      });
    } catch (error) {
      setActionFeedback({
        context: 'google-config-save',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to save Google Ads config',
        message: null
      });
    }
  }

  async function handleGoogleConnect(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      context: 'google-connect',
      loading: 'google-connect',
      error: null,
      message: null
    });

    try {
      if ((googleConnection.data?.config.missingFields.length ?? 0) > 0) {
        throw new Error('Save the Google Ads configuration first. Some required fields are still missing.');
      }

      const response = await startGoogleAdsOauth(
        {
          customerId: googleForm.customerId.trim(),
          loginCustomerId: googleForm.loginCustomerId.trim() || undefined
        },
        window.location.pathname
      );
      setActionFeedback({
        context: 'google-connect',
        loading: null,
        error: null,
        message: 'Redirecting to Google Ads…'
      });
      window.location.assign(response.authorizationUrl);
    } catch (error) {
      setActionFeedback({
        context: 'google-connect',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to start Google Ads OAuth',
        message: null
      });
    }
  }

  async function handleGoogleSync() {
    setActionFeedback({
      context: 'google-sync',
      loading: 'google-sync',
      error: null,
      message: null
    });

    try {
      const response = await syncGoogleAds(filters.startDate, filters.endDate);
      await loadConnections();
      setActionFeedback({
        context: 'google-sync',
        loading: null,
        error: null,
        message: `Queued ${response.enqueuedJobs} Google Ads sync jobs for ${response.dates.length} dates.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'google-sync',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to queue Google Ads sync',
        message: null
      });
    }
  }

  async function handleGoogleReconcile() {
    setActionFeedback({
      context: 'google-reconcile',
      loading: 'google-reconcile',
      error: null,
      message: null
    });

    try {
      const response = await reconcileGoogleAds();
      await loadConnections();
      setActionFeedback({
        context: 'google-reconcile',
        loading: null,
        error: null,
        message: `Queued ${response.enqueuedJobs} Google Ads reconciliation jobs.`
      });
    } catch (error) {
      setActionFeedback({
        context: 'google-reconcile',
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to reconcile Google Ads',
        message: null
      });
    }
  }

  const handleAppNavigation = useCallback(
    (key: string) => {
      if (key === 'order-details') {
        return;
      }

      if (key === 'dashboard') {
        closeOrderDetails();
        return;
      }

      if (key === 'identity-health' && !authState.user?.isAdmin) {
        return;
      }

      setSelectedOrderId(null);
      setOrderDetailsSection({
        data: null,
        loading: false,
        error: null
      });
      setCurrentPage(key as AppPage);
    },
    [authState.user?.isAdmin, closeOrderDetails]
  );
  const handleDashboardFiltersChange = useCallback((next: ReportingFilters) => {
    setFilters(normalizeReportingFilters(next));
  }, []);
  const handleDashboardGroupByChange = useCallback((value: TimeseriesGroupBy) => {
    setGroupBy(value);
  }, []);
  const handleApplyQuickRange = useCallback((range: Pick<ReportingFilters, 'startDate' | 'endDate'>) => {
    startTransition(() => {
      setFilters((current) => ({
        ...normalizeReportingFilters({
          ...current,
          ...range
        })
      }));
    });
  }, []);
  const handleClearDashboardFilters = useCallback(() => {
    startTransition(() => {
      setFilters((current) => ({
        ...current,
        source: '',
        campaign: ''
      }));
    });
  }, []);
  const handleAttributionFiltersChange = useCallback((next: AttributionFilters) => {
    setAttributionFilters((current) => ({
      ...current,
      ...next
    }));
  }, []);
  const handleClearAttributionFilters = useCallback(() => {
    startTransition(() => {
      setAttributionFilters(createDefaultAttributionFilters(reportingTimezone));
    });
  }, [reportingTimezone]);

  if (authState.checking) {
    return (
      <AuthGate
        eyebrow="Secure dashboard"
        title="Checking your session"
        description="The dashboard stays locked until an authenticated user is verified."
      />
    );
  }

  if (!authState.user) {
    return (
      <AuthGate
        eyebrow="Secure dashboard"
        title="ROAS Radar Login"
        description="Sign in with an app user account before viewing any reporting or admin tools."
      >
        <Form onSubmit={(event) => void handleLogin(event)}>
          <FieldGrid>
            <Field label="Email">
              <Input type="email" value={loginEmail} onChange={(event) => setLoginEmail(event.target.value)} required />
            </Field>
            <Field label="Password">
              <Input
                type="password"
                value={loginPassword}
                onChange={(event) => setLoginPassword(event.target.value)}
                required
              />
            </Field>
          </FieldGrid>
          {authState.error ? <Banner tone="error">{authState.error}</Banner> : null}
          <ButtonRow>
            <Button type="submit" disabled={loginSubmitting}>
              {loginSubmitting ? 'Signing in…' : 'Login'}
            </Button>
          </ButtonRow>
        </Form>
      </AuthGate>
    );
  }

  const authenticatedUser = authState.user;
  const isAdmin = authenticatedUser.isAdmin;
  const activeNavKey = currentPage;
  const shellNavItems: AppShellNavItem[] =
    currentPage === 'order-details'
      ? [
          ...(isAdmin
            ? AUTHENTICATED_NAV_ITEMS
            : AUTHENTICATED_NAV_ITEMS.filter((item) => item.key !== 'identity-health')),
          {
            key: 'order-details',
            label: 'Order details',
            shortLabel: 'Order',
            description: 'Contextual drill-in for a selected attributed Shopify order.'
          }
        ]
      : isAdmin
        ? AUTHENTICATED_NAV_ITEMS
        : AUTHENTICATED_NAV_ITEMS.filter((item) => item.key !== 'identity-health');
  const breadcrumbs: AppShellBreadcrumb[] =
    currentPage === 'dashboard'
      ? [
          { label: 'Authenticated app' },
          { label: 'Dashboard', current: true }
        ]
      : currentPage === 'attribution'
        ? [
            { label: 'Authenticated app' },
            { label: 'Attribution', current: true }
          ]
      : currentPage === 'identity-health'
        ? [
            { label: 'Authenticated app' },
            { label: 'Identity health', current: true }
          ]
      : currentPage === 'settings'
        ? [
            { label: 'Authenticated app' },
            { label: 'Settings', current: true }
          ]
        : [
            { label: 'Authenticated app' },
            { label: 'Dashboard', onClick: closeOrderDetails },
            { label: selectedOrderId ? `Order ${selectedOrderId}` : 'Order details', current: true }
          ];
  const shellHeaderActions = (
    <>
      {currentPage === 'order-details' ? (
        <Button type="button" tone="ghost" onClick={closeOrderDetails}>
          Back to dashboard
        </Button>
      ) : null}
      <Button type="button" onClick={() => void handleLogout()}>
        Logout
      </Button>
    </>
  );

  return (
    <AuthenticatedAppShell
      navItems={shellNavItems}
      activeNavKey={activeNavKey}
      onNavigate={handleAppNavigation}
      breadcrumbs={breadcrumbs}
      topbarMeta={
        <div className="space-y-3">
          <div className="space-y-1">
            <p className="font-semibold text-ink">{authenticatedUser.displayName}</p>
            <p>{authenticatedUser.email}</p>
          </div>
          <TitleBarTimestamp />
        </div>
      }
      headerActions={shellHeaderActions}
    >
      {currentPage === 'dashboard' ? (
        <Suspense
          fallback={
            <AuthenticatedViewFallback
              title="Dashboard"
              description="Loading reporting controls, summary widgets, tables, and charts."
            />
          }
        >
          <ReportingDashboard
            filters={filters}
            onFiltersChange={handleDashboardFiltersChange}
            groupBy={groupBy}
            onGroupByChange={handleDashboardGroupByChange}
            reportingTimezone={reportingTimezone}
            quickRanges={PRESETS}
            onApplyQuickRange={handleApplyQuickRange}
            onClearFilters={handleClearDashboardFilters}
            summaryCards={summaryCards}
            summarySection={dashboard.summary}
            campaignsSection={dashboard.campaigns}
            timeseriesSection={dashboard.timeseries}
            ordersSection={dashboard.orders}
            spendDetailsSection={dashboard.spendDetails}
            onOpenOrderDetails={(shopifyOrderId) => void openOrderDetails(shopifyOrderId)}
          />
        </Suspense>
      ) : null}

      {currentPage === 'attribution' ? (
        <Suspense
          fallback={
            <AuthenticatedViewFallback
              title="Attribution"
              description="Loading attribution model comparisons, order summaries, and rationale drilldown."
            />
          }
        >
          <AttributionDashboard
            filters={attributionFilters}
            onFiltersChange={handleAttributionFiltersChange}
            onClearFilters={handleClearAttributionFilters}
            activeModel={activeAttributionModel}
            onActiveModelChange={setActiveAttributionModel}
            reportingTimezone={reportingTimezone}
            resultsSection={attributionState.results}
            channelTotalsSection={attributionState.channelTotals}
            explainabilitySection={attributionState.explainability}
            selectedOrderId={selectedAttributionOrderId}
            onInspectOrder={(orderId, runId) => void openAttributionExplainability(orderId, runId)}
          />
        </Suspense>
      ) : null}

      {currentPage === 'order-details' ? (
        <section className="grid gap-section">
          <Panel
            title="Order details"
            description="Everything currently stored for this Shopify order, including line items, attribution credits, and raw payload."
            wide
          >
            <Suspense
              fallback={
                <SectionState loading empty={false} error={null} emptyLabel="">
                  <div />
                </SectionState>
              }
            >
              <OrderDetailsView
                selectedOrderId={selectedOrderId}
                reportingTimezone={reportingTimezone}
                orderDetailsSection={orderDetailsSection}
              />
            </Suspense>
          </Panel>
        </section>
      ) : null}

      {currentPage === 'identity-health' ? (
        <Suspense
          fallback={
            <AuthenticatedViewFallback
              title="Identity health"
              description="Loading merge telemetry, conflict drill-down, and backfill status."
            />
          }
        >
          <IdentityGraphHealthView
            filters={identityHealthFilters}
            onFiltersChange={(next) =>
              setIdentityHealthFilters({
                ...next,
                source: next.source ?? ''
              })
            }
            onRefresh={() => {
              void loadIdentityHealth();
            }}
            reportingTimezone={reportingTimezone}
            overviewSection={identityHealthOverview}
            conflictsSection={identityHealthConflicts}
          />
        </Suspense>
      ) : null}

      {currentPage === 'settings' ? (
        <Suspense
          fallback={
            <AuthenticatedViewFallback
              title="Settings"
              description="Loading reporting settings, integration health, and access controls."
            />
          }
        >
          <SettingsAdminView
            isAdmin={isAdmin}
            reportingTimezone={reportingTimezone}
            defaultReportingTimezone={DEFAULT_REPORTING_TIMEZONE}
            reportingTimezoneOptions={REPORTING_TIMEZONE_OPTIONS}
            filters={filters}
            appSettings={appSettings}
            settingsForm={settingsForm}
            setSettingsForm={(updater) => setSettingsForm((current) => updater(current))}
            usersSection={usersSection}
            newUserForm={newUserForm}
            setNewUserForm={(updater) => setNewUserForm((current) => updater(current))}
            shopifyConnection={shopifyConnection}
            shopifyBackfillRange={shopifyBackfillRange}
            setShopifyBackfillRange={(updater) => setShopifyBackfillRange((current) => updater(current))}
            shopifyOrderAttributionBackfillOptions={shopifyOrderAttributionBackfillOptions}
            setShopifyOrderAttributionBackfillOptions={(updater) =>
              setShopifyOrderAttributionBackfillOptions((current) => updater(current))
            }
            orderAttributionBackfillJob={orderAttributionBackfillJob}
            metaConnection={metaConnection}
            metaConfigForm={metaConfigForm}
            setMetaConfigForm={(updater) => setMetaConfigForm((current) => updater(current))}
            googleConnection={googleConnection}
            googleConfigForm={googleConfigForm}
            setGoogleConfigForm={(updater) => setGoogleConfigForm((current) => updater(current))}
            googleForm={googleForm}
            setGoogleForm={(updater) => setGoogleForm((current) => updater(current))}
            actionFeedback={actionFeedback}
            onSettingsSave={handleSettingsSave}
            onCreateUser={handleCreateUser}
            onShopifyBackfill={handleShopifyBackfill}
            onMetaConfigSave={handleMetaConfigSave}
            onGoogleConfigSave={handleGoogleConfigSave}
            onGoogleConnect={handleGoogleConnect}
            onShopifyTest={handleShopifyTest}
            onShopifyWebhookSync={handleShopifyWebhookSync}
            onShopifyAttributionRecovery={handleShopifyAttributionRecovery}
            onShopifyOrderAttributionBackfill={handleShopifyOrderAttributionBackfill}
            onOrderAttributionBackfillRefresh={() => {
              const jobId = orderAttributionBackfillJob.data?.jobId ?? readStoredOrderAttributionBackfillJobId();
              if (jobId) {
                void loadOrderAttributionBackfillJob(jobId, { preserveData: true });
              }
            }}
            onMetaConnect={handleMetaConnect}
            onMetaSync={handleMetaSync}
            onGoogleSync={handleGoogleSync}
            onGoogleReconcile={handleGoogleReconcile}
          />
        </Suspense>
      ) : null}
    </AuthenticatedAppShell>
  );
}

export default App;
