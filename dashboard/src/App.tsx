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
  backfillShopifyOrders,
  clearStoredAuthToken,
  createUser,
  fetchAppSettings,
  fetchCampaigns,
  fetchCurrentUser,
  fetchGoogleAdsStatus,
  fetchMetaAdsStatus,
  fetchOrderDetails,
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
  type AuthUser,
  type CampaignRow,
  type CreateUserPayload,
  type GoogleAdsConfigSummary,
  type GoogleAdsStatusResponse,
  type MetaAdsConnection,
  type MetaAdsConfigSummary,
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
const OrderDetailsView = lazy(() => import('./components/OrderDetailsView'));
const SettingsAdminView = lazy(() => import('./components/SettingsAdminView'));

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

type AppPage = 'dashboard' | 'settings' | 'order-details';

const AUTHENTICATED_NAV_ITEMS: AppShellNavItem[] = [
  {
    key: 'dashboard',
    label: 'Dashboard',
    description: 'Summary metrics, campaign performance, time-based revenue trends, and attributed order rows.'
  },
  {
    key: 'settings',
    label: 'Settings',
    description: 'Reporting timezone, platform connections, sync actions, and dashboard user access.'
  }
];

const DEFAULT_REPORTING_TIMEZONE = 'America/Los_Angeles';
const DEFAULT_GROUP_BY: TimeseriesGroupBy = 'day';
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

const DASHBOARD_QUERY_PARAM_KEYS = ['startDate', 'endDate', 'source', 'campaign', 'attributionModel', 'groupBy'] as const;
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
    campaign: ''
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
  const groupBy = params.get('groupBy');

  return {
    filters: normalizeReportingFilters({
      startDate: isValidDateInput(startDate) ? startDate : defaults.startDate,
      endDate: isValidDateInput(endDate) ? endDate : defaults.endDate,
      source: source ?? '',
      campaign: campaign ?? '',
      attributionModel: isAttributionModel(attributionModel) ? attributionModel : undefined
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
  const [shopifyConnection, setShopifyConnection] = useState<AsyncSection<ShopifyConnectionResponse>>(createLoadingSection());
  const [shopifyBackfillRange, setShopifyBackfillRange] = useState({
    startDate: buildAprilFirstDateInput(DEFAULT_REPORTING_TIMEZONE),
    endDate: buildYesterdayDateInput(DEFAULT_REPORTING_TIMEZONE)
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
      campaign: (deferredCampaign ?? '').trim()
    }),
    [deferredCampaign, deferredSource, filters.endDate, filters.startDate]
  );

  const dashboard = useDashboardData(appliedFilters, groupBy, authState.user !== null, dashboardRefreshKey);
  const reportingTimezone = appSettings.data?.reportingTimezone ?? settingsForm.reportingTimezone ?? DEFAULT_REPORTING_TIMEZONE;

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

  useEffect(() => {
    if (authState.user) {
      void loadAppSettings();
      void loadConnections();
      return;
    }

    setAppSettings(createLoadingSection());
    setShopifyConnection(createLoadingSection());
    setMetaConnection(createLoadingSection());
    setGoogleConnection(createLoadingSection());
  }, [authState.user, loadAppSettings, loadConnections]);

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
    setActionFeedback({
      context: null,
      loading: null,
      error: null,
      message: null
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

  const activeNavKey = currentPage;
  const shellNavItems: AppShellNavItem[] =
    currentPage === 'order-details'
      ? [
          ...AUTHENTICATED_NAV_ITEMS,
          {
            key: 'order-details',
            label: 'Order details',
            shortLabel: 'Order',
            description: 'Contextual drill-in for a selected attributed Shopify order.'
          }
        ]
      : AUTHENTICATED_NAV_ITEMS;
  const breadcrumbs: AppShellBreadcrumb[] =
    currentPage === 'dashboard'
      ? [
          { label: 'Authenticated app' },
          { label: 'Dashboard', current: true }
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
  const handleAppNavigation = useCallback(
    (key: string) => {
      if (key === 'order-details') {
        return;
      }

      if (key === 'dashboard') {
        closeOrderDetails();
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
    [closeOrderDetails]
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
            isAdmin={authState.user.isAdmin}
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
