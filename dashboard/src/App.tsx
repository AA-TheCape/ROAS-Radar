import { startTransition, useDeferredValue, useEffect, useMemo, useState, type FormEvent } from 'react';

import {
  backfillShopifyOrders,
  clearStoredAuthToken,
  connectGoogleAds,
  createUser,
  fetchAppSettings,
  fetchCampaigns,
  fetchCurrentUser,
  fetchGoogleAdsStatus,
  fetchMetaAdsStatus,
  fetchOrderDetails,
  fetchOrders,
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
  startMetaAdsOauth,
  syncGoogleAds,
  syncMetaAds,
  updateAppSettings,
  updateMetaAdsConfig,
  type AppSettings,
  type AuthUser,
  type CampaignRow,
  type CreateUserPayload,
  type GoogleAdsConnectionPayload,
  type GoogleAdsStatusResponse,
  type MetaAdsConnection,
  type MetaAdsConfigSummary,
  type OrderDetailsResponse,
  type OrderRow,
  type ReportingFilters,
  type ShopifyConnectionResponse,
  type ShopifyBackfillResponse,
  type ShopifyAttributionRecoveryResponse,
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
import OrderDetailsView from './components/OrderDetailsView';
import ReportingDashboard from './components/ReportingDashboard';
import SettingsAdminView from './components/SettingsAdminView';
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
};

type ActionFeedback = {
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

function formatDateTimeForClock(date: Date, reportingTimezone = DEFAULT_REPORTING_TIMEZONE): string {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: reportingTimezone,
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit'
  }).format(date);
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
    orders: createLoadingSection()
  });

  useEffect(() => {
    if (!enabled) {
      setState({
        summary: {
          data: null,
          loading: false,
          error: null
        },
        campaigns: createResolvedSection<CampaignRow[]>([]),
        timeseries: createResolvedSection<TimeseriesPoint[]>([]),
        orders: createResolvedSection<OrderRow[]>([])
      });
      return;
    }

    let cancelled = false;

    setState({
      summary: createLoadingSection(),
      campaigns: createLoadingSection(),
      timeseries: createLoadingSection(),
      orders: createLoadingSection()
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

    return () => {
      cancelled = true;
    };
  }, [enabled, filters, groupBy, refreshKey]);

  return state;
}

function formatOptionalDateTime(value: string | null | undefined, reportingTimezone: string): string {
  return value ? formatDateTimeLabel(value, reportingTimezone) : 'Not available';
}
function App() {
  const [authState, setAuthState] = useState<AuthState>({
    checking: true,
    user: null,
    error: null
  });
  const [loginEmail, setLoginEmail] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [loginSubmitting, setLoginSubmitting] = useState(false);
  const [filters, setFilters] = useState<ReportingFilters>(() => ({
    ...buildRange(30, DEFAULT_REPORTING_TIMEZONE),
    source: '',
    campaign: ''
  }));
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
  const [groupBy, setGroupBy] = useState<TimeseriesGroupBy>('day');
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
  const [googleConnection, setGoogleConnection] = useState<AsyncSection<GoogleAdsStatusResponse>>(createLoadingSection());
  const [googleForm, setGoogleForm] = useState<GoogleAdsConnectionPayload>({
    customerId: '',
    loginCustomerId: '',
    developerToken: '',
    clientId: '',
    clientSecret: '',
    refreshToken: ''
  });
  const [actionFeedback, setActionFeedback] = useState<ActionFeedback>({
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
  const [currentTime, setCurrentTime] = useState(() => new Date());
  const activeWindowTime = useMemo(
    () => formatDateTimeForClock(currentTime, reportingTimezone),
    [currentTime, reportingTimezone]
  );

  useEffect(() => {
    const timer = window.setInterval(() => {
      setCurrentTime(new Date());
    }, 60_000);

    return () => {
      window.clearInterval(timer);
    };
  }, []);

  async function loadAppSettings() {
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
  }

  async function loadConnections() {
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
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load ad connection state';
      setShopifyConnection(createErroredSection(message));
      setMetaConnection(createErroredSection(message));
      setGoogleConnection(createErroredSection(message));
    }
  }

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
  }, [authState.user]);

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

  async function openOrderDetails(shopifyOrderId: string) {
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
  }

  function closeOrderDetails() {
    setCurrentPage('dashboard');
    setSelectedOrderId(null);
    setOrderDetailsSection({
      data: null,
      loading: false,
      error: null
    });
  }

  async function loadUsers() {
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
  }

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
  }, [authState.user]);

  const summaryCards = useMemo(() => {
    const totals = dashboard.summary.data;

    return [
      {
        label: 'Visits',
        value: formatNumber(totals?.visits),
        detail: `${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`
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
      loading: null,
      error: null,
      message: null
    });
  }

  async function handleSettingsSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
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
        loading: null,
        error: null,
        message: `Saved reporting timezone as ${response.settings.reportingTimezone}.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to save dashboard settings',
        message: null
      });
    }
  }

  async function handleCreateUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
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
        loading: null,
        error: null,
        message: `Created user ${response.user.email}.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to create user',
        message: null
      });
    }
  }

  async function handleMetaConnect() {
    setActionFeedback({
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
        loading: null,
        error: null,
        message: 'Redirecting to Meta Ads…'
      });
      window.location.assign(response.authorizationUrl);
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to start Meta Ads OAuth',
        message: null
      });
    }
  }

  async function handleMetaConfigSave(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
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
        loading: null,
        error: null,
        message: 'Saved Meta Ads configuration.'
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to save Meta Ads configuration',
        message: null
      });
    }
  }

  async function handleShopifyTest() {
    setActionFeedback({
      loading: 'shopify-test',
      error: null,
      message: null
    });

    try {
      const response = await fetchShopifyConnection();
      setShopifyConnection(createResolvedSection(response));
      setActionFeedback({
        loading: null,
        error: null,
        message: response.connected
          ? `Shopify connection is active for ${response.shopDomain ?? 'the connected store'}.`
          : 'No active Shopify installation was found.'
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to verify Shopify connection',
        message: null
      });
    }
  }

  async function handleShopifyWebhookSync() {
    setActionFeedback({
      loading: 'shopify-webhooks',
      error: null,
      message: null
    });

    try {
      const response = await syncShopifyWebhooks();
      await loadConnections();
      setActionFeedback({
        loading: null,
        error: null,
        message: `Re-provisioned Shopify webhooks for ${response.shopDomain}.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to sync Shopify webhooks',
        message: null
      });
    }
  }

  async function handleShopifyBackfill(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
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
        loading: null,
        error: null,
        message: `Backfilled ${response.importedOrders} Shopify orders for ${response.startDate} to ${response.endDate} (${response.processedOrders} imported, ${response.duplicatedOrders} already present).`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to backfill Shopify orders',
        message: null
      });
    }
  }

  async function handleShopifyAttributionRecovery() {
    setActionFeedback({
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
        loading: null,
        error: null,
        message: `Rescanned ${response.rescannedOrders} unknown Shopify web orders for ${response.startDate} to ${response.endDate}; relinked ${response.relinkedOrders}, attributed ${response.shopifyHintAttributedOrders} from Shopify hints, and requeued ${response.requeuedOrders} for standard attribution.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to recover Shopify attribution hints',
        message: null
      });
    }
  }

  async function handleMetaSync() {
    setActionFeedback({
      loading: 'meta-sync',
      error: null,
      message: null
    });

    try {
      const response = await syncMetaAds(filters.startDate, filters.endDate);
      await loadConnections();
      setActionFeedback({
        loading: null,
        error: null,
        message: `Queued ${response.enqueuedJobs} Meta Ads sync jobs for ${response.dates.length} dates.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to queue Meta Ads sync',
        message: null
      });
    }
  }

  async function handleGoogleConnect(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setActionFeedback({
      loading: 'google-connect',
      error: null,
      message: null
    });

    try {
      const response = await connectGoogleAds({
        ...googleForm,
        loginCustomerId: googleForm.loginCustomerId?.trim() ? googleForm.loginCustomerId.trim() : undefined
      });
      await loadConnections();
      setActionFeedback({
        loading: null,
        error: null,
        message: `Connected Google Ads customer ${response.customerId}${response.customerName ? ` (${response.customerName})` : ''}.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to connect Google Ads',
        message: null
      });
    }
  }

  async function handleGoogleSync() {
    setActionFeedback({
      loading: 'google-sync',
      error: null,
      message: null
    });

    try {
      const response = await syncGoogleAds(filters.startDate, filters.endDate);
      await loadConnections();
      setActionFeedback({
        loading: null,
        error: null,
        message: `Queued ${response.enqueuedJobs} Google Ads sync jobs for ${response.dates.length} dates.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to queue Google Ads sync',
        message: null
      });
    }
  }

  async function handleGoogleReconcile() {
    setActionFeedback({
      loading: 'google-reconcile',
      error: null,
      message: null
    });

    try {
      const response = await reconcileGoogleAds();
      await loadConnections();
      setActionFeedback({
        loading: null,
        error: null,
        message: `Queued ${response.enqueuedJobs} Google Ads reconciliation jobs.`
      });
    } catch (error) {
      setActionFeedback({
        loading: null,
        error: error instanceof Error ? error.message : 'Failed to reconcile Google Ads',
        message: null
      });
    }
  }

  const pageEyebrow =
    currentPage === 'settings'
      ? 'Admin settings'
      : currentPage === 'order-details'
        ? 'Order drill-in'
        : 'MVP reporting dashboard';
  const pageTitle =
    currentPage === 'settings'
      ? 'Configure reporting settings and platform connections'
      : currentPage === 'order-details'
        ? `Inspect order #${selectedOrderId ?? 'Unknown'}`
        : 'Monitor acquisition performance across revenue, campaigns, and orders';
  const pageDescription =
    currentPage === 'settings'
      ? 'Configure store integrations, ad platform connections, and dashboard user access from one place.'
      : currentPage === 'order-details'
        ? 'Inspect the full stored Shopify order record, attribution credits, line items, and raw payload for one order.'
        : 'Monitor paid acquisition performance for a single Shopify store across headline metrics, campaign rows, time-based trends, and order-level attribution evidence.';
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
  const shellHeaderStatus = (
    <div className="grid gap-4">
      <div>
        <p className="text-caption uppercase tracking-[0.14em] text-ink-muted">Active window</p>
        <p className="mt-2 font-display text-display text-ink">
          {currentPage === 'order-details' ? `#${selectedOrderId ?? '—'}` : filters.endDate}
        </p>
        <p className="mt-2 text-body text-ink-soft">{activeWindowTime}</p>
      </div>
      <dl className="grid gap-3 text-body">
        <div className="rounded-card border border-line/70 bg-canvas-tint p-4">
          <dt className="text-caption uppercase tracking-[0.12em] text-ink-muted">Traffic scope</dt>
          <dd className="mt-2 text-ink-soft">
            {(filters.source ?? '').trim() || (filters.campaign ?? '').trim()
              ? `Filtered by ${[(filters.source ?? '').trim(), (filters.campaign ?? '').trim()].filter(Boolean).join(' / ')}`
              : 'All attributed traffic'}
          </dd>
        </div>
        <div className="rounded-card border border-line/70 bg-canvas-tint p-4">
          <dt className="text-caption uppercase tracking-[0.12em] text-ink-muted">Reporting timezone</dt>
          <dd className="mt-2 text-ink-soft">{reportingTimezone}</dd>
        </div>
      </dl>
    </div>
  );
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
      onNavigate={(key) => {
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
      }}
      breadcrumbs={breadcrumbs}
      eyebrow={pageEyebrow}
      title={pageTitle}
      description={pageDescription}
      topbarMeta={
        <div className="space-y-1">
          <p className="font-semibold text-ink">{authenticatedUser.displayName}</p>
          <p>{authenticatedUser.email}</p>
        </div>
      }
      headerStatus={shellHeaderStatus}
      headerActions={shellHeaderActions}
    >
      {currentPage === 'dashboard' ? (
        <ReportingDashboard
          filters={filters}
          onFiltersChange={setFilters}
          groupBy={groupBy}
          onGroupByChange={setGroupBy}
          reportingTimezone={reportingTimezone}
          quickRanges={PRESETS}
          onApplyQuickRange={(range) =>
            startTransition(() => {
              setFilters((current) => ({
                ...current,
                ...range
              }));
            })
          }
          onClearFilters={() =>
            startTransition(() => {
              setFilters((current) => ({
                ...current,
                source: '',
                campaign: ''
              }));
            })
          }
          summaryCards={summaryCards}
          summarySection={dashboard.summary}
          campaignsSection={dashboard.campaigns}
          timeseriesSection={dashboard.timeseries}
          ordersSection={dashboard.orders}
          onOpenOrderDetails={(shopifyOrderId) => void openOrderDetails(shopifyOrderId)}
        />
      ) : null}

      {currentPage === 'order-details' ? (
        <section className="dashboard-grid">
          <Panel
            title="Order details"
            description="Everything currently stored for this Shopify order, including line items, attribution credits, and raw payload."
            wide
          >
            <OrderDetailsView
              selectedOrderId={selectedOrderId}
              reportingTimezone={reportingTimezone}
              orderDetailsSection={orderDetailsSection}
            />
          </Panel>
        </section>
      ) : null}

      {currentPage === 'settings' ? (
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
          googleForm={googleForm}
          setGoogleForm={(updater) => setGoogleForm((current) => updater(current))}
          actionFeedback={actionFeedback}
          onSettingsSave={handleSettingsSave}
          onCreateUser={handleCreateUser}
          onShopifyBackfill={handleShopifyBackfill}
          onMetaConfigSave={handleMetaConfigSave}
          onGoogleConnect={handleGoogleConnect}
          onShopifyTest={handleShopifyTest}
          onShopifyWebhookSync={handleShopifyWebhookSync}
          onShopifyAttributionRecovery={handleShopifyAttributionRecovery}
          onMetaConnect={handleMetaConnect}
          onMetaSync={handleMetaSync}
          onGoogleSync={handleGoogleSync}
          onGoogleReconcile={handleGoogleReconcile}
        />
      ) : null}
    </AuthenticatedAppShell>
  );
}

export default App;
