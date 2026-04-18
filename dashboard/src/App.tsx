import { startTransition, useDeferredValue, useEffect, useMemo, useState, type FormEvent } from 'react';

import {
  backfillShopifyOrders,
  clearStoredAuthToken,
  connectGoogleAds,
  createUser,
  fetchCampaigns,
  fetchCurrentUser,
  fetchGoogleAdsStatus,
  fetchMetaAdsStatus,
  fetchOrders,
  fetchShopifyConnection,
  fetchSummary,
  fetchTimeseries,
  fetchUsers,
  getStoredAuthToken,
  login,
  logout,
  reconcileGoogleAds,
  storeAuthToken,
  syncShopifyWebhooks,
  startMetaAdsOauth,
  syncGoogleAds,
  syncMetaAds,
  updateMetaAdsConfig,
  type AuthUser,
  type CampaignRow,
  type CreateUserPayload,
  type GoogleAdsConnectionPayload,
  type GoogleAdsStatusResponse,
  type MetaAdsConnection,
  type MetaAdsConfigSummary,
  type OrderRow,
  type ReportingFilters,
  type ShopifyConnectionResponse,
  type ShopifyBackfillResponse,
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

type AppPage = 'dashboard' | 'settings';

const PRESETS = [
  { label: 'Today', value: () => buildRange(1) },
  { label: 'Yesterday', value: () => buildSingleDayRange(-1) },
  { label: 'Last 7D', value: () => buildRange(7) },
  { label: 'Last 30D', value: () => buildRange(30) },
  { label: 'Last 90D', value: () => buildRange(90) }
] as const;

const GROUP_BY_OPTIONS: Array<{ value: TimeseriesGroupBy; label: string }> = [
  { value: 'day', label: 'Daily' },
  { value: 'source', label: 'By source' },
  { value: 'campaign', label: 'By campaign' }
];

function formatDateInput(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function buildRange(days: number): Pick<ReportingFilters, 'startDate' | 'endDate'> {
  const end = new Date();
  const start = new Date(end);
  start.setUTCDate(end.getUTCDate() - (days - 1));

  return {
    startDate: formatDateInput(start),
    endDate: formatDateInput(end)
  };
}

function buildSingleDayRange(offsetDays: number): Pick<ReportingFilters, 'startDate' | 'endDate'> {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() + offsetDays);
  const value = formatDateInput(date);

  return {
    startDate: value,
    endDate: value
  };
}

function buildYesterdayDateInput(): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - 1);
  return formatDateInput(date);
}

function buildAprilFirstDateInput(): string {
  const date = new Date();
  date.setUTCMonth(3, 1);
  return formatDateInput(date);
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

function buildSeriesPath(points: TimeseriesPoint[]): string {
  if (points.length === 0) {
    return '';
  }

  const maxRevenue = Math.max(...points.map((point) => point.revenue), 1);

  return points
    .map((point, index) => {
      const x = points.length === 1 ? 320 : (index / (points.length - 1)) * 320;
      const y = 144 - (point.revenue / maxRevenue) * 120;
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');
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

function SectionState({
  loading,
  error,
  empty,
  emptyLabel,
  children
}: {
  loading: boolean;
  error: string | null;
  empty: boolean;
  emptyLabel: string;
  children: JSX.Element;
}) {
  if (loading) {
    return <div className="panel-state">Loading data…</div>;
  }

  if (error) {
    return <div className="panel-state panel-state-error">{error}</div>;
  }

  if (empty) {
    return <div className="panel-state">{emptyLabel}</div>;
  }

  return children;
}

function SummaryCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <article className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

function TimeseriesChart({ points, groupBy }: { points: TimeseriesPoint[]; groupBy: TimeseriesGroupBy }) {
  const path = buildSeriesPath(points);
  const maxRevenue = Math.max(...points.map((point) => point.revenue), 1);

  return (
    <div className="chart-card">
      <div className="chart-svg-shell">
        <svg viewBox="0 0 320 160" className="chart-svg" aria-label="Revenue timeseries">
          <defs>
            <linearGradient id="revenueFill" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(186, 87, 32, 0.35)" />
              <stop offset="100%" stopColor="rgba(186, 87, 32, 0.02)" />
            </linearGradient>
          </defs>
          <path d="M 0 144 H 320" className="chart-axis" />
          <path d={path} className="chart-line" />
          {path ? <path d={`${path} L 320 144 L 0 144 Z`} className="chart-area" /> : null}
          {points.map((point, index) => {
            const x = points.length === 1 ? 320 : (index / (points.length - 1)) * 320;
            const y = 144 - (point.revenue / maxRevenue) * 120;

            return <circle key={`${point.date}-${index}`} cx={x} cy={y} r="4" className="chart-dot" />;
          })}
        </svg>
      </div>
      <div className="chart-labels">
        {points.map((point) => (
          <div key={point.date} className="chart-label">
            <strong>{groupBy === 'day' ? formatDateLabel(point.date) : point.date}</strong>
            <span>{formatCurrency(point.revenue)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function formatOptionalDateTime(value: string | null | undefined): string {
  return value ? formatDateTimeLabel(value) : 'Not available';
}

function ConnectionState({
  loading,
  error,
  children
}: {
  loading: boolean;
  error: string | null;
  children: JSX.Element;
}) {
  if (loading) {
    return <div className="panel-state connection-state">Loading connection state…</div>;
  }

  if (error) {
    return <div className="panel-state panel-state-error connection-state">{error}</div>;
  }

  return children;
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
    ...buildRange(30),
    source: '',
    campaign: ''
  }));
  const [usersSection, setUsersSection] = useState<AsyncSection<AuthUser[]>>(createLoadingSection());
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
    startDate: buildAprilFirstDateInput(),
    endDate: buildYesterdayDateInput()
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
      void loadConnections();
      return;
    }

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
      });
  }, []);

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
        detail: `${formatDateLabel(filters.startDate)} to ${formatDateLabel(filters.endDate)}`
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
  }, [dashboard.summary.data, filters.endDate, filters.startDate]);

  const totalCampaignRevenue = useMemo(
    () => (dashboard.campaigns.data ?? []).reduce((sum, row) => sum + row.revenue, 0),
    [dashboard.campaigns.data]
  );

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
    setUsersSection({
      data: null,
      loading: false,
      error: null
    });
    setActionFeedback({
      loading: null,
      error: null,
      message: null
    });
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

  if (authState.checking) {
    return (
      <main className="auth-shell">
        <section className="auth-card">
          <p className="eyebrow">Secure dashboard</p>
          <h1>Checking your session</h1>
          <p className="hero-copy">The dashboard stays locked until an authenticated user is verified.</p>
        </section>
      </main>
    );
  }

  if (!authState.user) {
    return (
      <main className="auth-shell">
        <section className="auth-card">
          <p className="eyebrow">Secure dashboard</p>
          <h1>ROAS Radar Login</h1>
          <p className="hero-copy">Sign in with an app user account before viewing any reporting or admin tools.</p>
          <form className="credential-form" onSubmit={(event) => void handleLogin(event)}>
            <div className="credential-grid auth-grid">
              <label>
                <span>Email</span>
                <input type="email" value={loginEmail} onChange={(event) => setLoginEmail(event.target.value)} required />
              </label>
              <label>
                <span>Password</span>
                <input
                  type="password"
                  value={loginPassword}
                  onChange={(event) => setLoginPassword(event.target.value)}
                  required
                />
              </label>
            </div>
            {authState.error ? <div className="action-banner action-banner-error">{authState.error}</div> : null}
            <div className="button-row">
              <button type="submit" className="action-button" disabled={loginSubmitting}>
                {loginSubmitting ? 'Signing in…' : 'Login'}
              </button>
            </div>
          </form>
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-copy-block">
          <p className="eyebrow">{currentPage === 'settings' ? 'Admin settings' : 'MVP reporting dashboard'}</p>
          <h1>ROAS Radar</h1>
          <p className="hero-copy">
            {currentPage === 'settings'
              ? 'Configure store integrations, ad platform connections, and dashboard user access from one place.'
              : 'Monitor paid acquisition performance for a single Shopify store across headline metrics, campaign rows, time-based trends, and order-level attribution evidence.'}
          </p>
        </div>
        <div className="hero-status-card">
          <span>Active window</span>
          <strong>{filters.endDate}</strong>
          <small>
            {(filters.source ?? '').trim() || (filters.campaign ?? '').trim()
              ? `Filtered by ${[(filters.source ?? '').trim(), (filters.campaign ?? '').trim()].filter(Boolean).join(' / ')}`
              : 'All attributed traffic'}
          </small>
          <small>{`Signed in as ${authState.user.displayName} (${authState.user.email})`}</small>
          <div className="hero-status-actions">
            <button
              type="button"
              className="nav-link-button"
              onClick={() => setCurrentPage(currentPage === 'settings' ? 'dashboard' : 'settings')}
            >
              {currentPage === 'settings' ? 'Back to dashboard' : 'Settings'}
            </button>
          </div>
          <div className="button-row">
            <button type="button" className="action-button action-button-secondary" onClick={() => void handleLogout()}>
              Logout
            </button>
          </div>
        </div>
      </section>

      {currentPage === 'dashboard' ? <section className="control-bar">
        <div className="control-group">
          <label htmlFor="start-date">Start date</label>
          <input
            id="start-date"
            type="date"
            value={filters.startDate}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, startDate: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group">
          <label htmlFor="end-date">End date</label>
          <input
            id="end-date"
            type="date"
            value={filters.endDate}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, endDate: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group">
          <label htmlFor="source-filter">Source</label>
          <input
            id="source-filter"
            type="text"
            placeholder="google, meta, facebook"
            value={filters.source}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, source: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group">
          <label htmlFor="campaign-filter">Campaign</label>
          <input
            id="campaign-filter"
            type="text"
            placeholder="spring-sale"
            value={filters.campaign}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, campaign: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group control-group-wide">
          <label htmlFor="group-by">Timeseries grouping</label>
          <select
            id="group-by"
            value={groupBy}
            onChange={(event) => {
              setGroupBy(event.target.value as TimeseriesGroupBy);
            }}
          >
            {GROUP_BY_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
        <div className="control-group control-group-wide">
          <label>Quick ranges</label>
          <div className="preset-row">
            {PRESETS.map((preset) => (
              <button
                key={preset.label}
                type="button"
                className="preset-chip"
                onClick={() =>
                  startTransition(() => {
                    setFilters((current) => ({
                      ...current,
                      ...preset.value()
                    }));
                  })
                }
              >
                {preset.label}
              </button>
            ))}
            <button
              type="button"
              className="preset-chip preset-chip-secondary"
              onClick={() =>
                startTransition(() => {
                  setFilters((current) => ({
                    ...current,
                    source: '',
                    campaign: ''
                  }));
                })
              }
            >
              Clear filters
            </button>
          </div>
        </div>
      </section> : null}

      {currentPage === 'dashboard' ? <section className="summary-grid">
        {summaryCards.map((card) => (
          <SummaryCard key={card.label} label={card.label} value={card.value} detail={card.detail} />
        ))}
      </section> : null}

      <section className="dashboard-grid">
        {currentPage === 'settings' ? (
        <article className="panel panel-wide">
          <div className="panel-header">
            <h2>Settings</h2>
            <p>Manage store connections, ad platform credentials, and dashboard access here.</p>
          </div>
          {actionFeedback.error ? <div className="action-banner action-banner-error">{actionFeedback.error}</div> : null}
          {actionFeedback.message ? <div className="action-banner">{actionFeedback.message}</div> : null}
        </article>
        ) : null}

        {currentPage === 'settings' ? (
        <article className="panel panel-wide">
          <div className="panel-header">
            <h2>Ad connections</h2>
            <p>Connect spend sources here so ROAS Radar can populate spend and ROAS, not just attributed revenue.</p>
          </div>
          <div className="connections-grid">
            <section className="connection-card">
              <div className="connection-card-header">
                <div>
                  <h3>Shopify</h3>
                  <p>Checks the installed Shopify app connection and can re-provision webhooks if needed.</p>
                </div>
                <span className="status-pill">
                  {shopifyConnection.data?.status ??
                    (shopifyConnection.data?.connected ? 'active' : shopifyConnection.loading ? 'Loading' : 'Not connected')}
                </span>
              </div>
              <ConnectionState loading={shopifyConnection.loading} error={shopifyConnection.error}>
                <div className="connection-card-body">
                  <dl className="detail-list">
                    <div>
                      <dt>Shop</dt>
                      <dd>{shopifyConnection.data?.shop?.name ?? shopifyConnection.data?.shopDomain ?? 'Not connected'}</dd>
                    </div>
                    <div>
                      <dt>Domain</dt>
                      <dd>{shopifyConnection.data?.shopDomain ?? 'Not available'}</dd>
                    </div>
                    <div>
                      <dt>Installed</dt>
                      <dd>{formatOptionalDateTime(shopifyConnection.data?.installedAt)}</dd>
                    </div>
                    <div>
                      <dt>Webhooks</dt>
                      <dd>{shopifyConnection.data?.webhookBaseUrl ?? 'Not available'}</dd>
                    </div>
                  </dl>
                    {shopifyConnection.data?.reconnectUrl ? (
                      <div className="connection-note">Reconnect URL is available if the store needs to be reauthorized.</div>
                    ) : null}
                    <form className="credentials-form" onSubmit={handleShopifyBackfill}>
                      <div className="credentials-grid">
                        <label className="credential-field">
                          <span>Backfill start</span>
                          <input
                            type="date"
                            value={shopifyBackfillRange.startDate}
                            onChange={(event) =>
                              setShopifyBackfillRange((current) => ({ ...current, startDate: event.target.value }))
                            }
                            required
                          />
                        </label>
                        <label className="credential-field">
                          <span>Backfill end</span>
                          <input
                            type="date"
                            value={shopifyBackfillRange.endDate}
                            onChange={(event) =>
                              setShopifyBackfillRange((current) => ({ ...current, endDate: event.target.value }))
                            }
                            required
                          />
                        </label>
                      </div>
                      <div className="button-row">
                        <button
                          type="submit"
                          className="action-button action-button-secondary"
                          disabled={actionFeedback.loading !== null || !shopifyConnection.data?.connected}
                        >
                          {actionFeedback.loading === 'shopify-backfill'
                            ? 'Backfilling…'
                            : `Backfill Shopify orders`}
                        </button>
                      </div>
                    </form>
                    <div className="button-row">
                      <button
                        type="button"
                      className="action-button"
                      onClick={() => void handleShopifyTest()}
                      disabled={actionFeedback.loading !== null}
                    >
                      {actionFeedback.loading === 'shopify-test' ? 'Testing…' : 'Test Shopify connection'}
                    </button>
                    <button
                      type="button"
                      className="action-button action-button-secondary"
                      onClick={() => void handleShopifyWebhookSync()}
                      disabled={actionFeedback.loading !== null || !shopifyConnection.data?.connected}
                    >
                      {actionFeedback.loading === 'shopify-webhooks' ? 'Syncing…' : 'Sync Shopify webhooks'}
                    </button>
                  </div>
                </div>
              </ConnectionState>
            </section>

            <section className="connection-card">
              <div className="connection-card-header">
                <div>
                  <h3>Meta Ads</h3>
                  <p>Save your Meta app settings here, then start OAuth to attach the ad account.</p>
                </div>
                <span className="status-pill">
                  {metaConnection.data?.connection?.status ??
                    (metaConnection.data?.config.missingFields.length ? 'Needs config' : metaConnection.loading ? 'Loading' : 'Not connected')}
                </span>
              </div>
              <ConnectionState loading={metaConnection.loading} error={metaConnection.error}>
                <div className="connection-card-body">
                  <dl className="detail-list">
                    <div>
                      <dt>Config source</dt>
                      <dd>{metaConnection.data?.config.source ?? 'Not available'}</dd>
                    </div>
                    <div>
                      <dt>Ad account</dt>
                      <dd>
                        {metaConnection.data?.connection?.account_name ??
                          metaConnection.data?.config.adAccountId ??
                          'Not configured'}
                      </dd>
                    </div>
                    <div>
                      <dt>Last sync</dt>
                      <dd>{formatOptionalDateTime(metaConnection.data?.connection?.last_sync_completed_at)}</dd>
                    </div>
                    <div>
                      <dt>Sync status</dt>
                      <dd>{metaConnection.data?.connection?.last_sync_status ?? 'Not started'}</dd>
                    </div>
                  </dl>
                  {metaConnection.data?.config.missingFields.length ? (
                    <div className="connection-note connection-note-error">
                      Missing Meta config: {metaConnection.data.config.missingFields.join(', ')}
                    </div>
                  ) : null}
                  {metaConnection.data?.connection?.last_sync_error ? (
                    <div className="connection-note connection-note-error">{metaConnection.data.connection.last_sync_error}</div>
                  ) : null}
                  <form className="credentials-form" onSubmit={handleMetaConfigSave}>
                    <div className="credentials-grid">
                      <label className="credential-field">
                        <span>Meta app ID</span>
                        <input
                          type="text"
                          value={metaConfigForm.appId}
                          onChange={(event) =>
                            setMetaConfigForm((current) => ({ ...current, appId: event.target.value }))
                          }
                          placeholder="123456789012345"
                        />
                      </label>
                      <label className="credential-field">
                        <span>Ad account ID</span>
                        <input
                          type="text"
                          value={metaConfigForm.adAccountId}
                          onChange={(event) =>
                            setMetaConfigForm((current) => ({ ...current, adAccountId: event.target.value }))
                          }
                          placeholder="act_123456789012345 or 123456789012345"
                        />
                      </label>
                      <label className="credential-field credential-field-wide">
                        <span>Meta app secret</span>
                        <input
                          type="password"
                          value={metaConfigForm.appSecret}
                          onChange={(event) =>
                            setMetaConfigForm((current) => ({ ...current, appSecret: event.target.value }))
                          }
                          placeholder={
                            metaConnection.data?.config.appSecretConfigured
                              ? 'Leave blank to keep the saved secret'
                              : 'Paste the Meta app secret'
                          }
                        />
                      </label>
                      <label className="credential-field credential-field-wide">
                        <span>OAuth base URL</span>
                        <input
                          type="url"
                          value={metaConfigForm.appBaseUrl}
                          onChange={(event) =>
                            setMetaConfigForm((current) => ({ ...current, appBaseUrl: event.target.value }))
                          }
                          placeholder="https://roas-radar.api.thecapemarine.com"
                        />
                      </label>
                      <label className="credential-field credential-field-wide">
                        <span>Scopes</span>
                        <input
                          type="text"
                          value={metaConfigForm.appScopes}
                          onChange={(event) =>
                            setMetaConfigForm((current) => ({ ...current, appScopes: event.target.value }))
                          }
                          placeholder="ads_read"
                        />
                      </label>
                    </div>
                    <div className="button-row">
                      <button type="submit" className="action-button" disabled={actionFeedback.loading !== null}>
                        {actionFeedback.loading === 'meta-config-save' ? 'Saving…' : 'Save Meta config'}
                      </button>
                    </div>
                  </form>
                  <div className="button-row">
                    <button
                      type="button"
                      className="action-button"
                      onClick={() => void handleMetaConnect()}
                      disabled={actionFeedback.loading !== null || Boolean(metaConnection.data?.config.missingFields.length)}
                    >
                      {actionFeedback.loading === 'meta-connect' ? 'Opening Meta…' : 'Connect Meta Ads'}
                    </button>
                    <button
                      type="button"
                      className="action-button action-button-secondary"
                      onClick={() => void handleMetaSync()}
                      disabled={actionFeedback.loading !== null || metaConnection.data?.connection == null}
                    >
                      {actionFeedback.loading === 'meta-sync' ? 'Queueing…' : `Sync ${filters.startDate} to ${filters.endDate}`}
                    </button>
                  </div>
                </div>
              </ConnectionState>
            </section>

            <section className="connection-card">
              <div className="connection-card-header">
                <div>
                  <h3>Google Ads</h3>
                  <p>Creates the encrypted connection directly from Google Ads API credentials and refresh token.</p>
                </div>
                <span className="status-pill">
                  {googleConnection.data?.connection?.status ?? (googleConnection.loading ? 'Loading' : 'Not connected')}
                </span>
              </div>
              <ConnectionState loading={googleConnection.loading} error={googleConnection.error}>
                <div className="connection-card-body">
                  <dl className="detail-list">
                    <div>
                      <dt>Customer</dt>
                      <dd>
                        {googleConnection.data?.connection?.customer_descriptive_name ??
                          googleConnection.data?.connection?.customer_id ??
                          'Not connected'}
                      </dd>
                    </div>
                    <div>
                      <dt>Currency</dt>
                      <dd>{googleConnection.data?.connection?.currency_code ?? 'Not available'}</dd>
                    </div>
                    <div>
                      <dt>Last sync</dt>
                      <dd>{formatOptionalDateTime(googleConnection.data?.connection?.last_sync_completed_at)}</dd>
                    </div>
                    <div>
                      <dt>Reconciliation</dt>
                      <dd>{googleConnection.data?.reconciliation?.status ?? 'Not run'}</dd>
                    </div>
                  </dl>
                  {googleConnection.data?.connection?.last_sync_error ? (
                    <div className="connection-note connection-note-error">
                      {googleConnection.data.connection.last_sync_error}
                    </div>
                  ) : null}
                  {googleConnection.data?.reconciliation?.missing_dates?.length ? (
                    <div className="connection-note">
                      Missing dates: {googleConnection.data.reconciliation.missing_dates.join(', ')}
                    </div>
                  ) : null}
                  <form className="credential-form" onSubmit={(event) => void handleGoogleConnect(event)}>
                    <div className="credential-grid">
                      <label>
                        <span>Customer ID</span>
                        <input
                          type="text"
                          value={googleForm.customerId}
                          onChange={(event) =>
                            setGoogleForm((current) => ({ ...current, customerId: event.target.value }))
                          }
                          placeholder="123-456-7890"
                          required
                        />
                      </label>
                      <label>
                        <span>Login customer ID</span>
                        <input
                          type="text"
                          value={googleForm.loginCustomerId ?? ''}
                          onChange={(event) =>
                            setGoogleForm((current) => ({ ...current, loginCustomerId: event.target.value }))
                          }
                          placeholder="Optional MCC login"
                        />
                      </label>
                      <label>
                        <span>Developer token</span>
                        <input
                          type="password"
                          value={googleForm.developerToken}
                          onChange={(event) =>
                            setGoogleForm((current) => ({ ...current, developerToken: event.target.value }))
                          }
                          required
                        />
                      </label>
                      <label>
                        <span>Client ID</span>
                        <input
                          type="password"
                          value={googleForm.clientId}
                          onChange={(event) =>
                            setGoogleForm((current) => ({ ...current, clientId: event.target.value }))
                          }
                          required
                        />
                      </label>
                      <label>
                        <span>Client secret</span>
                        <input
                          type="password"
                          value={googleForm.clientSecret}
                          onChange={(event) =>
                            setGoogleForm((current) => ({ ...current, clientSecret: event.target.value }))
                          }
                          required
                        />
                      </label>
                      <label>
                        <span>Refresh token</span>
                        <input
                          type="password"
                          value={googleForm.refreshToken}
                          onChange={(event) =>
                            setGoogleForm((current) => ({ ...current, refreshToken: event.target.value }))
                          }
                          required
                        />
                      </label>
                    </div>
                    <div className="button-row">
                      <button type="submit" className="action-button" disabled={actionFeedback.loading !== null}>
                        {actionFeedback.loading === 'google-connect' ? 'Saving…' : 'Connect Google Ads'}
                      </button>
                      <button
                        type="button"
                        className="action-button action-button-secondary"
                        onClick={() => void handleGoogleSync()}
                        disabled={actionFeedback.loading !== null || googleConnection.data?.connection == null}
                      >
                        {actionFeedback.loading === 'google-sync' ? 'Queueing…' : `Sync ${filters.startDate} to ${filters.endDate}`}
                      </button>
                      <button
                        type="button"
                        className="action-button action-button-secondary"
                        onClick={() => void handleGoogleReconcile()}
                        disabled={actionFeedback.loading !== null || googleConnection.data?.connection == null}
                      >
                        {actionFeedback.loading === 'google-reconcile' ? 'Running…' : 'Reconcile gaps'}
                      </button>
                    </div>
                  </form>
                </div>
              </ConnectionState>
            </section>
          </div>
        </article>
        ) : null}

        {currentPage === 'settings' && authState.user.isAdmin ? (
          <article className="panel panel-wide">
            <div className="panel-header">
              <h2>User access</h2>
              <p>All dashboard reporting and admin tools are locked behind app-user authentication.</p>
            </div>
            <SectionState
              loading={usersSection.loading}
              error={usersSection.error}
              empty={false}
              emptyLabel=""
            >
              <div className="connection-card-body">
                <form className="credential-form" onSubmit={(event) => void handleCreateUser(event)}>
                  <div className="credential-grid">
                    <label>
                      <span>Display name</span>
                      <input
                        type="text"
                        value={newUserForm.displayName}
                        onChange={(event) =>
                          setNewUserForm((current) => ({ ...current, displayName: event.target.value }))
                        }
                        required
                      />
                    </label>
                    <label>
                      <span>Email</span>
                      <input
                        type="email"
                        value={newUserForm.email}
                        onChange={(event) => setNewUserForm((current) => ({ ...current, email: event.target.value }))}
                        required
                      />
                    </label>
                    <label>
                      <span>Password</span>
                      <input
                        type="password"
                        value={newUserForm.password}
                        onChange={(event) =>
                          setNewUserForm((current) => ({ ...current, password: event.target.value }))
                        }
                        minLength={12}
                        required
                      />
                    </label>
                    <label className="checkbox-label">
                      <span>Admin access</span>
                      <input
                        type="checkbox"
                        checked={Boolean(newUserForm.isAdmin)}
                        onChange={(event) =>
                          setNewUserForm((current) => ({ ...current, isAdmin: event.target.checked }))
                        }
                      />
                    </label>
                  </div>
                  <div className="button-row">
                    <button type="submit" className="action-button" disabled={actionFeedback.loading !== null}>
                      {actionFeedback.loading === 'user-create' ? 'Creating…' : 'Add user'}
                    </button>
                  </div>
                </form>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>User</th>
                        <th>Role</th>
                        <th>Status</th>
                        <th>Last login</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(usersSection.data ?? []).map((user) => (
                        <tr key={user.id}>
                          <td>
                            <div className="primary-cell">
                              <strong>{user.displayName}</strong>
                              <span>{user.email}</span>
                            </div>
                          </td>
                          <td>{user.isAdmin ? 'Admin' : 'Viewer'}</td>
                          <td>{user.status}</td>
                          <td>{formatOptionalDateTime(user.lastLoginAt)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </SectionState>
          </article>
        ) : null}

        {currentPage === 'dashboard' ? <article className="panel panel-wide">
          <div className="panel-header">
            <h2>Revenue trend</h2>
            <p>Uses the reporting timeseries contract and switches grouping without changing the rest of the dashboard.</p>
          </div>
          <SectionState
            loading={dashboard.timeseries.loading}
            error={dashboard.timeseries.error}
            empty={!dashboard.timeseries.data?.length}
            emptyLabel="No timeseries data returned for this filter range."
          >
            <TimeseriesChart points={dashboard.timeseries.data ?? []} groupBy={groupBy} />
          </SectionState>
        </article> : null}

        {currentPage === 'dashboard' ? <article className="panel">
          <div className="panel-header">
            <h2>Campaign performance</h2>
            <p>Top campaign rows ordered by revenue, matching the API’s table response.</p>
          </div>
          <SectionState
            loading={dashboard.campaigns.loading}
            error={dashboard.campaigns.error}
            empty={!dashboard.campaigns.data?.length}
            emptyLabel="No campaign rows matched the current filters."
          >
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Campaign</th>
                    <th>Source</th>
                    <th>Visits</th>
                    <th>Orders</th>
                    <th>Revenue</th>
                    <th>CVR</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard.campaigns.data ?? []).map((row) => (
                    <tr key={`${row.source}-${row.medium}-${row.campaign}-${row.content ?? 'none'}`}>
                      <td>
                        <div className="primary-cell">
                          <strong>{row.campaign}</strong>
                          <span>{row.content ?? 'No content tag'}</span>
                        </div>
                      </td>
                      <td>{`${row.source} / ${row.medium}`}</td>
                      <td>{formatNumber(row.visits)}</td>
                      <td>{formatNumber(row.orders)}</td>
                      <td>{formatCurrency(row.revenue)}</td>
                      <td>{formatPercent(row.conversionRate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </SectionState>
        </article> : null}

        {currentPage === 'dashboard' ? <article className="panel">
          <div className="panel-header">
            <h2>Campaign mix</h2>
            <p>Revenue share makes it obvious which campaigns dominate the selected window.</p>
          </div>
          <SectionState
            loading={dashboard.campaigns.loading}
            error={dashboard.campaigns.error}
            empty={!dashboard.campaigns.data?.length}
            emptyLabel="No campaign mix available yet."
          >
            <div className="stack-list">
              {(dashboard.campaigns.data ?? []).map((row) => {
                const share = totalCampaignRevenue > 0 ? row.revenue / totalCampaignRevenue : 0;

                return (
                  <div key={`${row.source}-${row.campaign}`} className="stack-row">
                    <div className="stack-copy">
                      <strong>{row.campaign}</strong>
                      <span>{`${row.source} / ${row.medium}`}</span>
                    </div>
                    <div className="stack-bar-shell">
                      <div className="stack-bar" style={{ width: `${Math.max(share * 100, 2)}%` }} />
                    </div>
                    <div className="stack-value">{formatPercent(share)}</div>
                  </div>
                );
              })}
            </div>
          </SectionState>
        </article> : null}

        {currentPage === 'dashboard' ? <article className="panel panel-wide">
          <div className="panel-header">
            <h2>Attributed orders</h2>
            <p>Order-level attribution rows help debug why a sale was assigned to a source and campaign.</p>
          </div>
          <SectionState
            loading={dashboard.orders.loading}
            error={dashboard.orders.error}
            empty={!dashboard.orders.data?.length}
            emptyLabel="No attributed orders were returned for this range."
          >
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Order</th>
                    <th>Processed</th>
                    <th>Source</th>
                    <th>Campaign</th>
                    <th>Total</th>
                    <th>Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard.orders.data ?? []).map((row) => (
                    <tr key={row.shopifyOrderId}>
                      <td>
                        <div className="primary-cell">
                          <strong>#{row.shopifyOrderId}</strong>
                          <span>{row.medium ?? 'No medium'}</span>
                        </div>
                      </td>
                      <td>{formatDateTimeLabel(row.processedAt)}</td>
                      <td>{row.source ?? 'Unattributed'}</td>
                      <td>{row.campaign ?? 'No campaign'}</td>
                      <td>{formatCurrency(row.totalPrice)}</td>
                      <td>
                        <span className="reason-pill">{row.attributionReason}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </SectionState>
        </article> : null}
      </section>
    </main>
  );
}

export default App;
