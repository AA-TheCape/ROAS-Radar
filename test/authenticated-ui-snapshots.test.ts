import assert from 'node:assert/strict';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const requireFromDashboard = createRequire(path.join(repoRoot, 'dashboard/package.json'));
const snapshotFile = path.join(__dirname, '__snapshots__', 'authenticated-ui-snapshots.json');

const React = requireFromDashboard('react') as typeof import('../dashboard/node_modules/react');
const { renderToStaticMarkup } = requireFromDashboard('react-dom/server') as typeof import('../dashboard/node_modules/react-dom/server');
const h = React.createElement;

const storage = new Map<string, string>();

Object.assign(globalThis, {
  window: {
    __ROAS_RADAR_RUNTIME_CONFIG__: {},
    location: { pathname: '/' },
    localStorage: {
      getItem(key: string) {
        return storage.get(key) ?? null;
      },
      setItem(key: string, value: string) {
        storage.set(key, value);
      },
      removeItem(key: string) {
        storage.delete(key);
      }
    }
  }
});

function normalizeHtml(value: string) {
  return value.replace(/\s+</g, '<').replace(/>\s+/g, '>').replace(/\s{2,}/g, ' ').trim();
}

function noop() {}

async function loadModule<T>(relativePath: string): Promise<T> {
  const moduleUrl = pathToFileURL(path.join(repoRoot, relativePath)).href;
  return import(moduleUrl) as Promise<T>;
}

async function renderSnapshots() {
  const [
    { default: AuthenticatedAppShell },
    { default: ReportingDashboard },
    { default: OrderDetailsView },
    { default: SettingsAdminView }
  ] = await Promise.all([
    loadModule<typeof import('../dashboard/src/components/AuthenticatedAppShell')>(
      'dashboard/src/components/AuthenticatedAppShell.tsx'
    ),
    loadModule<typeof import('../dashboard/src/components/ReportingDashboard')>(
      'dashboard/src/components/ReportingDashboard.tsx'
    ),
    loadModule<typeof import('../dashboard/src/components/OrderDetailsView')>(
      'dashboard/src/components/OrderDetailsView.tsx'
    ),
    loadModule<typeof import('../dashboard/src/components/SettingsAdminView')>(
      'dashboard/src/components/SettingsAdminView.tsx'
    )
  ]);

  const navItems = [
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

  function renderRoute({
    activeNavKey,
    breadcrumbs,
    children
  }: {
    activeNavKey: string;
    breadcrumbs: Array<{ label: string; current?: boolean }>;
    children: unknown;
  }) {
    return normalizeHtml(
      renderToStaticMarkup(
        h(
          AuthenticatedAppShell,
          {
            navItems:
              activeNavKey === 'order-details'
                ? [
                    ...navItems,
                    {
                      key: 'order-details',
                      label: 'Order details',
                      shortLabel: 'Order',
                      description: 'Contextual drill-in for a selected attributed Shopify order.'
                    }
                  ]
            : navItems,
            activeNavKey,
            onNavigate: noop,
            breadcrumbs,
            topbarMeta: h(
              'div',
              { className: 'space-y-3' },
              h(
                'div',
                { className: 'space-y-1' },
                h('p', { className: 'font-semibold text-ink' }, 'Taylor Operator'),
                h('p', null, 'taylor@roasradar.dev')
              ),
              h(
                'div',
                { 'aria-label': 'Current timestamp', className: 'space-y-1 border-t border-line/60 pt-3 text-caption text-ink-muted' },
                h('p', { className: 'font-semibold uppercase tracking-[0.14em] text-teal' }, 'Current time'),
                h('p', null, 'Apr 20, 12:15 PM PDT'),
                h('p', null, 'UTC Apr 20, 7:15 PM')
              )
            ),
            headerActions: h('button', { type: 'button' }, 'Logout')
          },
          children
        )
      )
    );
  }

  const dashboard = renderRoute({
    activeNavKey: 'dashboard',
    breadcrumbs: [
      { label: 'Authenticated app' },
      { label: 'Dashboard', current: true }
    ],
    children: h(ReportingDashboard, {
      filters: {
        startDate: '2026-04-01',
        endDate: '2026-04-20',
        source: '',
        campaign: ''
      },
      onFiltersChange: noop,
      groupBy: 'day',
      onGroupByChange: noop,
      reportingTimezone: 'America/Los_Angeles',
      quickRanges: [
        { label: 'Today', value: () => ({ startDate: '2026-04-20', endDate: '2026-04-20' }) },
        { label: 'Last 7D', value: () => ({ startDate: '2026-04-14', endDate: '2026-04-20' }) },
        { label: 'Last 30D', value: () => ({ startDate: '2026-03-22', endDate: '2026-04-20' }) }
      ],
      onApplyQuickRange: noop,
      onClearFilters: noop,
      summaryCards: [
        { label: 'Visits', value: '12,480', detail: 'Apr 1 to Apr 20' },
        { label: 'Orders', value: '324', detail: '2.6% conversion' },
        { label: 'Revenue', value: '$48,920.00', detail: '4.3 ROAS' },
        { label: 'AOV', value: '$150.99', detail: '324 attributed orders' }
      ],
      summarySection: {
        data: {
          visits: 12480,
          orders: 324,
          revenue: 48920,
          spend: 11376,
          conversionRate: 0.02596,
          roas: 4.3
        },
        loading: false,
        error: null
      },
      campaignsSection: { data: [], loading: false, error: null },
      timeseriesSection: { data: [], loading: false, error: null },
      ordersSection: { data: [], loading: false, error: null },
      spendDetailsSection: { data: [], loading: false, error: null },
      onOpenOrderDetails: noop
    })
  });

  const orderDetails = renderRoute({
    activeNavKey: 'order-details',
    breadcrumbs: [
      { label: 'Authenticated app' },
      { label: 'Dashboard' },
      { label: 'Order 1105', current: true }
    ],
    children: h(OrderDetailsView, {
      selectedOrderId: '1105',
      reportingTimezone: 'America/Los_Angeles',
      orderDetailsSection: {
        loading: false,
        error: null,
        data: {
          order: {
            shopifyOrderId: '1105',
            shopifyOrderNumber: 'RR-1105',
            shopifyCustomerId: 'gid://shopify/Customer/99',
            customerIdentityId: 'cust_449',
            email: 'alex@example.com',
            emailHash: 'hash_abc123',
            currencyCode: 'USD',
            subtotalPrice: 180,
            totalPrice: 195,
            financialStatus: 'paid',
            fulfillmentStatus: 'fulfilled',
            processedAt: '2026-04-20T18:00:00.000Z',
            createdAtShopify: '2026-04-20T17:42:00.000Z',
            updatedAtShopify: '2026-04-20T18:30:00.000Z',
            landingSessionId: 'sess_123',
            checkoutToken: 'check_456',
            cartToken: 'cart_789',
            sourceName: 'web',
            ingestedAt: '2026-04-20T18:31:00.000Z',
            rawPayload: { orderNumber: 'RR-1105', note: 'vip customer' }
          },
          lineItems: [
            {
              shopifyLineItemId: 'line_1',
              shopifyProductId: 'prod_1',
              shopifyVariantId: 'var_1',
              sku: 'SKU-RED-01',
              title: 'Performance Hoodie',
              variantTitle: 'Red / Medium',
              vendor: 'ROAS Radar',
              quantity: 2,
              price: 90,
              totalDiscount: 15,
              fulfillmentStatus: 'fulfilled',
              requiresShipping: true,
              taxable: true,
              ingestedAt: '2026-04-20T18:31:00.000Z',
              rawPayload: { lineItemId: 'line_1' }
            }
          ],
          attributionCredits: [
            {
              attributionModel: 'last_touch',
              touchpointPosition: 1,
              sessionId: 'sess_123',
              touchpointOccurredAt: '2026-04-20T16:15:00.000Z',
              source: 'google',
              medium: 'cpc',
              campaign: 'brand-search',
              content: null,
              term: null,
              clickIdType: 'gclid',
              clickIdValue: 'abc-123',
              creditWeight: 1,
              revenueCredit: 195,
              isPrimary: true,
              attributionReason: 'matched checkout token',
              createdAt: '2026-04-20T18:31:00.000Z',
              modelVersion: 2
            }
          ]
        }
      }
    })
  });

  const settings = renderRoute({
    activeNavKey: 'settings',
    breadcrumbs: [
      { label: 'Authenticated app' },
      { label: 'Settings', current: true }
    ],
    children: h(SettingsAdminView, {
      isAdmin: true,
      reportingTimezone: 'America/Los_Angeles',
      defaultReportingTimezone: 'America/Los_Angeles',
      reportingTimezoneOptions: ['America/Los_Angeles', 'UTC'],
      filters: { startDate: '2026-04-01', endDate: '2026-04-20' },
      appSettings: {
        data: {
          reportingTimezone: 'America/Los_Angeles',
          updatedAt: '2026-04-20T18:45:00.000Z'
        },
        loading: false,
        error: null
      },
      settingsForm: { reportingTimezone: 'America/Los_Angeles' },
      setSettingsForm: noop,
      usersSection: {
        data: [
          {
            id: 1,
            email: 'taylor@roasradar.dev',
            displayName: 'Taylor Operator',
            isAdmin: true,
            status: 'active',
            lastLoginAt: '2026-04-20T19:15:00.000Z',
            createdAt: '2026-04-01T12:00:00.000Z'
          }
        ],
        loading: false,
        error: null
      },
      newUserForm: {
        email: 'new.user@example.com',
        password: 'super-secret-password',
        displayName: 'New User',
        isAdmin: false
      },
      setNewUserForm: noop,
      shopifyConnection: {
        data: {
          connected: true,
          shopDomain: 'demo-shop.myshopify.com',
          status: 'active',
          installedAt: '2026-04-01T13:00:00.000Z',
          webhookBaseUrl: 'https://api.roasradar.dev',
          reconnectUrl: null,
          shop: {
            name: 'Demo Shop',
            email: 'owner@demo-shop.com',
            currency: 'USD'
          }
        },
        loading: false,
        error: null
      },
      shopifyBackfillRange: { startDate: '2026-04-01', endDate: '2026-04-20' },
      setShopifyBackfillRange: noop,
      shopifyOrderAttributionBackfillOptions: {
        dryRun: true,
        limit: '500',
        webOrdersOnly: true,
        skipShopifyWriteback: false
      },
      setShopifyOrderAttributionBackfillOptions: noop,
      orderAttributionBackfillJob: {
        data: null,
        loading: false,
        error: null
      },
      metaConnection: {
        data: {
          config: {
            source: 'database',
            appId: 'meta-app-id',
            appBaseUrl: 'https://app.roasradar.dev',
            appScopes: ['ads_read'],
            adAccountId: 'act_123',
            appSecretConfigured: true,
            missingFields: []
          },
          connection: {
            id: 2,
            ad_account_id: 'act_123',
            granted_scopes: ['ads_read'],
            token_expires_at: null,
            last_refreshed_at: '2026-04-20T12:00:00.000Z',
            last_sync_started_at: '2026-04-20T15:00:00.000Z',
            last_sync_completed_at: '2026-04-20T15:04:00.000Z',
            last_sync_status: 'success',
            last_sync_error: null,
            status: 'connected',
            account_name: 'North America Prospecting',
            account_currency: 'USD'
          }
        },
        loading: false,
        error: null
      },
      metaConfigForm: {
        appId: 'meta-app-id',
        appSecret: '',
        appBaseUrl: 'https://app.roasradar.dev',
        appScopes: 'ads_read',
        adAccountId: 'act_123'
      },
      setMetaConfigForm: noop,
      googleConnection: {
        data: {
          config: {
            source: 'database',
            developerTokenConfigured: true,
            appBaseUrl: 'https://app.roasradar.dev',
            appScopes: ['https://www.googleapis.com/auth/adwords'],
            clientId: 'client-id',
            clientSecretConfigured: true,
            missingFields: []
          },
          connection: {
            id: 3,
            customer_id: '123-456-7890',
            login_customer_id: '111-222-3333',
            token_scopes: ['https://www.googleapis.com/auth/adwords'],
            last_refreshed_at: '2026-04-20T12:00:00.000Z',
            last_sync_started_at: '2026-04-20T16:00:00.000Z',
            last_sync_completed_at: '2026-04-20T16:06:00.000Z',
            last_sync_status: 'success',
            last_sync_error: null,
            status: 'connected',
            customer_descriptive_name: 'Demo Google Ads',
            currency_code: 'USD'
          },
          reconciliation: {
            checked_range_start: '2026-04-01',
            checked_range_end: '2026-04-20',
            missing_dates: [],
            enqueued_jobs: 0,
            status: 'up_to_date',
            checked_at: '2026-04-20T16:07:00.000Z'
          }
        },
        loading: false,
        error: null
      },
      googleConfigForm: {
        developerToken: 'developer-token',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        appBaseUrl: 'https://app.roasradar.dev',
        appScopes: 'https://www.googleapis.com/auth/adwords'
      },
      setGoogleConfigForm: noop,
      googleForm: {
        customerId: '123-456-7890',
        loginCustomerId: '111-222-3333',
        developerToken: 'developer-token',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        refreshToken: 'refresh-token'
      },
      setGoogleForm: noop,
      actionFeedback: { loading: null, error: null, message: 'Saved Meta Ads configuration.' },
      onSettingsSave: noop,
      onCreateUser: noop,
      onShopifyBackfill: noop,
      onMetaConfigSave: noop,
      onGoogleConnect: noop,
      onShopifyTest: noop,
      onShopifyWebhookSync: noop,
      onShopifyAttributionRecovery: noop,
      onShopifyOrderAttributionBackfill: noop,
      onMetaConnect: noop,
      onMetaSync: noop,
      onGoogleSync: noop,
      onGoogleReconcile: noop
    })
  });

  return { dashboard, orderDetails, settings };
}

test('authenticated route snapshots stay stable', async () => {
  const snapshots = await renderSnapshots();

  if (process.env.UPDATE_SNAPSHOTS === '1') {
    mkdirSync(path.dirname(snapshotFile), { recursive: true });
    writeFileSync(snapshotFile, `${JSON.stringify(snapshots, null, 2)}\n`);
  }

  const expected = JSON.parse(readFileSync(snapshotFile, 'utf8')) as Record<string, string>;
  assert.deepEqual(snapshots, expected);
});
