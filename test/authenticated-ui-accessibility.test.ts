import assert from 'node:assert/strict';
import path from 'node:path';
import test from 'node:test';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const requireFromDashboard = createRequire(path.join(repoRoot, 'dashboard/package.json'));

const React = requireFromDashboard('react') as typeof import('../dashboard/node_modules/react');
const { renderToStaticMarkup } = requireFromDashboard('react-dom/server') as typeof import('../dashboard/node_modules/react-dom/server');
const { createRoot } = requireFromDashboard('react-dom/client') as typeof import('../dashboard/node_modules/react-dom/client');
const { flushSync } = requireFromDashboard('react-dom') as typeof import('../dashboard/node_modules/react-dom');
const axe = requireFromDashboard('axe-core') as typeof import('../dashboard/node_modules/axe-core');
const { JSDOM } = requireFromDashboard('jsdom') as typeof import('../dashboard/node_modules/jsdom');
const h = React.createElement;

async function loadModule<T>(relativePath: string): Promise<T> {
  return import(pathToFileURL(path.join(repoRoot, relativePath)).href) as Promise<T>;
}

function createDom(markup = '<!doctype html><html><body></body></html>') {
  const dom = new JSDOM(markup, { pretendToBeVisual: true, url: 'http://localhost/' });
  Object.assign(globalThis, {
    window: dom.window,
    document: dom.window.document,
    HTMLElement: dom.window.HTMLElement,
    SVGElement: dom.window.SVGElement,
    Node: dom.window.Node,
    Event: dom.window.Event,
    KeyboardEvent: dom.window.KeyboardEvent,
    getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
    requestAnimationFrame: (callback: FrameRequestCallback) => setTimeout(callback, 0),
    cancelAnimationFrame: (handle: number) => clearTimeout(handle)
  });
  Object.defineProperty(globalThis, 'navigator', {
    configurable: true,
    value: dom.window.navigator
  });
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: dom.window.localStorage
  });
  if (!dom.window.matchMedia) {
    dom.window.matchMedia = ((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener() {},
      removeListener() {},
      addEventListener() {},
      removeEventListener() {},
      dispatchEvent() {
        return false;
      }
    })) as typeof dom.window.matchMedia;
  }

  return dom;
}

function renderRouteShell(
  AuthenticatedAppShell: typeof import('../dashboard/src/components/AuthenticatedAppShell').default,
  activeNavKey: string,
  children: React.ReactNode
) {
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

  return renderToStaticMarkup(
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
        onNavigate() {},
        breadcrumbs: [
          { label: 'Authenticated app' },
          {
            label: activeNavKey === 'settings' ? 'Settings' : activeNavKey === 'order-details' ? 'Order details' : 'Dashboard',
            current: true
          }
        ],
        topbarMeta: h(
          'div',
          { className: 'space-y-1' },
          h('p', { className: 'font-semibold text-ink' }, 'Taylor Operator'),
          h('p', null, 'taylor@roasradar.dev')
        ),
        headerActions: h('button', { type: 'button' }, 'Logout')
      },
      children
    )
  );
}

async function runAxe(markup: string) {
  const dom = createDom(`<!doctype html><html><body>${markup}</body></html>`);
  const results = await axe.run(dom.window.document.body, {
    rules: {
      'color-contrast': { enabled: false }
    }
  });
  dom.window.close();
  return results;
}

test('authenticated dashboard, order details, and settings pass automated accessibility checks', async () => {
  const [
    { default: AuthenticatedAppShell },
    { default: ReportingDashboard },
    { default: OrderDetailsView },
    { default: SettingsAdminView }
  ] = await Promise.all([
    loadModule<typeof import('../dashboard/src/components/AuthenticatedAppShell')>('dashboard/src/components/AuthenticatedAppShell.tsx'),
    loadModule<typeof import('../dashboard/src/components/ReportingDashboard')>('dashboard/src/components/ReportingDashboard.tsx'),
    loadModule<typeof import('../dashboard/src/components/OrderDetailsView')>('dashboard/src/components/OrderDetailsView.tsx'),
    loadModule<typeof import('../dashboard/src/components/SettingsAdminView')>('dashboard/src/components/SettingsAdminView.tsx')
  ]);

  const dashboardMarkup = renderRouteShell(
    AuthenticatedAppShell,
    'dashboard',
    h(ReportingDashboard, {
      filters: { startDate: '2026-04-01', endDate: '2026-04-20', source: '', campaign: '', attributionTier: '' },
      onFiltersChange() {},
      groupBy: 'day',
      onGroupByChange() {},
      reportingTimezone: 'America/Los_Angeles',
      quickRanges: [
        { label: 'Today', value: () => ({ startDate: '2026-04-20', endDate: '2026-04-20' }) },
        { label: 'Last 7D', value: () => ({ startDate: '2026-04-14', endDate: '2026-04-20' }) }
      ],
      onApplyQuickRange() {},
      onClearFilters() {},
      summaryCards: [
        { label: 'Visits', value: '12,480', detail: 'Apr 1 to Apr 20' },
        { label: 'Orders', value: '324', detail: '2.6% conversion' },
        { label: 'Revenue', value: '$48,920.00', detail: '4.3 ROAS' },
        { label: 'AOV', value: '$150.99', detail: '324 attributed orders' }
      ],
      summarySection: {
        data: { visits: 12480, orders: 324, revenue: 48920, spend: 11376, conversionRate: 0.02596, roas: 4.3 },
        loading: false,
        error: null
      },
      campaignsSection: {
        data: [
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'Spring Search',
            content: 'hero',
            visits: 4800,
            orders: 122,
            revenue: 18320,
            conversionRate: 0.0254
          }
        ],
        loading: false,
        error: null
      },
      timeseriesSection: {
        data: [
          { date: '2026-04-18', visits: 540, orders: 21, revenue: 3210 },
          { date: '2026-04-19', visits: 610, orders: 24, revenue: 4020 },
          { date: '2026-04-20', visits: 575, orders: 23, revenue: 3680 }
        ],
        loading: false,
        error: null
      },
      ordersSection: {
        data: [
          {
            shopifyOrderId: '1105',
            processedAt: '2026-04-20T18:00:00.000Z',
            orderOccurredAtUtc: '2026-04-20T18:00:00.000Z',
            source: 'google',
            medium: 'cpc',
            campaign: 'Spring Search',
            totalPrice: 195,
            attributionReason: 'matched_by_landing_session',
            primaryCreditAttributionReason: 'matched_by_landing_session',
            attributionTier: 'deterministic_first_party',
            attributionTierLabel: 'Deterministic first-party',
            attributionTierDescription:
              'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
            attributionSource: 'landing_session_id',
            attributionMatchedAt: '2026-04-20T18:00:30.000Z',
            confidenceScore: 1,
            sessionId: 'sess_123'
          }
        ],
        loading: false,
        error: null
      },
      spendDetailsSection: {
        data: [
          {
            source: 'google',
            medium: 'cpc',
            channel: 'google / cpc',
            subtotal: 7320,
            campaigns: [
              { campaign: 'Spring Search', spend: 5220 },
              { campaign: 'Brand Search', spend: 2100 }
            ]
          }
        ],
        loading: false,
        error: null
      },
      onOpenOrderDetails() {}
    })
  );

  const orderDetailsMarkup = renderRouteShell(
    AuthenticatedAppShell,
    'order-details',
    h(OrderDetailsView, {
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
            orderOccurredAtUtc: '2026-04-20T18:00:00.000Z',
            attributionTier: 'deterministic_first_party',
            attributionTierLabel: 'Deterministic first-party',
            attributionTierDescription:
              'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
            attributionSource: 'landing_session_id',
            attributionMatchedAt: '2026-04-20T18:00:30.000Z',
            attributionReason: 'matched_by_landing_session',
            confidenceScore: 1,
            sessionId: 'sess_123',
            attributedSource: 'google',
            attributedMedium: 'cpc',
            attributedCampaign: 'brand-search',
            attributedContent: null,
            attributedTerm: null,
            attributedClickIdType: 'gclid',
            attributedClickIdValue: 'abc-123',
            attributionSnapshot: {
              confidenceScore: 1,
              winner: {
                sessionId: 'sess_123',
                source: 'google',
                medium: 'cpc',
                campaign: 'brand-search'
              }
            },
            attributionSnapshotUpdatedAt: '2026-04-20T18:00:30.000Z',
            ingestedAt: '2026-04-20T18:31:00.000Z',
            rawPayload: { orderNumber: 'RR-1105' }
          },
          lineItems: [
            {
              shopifyLineItemId: 'line_1',
              shopifyOrderId: '1105',
              title: 'Performance Hoodie',
              variantTitle: 'Black / Large',
              sku: 'HOODIE-BLK-L',
              vendor: 'ROAS Radar',
              quantity: 1,
              price: 95,
              fulfillmentStatus: 'fulfilled',
              rawPayload: { sku: 'HOODIE-BLK-L' }
            }
          ],
          attributionCredits: [
            {
              attributionModel: 'last-touch',
              touchpointPosition: 1,
              source: 'google',
              medium: 'cpc',
              campaign: 'Spring Search',
              touchpointOccurredAt: '2026-04-20T17:00:00.000Z',
              revenueCredit: 195,
              creditWeight: 1,
              attributionReason: 'last-touch',
              clickIdType: 'gclid',
              clickIdValue: 'gclid_123'
            }
          ]
        }
      }
    })
  );

  const settingsMarkup = renderRouteShell(
    AuthenticatedAppShell,
    'settings',
    h(SettingsAdminView, {
      isAdmin: true,
      reportingTimezone: 'America/Los_Angeles',
      defaultReportingTimezone: 'America/Los_Angeles',
      reportingTimezoneOptions: ['America/Los_Angeles', 'UTC'],
      filters: { startDate: '2026-04-01', endDate: '2026-04-20' },
      appSettings: {
        data: { reportingTimezone: 'America/Los_Angeles', updatedAt: '2026-04-20T12:00:00.000Z' },
        loading: false,
        error: null
      },
      settingsForm: { reportingTimezone: 'America/Los_Angeles' },
      setSettingsForm() {},
      usersSection: {
        data: [
          {
            id: 'user_1',
            email: 'taylor@roasradar.dev',
            displayName: 'Taylor Operator',
            status: 'active',
            isAdmin: true,
            lastLoginAt: '2026-04-20T16:45:00.000Z'
          }
        ],
        loading: false,
        error: null
      },
      newUserForm: { email: 'alex@roasradar.dev', password: 'supersecurepass', displayName: 'Alex', isAdmin: false },
      setNewUserForm() {},
      shopifyConnection: {
        data: { connected: true, storeDomain: 'example.myshopify.com', webhookConfigured: true, lastSyncAt: '2026-04-20T15:00:00.000Z' },
        loading: false,
        error: null
      },
      shopifyBackfillRange: { startDate: '2026-04-01', endDate: '2026-04-20' },
      setShopifyBackfillRange() {},
      shopifyOrderAttributionBackfillOptions: {
        dryRun: true,
        limit: '500',
        webOrdersOnly: true,
        skipShopifyWriteback: false
      },
      setShopifyOrderAttributionBackfillOptions() {},
      orderAttributionBackfillJob: {
        data: null,
        loading: false,
        error: null
      },
      metaConnection: {
        data: {
          config: {
            appId: 'meta-app',
            appBaseUrl: 'https://example.com',
            appScopes: ['ads_read'],
            adAccountId: 'act_123',
            missingFields: []
          },
          connection: {
            account_id: 'act_123',
            account_name: 'Meta Account',
            status: 'connected',
            last_sync_at: '2026-04-20T15:30:00.000Z',
            last_sync_status: 'success'
          }
        },
        loading: false,
        error: null
      },
      metaConfigForm: {
        appId: 'meta-app',
        appSecret: '',
        appBaseUrl: 'https://example.com',
        appScopes: 'ads_read',
        adAccountId: 'act_123'
      },
      setMetaConfigForm() {},
      googleConnection: {
        data: {
          config: {
            source: 'database',
            developerTokenConfigured: true,
            appBaseUrl: 'https://example.com',
            appScopes: ['https://www.googleapis.com/auth/adwords'],
            clientId: 'client-id',
            clientSecretConfigured: true,
            missingFields: []
          },
          connection: {
            customer_id: '123-456-7890',
            login_customer_id: '111-222-3333',
            status: 'connected',
            last_sync_at: '2026-04-20T15:30:00.000Z',
            last_sync_status: 'success'
          }
        },
        loading: false,
        error: null
      },
      googleConfigForm: {
        developerToken: 'token',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        appBaseUrl: 'https://example.com',
        appScopes: 'https://www.googleapis.com/auth/adwords'
      },
      setGoogleConfigForm() {},
      googleForm: {
        customerId: '123-456-7890',
        loginCustomerId: '111-222-3333',
        developerToken: 'token',
        clientId: 'client-id',
        clientSecret: 'client-secret',
        refreshToken: 'refresh-token'
      },
      setGoogleForm() {},
      actionFeedback: { context: null, loading: null, error: null, message: null },
      onSettingsSave() {},
      onCreateUser() {},
      onShopifyBackfill() {},
      onMetaConfigSave() {},
      onGoogleConnect() {},
      onShopifyTest() {},
      onShopifyWebhookSync() {},
      onShopifyAttributionRecovery() {},
      onShopifyOrderAttributionBackfill() {},
      onOrderAttributionBackfillRefresh() {},
      onMetaConnect() {},
      onMetaSync() {},
      onGoogleSync() {},
      onGoogleReconcile() {}
    })
  );

  for (const [surface, markup] of [
    ['dashboard', dashboardMarkup],
    ['order-details', orderDetailsMarkup],
    ['settings', settingsMarkup]
  ] as const) {
    const results = await runAxe(markup);
    assert.equal(results.violations.length, 0, `${surface} violations:\n${results.violations.map((item) => `${item.id}: ${item.help}`).join('\n')}`);
  }
});

test('modal traps focus, closes on escape, and restores the previous focus target', async () => {
  const dom = createDom();
  const { Modal, Button } = await loadModule<typeof import('../dashboard/src/components/AuthenticatedUi')>(
    'dashboard/src/components/AuthenticatedUi.tsx'
  );

  const container = dom.window.document.createElement('div');
  dom.window.document.body.append(container);
  const trigger = dom.window.document.createElement('button');
  trigger.textContent = 'Open modal';
  dom.window.document.body.prepend(trigger);
  trigger.focus();

  const root = createRoot(container);

  flushSync(() => {
    root.render(
      h(Modal, {
        open: true,
        title: 'Modal preview',
        description: 'Focused multi-step input.',
        onClose() {
          root.render(h('div'));
        },
        footer: h(Button, { type: 'button' }, 'Save'),
        children: h(
          'div',
          { className: 'grid gap-4' },
          h('input', { type: 'text', placeholder: 'First field' }),
          h('button', { type: 'button' }, 'Secondary action')
        )
      })
    );
  });
  await new Promise((resolve) => setTimeout(resolve, 0));

  const dialog = dom.window.document.querySelector('dialog') as HTMLDialogElement | null;
  assert.ok(dialog, 'dialog should render');
  const closeButton = dom.window.document.querySelector('[aria-label="Close modal"]') as HTMLButtonElement | null;
  const saveButton = Array.from(dom.window.document.querySelectorAll('button')).find(
    (button) => button.textContent === 'Save'
  ) as HTMLButtonElement | undefined;

  assert.equal((dom.window.document.activeElement as HTMLElement | null)?.getAttribute('aria-label'), 'Close modal');
  assert.ok(closeButton, 'close button should render');
  assert.ok(saveButton, 'save button should render');

  closeButton.focus();
  dom.window.document.dispatchEvent(
    new dom.window.KeyboardEvent('keydown', { key: 'Tab', shiftKey: true, bubbles: true, cancelable: true })
  );
  assert.equal(dom.window.document.activeElement, saveButton);

  saveButton.focus();
  dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Tab', bubbles: true, cancelable: true }));
  assert.equal(dom.window.document.activeElement, closeButton);

  dom.window.document.dispatchEvent(
    new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true })
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(dom.window.document.activeElement, trigger);

  flushSync(() => {
    root.unmount();
  });
  dom.window.close();
});
