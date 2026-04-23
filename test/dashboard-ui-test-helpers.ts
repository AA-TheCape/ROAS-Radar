import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
export const repoRoot = path.resolve(__dirname, '..');
const requireFromDashboard = createRequire(path.join(repoRoot, 'dashboard/package.json'));

export const React = requireFromDashboard('react') as typeof import('../dashboard/node_modules/react');
export const { renderToStaticMarkup } = requireFromDashboard('react-dom/server') as typeof import('../dashboard/node_modules/react-dom/server');
export const { createRoot } = requireFromDashboard('react-dom/client') as typeof import('../dashboard/node_modules/react-dom/client');
export const { flushSync } = requireFromDashboard('react-dom') as typeof import('../dashboard/node_modules/react-dom');
export const { JSDOM } = requireFromDashboard('jsdom') as typeof import('../dashboard/node_modules/jsdom');
export const h = React.createElement;

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type DomOptions = {
  markup?: string;
  width?: number;
  height?: number;
  url?: string;
};

type MountedUi = {
  cleanup: () => void;
  container: HTMLDivElement;
  dom: import('../dashboard/node_modules/jsdom').JSDOM;
  root: import('../dashboard/node_modules/react-dom/client').Root;
};

function installDomGlobals(dom: import('../dashboard/node_modules/jsdom').JSDOM, width: number, height: number) {
  const { window } = dom;

  Object.defineProperty(window, 'innerWidth', { configurable: true, value: width });
  Object.defineProperty(window, 'innerHeight', { configurable: true, value: height });

  if (!window.matchMedia) {
    window.matchMedia = ((query: string) => ({
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
    })) as typeof window.matchMedia;
  }

  class ResizeObserver {
    private readonly callback: ResizeObserverCallback;

    constructor(callback: ResizeObserverCallback) {
      this.callback = callback;
    }

    observe(target: Element) {
      this.callback(
        [
          {
            target,
            contentRect: {
              x: 0,
              y: 0,
              top: 0,
              right: width,
              bottom: height,
              left: 0,
              width,
              height,
              toJSON() {
                return { width, height };
              }
            }
          } as ResizeObserverEntry
        ],
        this as unknown as ResizeObserver
      );
    }

    unobserve() {}

    disconnect() {}
  }

  const elementWidth = width;
  const elementHeight = Math.max(320, Math.min(height, 900));

  Object.defineProperty(window.HTMLElement.prototype, 'clientWidth', {
    configurable: true,
    get() {
      return elementWidth;
    }
  });
  Object.defineProperty(window.HTMLElement.prototype, 'clientHeight', {
    configurable: true,
    get() {
      return elementHeight;
    }
  });
  Object.defineProperty(window.HTMLElement.prototype, 'offsetWidth', {
    configurable: true,
    get() {
      return elementWidth;
    }
  });
  Object.defineProperty(window.HTMLElement.prototype, 'offsetHeight', {
    configurable: true,
    get() {
      return elementHeight;
    }
  });

  window.HTMLElement.prototype.getBoundingClientRect = function getBoundingClientRect() {
    return {
      x: 0,
      y: 0,
      top: 0,
      right: elementWidth,
      bottom: elementHeight,
      left: 0,
      width: elementWidth,
      height: elementHeight,
      toJSON() {
        return { width: elementWidth, height: elementHeight };
      }
    };
  };

  if (!window.SVGElement.prototype.getBBox) {
    window.SVGElement.prototype.getBBox = () => ({ x: 0, y: 0, width: 120, height: 24 });
  }

  if (!window.SVGElement.prototype.getComputedTextLength) {
    window.SVGElement.prototype.getComputedTextLength = () => 120;
  }

  Object.assign(globalThis, {
    window,
    document: window.document,
    navigator: window.navigator,
    HTMLElement: window.HTMLElement,
    SVGElement: window.SVGElement,
    Node: window.Node,
    Event: window.Event,
    KeyboardEvent: window.KeyboardEvent,
    MouseEvent: window.MouseEvent,
    getComputedStyle: window.getComputedStyle.bind(window),
    requestAnimationFrame: (callback: FrameRequestCallback) => setTimeout(() => callback(Date.now()), 0),
    cancelAnimationFrame: (handle: number) => clearTimeout(handle),
    ResizeObserver
  });

  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: window.localStorage
  });
}

export function createDom({
  markup = '<!doctype html><html><body></body></html>',
  width = 1280,
  height = 900,
  url = 'http://localhost/'
}: DomOptions = {}) {
  const dom = new JSDOM(markup, { pretendToBeVisual: true, url });
  installDomGlobals(dom, width, height);
  return dom;
}

export async function loadDashboardModule<T>(relativePath: string): Promise<T> {
  return import(pathToFileURL(path.join(repoRoot, relativePath)).href) as Promise<T>;
}

export async function tick(ms = 0) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function mountUi(element: React.ReactElement, options: Omit<DomOptions, 'markup'> = {}): Promise<MountedUi> {
  const dom = createDom(options);
  const container = dom.window.document.createElement('div');
  dom.window.document.body.appendChild(container);
  const root = createRoot(container);

  flushSync(() => {
    root.render(element);
  });
  await tick();

  return {
    dom,
    container,
    root,
    cleanup: () => {
      flushSync(() => {
        root.unmount();
      });
      dom.window.close();
    }
  };
}

export function click(element: Element) {
  element.dispatchEvent(new window.MouseEvent('click', { bubbles: true, cancelable: true }));
}

export function keydown(element: Element, key: string, options: { shiftKey?: boolean } = {}) {
  element.dispatchEvent(
    new window.KeyboardEvent('keydown', {
      key,
      bubbles: true,
      cancelable: true,
      shiftKey: options.shiftKey ?? false
    })
  );
}

export function changeInputValue(element: HTMLInputElement | HTMLSelectElement, value: string) {
  const descriptor = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(element), 'value');
  descriptor?.set?.call(element, value);
  element.dispatchEvent(new window.Event('input', { bubbles: true, cancelable: true }));
  element.dispatchEvent(new window.Event('change', { bubbles: true, cancelable: true }));
}

export function normalizeHtml(value: string) {
  return value.replace(/\s+</g, '<').replace(/>\s+/g, '>').replace(/\s{2,}/g, ' ').trim();
}

export function noop() {}

export function createShellProps(overrides: Partial<import('../dashboard/src/components/AuthenticatedAppShell').AuthenticatedAppShellProps> = {}) {
  return {
    navItems: [
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
    ],
    activeNavKey: 'dashboard',
    onNavigate: noop,
    breadcrumbs: [
      { label: 'Authenticated app' },
      { label: 'Dashboard', current: true }
    ],
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
    headerActions: h('button', { type: 'button' }, 'Logout'),
    children: h('div', null, 'Shell content'),
    ...overrides
  };
}

export function createReportingDashboardProps(
  overrides: Partial<import('../dashboard/src/components/ReportingDashboard').default extends (props: infer P) => unknown ? P : never> = {}
) {
  const base = {
    filters: {
      startDate: '2026-04-01',
      endDate: '2026-04-20',
      source: '',
      campaign: ''
    },
    onFiltersChange: noop,
    groupBy: 'day' as const,
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
    } satisfies AsyncSection<import('../dashboard/src/lib/api').SummaryTotals>,
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
        },
        {
          source: 'meta',
          medium: 'paid_social',
          campaign: 'Prospecting Carousel',
          content: 'video',
          visits: 3600,
          orders: 84,
          revenue: 12960,
          conversionRate: 0.0233
        }
      ],
      loading: false,
      error: null
    } satisfies AsyncSection<import('../dashboard/src/lib/api').CampaignRow[]>,
    timeseriesSection: {
      data: [
        { date: '2026-04-18', visits: 540, orders: 21, revenue: 3210 },
        { date: '2026-04-19', visits: 610, orders: 24, revenue: 4020 },
        { date: '2026-04-20', visits: 575, orders: 23, revenue: 3680 }
      ],
      loading: false,
      error: null
    } satisfies AsyncSection<import('../dashboard/src/lib/api').TimeseriesPoint[]>,
    ordersSection: {
      data: [
        {
          shopifyOrderId: '1105',
          processedAt: '2026-04-20T18:00:00.000Z',
          source: 'google',
          medium: 'cpc',
          campaign: 'Spring Search',
          totalPrice: 195,
          attributionReason: 'last-touch'
        },
        {
          shopifyOrderId: '1104',
          processedAt: '2026-04-19T18:00:00.000Z',
          source: 'meta',
          medium: 'paid_social',
          campaign: 'Prospecting Carousel',
          totalPrice: 150,
          attributionReason: 'linear'
        }
      ],
      loading: false,
      error: null
    } satisfies AsyncSection<import('../dashboard/src/lib/api').OrderRow[]>,
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
        },
        {
          source: 'meta',
          medium: 'paid_social',
          channel: 'meta / paid_social',
          subtotal: 4056,
          campaigns: [
            { campaign: 'Prospecting Carousel', spend: 2556 },
            { campaign: 'Retargeting Video', spend: 1500 }
          ]
        }
      ],
      loading: false,
      error: null
    } satisfies AsyncSection<import('../dashboard/src/lib/api').SpendDetailChannelGroup[]>,
    onOpenOrderDetails: noop
  };

  return {
    ...base,
    ...overrides
  };
}

export function createOrderDetailsProps(
  overrides: Partial<import('../dashboard/src/components/OrderDetailsView').default extends (props: infer P) => unknown ? P : never> = {}
) {
  const base = {
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
          },
          {
            shopifyLineItemId: 'line_2',
            shopifyProductId: 'prod_2',
            shopifyVariantId: 'var_2',
            sku: 'SKU-BLK-02',
            title: 'Attribution Tee',
            variantTitle: 'Black / Large',
            vendor: 'ROAS Radar',
            quantity: 1,
            price: 15,
            totalDiscount: 0,
            fulfillmentStatus: 'fulfilled',
            requiresShipping: true,
            taxable: true,
            ingestedAt: '2026-04-20T18:31:00.000Z',
            rawPayload: { lineItemId: 'line_2' }
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
          },
          {
            attributionModel: 'linear',
            touchpointPosition: 2,
            sessionId: 'sess_124',
            touchpointOccurredAt: '2026-04-20T15:45:00.000Z',
            source: 'meta',
            medium: 'paid_social',
            campaign: 'prospecting',
            content: 'video',
            term: null,
            clickIdType: 'fbclid',
            clickIdValue: 'xyz-987',
            creditWeight: 0.5,
            revenueCredit: 97.5,
            isPrimary: false,
            attributionReason: 'modeled credit',
            createdAt: '2026-04-20T18:31:00.000Z',
            modelVersion: 2
          }
        ]
      }
    } satisfies AsyncSection<import('../dashboard/src/lib/api').OrderDetailsResponse>
  };

  return {
    ...base,
    ...overrides
  };
}

export function createSettingsAdminProps(
  overrides: Partial<import('../dashboard/src/components/SettingsAdminView').default extends (props: infer P) => unknown ? P : never> = {}
) {
  const base = {
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
    } satisfies AsyncSection<import('../dashboard/src/lib/api').AppSettings>,
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
    } satisfies AsyncSection<import('../dashboard/src/lib/api').AuthUser[]>,
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
    } satisfies AsyncSection<import('../dashboard/src/lib/api').ShopifyConnectionResponse>,
    shopifyBackfillRange: { startDate: '2026-04-01', endDate: '2026-04-20' },
    setShopifyBackfillRange: noop,
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
    } satisfies AsyncSection<{
      config: import('../dashboard/src/lib/api').MetaAdsConfigSummary;
      connection: import('../dashboard/src/lib/api').MetaAdsConnection | null;
    }>,
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
    } satisfies AsyncSection<import('../dashboard/src/lib/api').GoogleAdsStatusResponse>,
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
    actionFeedback: {
      context: null,
      loading: null,
      error: null,
      message: 'Saved Meta Ads configuration.'
    },
    onSettingsSave: noop,
    onCreateUser: noop,
    onShopifyBackfill: noop,
    onMetaConfigSave: noop,
    onGoogleConnect: noop,
    onShopifyTest: noop,
    onShopifyWebhookSync: noop,
    onShopifyAttributionRecovery: noop,
    onMetaConnect: noop,
    onMetaSync: noop,
    onGoogleSync: noop,
    onGoogleReconcile: noop
  };

  return {
    ...base,
    ...overrides
  };
}
