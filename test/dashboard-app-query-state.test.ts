import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createDom,
  createRoot,
  flushSync,
  h,
  loadDashboardModule,
  tick
} from './dashboard-ui-test-helpers';

type FetchCall = {
  path: string;
  query: URLSearchParams;
};

function findGroupBySelect(container: ParentNode): HTMLSelectElement | null {
  return (
    Array.from(container.querySelectorAll<HTMLSelectElement>('select')).find(
      (element) =>
        element.querySelector('option[value="day"]') &&
        element.querySelector('option[value="source"]') &&
        element.querySelector('option[value="campaign"]')
    ) ?? null
  );
}

const TEST_USER = {
  id: 1,
  email: 'taylor@roasradar.dev',
  displayName: 'Taylor Operator',
  isAdmin: true,
  status: 'active' as const,
  lastLoginAt: '2026-04-20T19:15:00.000Z',
  createdAt: '2026-04-01T12:00:00.000Z'
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json'
    }
  });
}

function createFetchStub(calls: FetchCall[]) {
  return async (input: string | URL | Request) => {
    const rawUrl = typeof input === 'string' ? input : input instanceof Request ? input.url : input.toString();
    const baseUrl = globalThis.window?.location?.origin ?? 'http://localhost';
    const url = new URL(rawUrl, baseUrl);

    calls.push({
      path: url.pathname,
      query: new URLSearchParams(url.search)
    });

    if (url.pathname === '/api/auth/me') {
      return jsonResponse({ user: TEST_USER });
    }

    if (url.pathname === '/api/settings') {
      return jsonResponse({
        reportingTimezone: 'America/Los_Angeles',
        updatedAt: '2026-04-20T18:45:00.000Z'
      });
    }

    if (url.pathname === '/api/admin/shopify/connection') {
      return jsonResponse({
        connected: true,
        shopDomain: 'demo-shop.myshopify.com',
        status: 'active'
      });
    }

    if (url.pathname === '/api/admin/meta-ads/status') {
      return jsonResponse({
        config: {
          source: 'database',
          appId: 'meta-app-id',
          appBaseUrl: 'https://app.roasradar.dev',
          appScopes: ['ads_read'],
          adAccountId: 'act_123',
          appSecretConfigured: true,
          missingFields: []
        },
        connection: null
      });
    }

    if (url.pathname === '/api/admin/google-ads/status') {
      return jsonResponse({
        config: {
          source: 'database',
          clientId: 'client-id',
          appBaseUrl: 'https://app.roasradar.dev',
          appScopes: ['https://www.googleapis.com/auth/adwords'],
          clientSecretConfigured: true,
          developerTokenConfigured: true,
          missingFields: []
        },
        connection: null,
        reconciliation: null
      });
    }

    if (url.pathname === '/api/reporting/summary') {
      return jsonResponse({
        range: {
          startDate: url.searchParams.get('startDate'),
          endDate: url.searchParams.get('endDate')
        },
        totals: {
          visits: 1200,
          orders: 48,
          revenue: 5210.5,
          spend: 0,
          conversionRate: 0.04,
          roas: null
        }
      });
    }

    if (url.pathname === '/api/reporting/campaigns') {
      return jsonResponse({
        rows: [],
        nextCursor: null
      });
    }

    if (url.pathname === '/api/reporting/timeseries') {
      return jsonResponse({
        points: []
      });
    }

    if (url.pathname === '/api/reporting/orders') {
      return jsonResponse({
        rows: []
      });
    }

    throw new Error(`Unexpected fetch: ${url.pathname}`);
  };
}

async function waitForDashboardControls(container: HTMLDivElement) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const dateInputs = container.querySelectorAll('input[type="date"]');
    const sourceInput = container.querySelector('input[placeholder="google, meta, facebook"]');
    const campaignInput = container.querySelector('input[placeholder="spring-sale"]');
    const groupBySelect = findGroupBySelect(container);

    if (dateInputs.length >= 2 && sourceInput && campaignInput && groupBySelect) {
      return;
    }

    await tick(50);
  }

  throw new Error(`Dashboard controls did not render in time. Rendered content: ${container.textContent ?? ''}`);
}

async function settleApp() {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    await tick(10);
  }
}

test('dashboard query-state helpers preserve valid control state and keep unrelated params', async () => {
  const dom = createDom();

  try {
    const appModule = await loadDashboardModule<typeof import('../dashboard/src/App')>('dashboard/src/App.tsx');

    assert.deepEqual(
      appModule.readDashboardStateFromSearch(
        '?foo=bar&startDate=2026-04-02&endDate=2026-04-18&source=meta&campaign=retargeting&groupBy=campaign'
      ),
      {
        filters: {
          startDate: '2026-04-02',
          endDate: '2026-04-18',
          source: 'meta',
          campaign: 'retargeting',
          attributionModel: undefined
        },
        groupBy: 'campaign'
      }
    );

    assert.equal(
      appModule.applyDashboardStateToSearch(
        '?foo=bar&groupBy=day',
        {
          startDate: '2026-04-05',
          endDate: '2026-04-14',
          source: 'google',
          campaign: '',
          attributionModel: undefined
        },
        'source'
      ),
      'foo=bar&startDate=2026-04-05&endDate=2026-04-14&source=google&groupBy=source'
    );
  } finally {
    dom.window.close();
  }
});

test('dashboard controls hydrate from query params and send matching outbound requests', async () => {
  const calls: FetchCall[] = [];
  const dom = createDom({
    url: 'http://localhost/?startDate=2026-04-03&endDate=2026-04-11&source=google&campaign=brand-search&groupBy=campaign'
  });
  const previousFetch = globalThis.fetch;
  const container = dom.window.document.createElement('div');
  dom.window.document.body.appendChild(container);
  dom.window.localStorage.setItem('roas_radar_auth_token', 'test-token');
  globalThis.fetch = createFetchStub(calls) as typeof globalThis.fetch;

  try {
    const { default: App } = await loadDashboardModule<typeof import('../dashboard/src/App')>('dashboard/src/App.tsx');
    const root = createRoot(container);

    flushSync(() => {
      root.render(h(App));
    });

    await waitForDashboardControls(container);
    await settleApp();

    const [startDateInput, endDateInput] = Array.from(container.querySelectorAll<HTMLInputElement>('input[type="date"]'));
    const sourceInput = container.querySelector('input[placeholder="google, meta, facebook"]') as HTMLInputElement;
    const campaignInput = container.querySelector('input[placeholder="spring-sale"]') as HTMLInputElement;
    const groupBySelect = findGroupBySelect(container) as HTMLSelectElement;

    assert.equal(startDateInput.value, '2026-04-03');
    assert.equal(endDateInput.value, '2026-04-11');
    assert.equal(sourceInput.value, 'google');
    assert.equal(campaignInput.value, 'brand-search');
    assert.equal(groupBySelect.value, 'campaign');

    const initialSummaryCall = calls.find((call) => call.path === '/api/reporting/summary');
    const initialTimeseriesCall = calls.find((call) => call.path === '/api/reporting/timeseries');

    assert.ok(initialSummaryCall);
    assert.equal(initialSummaryCall.query.get('startDate'), '2026-04-03');
    assert.equal(initialSummaryCall.query.get('endDate'), '2026-04-11');
    assert.equal(initialSummaryCall.query.get('source'), 'google');
    assert.equal(initialSummaryCall.query.get('campaign'), 'brand-search');
    assert.ok(initialTimeseriesCall);
    assert.equal(initialTimeseriesCall.query.get('groupBy'), 'campaign');

    assert.equal(
      dom.window.location.search,
      '?startDate=2026-04-03&endDate=2026-04-11&source=google&campaign=brand-search&groupBy=campaign'
    );

    flushSync(() => {
      root.unmount();
    });
  } finally {
    globalThis.fetch = previousFetch;
    dom.window.close();
  }
});
