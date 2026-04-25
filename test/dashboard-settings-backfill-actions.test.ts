import assert from 'node:assert/strict';
import test from 'node:test';

import {
  React,
  click,
  createDom,
  h,
  loadDashboardModule,
  mountUi,
  tick
} from './dashboard-ui-test-helpers';

type FetchCall = {
  path: string;
  method: string;
  body: unknown;
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json'
    }
  });
}

function parseBody(init?: RequestInit): unknown {
  if (!init?.body || typeof init.body !== 'string') {
    return null;
  }

  return JSON.parse(init.body);
}

function createFetchStub(calls: FetchCall[]) {
  return async (input: string | URL | Request, init?: RequestInit) => {
    const request = input instanceof Request ? input : null;
    const rawUrl = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
    const baseUrl = globalThis.window?.location?.origin ?? 'http://localhost';
    const url = new URL(rawUrl, baseUrl);
    const method = init?.method ?? request?.method ?? 'GET';
    const body = parseBody(init);

    calls.push({
      path: url.pathname,
      method,
      body
    });

    if (url.pathname === '/api/admin/shopify/orders/recover-attribution') {
      return jsonResponse({
        ok: true,
        startDate: (body as { startDate: string }).startDate,
        endDate: (body as { endDate: string }).endDate,
        scannedOrders: 18,
        recoveredOrders: 7,
        unrecoverableOrders: 3
      });
    }

    if (url.pathname === '/api/admin/attribution/orders/backfill' && method === 'POST') {
      return jsonResponse(
        {
          ok: true,
          jobId: `job-${calls.filter((call) => call.path === '/api/admin/attribution/orders/backfill').length}`,
          status: 'queued',
          submittedAt: '2026-04-20T19:10:00.000Z',
          submittedBy: 'taylor@roasradar.dev',
          options: body
        },
        202
      );
    }

    throw new Error(`Unexpected fetch: ${method} ${url.pathname}`);
  };
}

async function waitForButton(container: ParentNode, label: string) {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const button = Array.from(container.querySelectorAll<HTMLButtonElement>('button')).find((candidate) =>
      candidate.textContent?.includes(label)
    );

    if (button) {
      return button;
    }

    await tick(20);
  }

  throw new Error(`Timed out waiting for button: ${label}`);
}

function createSettingsHarness(
  SettingsAdminView: typeof import('../dashboard/src/components/SettingsAdminView').default,
  api: typeof import('../dashboard/src/lib/api')
) {
  return function SettingsHarness() {
    const [shopifyBackfillRange, setShopifyBackfillRange] = React.useState({
      startDate: '2026-04-01',
      endDate: '2026-04-20'
    });
    const [shopifyOrderAttributionBackfillOptions, setShopifyOrderAttributionBackfillOptions] = React.useState({
      dryRun: true,
      limit: '500',
      webOrdersOnly: true,
      skipShopifyWriteback: false
    });
    const [actionFeedback, setActionFeedback] = React.useState({
      context: null,
      loading: null,
      error: null,
      message: null
    });
    const [orderAttributionBackfillJob, setOrderAttributionBackfillJob] = React.useState({
      data: null,
      loading: false,
      error: null
    });

    return h(SettingsAdminView, {
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
      setSettingsForm: () => {},
      usersSection: {
        data: [],
        loading: false,
        error: null
      },
      newUserForm: {
        email: '',
        password: '',
        displayName: '',
        isAdmin: false
      },
      setNewUserForm: () => {},
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
      shopifyBackfillRange,
      setShopifyBackfillRange,
      shopifyOrderAttributionBackfillOptions,
      setShopifyOrderAttributionBackfillOptions,
      orderAttributionBackfillJob,
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
          connection: null
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
      setMetaConfigForm: () => {},
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
          connection: null,
          reconciliation: null
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
      setGoogleConfigForm: () => {},
      googleForm: {
        customerId: '',
        loginCustomerId: ''
      },
      setGoogleForm: () => {},
      actionFeedback,
      onSettingsSave: () => {},
      onCreateUser: () => {},
      onShopifyBackfill: () => {},
      onMetaConfigSave: () => {},
      onGoogleConfigSave: () => {},
      onGoogleConnect: () => {},
      onShopifyTest: () => {},
      onShopifyWebhookSync: () => {},
      onShopifyAttributionRecovery: async () => {
        const response = await api.recoverShopifyAttributionHints(shopifyBackfillRange.startDate, shopifyBackfillRange.endDate);
        setActionFeedback({
          context: 'shopify-attribution-recovery',
          loading: null,
          error: null,
          message: `Recovered ${response.recoveredOrders} Shopify attribution hints.`
        });
      },
      onShopifyOrderAttributionBackfill: async () => {
        const response = await api.enqueueOrderAttributionBackfill({
          startDate: shopifyBackfillRange.startDate,
          endDate: shopifyBackfillRange.endDate,
          dryRun: shopifyOrderAttributionBackfillOptions.dryRun,
          limit: Number(shopifyOrderAttributionBackfillOptions.limit),
          webOrdersOnly: shopifyOrderAttributionBackfillOptions.webOrdersOnly,
          skipShopifyWriteback: shopifyOrderAttributionBackfillOptions.skipShopifyWriteback
        });

        setOrderAttributionBackfillJob({
          data: {
            ...response,
            startedAt: null,
            completedAt: null,
            report: null,
            error: null
          },
          loading: false,
          error: null
        });
        setActionFeedback({
          context: 'shopify-order-attribution-backfill',
          loading: null,
          error: null,
          message: `Queued order attribution backfill job ${response.jobId}.`
        });
      },
      onOrderAttributionBackfillRefresh: () => {},
      onMetaConnect: () => {},
      onMetaSync: () => {},
      onGoogleSync: () => {},
      onGoogleReconcile: () => {}
    });
  };
}

test('settings recovery harness preserves the existing recovery actions and submits dry-run payload defaults', async () => {
  const calls: FetchCall[] = [];
  const dom = createDom();
  const previousFetch = globalThis.fetch;
  globalThis.fetch = createFetchStub(calls) as typeof globalThis.fetch;

  try {
    const [{ default: SettingsAdminView }, api] = await Promise.all([
      loadDashboardModule<typeof import('../dashboard/src/components/SettingsAdminView')>(
        'dashboard/src/components/SettingsAdminView.tsx'
      ),
      loadDashboardModule<typeof import('../dashboard/src/lib/api')>('dashboard/src/lib/api.ts')
    ]);

    const SettingsHarness = createSettingsHarness(SettingsAdminView, api);
    const mounted = await mountUi(h(SettingsHarness));

    try {
      assert.match(mounted.container.textContent ?? '', /Backfill Shopify orders/);
      assert.match(mounted.container.textContent ?? '', /Recover attribution hints/);
      assert.match(mounted.container.textContent ?? '', /Select attribution backfill/);

      const recoverButton = await waitForButton(mounted.container, 'Recover attribution hints');
      click(recoverButton);
      await tick();

      const recoveryCall = calls.find((call) => call.path === '/api/admin/shopify/orders/recover-attribution');
      assert.ok(recoveryCall);
      assert.deepEqual(recoveryCall.body, {
        startDate: '2026-04-01',
        endDate: '2026-04-20'
      });

      const selectButton = await waitForButton(mounted.container, 'Select attribution backfill');
      click(selectButton);
      await tick();

      const limitInput = mounted.container.querySelector('#shopify-order-attribution-limit') as HTMLInputElement | null;
      const dryRunInput = mounted.container.querySelector('#shopify-order-attribution-dry-run') as HTMLInputElement | null;
      const webOnlyInput = mounted.container.querySelector('#shopify-order-attribution-web-only') as HTMLInputElement | null;
      const skipWritebackInput = mounted.container.querySelector(
        '#shopify-order-attribution-skip-writeback'
      ) as HTMLInputElement | null;

      assert.ok(limitInput);
      assert.equal(limitInput.value, '500');
      assert.ok(dryRunInput);
      assert.equal(dryRunInput.checked, true);
      assert.ok(webOnlyInput);
      assert.equal(webOnlyInput.checked, true);
      assert.ok(skipWritebackInput);
      assert.equal(skipWritebackInput.checked, false);

      const queueButton = await waitForButton(mounted.container, 'Queue order attribution backfill');
      click(queueButton);
      await tick();

      assert.equal(mounted.dom.window.document.querySelector('dialog'), null);

      const backfillCall = calls.find((call) => call.path === '/api/admin/attribution/orders/backfill');
      assert.ok(backfillCall);
      assert.deepEqual(backfillCall.body, {
        startDate: '2026-04-01',
        endDate: '2026-04-20',
        dryRun: true,
        limit: 500,
        webOrdersOnly: true,
        skipShopifyWriteback: false
      });
    } finally {
      mounted.cleanup();
    }
  } finally {
    globalThis.fetch = previousFetch;
    dom.window.close();
  }
});

test('settings recovery harness gates non-dry-run backfills until the operator confirms the final payload', async () => {
  const calls: FetchCall[] = [];
  const dom = createDom();
  const previousFetch = globalThis.fetch;
  globalThis.fetch = createFetchStub(calls) as typeof globalThis.fetch;

  try {
    const [{ default: SettingsAdminView }, api] = await Promise.all([
      loadDashboardModule<typeof import('../dashboard/src/components/SettingsAdminView')>(
        'dashboard/src/components/SettingsAdminView.tsx'
      ),
      loadDashboardModule<typeof import('../dashboard/src/lib/api')>('dashboard/src/lib/api.ts')
    ]);

    const SettingsHarness = createSettingsHarness(SettingsAdminView, api);
    const mounted = await mountUi(h(SettingsHarness));

    try {
      const selectButton = await waitForButton(mounted.container, 'Select attribution backfill');
      click(selectButton);
      await tick();

      const limitInput = mounted.container.querySelector('#shopify-order-attribution-limit') as HTMLInputElement | null;
      const dryRunInput = mounted.container.querySelector('#shopify-order-attribution-dry-run') as HTMLInputElement | null;
      const webOnlyInput = mounted.container.querySelector('#shopify-order-attribution-web-only') as HTMLInputElement | null;
      const skipWritebackInput = mounted.container.querySelector(
        '#shopify-order-attribution-skip-writeback'
      ) as HTMLInputElement | null;

      assert.ok(limitInput);
      assert.ok(dryRunInput);
      assert.ok(webOnlyInput);
      assert.ok(skipWritebackInput);

      click(dryRunInput);
      click(webOnlyInput);
      click(skipWritebackInput);
      await tick();

      const queueButton = await waitForButton(mounted.container, 'Queue order attribution backfill');
      click(queueButton);
      await tick();

      assert.equal(calls.filter((call) => call.path === '/api/admin/attribution/orders/backfill').length, 0);
      assert.ok(mounted.dom.window.document.querySelector('dialog'));
      assert.match(mounted.container.textContent ?? '', /Shopify writeback is disabled for this run/);
      assert.match(mounted.container.textContent ?? '', /Order scan limit\s*500/);
      assert.match(mounted.container.textContent ?? '', /Web orders only\s*No/);

      const confirmButton = await waitForButton(mounted.dom.window.document, 'Yes, queue backfill');
      click(confirmButton);
      await tick();

      const backfillCall = calls.find((call) => call.path === '/api/admin/attribution/orders/backfill');
      assert.ok(backfillCall);
      assert.deepEqual(backfillCall.body, {
        startDate: '2026-04-01',
        endDate: '2026-04-20',
        dryRun: false,
        limit: 500,
        webOrdersOnly: false,
        skipShopifyWriteback: true
      });
      assert.equal(mounted.dom.window.document.querySelector('dialog'), null);
    } finally {
      mounted.cleanup();
    }
  } finally {
    globalThis.fetch = previousFetch;
    dom.window.close();
  }
});
