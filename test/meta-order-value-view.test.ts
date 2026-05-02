import assert from 'node:assert/strict';
import test from 'node:test';

import { changeInputValue, click, createDom, createRoot, flushSync, h, loadDashboardModule, tick } from './dashboard-ui-test-helpers';

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json'
    }
  });
}

function createMetaOrderValueFetchStub() {
  return async (input: string | URL | Request) => {
    const rawUrl = typeof input === 'string' ? input : input instanceof Request ? input.url : input.toString();
    const baseUrl = globalThis.window?.location?.origin ?? 'http://localhost';
    const url = new URL(rawUrl, baseUrl);

    if (url.pathname !== '/api/reporting/meta-order-value') {
      throw new Error(`Unexpected fetch: ${url.pathname}`);
    }

    return jsonResponse({
      scope: {
        organizationId: 1
      },
      range: {
        startDate: url.searchParams.get('startDate'),
        endDate: url.searchParams.get('endDate')
      },
      filters: {
        campaignIds: [],
        campaignSearch: url.searchParams.get('campaignSearch'),
        actionType: url.searchParams.get('actionType')
      },
      sort: {
        by: url.searchParams.get('sortBy') ?? 'reportDate',
        direction: url.searchParams.get('sortDirection') ?? 'desc'
      },
      pagination: {
        limit: Number(url.searchParams.get('limit') ?? '8'),
        offset: Number(url.searchParams.get('offset') ?? '0'),
        returned: 2,
        totalRows: 2,
        hasMore: false
      },
      totals: {
        attributedRevenue: 12960,
        purchaseCount: 84,
        spend: 2556,
        roas: 5.07
      },
      rows: [
        {
          date: '2026-04-19',
          campaignId: '2385001',
          campaignName: 'Prospecting Carousel',
          attributedRevenue: 8640,
          purchaseCount: 54,
          spend: 1710,
          roas: 5.05,
          calculatedRoas: 5.05,
          canonicalActionType: 'purchase',
          canonicalSelectionMode: 'priority',
          currency: 'USD'
        },
        {
          date: '2026-04-18',
          campaignId: '2385002',
          campaignName: 'Retargeting Video',
          attributedRevenue: 4320,
          purchaseCount: 30,
          spend: 846,
          roas: null,
          calculatedRoas: 5.11,
          canonicalActionType: 'omni_purchase',
          canonicalSelectionMode: 'fallback',
          currency: 'USD'
        }
      ]
    });
  };
}

async function waitForMetaView(container: HTMLDivElement) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const startDateInput = container.querySelector('#meta-order-value-start-date') as HTMLInputElement | null;
    const endDateInput = container.querySelector('#meta-order-value-end-date') as HTMLInputElement | null;
    const searchInput = container.querySelector('input[placeholder="Campaign name or ID"]') as HTMLInputElement | null;

    if (startDateInput && endDateInput && searchInput && /Meta attributed revenue explorer/.test(container.textContent ?? '')) {
      return;
    }

    await tick(20);
  }

  throw new Error(`Meta order value view did not render in time. Rendered content: ${container.textContent ?? ''}`);
}

test('meta order value view renders live totals, timezone-aware date rows, and campaign breakdown columns', async () => {
  const dom = createDom();
  const previousFetch = globalThis.fetch;
  const container = dom.window.document.createElement('div');
  dom.window.document.body.appendChild(container);
  dom.window.localStorage.setItem('roas_radar_auth_token', 'test-token');
  globalThis.fetch = createMetaOrderValueFetchStub() as typeof globalThis.fetch;

  try {
    const { default: MetaOrderValueView } = await loadDashboardModule<
      typeof import('../dashboard/src/components/MetaOrderValueView')
    >('dashboard/src/components/MetaOrderValueView.tsx');
    const root = createRoot(container);

    flushSync(() => {
      root.render(h(MetaOrderValueView, { reportingTimezone: 'America/Los_Angeles' }));
    });

    await waitForMetaView(container);

    const text = container.textContent ?? '';
    assert.match(text, /Meta attributed revenue explorer/);
    assert.match(text, /Campaign-day breakdown/);
    assert.match(text, /Attributed revenue/);
    assert.match(text, /\$12,960/);
    assert.match(text, /Prospecting Carousel/);
    assert.match(text, /Retargeting Video/);
    assert.match(text, /Order date\/time/);
    assert.match(text, /Action type/);
    assert.match(text, /Fallback/);

    flushSync(() => {
      root.unmount();
    });
  } finally {
    globalThis.fetch = previousFetch;
    dom.window.close();
  }
});

test('meta order value view keeps date constraints and filters wired for responsive operators', async () => {
  const dom = createDom({ width: 375, height: 900 });
  const previousFetch = globalThis.fetch;
  const container = dom.window.document.createElement('div');
  dom.window.document.body.appendChild(container);
  dom.window.localStorage.setItem('roas_radar_auth_token', 'test-token');
  globalThis.fetch = createMetaOrderValueFetchStub() as typeof globalThis.fetch;

  try {
    const { default: MetaOrderValueView } = await loadDashboardModule<
      typeof import('../dashboard/src/components/MetaOrderValueView')
    >('dashboard/src/components/MetaOrderValueView.tsx');
    const root = createRoot(container);

    flushSync(() => {
      root.render(h(MetaOrderValueView, { reportingTimezone: 'America/Los_Angeles' }));
    });

    await waitForMetaView(container);

    const startDateInput = container.querySelector('#meta-order-value-start-date') as HTMLInputElement;
    const endDateInput = container.querySelector('#meta-order-value-end-date') as HTMLInputElement;
    const lastSevenDaysButton = Array.from(container.querySelectorAll<HTMLButtonElement>('button')).find(
      (button) => button.textContent?.trim() === 'Last 7D'
    );
    const campaignSearchInput = container.querySelector('input[placeholder="Campaign name or ID"]') as HTMLInputElement;

    assert.ok(startDateInput);
    assert.ok(endDateInput);
    assert.equal(startDateInput.getAttribute('max'), endDateInput.value);
    assert.equal(endDateInput.getAttribute('min'), startDateInput.value);
    assert.ok(lastSevenDaysButton);

    click(lastSevenDaysButton);
    await tick(20);

    assert.ok(startDateInput.value <= endDateInput.value);

    changeInputValue(campaignSearchInput, 'Prospecting');
    await tick(20);

    assert.equal(campaignSearchInput.value, 'Prospecting');
    assert.match(container.textContent ?? '', /Rows in window/);
    assert.match(container.textContent ?? '', /Prospecting Carousel/);

    flushSync(() => {
      root.unmount();
    });
  } finally {
    globalThis.fetch = previousFetch;
    dom.window.close();
  }
});
