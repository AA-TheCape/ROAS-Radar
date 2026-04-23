import assert from 'node:assert/strict';
import test from 'node:test';

import {
  click,
  createOrderDetailsProps,
  createReportingDashboardProps,
  createSettingsAdminProps,
  createShellProps,
  h,
  loadDashboardModule,
  mountUi,
  tick
} from './dashboard-ui-test-helpers';

test('authenticated shell mobile navigation opens and closes on route change', async () => {
  const { default: AuthenticatedAppShell } = await loadDashboardModule<
    typeof import('../dashboard/src/components/AuthenticatedAppShell')
  >('dashboard/src/components/AuthenticatedAppShell.tsx');

  let activeNavKey = 'dashboard';
  const mounted = await mountUi(h(AuthenticatedAppShell, createShellProps({ activeNavKey })), { width: 768, height: 900 });

  try {
    const toggle = mounted.container.querySelector('button[aria-controls="app-shell-mobile-nav"]') as HTMLButtonElement;
    assert.ok(toggle);

    click(toggle);
    await tick();
    assert.ok(mounted.dom.window.document.getElementById('app-shell-mobile-nav'));

    activeNavKey = 'settings';
    mounted.root.render(h(AuthenticatedAppShell, createShellProps({ activeNavKey })));
    await tick();

    assert.equal(mounted.dom.window.document.getElementById('app-shell-mobile-nav'), null);
  } finally {
    mounted.cleanup();
  }
});

test('authenticated shell removes deprecated workspace and header cards without leaving layout gaps', async () => {
  const { default: AuthenticatedAppShell } = await loadDashboardModule<
    typeof import('../dashboard/src/components/AuthenticatedAppShell')
  >('dashboard/src/components/AuthenticatedAppShell.tsx');

  const mounted = await mountUi(h(AuthenticatedAppShell, createShellProps()), { width: 1440, height: 900 });

  try {
    assert.doesNotMatch(mounted.container.textContent ?? '', /Workspace/);
    assert.doesNotMatch(mounted.container.textContent ?? '', /Active window/);
    assert.match(mounted.container.textContent ?? '', /Current time/);
    assert.match(mounted.container.textContent ?? '', /UTC Apr 20, 7:15 PM/);
    assert.equal(mounted.container.querySelector('aside[aria-label="Section navigation"]'), null);
    assert.equal(mounted.container.querySelector('[aria-label="Current workspace status"]'), null);
    assert.ok(mounted.container.querySelector('[aria-label="Current timestamp"]'));
    assert.ok(mounted.container.querySelector('#app-shell-main'));
  } finally {
    mounted.cleanup();
  }
});

test('title bar timestamp updates on the next minute boundary and clears timers on unmount', async () => {
  const { default: TitleBarTimestamp } = await loadDashboardModule<
    typeof import('../dashboard/src/components/TitleBarTimestamp')
  >('dashboard/src/components/TitleBarTimestamp.tsx');

  let now = new Date('2026-04-20T19:14:15.250Z');
  let timeoutCallback: (() => void) | null = null;
  let intervalCallback: (() => void) | null = null;
  let timeoutDelay = -1;
  let intervalDelay = -1;
  const clearedTimeouts: number[] = [];
  const clearedIntervals: number[] = [];

  const mounted = await mountUi(
    h(TitleBarTimestamp, {
      getNow: () => now,
      scheduleTimeout: ((callback: () => void, delay?: number) => {
        timeoutCallback = callback;
        timeoutDelay = delay ?? 0;
        return 11 as ReturnType<typeof setTimeout>;
      }) as typeof setTimeout,
      clearScheduledTimeout: ((timer: number) => {
        clearedTimeouts.push(timer);
      }) as typeof clearTimeout,
      scheduleInterval: ((callback: () => void, delay?: number) => {
        intervalCallback = callback;
        intervalDelay = delay ?? 0;
        return 29 as ReturnType<typeof setInterval>;
      }) as typeof setInterval,
      clearScheduledInterval: ((timer: number) => {
        clearedIntervals.push(timer);
      }) as typeof clearInterval
    })
  );

  try {
    assert.match(mounted.container.textContent ?? '', /UTC Apr 20, 7:14 PM/);
    assert.equal(timeoutDelay, 44_750);

    now = new Date('2026-04-20T19:15:00.000Z');
    timeoutCallback?.();
    await tick();

    assert.equal(intervalDelay, 60_000);
    assert.match(mounted.container.textContent ?? '', /UTC Apr 20, 7:15 PM/);

    now = new Date('2026-04-20T19:16:00.000Z');
    intervalCallback?.();
    await tick();

    assert.match(mounted.container.textContent ?? '', /UTC Apr 20, 7:16 PM/);
  } finally {
    mounted.cleanup();
  }

  assert.deepEqual(clearedTimeouts, [11]);
  assert.deepEqual(clearedIntervals, [29]);
});

test('reporting dashboard search and order drill-in stay wired for high-traffic workflows', async () => {
  const { default: ReportingDashboard } = await loadDashboardModule<
    typeof import('../dashboard/src/components/ReportingDashboard')
  >('dashboard/src/components/ReportingDashboard.tsx');

  let openedOrderId: string | null = null;
  const mounted = await mountUi(
    h(
      ReportingDashboard,
      createReportingDashboardProps({
        onOpenOrderDetails: (shopifyOrderId: string) => {
          openedOrderId = shopifyOrderId;
        }
      })
    )
  );

  try {
    assert.match(mounted.container.textContent ?? '', /Campaign performance/);
    assert.match(mounted.container.textContent ?? '', /Attributed orders/);

    const orderButton = mounted.container.querySelector('button[aria-label="Open order details for Shopify order 1105"]');
    assert.ok(orderButton);

    click(orderButton);
    await tick();
    assert.equal(openedOrderId, '1105');
  } finally {
    mounted.cleanup();
  }
});

test('reporting dashboard renders the bottom spend report grouped by channel then campaign', async () => {
  const { default: ReportingDashboard } = await loadDashboardModule<
    typeof import('../dashboard/src/components/ReportingDashboard')
  >('dashboard/src/components/ReportingDashboard.tsx');

  const mounted = await mountUi(h(ReportingDashboard, createReportingDashboardProps()));

  try {
    const text = mounted.container.textContent ?? '';
    assert.match(text, /Marketing spend detail/);
    assert.match(text, /Google \/ Cpc/);
    assert.match(text, /Spring Search/);
    assert.match(text, /Channel subtotal/);
  } finally {
    mounted.cleanup();
  }
});

test('order details empty state stays explicit when no drill-in selection is active', async () => {
  const { default: OrderDetailsView } = await loadDashboardModule<typeof import('../dashboard/src/components/OrderDetailsView')>(
    'dashboard/src/components/OrderDetailsView.tsx'
  );

  const mounted = await mountUi(
    h(
      OrderDetailsView,
      createOrderDetailsProps({
        selectedOrderId: null,
        orderDetailsSection: {
          loading: false,
          error: null,
          data: null
        }
      })
    )
  );

  try {
    assert.match(mounted.container.textContent ?? '', /No order selected\./);
  } finally {
    mounted.cleanup();
  }
});

test('settings admin view keeps user management gated for non-admin access', async () => {
  const { default: SettingsAdminView } = await loadDashboardModule<
    typeof import('../dashboard/src/components/SettingsAdminView')
  >('dashboard/src/components/SettingsAdminView.tsx');

  const mounted = await mountUi(h(SettingsAdminView, createSettingsAdminProps({ isAdmin: false })));

  try {
    assert.match(mounted.container.textContent ?? '', /Settings operations/);
    assert.match(mounted.container.textContent ?? '', /Shopify connection/);
    assert.doesNotMatch(mounted.container.textContent ?? '', /User access/);
    assert.doesNotMatch(mounted.container.textContent ?? '', /Create app access/);
  } finally {
    mounted.cleanup();
  }
});
