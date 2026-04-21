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
