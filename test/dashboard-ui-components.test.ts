import assert from 'node:assert/strict';
import test from 'node:test';

import {
  React,
  changeInputValue,
  click,
  h,
  keydown,
  loadDashboardModule,
  mountUi,
  normalizeHtml,
  noop,
  renderToStaticMarkup,
  tick
} from './dashboard-ui-test-helpers';

test('section states and fields expose stable accessible markup', async () => {
  const ui = await loadDashboardModule<typeof import('../dashboard/src/components/AuthenticatedUi')>(
    'dashboard/src/components/AuthenticatedUi.tsx'
  );

  const loadingMarkup = normalizeHtml(
    renderToStaticMarkup(
      h(
        ui.SectionState,
        { loading: true, error: null, empty: false, emptyLabel: 'No rows' },
        h('div', null, 'Loaded')
      )
    )
  );
  const errorMarkup = normalizeHtml(
    renderToStaticMarkup(
      h(
        ui.SectionState,
        { loading: false, error: 'Summary API failed.', empty: false, emptyLabel: 'No rows' },
        h('div', null, 'Loaded')
      )
    )
  );
  const emptyMarkup = normalizeHtml(
    renderToStaticMarkup(
      h(
        ui.SectionState,
        { loading: false, error: null, empty: true, emptyLabel: 'No rows' },
        h('div', null, 'Loaded')
      )
    )
  );
  const successMarkup = normalizeHtml(
    renderToStaticMarkup(
      h(
        ui.SectionState,
        { loading: false, error: null, empty: false, emptyLabel: 'No rows' },
        h('div', null, 'Loaded')
      )
    )
  );
  const fieldMarkup = normalizeHtml(
    renderToStaticMarkup(
      h(
        ui.Field,
        {
          label: 'Reporting timezone',
          hint: 'Use an IANA timezone.',
          description: 'Stored in app settings.',
          error: 'Timezone is required.',
          required: true
        },
        h(ui.Input, { value: '', onChange: noop })
      )
    )
  );

  assert.match(loadingMarkup, /animate-pulse/);
  assert.match(errorMarkup, /Unable to load data/);
  assert.match(errorMarkup, /Summary API failed\./);
  assert.match(emptyMarkup, /No results returned/);
  assert.match(emptyMarkup, /No rows/);
  assert.match(successMarkup, /Loaded/);
  assert.match(fieldMarkup, /aria-invalid="true"/);
  assert.match(fieldMarkup, /aria-describedby="[^"]+"/);
  assert.match(fieldMarkup, /Timezone is required\./);
  assert.match(fieldMarkup, /Use an IANA timezone\./);
});

test('tabs support keyboard navigation across authenticated panels', async () => {
  const ui = await loadDashboardModule<typeof import('../dashboard/src/components/AuthenticatedUi')>(
    'dashboard/src/components/AuthenticatedUi.tsx'
  );
  const mounted = await mountUi(
    h(ui.Tabs, {
      ariaLabel: 'Settings sections',
      items: [
        { value: 'general', label: 'General', panel: h('div', null, 'General panel') },
        { value: 'connections', label: 'Connections', panel: h('div', null, 'Connections panel') },
        { value: 'users', label: 'Users', panel: h('div', null, 'Users panel') }
      ]
    })
  );

  try {
    const tabs = Array.from(mounted.container.querySelectorAll<HTMLButtonElement>('[role="tab"]'));
    assert.equal(tabs.length, 3);
    assert.match(mounted.container.textContent ?? '', /General panel/);
    assert.equal(tabs[0].getAttribute('aria-selected'), 'true');

    keydown(tabs[0], 'ArrowRight');
    await tick();

    assert.equal(mounted.dom.window.document.activeElement?.textContent?.trim(), 'Connections');
    assert.equal(tabs[1].getAttribute('aria-selected'), 'true');
    assert.match(mounted.container.textContent ?? '', /Connections panel/);

    keydown(tabs[1], 'End');
    await tick();

    assert.equal(mounted.dom.window.document.activeElement?.textContent?.trim(), 'Users');
    assert.equal(tabs[2].getAttribute('aria-selected'), 'true');
    assert.match(mounted.container.textContent ?? '', /Users panel/);
  } finally {
    mounted.cleanup();
  }
});

test('modal traps focus and closes on escape for authenticated overlays', async () => {
  const ui = await loadDashboardModule<typeof import('../dashboard/src/components/AuthenticatedUi')>(
    'dashboard/src/components/AuthenticatedUi.tsx'
  );

  let closeCount = 0;
  const mounted = await mountUi(
    h(
      'div',
      null,
      h('button', { type: 'button', id: 'opener' }, 'Open settings modal'),
      h(
        ui.Modal,
        {
          open: true,
          title: 'Edit reporting timezone',
          description: 'Keep revenue rollups aligned to one timezone.',
          onClose: () => {
            closeCount += 1;
          },
          footer: h('button', { type: 'button' }, 'Save changes')
        },
        h('button', { type: 'button' }, 'Cancel')
      )
    )
  );

  try {
    const opener = mounted.dom.window.document.getElementById('opener') as HTMLButtonElement;
    opener.focus();

    mounted.root.render(
      h(
        'div',
        null,
        h('button', { type: 'button', id: 'opener' }, 'Open settings modal'),
        h(
          ui.Modal,
          {
            open: true,
            title: 'Edit reporting timezone',
            description: 'Keep revenue rollups aligned to one timezone.',
            onClose: () => {
              closeCount += 1;
            },
            footer: h('button', { type: 'button' }, 'Save changes')
          },
          h('button', { type: 'button' }, 'Cancel')
        )
      )
    );
    await tick(20);

    const activeLabel = (mounted.dom.window.document.activeElement as HTMLElement | null)?.getAttribute('aria-label');
    assert.equal(activeLabel, 'Close modal');

    keydown(mounted.dom.window.document.activeElement as Element, 'Tab', { shiftKey: true });
    await tick();
    assert.equal((mounted.dom.window.document.activeElement as HTMLElement | null)?.textContent?.trim(), 'Save changes');

    keydown(mounted.dom.window.document.activeElement as Element, 'Escape');
    await tick();
    assert.equal(closeCount, 1);
  } finally {
    mounted.cleanup();
  }
});

test('nivo charts render smoke coverage for dashboard data visuals', async () => {
  const charts = await loadDashboardModule<typeof import('../dashboard/src/components/charts')>(
    'dashboard/src/components/charts/index.ts'
  );

  const mounted = await mountUi(
    h(
      'div',
      { className: 'grid gap-6' },
      h(charts.NivoAreaChart, {
        data: [
          {
            id: 'Revenue',
            data: [
              { x: 'Apr 18', y: 3210 },
              { x: 'Apr 19', y: 4020 },
              { x: 'Apr 20', y: 3680 }
            ]
          }
        ],
        label: 'Revenue trend chart',
        description: 'Area chart showing revenue trend across the reporting window.',
        summary: 'Three daily revenue points render in the trend chart.'
      }),
      h(charts.NivoBarChart, {
        data: [
          { campaign: 'Spring Search', revenue: 18320 },
          { campaign: 'Prospecting Carousel', revenue: 12960 }
        ],
        keys: ['revenue'],
        indexBy: 'campaign',
        label: 'Campaign mix chart',
        description: 'Horizontal revenue bars for the leading campaigns.',
        summary: 'Two campaign rows render in the bar chart.'
      }),
      h(charts.NivoPieChart, {
        data: [
          { id: 'google', label: 'Google', value: 18320, revenueLabel: '$18.3K' },
          { id: 'meta', label: 'Meta', value: 12960, revenueLabel: '$13.0K' }
        ],
        label: 'Source contribution chart',
        description: 'Pie chart showing attributed revenue split by source.',
        summary: 'Two source slices render in the contribution chart.'
      })
    ),
    { width: 1440, height: 900 }
  );

  try {
    await tick(20);

    assert.equal(mounted.container.querySelectorAll('figure[role="group"]').length, 3);
    assert.ok(mounted.container.querySelectorAll('svg').length >= 3);
    assert.match(mounted.container.textContent ?? '', /Revenue trend chart/);
    assert.match(mounted.container.textContent ?? '', /Campaign mix chart/);
    assert.match(mounted.container.textContent ?? '', /Source contribution chart/);
  } finally {
    mounted.cleanup();
  }
});
