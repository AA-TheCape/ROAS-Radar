import {
  startTransition,
  useDeferredValue,
  useEffect,
  useMemo,
  useState,
  type ReactNode
} from 'react';

import {
  fetchCampaigns,
  fetchChannels,
  fetchCreatives,
  fetchModels,
  fetchOverview,
  fetchTimeseries,
  type AttributionModel,
  type CampaignRow,
  type ChannelRow,
  type CreativeRow,
  type Filters,
  type TimeseriesPoint
} from './lib/api';
import { formatCompactCurrency, formatCurrency, formatDateLabel, formatNumber, formatPercent } from './lib/format';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type DashboardState = {
  overview: AsyncSection<{
    visits: number;
    orders: number;
    revenue: number;
    spend: number;
    clicks: number;
    impressions: number;
    conversionRate: number;
    roas: number | null;
    cac: number | null;
    averageOrderValue: number | null;
    clickThroughRate: number | null;
    newCustomerOrders: number;
    returningCustomerOrders: number;
    newCustomerRevenue: number;
    returningCustomerRevenue: number;
  }>;
  timeseries: AsyncSection<TimeseriesPoint[]>;
  channels: AsyncSection<ChannelRow[]>;
  campaigns: AsyncSection<CampaignRow[]>;
  creatives: AsyncSection<CreativeRow[]>;
};

type ModelState = {
  defaultModel: AttributionModel;
  supportedModels: AttributionModel[];
};

type CreativeSortKey =
  | 'creativeName'
  | 'creativeId'
  | 'campaign'
  | 'revenue'
  | 'spend'
  | 'roas'
  | 'orders'
  | 'visits'
  | 'clicks'
  | 'clickThroughRate'
  | 'conversionRate'
  | 'costPerClick';

type CreativeSortState = {
  key: CreativeSortKey;
  direction: 'asc' | 'desc';
};

const MODEL_LABELS: Record<AttributionModel, string> = {
  last_touch: 'Last touch',
  first_touch: 'First touch',
  linear: 'Linear',
  position_based: 'Position based',
  time_decay: 'Time decay'
};

const PRESETS = [
  { label: 'Last 7D', days: 7 },
  { label: 'Last 30D', days: 30 },
  { label: 'Last 90D', days: 90 }
];

function formatDateInput(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function buildRange(days: number) {
  const end = new Date();
  const start = new Date(end);
  start.setUTCDate(end.getUTCDate() - (days - 1));

  return {
    startDate: formatDateInput(start),
    endDate: formatDateInput(end)
  };
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

function useDashboardData(filters: Filters) {
  const [state, setState] = useState<DashboardState>({
    overview: createLoadingSection(),
    timeseries: createLoadingSection(),
    channels: createLoadingSection(),
    campaigns: createLoadingSection(),
    creatives: createLoadingSection()
  });

  useEffect(() => {
    let cancelled = false;

    setState({
      overview: createLoadingSection(),
      timeseries: createLoadingSection(),
      channels: createLoadingSection(),
      campaigns: createLoadingSection(),
      creatives: createLoadingSection()
    });

    fetchOverview(filters)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            overview: createResolvedSection(response.data.totals)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            overview: createErroredSection(error.message)
          }));
        }
      });

    fetchTimeseries(filters)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            timeseries: createResolvedSection(response.data.points)
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

    fetchChannels(filters, 10)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            channels: createResolvedSection(response.data.rows)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            channels: createErroredSection(error.message)
          }));
        }
      });

    fetchCampaigns(filters, 12)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            campaigns: createResolvedSection(response.data.rows)
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

    fetchCreatives(filters, 100)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            creatives: createResolvedSection(response.data.rows)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            creatives: createErroredSection(error.message)
          }));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [filters]);

  return state;
}

function compareValues(left: string | number | null, right: string | number | null) {
  if (typeof left === 'number' || typeof right === 'number') {
    return (typeof left === 'number' ? left : -Infinity) - (typeof right === 'number' ? right : -Infinity);
  }

  return (left ?? '').localeCompare(right ?? '', undefined, { sensitivity: 'base' });
}

function sortCreativeRows(rows: CreativeRow[], sort: CreativeSortState): CreativeRow[] {
  const nextRows = [...rows];

  nextRows.sort((left, right) => {
    let result = 0;

    switch (sort.key) {
      case 'creativeName':
        result = compareValues(left.creativeName, right.creativeName);
        break;
      case 'creativeId':
        result = compareValues(left.creativeId, right.creativeId);
        break;
      case 'campaign':
        result = compareValues(left.campaign, right.campaign);
        break;
      case 'revenue':
        result = compareValues(left.revenue, right.revenue);
        break;
      case 'spend':
        result = compareValues(left.spend, right.spend);
        break;
      case 'roas':
        result = compareValues(left.roas ?? -Infinity, right.roas ?? -Infinity);
        break;
      case 'orders':
        result = compareValues(left.orders, right.orders);
        break;
      case 'visits':
        result = compareValues(left.visits, right.visits);
        break;
      case 'clicks':
        result = compareValues(left.clicks, right.clicks);
        break;
      case 'clickThroughRate':
        result = compareValues(left.clickThroughRate ?? -Infinity, right.clickThroughRate ?? -Infinity);
        break;
      case 'conversionRate':
        result = compareValues(left.conversionRate, right.conversionRate);
        break;
      case 'costPerClick':
        result = compareValues(left.costPerClick ?? -Infinity, right.costPerClick ?? -Infinity);
        break;
    }

    if (result === 0) {
      result = compareValues(left.creativeName, right.creativeName);
    }

    return sort.direction === 'asc' ? result : -result;
  });

  return nextRows;
}

function downloadCreativeCsv(rows: CreativeRow[], filters: Filters) {
  const header = [
    'source',
    'medium',
    'campaign',
    'campaign_id',
    'campaign_name',
    'ad_id',
    'ad_name',
    'creative_id',
    'creative_name',
    'content',
    'visits',
    'orders',
    'revenue',
    'spend',
    'roas',
    'clicks',
    'impressions',
    'ctr',
    'conversion_rate',
    'cpc'
  ];

  const csvLines = [
    header.join(','),
    ...rows.map((row) =>
      [
        row.source,
        row.medium,
        row.campaign,
        row.campaignId ?? '',
        row.campaignName ?? '',
        row.adId ?? '',
        row.adName ?? '',
        row.creativeId ?? '',
        row.creativeName,
        row.content,
        row.visits,
        row.orders,
        row.revenue,
        row.spend,
        row.roas ?? '',
        row.clicks,
        row.impressions,
        row.clickThroughRate ?? '',
        row.conversionRate,
        row.costPerClick ?? ''
      ]
        .map((value) => `"${String(value).split('"').join('""')}"`)
        .join(',')
    )
  ];

  const blob = new Blob([csvLines.join('\n')], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `creative-performance-${filters.startDate}-to-${filters.endDate}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function App() {
  const [modelState, setModelState] = useState<ModelState | null>(null);
  const [modelError, setModelError] = useState<string | null>(null);
  const [isModelLoading, setIsModelLoading] = useState(true);
  const [filters, setFilters] = useState<Filters>({
    ...buildRange(30),
    attributionModel: 'last_touch'
  });
  const [creativeSearch, setCreativeSearch] = useState('');
  const [creativeSort, setCreativeSort] = useState<CreativeSortState>({
    key: 'revenue',
    direction: 'desc'
  });

  useEffect(() => {
    let cancelled = false;

    fetchModels()
      .then((response) => {
        if (cancelled) {
          return;
        }

        const nextState = {
          defaultModel: response.data.defaultModel,
          supportedModels: response.data.supportedModels
        };

        setModelState(nextState);
        startTransition(() => {
          setFilters((current) => ({
            ...current,
            attributionModel: nextState.defaultModel
          }));
        });
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setModelError(error.message);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsModelLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const dashboard = useDashboardData(filters);
  const deferredCreativeSearch = useDeferredValue(creativeSearch);
  const headline = dashboard.overview.data;
  const hasAnyData = Boolean(
    (dashboard.timeseries.data && dashboard.timeseries.data.length > 0) ||
      (dashboard.channels.data && dashboard.channels.data.length > 0) ||
      (dashboard.creatives.data && dashboard.creatives.data.length > 0) ||
      (headline && headline.revenue > 0)
  );

  const loadingCount = [
    dashboard.overview.loading,
    dashboard.timeseries.loading,
    dashboard.channels.loading,
    dashboard.campaigns.loading,
    dashboard.creatives.loading
  ].filter(Boolean).length;

  const readySections = [
    dashboard.overview.data,
    dashboard.timeseries.data,
    dashboard.channels.data,
    dashboard.campaigns.data,
    dashboard.creatives.data
  ].filter(Boolean).length;

  const summaryCards = useMemo(
    () => [
      {
        label: 'Attributed revenue',
        value: formatCurrency(headline?.revenue),
        detail: `${formatNumber(headline?.orders)} orders`
      },
      {
        label: 'Spend',
        value: formatCurrency(headline?.spend),
        detail: `${formatNumber(headline?.clicks)} clicks`
      },
      {
        label: 'ROAS',
        value: formatNumber(headline?.roas),
        detail: `${formatPercent(headline?.clickThroughRate)} CTR`
      },
      {
        label: 'CAC',
        value: formatCurrency(headline?.cac),
        detail: `${formatCurrency(headline?.averageOrderValue)} AOV`
      },
      {
        label: 'Conversion rate',
        value: formatPercent(headline?.conversionRate),
        detail: `${formatNumber(headline?.visits)} visits`
      }
    ],
    [headline]
  );

  const activeChannelLabel =
    filters.source && filters.medium ? `${filters.source} / ${filters.medium}` : filters.source ?? filters.medium ?? null;

  const filteredCreativeRows = useMemo(() => {
    const rows = dashboard.creatives.data ?? [];
    const search = deferredCreativeSearch.trim().toLowerCase();
    const searchFilteredRows = search
      ? rows.filter((row) =>
          [
            row.source,
            row.medium,
            row.campaign,
            row.campaignId,
            row.campaignName,
            row.adId,
            row.adName,
            row.creativeId,
            row.creativeName,
            row.content
          ]
            .filter(Boolean)
            .some((value) => value!.toLowerCase().includes(search))
        )
      : rows;

    return sortCreativeRows(searchFilteredRows, creativeSort);
  }, [creativeSort, dashboard.creatives.data, deferredCreativeSearch]);

  const creativeTotals = useMemo(
    () =>
      filteredCreativeRows.reduce(
        (totals, row) => ({
          revenue: totals.revenue + row.revenue,
          spend: totals.spend + row.spend,
          orders: totals.orders + row.orders,
          visits: totals.visits + row.visits
        }),
        { revenue: 0, spend: 0, orders: 0, visits: 0 }
      ),
    [filteredCreativeRows]
  );

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <p className="eyebrow">Unified attribution dashboard</p>
          <h1>ROAS Radar</h1>
          <p className="hero-copy">
            Compare channels, campaigns, and native ad creatives under one attribution model with drill-down filters
            that keep revenue and spend reconciled.
          </p>
        </div>
        <div className="hero-status-card">
          <span>Sections ready</span>
          <strong>
            {readySections}/5
          </strong>
          <small>{loadingCount > 0 ? `${loadingCount} updating` : 'All sections in sync'}</small>
        </div>
      </section>

      <section className="control-bar">
        <div className="control-group">
          <label htmlFor="attribution-model">Attribution model</label>
          <select
            id="attribution-model"
            value={filters.attributionModel}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({
                  ...current,
                  attributionModel: event.target.value as AttributionModel
                }));
              })
            }
            disabled={isModelLoading || !modelState}
          >
            {(modelState?.supportedModels ?? ['last_touch']).map((model) => (
              <option key={model} value={model}>
                {MODEL_LABELS[model]}
              </option>
            ))}
          </select>
          {modelError ? <span className="field-error">{modelError}</span> : null}
        </div>

        <div className="control-group">
          <label htmlFor="start-date">Start date</label>
          <input
            id="start-date"
            type="date"
            value={filters.startDate}
            max={filters.endDate}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({
                  ...current,
                  startDate: event.target.value
                }));
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
            min={filters.startDate}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({
                  ...current,
                  endDate: event.target.value
                }));
              })
            }
          />
        </div>

        <div className="preset-row" aria-label="Date range presets">
          {PRESETS.map((preset) => (
            <button
              key={preset.label}
              className="preset-chip"
              type="button"
              onClick={() =>
                startTransition(() => {
                  setFilters((current) => ({
                    ...current,
                    ...buildRange(preset.days)
                  }));
                })
              }
            >
              {preset.label}
            </button>
          ))}
        </div>
      </section>

      {activeChannelLabel || filters.campaign ? (
        <section className="drilldown-bar">
          <div className="drilldown-summary">
            <span>Active context</span>
            <strong>
              {activeChannelLabel ? activeChannelLabel : 'All channels'}
              {filters.campaign ? ` -> ${filters.campaign}` : ''}
            </strong>
          </div>
          <div className="drilldown-actions">
            {filters.campaign ? (
              <button
                type="button"
                className="ghost-chip"
                onClick={() =>
                  startTransition(() => {
                    setFilters((current) => ({
                      ...current,
                      campaign: undefined,
                      content: undefined
                    }));
                  })
                }
              >
                Clear campaign
              </button>
            ) : null}
            {activeChannelLabel ? (
              <button
                type="button"
                className="ghost-chip"
                onClick={() =>
                  startTransition(() => {
                    setFilters((current) => ({
                      ...current,
                      source: undefined,
                      medium: undefined,
                      campaign: undefined,
                      content: undefined
                    }));
                  })
                }
              >
                Clear channel
              </button>
            ) : null}
          </div>
        </section>
      ) : null}

      {!hasAnyData &&
      !dashboard.overview.loading &&
      !dashboard.timeseries.loading &&
      !dashboard.channels.loading &&
      !dashboard.campaigns.loading &&
      !dashboard.creatives.loading ? (
        <section className="empty-state">
          <h2>No attributed activity for this filter range</h2>
          <p>Try a wider date window or switch attribution models to compare how credit shifts across your creatives.</p>
        </section>
      ) : null}

      <section className="summary-grid">
        {summaryCards.map((card) => (
          <MetricCard
            key={card.label}
            label={card.label}
            value={card.value}
            detail={card.detail}
            loading={dashboard.overview.loading}
            error={dashboard.overview.error}
          />
        ))}
      </section>

      <section className="dashboard-grid">
        <Panel
          title="Revenue vs spend"
          subtitle={`${MODEL_LABELS[filters.attributionModel]} model, ${filters.startDate} to ${filters.endDate}`}
          loading={dashboard.timeseries.loading}
          error={dashboard.timeseries.error}
          empty={!dashboard.timeseries.data?.length}
        >
          {dashboard.timeseries.data ? (
            <DualLineChart
              points={dashboard.timeseries.data}
              revenueLabel={headline ? formatCompactCurrency(headline.revenue) : 'N/A'}
              spendLabel={headline ? formatCompactCurrency(headline.spend) : 'N/A'}
            />
          ) : null}
        </Panel>

        <Panel
          title="Efficiency curve"
          subtitle="Daily ROAS and conversion rate"
          loading={dashboard.timeseries.loading}
          error={dashboard.timeseries.error}
          empty={!dashboard.timeseries.data?.length}
        >
          {dashboard.timeseries.data ? <EfficiencyBars points={dashboard.timeseries.data} /> : null}
        </Panel>

        <Panel
          title="Channel comparison"
          subtitle="Select a channel to scope the campaign and creative views"
          loading={dashboard.channels.loading}
          error={dashboard.channels.error}
          empty={!dashboard.channels.data?.length}
        >
          {dashboard.channels.data ? (
            <ChannelTable
              rows={dashboard.channels.data}
              selectedChannel={activeChannelLabel}
              onSelect={(row) =>
                startTransition(() => {
                  setFilters((current) => ({
                    ...current,
                    source: row.source,
                    medium: row.medium,
                    campaign: undefined,
                    content: undefined
                  }));
                })
              }
            />
          ) : null}
        </Panel>

        <Panel
          title="Campaign leaders"
          subtitle={activeChannelLabel ? `Campaigns within ${activeChannelLabel}` : 'Select a campaign to inspect creatives'}
          loading={dashboard.campaigns.loading}
          error={dashboard.campaigns.error}
          empty={!dashboard.campaigns.data?.length}
        >
          {dashboard.campaigns.data ? (
            <CampaignTable
              rows={dashboard.campaigns.data}
              selectedCampaign={filters.campaign ?? null}
              onSelect={(row) =>
                startTransition(() => {
                  setFilters((current) => ({
                    ...current,
                    source: row.source,
                    medium: row.medium,
                    campaign: row.campaign,
                    content: undefined
                  }));
                })
              }
            />
          ) : null}
        </Panel>

        <Panel
          title="Creative revenue mix"
          subtitle="Attributed revenue and spend at the native creative level"
          loading={dashboard.creatives.loading}
          error={dashboard.creatives.error}
          empty={!filteredCreativeRows.length}
          className="panel-wide"
        >
          <CreativeMixChart rows={filteredCreativeRows.slice(0, 8)} />
        </Panel>

        <Panel
          title="Creative analysis"
          subtitle="Sort, search, and export creative performance in the current drill-down context"
          loading={dashboard.creatives.loading}
          error={dashboard.creatives.error}
          empty={!filteredCreativeRows.length}
          className="panel-wide"
        >
          <CreativeTable
            rows={filteredCreativeRows}
            search={creativeSearch}
            sort={creativeSort}
            totals={creativeTotals}
            onSearchChange={setCreativeSearch}
            onSortChange={(key) =>
              setCreativeSort((current) => ({
                key,
                direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
              }))
            }
            onExport={() => downloadCreativeCsv(filteredCreativeRows, filters)}
            revenueReference={headline?.revenue ?? 0}
          />
        </Panel>
      </section>
    </main>
  );
}

function MetricCard(props: {
  label: string;
  value: string;
  detail: string;
  loading: boolean;
  error: string | null;
}) {
  return (
    <article className="metric-card">
      <span>{props.label}</span>
      <strong>{props.loading ? 'Loading...' : props.error ? 'Unavailable' : props.value}</strong>
      <small>{props.error ?? props.detail}</small>
    </article>
  );
}

function Panel(props: {
  title: string;
  subtitle: string;
  loading: boolean;
  error: string | null;
  empty: boolean;
  children: ReactNode;
  className?: string;
}) {
  return (
    <article className={props.className ? `panel ${props.className}` : 'panel'}>
      <div className="panel-header">
        <div>
          <h2>{props.title}</h2>
          <p>{props.subtitle}</p>
        </div>
      </div>
      {props.loading ? <div className="panel-state">Loading data…</div> : null}
      {!props.loading && props.error ? <div className="panel-state panel-state-error">{props.error}</div> : null}
      {!props.loading && !props.error && props.empty ? (
        <div className="panel-state">No data for the current global filters.</div>
      ) : null}
      {!props.loading && !props.error && !props.empty ? props.children : null}
    </article>
  );
}

function DualLineChart(props: { points: TimeseriesPoint[]; revenueLabel: string; spendLabel: string }) {
  const width = 760;
  const height = 280;
  const padding = 24;
  const values = props.points.flatMap((point) => [point.revenue, point.spend]);
  const maxValue = Math.max(...values, 1);
  const stepX = props.points.length > 1 ? (width - padding * 2) / (props.points.length - 1) : 0;

  const toY = (value: number) => height - padding - (value / maxValue) * (height - padding * 2);
  const pathFor = (selector: (point: TimeseriesPoint) => number) =>
    props.points
      .map((point, index) => `${index === 0 ? 'M' : 'L'} ${padding + index * stepX} ${toY(selector(point))}`)
      .join(' ');

  return (
    <div className="chart-wrap">
      <div className="chart-legend">
        <span>
          <i className="legend-swatch legend-swatch-revenue" />
          Revenue {props.revenueLabel}
        </span>
        <span>
          <i className="legend-swatch legend-swatch-spend" />
          Spend {props.spendLabel}
        </span>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" role="img" aria-label="Revenue and spend line chart">
        {[0, 0.25, 0.5, 0.75, 1].map((tick) => {
          const y = padding + (height - padding * 2) * tick;
          return <line key={tick} x1={padding} y1={y} x2={width - padding} y2={y} className="chart-grid-line" />;
        })}
        <path d={pathFor((point) => point.revenue)} className="chart-line chart-line-revenue" />
        <path d={pathFor((point) => point.spend)} className="chart-line chart-line-spend" />
        {props.points.map((point, index) => {
          const x = padding + index * stepX;
          return (
            <g key={point.date}>
              <circle cx={x} cy={toY(point.revenue)} r="4" className="chart-dot chart-dot-revenue" />
              <circle cx={x} cy={toY(point.spend)} r="4" className="chart-dot chart-dot-spend" />
            </g>
          );
        })}
      </svg>
      <div className="chart-axis">
        {props.points.map((point) => (
          <span key={point.date}>{formatDateLabel(point.date)}</span>
        ))}
      </div>
    </div>
  );
}

function EfficiencyBars(props: { points: TimeseriesPoint[] }) {
  const maxRoas = Math.max(...props.points.map((point) => point.roas ?? 0), 1);

  return (
    <div className="efficiency-bars">
      {props.points.map((point) => {
        const roasHeight = `${((point.roas ?? 0) / maxRoas) * 100}%`;
        const conversionHeight = `${Math.max(point.conversionRate * 100 * 6, 6)}%`;

        return (
          <div className="efficiency-day" key={point.date}>
            <div className="efficiency-stack">
              <div className="efficiency-bar efficiency-bar-roas" style={{ height: roasHeight }} />
              <div className="efficiency-bar efficiency-bar-conversion" style={{ height: conversionHeight }} />
            </div>
            <div className="efficiency-meta">
              <strong>{formatNumber(point.roas)}</strong>
              <span>{formatPercent(point.conversionRate)}</span>
              <small>{formatDateLabel(point.date)}</small>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function ChannelTable(props: {
  rows: ChannelRow[];
  selectedChannel: string | null;
  onSelect: (row: ChannelRow) => void;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Channel</th>
            <th>Revenue</th>
            <th>Spend</th>
            <th>ROAS</th>
            <th>CAC</th>
            <th>CVR</th>
            <th>Share</th>
          </tr>
        </thead>
        <tbody>
          {props.rows.map((row) => {
            const channelName = `${row.source} / ${row.medium}`;
            const cac = row.orders > 0 ? row.spend / row.orders : null;

            return (
              <tr key={channelName} className={props.selectedChannel === channelName ? 'is-selected' : undefined}>
                <td>
                  <button type="button" className="table-select" onClick={() => props.onSelect(row)}>
                    <div className="channel-cell">
                      <strong>{channelName}</strong>
                      <span>{formatNumber(row.visits)} visits</span>
                    </div>
                  </button>
                </td>
                <td>{formatCurrency(row.revenue)}</td>
                <td>{formatCurrency(row.spend)}</td>
                <td>{formatNumber(row.roas)}</td>
                <td>{formatCurrency(cac)}</td>
                <td>{formatPercent(row.conversionRate)}</td>
                <td>{formatPercent(row.shareOfRevenue)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function CampaignTable(props: {
  rows: CampaignRow[];
  selectedCampaign: string | null;
  onSelect: (row: CampaignRow) => void;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Campaign</th>
            <th>Revenue</th>
            <th>Spend</th>
            <th>ROAS</th>
            <th>Orders</th>
            <th>CVR</th>
          </tr>
        </thead>
        <tbody>
          {props.rows.map((row) => (
            <tr key={`${row.source}-${row.medium}-${row.campaign}`} className={props.selectedCampaign === row.campaign ? 'is-selected' : undefined}>
              <td>
                <button type="button" className="table-select" onClick={() => props.onSelect(row)}>
                  <div className="channel-cell">
                    <strong>{row.campaign}</strong>
                    <span>
                      {row.source} / {row.medium}
                    </span>
                  </div>
                </button>
              </td>
              <td>{formatCurrency(row.revenue)}</td>
              <td>{formatCurrency(row.spend)}</td>
              <td>{formatNumber(row.roas)}</td>
              <td>{formatNumber(row.orders)}</td>
              <td>{formatPercent(row.conversionRate)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CreativeMixChart(props: { rows: CreativeRow[] }) {
  const maxValue = Math.max(...props.rows.flatMap((row) => [row.revenue, row.spend]), 1);

  return (
    <div className="creative-mix">
      {props.rows.map((row) => (
        <div className="creative-mix-row" key={`${row.campaign}-${row.creativeId ?? row.creativeName}`}>
          <div className="creative-mix-copy">
            <strong>{row.creativeName}</strong>
            <span>
              {row.creativeId ?? 'No creative ID'} · {row.campaign}
            </span>
          </div>
          <div className="creative-mix-bars">
            <div className="creative-mix-bar-track">
              <div className="creative-mix-bar creative-mix-bar-revenue" style={{ width: `${(row.revenue / maxValue) * 100}%` }} />
            </div>
            <div className="creative-mix-bar-track">
              <div className="creative-mix-bar creative-mix-bar-spend" style={{ width: `${(row.spend / maxValue) * 100}%` }} />
            </div>
          </div>
          <div className="creative-mix-metrics">
            <strong>{formatCurrency(row.revenue)}</strong>
            <span>{formatCurrency(row.spend)} spend</span>
            <small>{formatNumber(row.roas)} ROAS</small>
          </div>
        </div>
      ))}
    </div>
  );
}

function CreativeTable(props: {
  rows: CreativeRow[];
  search: string;
  sort: CreativeSortState;
  totals: { revenue: number; spend: number; orders: number; visits: number };
  revenueReference: number;
  onSearchChange: (value: string) => void;
  onSortChange: (key: CreativeSortKey) => void;
  onExport: () => void;
}) {
  return (
    <div className="creative-table-wrap">
      <div className="table-toolbar">
        <label className="toolbar-search">
          <span>Filter creatives</span>
          <input
            type="search"
            value={props.search}
            onChange={(event) => props.onSearchChange(event.target.value)}
            placeholder="Search creative name, ID, ad, or campaign"
          />
        </label>
        <div className="toolbar-meta">
          <span>
            Displaying {formatNumber(props.rows.length)} creatives · {formatCurrency(props.totals.revenue)} revenue
          </span>
          <span>
            {formatPercent(props.revenueReference > 0 ? props.totals.revenue / props.revenueReference : 0)} of current
            attributed revenue
          </span>
          <button type="button" className="export-button" onClick={props.onExport}>
            Export CSV
          </button>
        </div>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <SortableHeader label="Creative" sortKey="creativeName" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="Creative ID" sortKey="creativeId" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="Campaign" sortKey="campaign" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="Revenue" sortKey="revenue" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="Spend" sortKey="spend" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="ROAS" sortKey="roas" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="Orders" sortKey="orders" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="CVR" sortKey="conversionRate" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="CTR" sortKey="clickThroughRate" state={props.sort} onClick={props.onSortChange} />
              <SortableHeader label="CPC" sortKey="costPerClick" state={props.sort} onClick={props.onSortChange} />
            </tr>
          </thead>
          <tbody>
            {props.rows.map((row) => (
              <tr key={`${row.campaign}-${row.creativeId ?? row.creativeName}`}>
                <td>
                  <div className="creative-cell">
                    <strong>{row.creativeName}</strong>
                    <span>{row.adName ?? row.content}</span>
                  </div>
                </td>
                <td>{row.creativeId ?? 'N/A'}</td>
                <td>
                  <div className="creative-cell">
                    <strong>{row.campaignName ?? row.campaign}</strong>
                    <span>{row.campaignId ?? `${row.source} / ${row.medium}`}</span>
                  </div>
                </td>
                <td>{formatCurrency(row.revenue)}</td>
                <td>{formatCurrency(row.spend)}</td>
                <td>{formatNumber(row.roas)}</td>
                <td>{formatNumber(row.orders)}</td>
                <td>{formatPercent(row.conversionRate)}</td>
                <td>{formatPercent(row.clickThroughRate)}</td>
                <td>{formatCurrency(row.costPerClick)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SortableHeader(props: {
  label: string;
  sortKey: CreativeSortKey;
  state: CreativeSortState;
  onClick: (key: CreativeSortKey) => void;
}) {
  const isActive = props.state.key === props.sortKey;

  return (
    <th>
      <button type="button" className={isActive ? 'sort-button is-active' : 'sort-button'} onClick={() => props.onClick(props.sortKey)}>
        {props.label}
        <span>{isActive ? (props.state.direction === 'desc' ? '↓' : '↑') : '↕'}</span>
      </button>
    </th>
  );
}

export default App;
