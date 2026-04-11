import { startTransition, useEffect, useMemo, useState, type ReactNode } from 'react';

import {
  fetchCampaigns,
  fetchChannels,
  fetchModels,
  fetchOverview,
  fetchTimeseries,
  type AttributionModel,
  type CampaignRow,
  type ChannelRow,
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
};

type ModelState = {
  defaultModel: AttributionModel;
  supportedModels: AttributionModel[];
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
    campaigns: createLoadingSection()
  });

  useEffect(() => {
    let cancelled = false;

    setState({
      overview: createLoadingSection(),
      timeseries: createLoadingSection(),
      channels: createLoadingSection(),
      campaigns: createLoadingSection()
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

    fetchChannels(filters)
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

    fetchCampaigns(filters)
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

    return () => {
      cancelled = true;
    };
  }, [filters]);

  return state;
}

function App() {
  const [modelState, setModelState] = useState<ModelState | null>(null);
  const [modelError, setModelError] = useState<string | null>(null);
  const [isModelLoading, setIsModelLoading] = useState(true);
  const [filters, setFilters] = useState<Filters>({
    ...buildRange(30),
    attributionModel: 'last_touch'
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

  const headline = dashboard.overview.data;
  const hasAnyData = Boolean(
    (dashboard.timeseries.data && dashboard.timeseries.data.length > 0) ||
      (dashboard.channels.data && dashboard.channels.data.length > 0) ||
      (headline && headline.revenue > 0)
  );

  const loadingCount = [
    dashboard.overview.loading,
    dashboard.timeseries.loading,
    dashboard.channels.loading,
    dashboard.campaigns.loading
  ].filter(Boolean).length;

  const readySections = [
    dashboard.overview.data,
    dashboard.timeseries.data,
    dashboard.channels.data,
    dashboard.campaigns.data
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

  return (
    <main className="app-shell">
      <section className="hero">
        <div>
          <p className="eyebrow">Unified attribution dashboard</p>
          <h1>ROAS Radar</h1>
          <p className="hero-copy">
            Compare channels, spend, and revenue under one attribution model with global filters that keep every
            visualization aligned.
          </p>
        </div>
        <div className="hero-status-card">
          <span>Sections ready</span>
          <strong>
            {readySections}/4
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

      {!hasAnyData &&
      !dashboard.overview.loading &&
      !dashboard.timeseries.loading &&
      !dashboard.channels.loading &&
      !dashboard.campaigns.loading ? (
        <section className="empty-state">
          <h2>No attributed activity for this filter range</h2>
          <p>Try a wider date window or switch attribution models to compare how credit changes across channels.</p>
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
          subtitle="Source and medium performance under the selected model"
          loading={dashboard.channels.loading}
          error={dashboard.channels.error}
          empty={!dashboard.channels.data?.length}
        >
          {dashboard.channels.data ? <ChannelTable rows={dashboard.channels.data} /> : null}
        </Panel>

        <Panel
          title="Campaign leaders"
          subtitle="Top campaigns ranked by attributed revenue"
          loading={dashboard.campaigns.loading}
          error={dashboard.campaigns.error}
          empty={!dashboard.campaigns.data?.length}
        >
          {dashboard.campaigns.data ? <CampaignGrid rows={dashboard.campaigns.data} /> : null}
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
}) {
  return (
    <article className="panel">
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

function ChannelTable(props: { rows: ChannelRow[] }) {
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
              <tr key={channelName}>
                <td>
                  <div className="channel-cell">
                    <strong>{channelName}</strong>
                    <span>{formatNumber(row.visits)} visits</span>
                  </div>
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

function CampaignGrid(props: { rows: CampaignRow[] }) {
  return (
    <div className="campaign-grid">
      {props.rows.map((row) => (
        <article className="campaign-card" key={`${row.source}-${row.campaign}-${row.content}`}>
          <div className="campaign-heading">
            <span>{row.source}</span>
            <strong>{row.campaign}</strong>
            <small>{row.content}</small>
          </div>
          <div className="campaign-metric-row">
            <div>
              <span>Revenue</span>
              <strong>{formatCurrency(row.revenue)}</strong>
            </div>
            <div>
              <span>Spend</span>
              <strong>{formatCurrency(row.spend)}</strong>
            </div>
            <div>
              <span>ROAS</span>
              <strong>{formatNumber(row.roas)}</strong>
            </div>
            <div>
              <span>Conversion</span>
              <strong>{formatPercent(row.conversionRate)}</strong>
            </div>
          </div>
        </article>
      ))}
    </div>
  );
}

export default App;
