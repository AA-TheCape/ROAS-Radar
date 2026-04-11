import { startTransition, useDeferredValue, useEffect, useMemo, useState } from 'react';

import {
  fetchCampaigns,
  fetchOrders,
  fetchSummary,
  fetchTimeseries,
  type CampaignRow,
  type OrderRow,
  type ReportingFilters,
  type SummaryTotals,
  type TimeseriesGroupBy,
  type TimeseriesPoint
} from './lib/api';
import {
  formatCurrency,
  formatDateLabel,
  formatDateTimeLabel,
  formatNumber,
  formatPercent
} from './lib/format';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type DashboardState = {
  summary: AsyncSection<SummaryTotals>;
  campaigns: AsyncSection<CampaignRow[]>;
  timeseries: AsyncSection<TimeseriesPoint[]>;
  orders: AsyncSection<OrderRow[]>;
};

const PRESETS = [
  { label: 'Last 7D', days: 7 },
  { label: 'Last 30D', days: 30 },
  { label: 'Last 90D', days: 90 }
] as const;

const GROUP_BY_OPTIONS: Array<{ value: TimeseriesGroupBy; label: string }> = [
  { value: 'day', label: 'Daily' },
  { value: 'source', label: 'By source' },
  { value: 'campaign', label: 'By campaign' }
];

function formatDateInput(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function buildRange(days: number): Pick<ReportingFilters, 'startDate' | 'endDate'> {
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

function buildSeriesPath(points: TimeseriesPoint[]): string {
  if (points.length === 0) {
    return '';
  }

  const maxRevenue = Math.max(...points.map((point) => point.revenue), 1);

  return points
    .map((point, index) => {
      const x = points.length === 1 ? 320 : (index / (points.length - 1)) * 320;
      const y = 144 - (point.revenue / maxRevenue) * 120;
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');
}

function useDashboardData(filters: ReportingFilters, groupBy: TimeseriesGroupBy) {
  const [state, setState] = useState<DashboardState>({
    summary: createLoadingSection(),
    campaigns: createLoadingSection(),
    timeseries: createLoadingSection(),
    orders: createLoadingSection()
  });

  useEffect(() => {
    let cancelled = false;

    setState({
      summary: createLoadingSection(),
      campaigns: createLoadingSection(),
      timeseries: createLoadingSection(),
      orders: createLoadingSection()
    });

    fetchSummary(filters)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            summary: createResolvedSection(response.totals)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            summary: createErroredSection(error.message)
          }));
        }
      });

    fetchCampaigns(filters, 12)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            campaigns: createResolvedSection(response.rows)
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

    fetchTimeseries(filters, groupBy)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            timeseries: createResolvedSection(response.points)
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

    fetchOrders(filters, 10)
      .then((response) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            orders: createResolvedSection(response.rows)
          }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setState((current) => ({
            ...current,
            orders: createErroredSection(error.message)
          }));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [filters, groupBy]);

  return state;
}

function SectionState({
  loading,
  error,
  empty,
  emptyLabel,
  children
}: {
  loading: boolean;
  error: string | null;
  empty: boolean;
  emptyLabel: string;
  children: JSX.Element;
}) {
  if (loading) {
    return <div className="panel-state">Loading data…</div>;
  }

  if (error) {
    return <div className="panel-state panel-state-error">{error}</div>;
  }

  if (empty) {
    return <div className="panel-state">{emptyLabel}</div>;
  }

  return children;
}

function SummaryCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <article className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

function TimeseriesChart({ points, groupBy }: { points: TimeseriesPoint[]; groupBy: TimeseriesGroupBy }) {
  const path = buildSeriesPath(points);
  const maxRevenue = Math.max(...points.map((point) => point.revenue), 1);

  return (
    <div className="chart-card">
      <div className="chart-svg-shell">
        <svg viewBox="0 0 320 160" className="chart-svg" aria-label="Revenue timeseries">
          <defs>
            <linearGradient id="revenueFill" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(186, 87, 32, 0.35)" />
              <stop offset="100%" stopColor="rgba(186, 87, 32, 0.02)" />
            </linearGradient>
          </defs>
          <path d="M 0 144 H 320" className="chart-axis" />
          <path d={path} className="chart-line" />
          {path ? <path d={`${path} L 320 144 L 0 144 Z`} className="chart-area" /> : null}
          {points.map((point, index) => {
            const x = points.length === 1 ? 320 : (index / (points.length - 1)) * 320;
            const y = 144 - (point.revenue / maxRevenue) * 120;

            return <circle key={`${point.date}-${index}`} cx={x} cy={y} r="4" className="chart-dot" />;
          })}
        </svg>
      </div>
      <div className="chart-labels">
        {points.map((point) => (
          <div key={point.date} className="chart-label">
            <strong>{groupBy === 'day' ? formatDateLabel(point.date) : point.date}</strong>
            <span>{formatCurrency(point.revenue)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function App() {
  const [filters, setFilters] = useState<ReportingFilters>(() => ({
    ...buildRange(30),
    source: '',
    campaign: ''
  }));
  const [groupBy, setGroupBy] = useState<TimeseriesGroupBy>('day');

  const deferredSource = useDeferredValue(filters.source);
  const deferredCampaign = useDeferredValue(filters.campaign);

  const appliedFilters = useMemo<ReportingFilters>(
    () => ({
      startDate: filters.startDate,
      endDate: filters.endDate,
      source: (deferredSource ?? '').trim(),
      campaign: (deferredCampaign ?? '').trim()
    }),
    [deferredCampaign, deferredSource, filters.endDate, filters.startDate]
  );

  const dashboard = useDashboardData(appliedFilters, groupBy);

  const summaryCards = useMemo(() => {
    const totals = dashboard.summary.data;

    return [
      {
        label: 'Visits',
        value: formatNumber(totals?.visits),
        detail: `${formatDateLabel(filters.startDate)} to ${formatDateLabel(filters.endDate)}`
      },
      {
        label: 'Orders',
        value: formatNumber(totals?.orders),
        detail: `${formatPercent(totals?.conversionRate)} conversion`
      },
      {
        label: 'Revenue',
        value: formatCurrency(totals?.revenue),
        detail: totals?.roas == null ? 'ROAS pending spend data' : `${formatNumber(totals.roas)} ROAS`
      },
      {
        label: 'AOV',
        value:
          totals && totals.orders > 0
            ? formatCurrency(totals.revenue / totals.orders)
            : formatCurrency(null),
        detail: `${formatNumber(totals?.orders)} attributed orders`
      }
    ];
  }, [dashboard.summary.data, filters.endDate, filters.startDate]);

  const totalCampaignRevenue = useMemo(
    () => (dashboard.campaigns.data ?? []).reduce((sum, row) => sum + row.revenue, 0),
    [dashboard.campaigns.data]
  );

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-copy-block">
          <p className="eyebrow">MVP reporting dashboard</p>
          <h1>ROAS Radar</h1>
          <p className="hero-copy">
            Monitor paid acquisition performance for a single Shopify store across headline metrics, campaign rows,
            time-based trends, and order-level attribution evidence.
          </p>
        </div>
        <div className="hero-status-card">
          <span>Active window</span>
          <strong>{filters.endDate}</strong>
          <small>
            {(filters.source ?? '').trim() || (filters.campaign ?? '').trim()
              ? `Filtered by ${[(filters.source ?? '').trim(), (filters.campaign ?? '').trim()].filter(Boolean).join(' / ')}`
              : 'All attributed traffic'}
          </small>
        </div>
      </section>

      <section className="control-bar">
        <div className="control-group">
          <label htmlFor="start-date">Start date</label>
          <input
            id="start-date"
            type="date"
            value={filters.startDate}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, startDate: event.target.value }));
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
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, endDate: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group">
          <label htmlFor="source-filter">Source</label>
          <input
            id="source-filter"
            type="text"
            placeholder="google, meta, facebook"
            value={filters.source}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, source: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group">
          <label htmlFor="campaign-filter">Campaign</label>
          <input
            id="campaign-filter"
            type="text"
            placeholder="spring-sale"
            value={filters.campaign}
            onChange={(event) =>
              startTransition(() => {
                setFilters((current) => ({ ...current, campaign: event.target.value }));
              })
            }
          />
        </div>
        <div className="control-group control-group-wide">
          <label htmlFor="group-by">Timeseries grouping</label>
          <select
            id="group-by"
            value={groupBy}
            onChange={(event) => {
              setGroupBy(event.target.value as TimeseriesGroupBy);
            }}
          >
            {GROUP_BY_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
        <div className="control-group control-group-wide">
          <label>Quick ranges</label>
          <div className="preset-row">
            {PRESETS.map((preset) => (
              <button
                key={preset.days}
                type="button"
                className="preset-chip"
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
            <button
              type="button"
              className="preset-chip preset-chip-secondary"
              onClick={() =>
                startTransition(() => {
                  setFilters((current) => ({
                    ...current,
                    source: '',
                    campaign: ''
                  }));
                })
              }
            >
              Clear filters
            </button>
          </div>
        </div>
      </section>

      <section className="summary-grid">
        {summaryCards.map((card) => (
          <SummaryCard key={card.label} label={card.label} value={card.value} detail={card.detail} />
        ))}
      </section>

      <section className="dashboard-grid">
        <article className="panel panel-wide">
          <div className="panel-header">
            <h2>Revenue trend</h2>
            <p>Uses the reporting timeseries contract and switches grouping without changing the rest of the dashboard.</p>
          </div>
          <SectionState
            loading={dashboard.timeseries.loading}
            error={dashboard.timeseries.error}
            empty={!dashboard.timeseries.data?.length}
            emptyLabel="No timeseries data returned for this filter range."
          >
            <TimeseriesChart points={dashboard.timeseries.data ?? []} groupBy={groupBy} />
          </SectionState>
        </article>

        <article className="panel">
          <div className="panel-header">
            <h2>Campaign performance</h2>
            <p>Top campaign rows ordered by revenue, matching the API’s table response.</p>
          </div>
          <SectionState
            loading={dashboard.campaigns.loading}
            error={dashboard.campaigns.error}
            empty={!dashboard.campaigns.data?.length}
            emptyLabel="No campaign rows matched the current filters."
          >
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Campaign</th>
                    <th>Source</th>
                    <th>Visits</th>
                    <th>Orders</th>
                    <th>Revenue</th>
                    <th>CVR</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard.campaigns.data ?? []).map((row) => (
                    <tr key={`${row.source}-${row.medium}-${row.campaign}-${row.content ?? 'none'}`}>
                      <td>
                        <div className="primary-cell">
                          <strong>{row.campaign}</strong>
                          <span>{row.content ?? 'No content tag'}</span>
                        </div>
                      </td>
                      <td>{`${row.source} / ${row.medium}`}</td>
                      <td>{formatNumber(row.visits)}</td>
                      <td>{formatNumber(row.orders)}</td>
                      <td>{formatCurrency(row.revenue)}</td>
                      <td>{formatPercent(row.conversionRate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </SectionState>
        </article>

        <article className="panel">
          <div className="panel-header">
            <h2>Campaign mix</h2>
            <p>Revenue share makes it obvious which campaigns dominate the selected window.</p>
          </div>
          <SectionState
            loading={dashboard.campaigns.loading}
            error={dashboard.campaigns.error}
            empty={!dashboard.campaigns.data?.length}
            emptyLabel="No campaign mix available yet."
          >
            <div className="stack-list">
              {(dashboard.campaigns.data ?? []).map((row) => {
                const share = totalCampaignRevenue > 0 ? row.revenue / totalCampaignRevenue : 0;

                return (
                  <div key={`${row.source}-${row.campaign}`} className="stack-row">
                    <div className="stack-copy">
                      <strong>{row.campaign}</strong>
                      <span>{`${row.source} / ${row.medium}`}</span>
                    </div>
                    <div className="stack-bar-shell">
                      <div className="stack-bar" style={{ width: `${Math.max(share * 100, 2)}%` }} />
                    </div>
                    <div className="stack-value">{formatPercent(share)}</div>
                  </div>
                );
              })}
            </div>
          </SectionState>
        </article>

        <article className="panel panel-wide">
          <div className="panel-header">
            <h2>Attributed orders</h2>
            <p>Order-level attribution rows help debug why a sale was assigned to a source and campaign.</p>
          </div>
          <SectionState
            loading={dashboard.orders.loading}
            error={dashboard.orders.error}
            empty={!dashboard.orders.data?.length}
            emptyLabel="No attributed orders were returned for this range."
          >
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Order</th>
                    <th>Processed</th>
                    <th>Source</th>
                    <th>Campaign</th>
                    <th>Total</th>
                    <th>Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {(dashboard.orders.data ?? []).map((row) => (
                    <tr key={row.shopifyOrderId}>
                      <td>
                        <div className="primary-cell">
                          <strong>#{row.shopifyOrderId}</strong>
                          <span>{row.medium ?? 'No medium'}</span>
                        </div>
                      </td>
                      <td>{formatDateTimeLabel(row.processedAt)}</td>
                      <td>{row.source ?? 'Unattributed'}</td>
                      <td>{row.campaign ?? 'No campaign'}</td>
                      <td>{formatCurrency(row.totalPrice)}</td>
                      <td>
                        <span className="reason-pill">{row.attributionReason}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </SectionState>
        </article>
      </section>
    </main>
  );
}

export default App;
