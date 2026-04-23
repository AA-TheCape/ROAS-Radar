import React, { Suspense, lazy, memo, useEffect, useMemo, useRef, useState } from 'react';

import {
  formatCompactCurrency,
  formatCurrency,
  formatDateLabel,
  formatDateTimeLabel,
  formatNumber,
  formatPercent
} from '../lib/format';
import type {
  CampaignRow,
  OrderRow,
  ReportingFilters,
  SpendDetailChannelGroup,
  SummaryTotals,
  TimeseriesGroupBy,
  TimeseriesPoint
} from '../lib/api';
import {
  Badge,
  Button,
  Card,
  DataTableToolbar,
  CardDescription,
  CardHeader,
  CardTitle,
  Eyebrow,
  Field,
  Input,
  MetricCopy,
  MetricValue,
  Panel,
  PrimaryCell,
  SectionState,
  SortableTableHeaderCell,
  Select,
  Table,
  TableBody,
  TableCell,
  TableEmptyRow,
  TableHead,
  TableHeaderCell,
  TableMeta,
  TablePagination,
  TableSearchField,
  TableFilterBar,
  TableRow,
  TableWrap
} from './AuthenticatedUi';
import { matchesQuery, paginateRows, sortRows, type SortState } from '../lib/dataTable';

const NivoAreaChart = lazy(async () => {
  const module = await import('./charts');
  return { default: module.NivoAreaChart };
});

const NivoBarChart = lazy(async () => {
  const module = await import('./charts');
  return { default: module.NivoBarChart };
});

const NivoPieChart = lazy(async () => {
  const module = await import('./charts');
  return { default: module.NivoPieChart };
});

type DashboardSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type SummaryCardData = {
  label: string;
  value: string;
  detail: string;
};

type ReportingDashboardProps = {
  filters: ReportingFilters;
  onFiltersChange: (next: ReportingFilters) => void;
  groupBy: TimeseriesGroupBy;
  onGroupByChange: (value: TimeseriesGroupBy) => void;
  reportingTimezone: string;
  quickRanges: ReadonlyArray<{
    label: string;
    value: (reportingTimezone: string) => Pick<ReportingFilters, 'startDate' | 'endDate'>;
  }>;
  onApplyQuickRange: (range: Pick<ReportingFilters, 'startDate' | 'endDate'>) => void;
  onClearFilters: () => void;
  summaryCards: SummaryCardData[];
  summarySection: DashboardSection<SummaryTotals>;
  campaignsSection: DashboardSection<CampaignRow[]>;
  timeseriesSection: DashboardSection<TimeseriesPoint[]>;
  ordersSection: DashboardSection<OrderRow[]>;
  spendDetailsSection: DashboardSection<SpendDetailChannelGroup[]>;
  onOpenOrderDetails: (shopifyOrderId: string) => void;
};

const GROUP_BY_OPTIONS: Array<{ value: TimeseriesGroupBy; label: string }> = [
  { value: 'day', label: 'Daily' },
  { value: 'source', label: 'By source' },
  { value: 'campaign', label: 'By campaign' }
];

type CampaignSortKey = 'campaign' | 'source' | 'visits' | 'orders' | 'revenue' | 'conversionRate';
type OrderSortKey = 'order' | 'processedAt' | 'source' | 'campaign' | 'totalPrice';
export type DateField = 'startDate' | 'endDate';

const CAMPAIGN_PAGE_SIZE = 6;
const ORDER_PAGE_SIZE = 8;

const SUMMARY_CARD_DECOR: Record<
  string,
  {
    accent: string;
    pillTone: 'brand' | 'teal' | 'warning' | 'success';
    pillLabel: string;
  }
> = {
  Visits: {
    accent: 'from-brand/90 via-brand/70 to-warning/70',
    pillTone: 'brand',
    pillLabel: 'Traffic'
  },
  Orders: {
    accent: 'from-teal/95 via-teal/80 to-brand/70',
    pillTone: 'teal',
    pillLabel: 'Conversion'
  },
  Revenue: {
    accent: 'from-warning/90 via-brand/80 to-brand/60',
    pillTone: 'warning',
    pillLabel: 'Revenue'
  },
  Spend: {
    accent: 'from-brand/85 via-teal/75 to-success/70',
    pillTone: 'teal',
    pillLabel: 'Media'
  },
  AOV: {
    accent: 'from-success/90 via-teal/80 to-brand/65',
    pillTone: 'success',
    pillLabel: 'Efficiency'
  }
};

const chartSuspenseFallback = (
  <div className="min-h-[280px] animate-pulse rounded-card border border-line/60 bg-surface-alt/70" aria-hidden="true" />
);

function normalizeDateRange(filters: ReportingFilters): ReportingFilters {
  if (filters.startDate && filters.endDate && filters.startDate > filters.endDate) {
    return {
      ...filters,
      endDate: filters.startDate
    };
  }

  return filters;
}

export function applyDateRangeChange(filters: ReportingFilters, field: DateField, value: string) {
  const nextFilters = normalizeDateRange({
    ...filters,
    [field]: value
  });

  if (field === 'startDate' && value && filters.endDate && value > filters.endDate) {
    return {
      nextFilters,
      feedback: 'End date was adjusted to match the new start date so the range stays valid.'
    };
  }

  if (field === 'endDate' && value && filters.startDate && value < filters.startDate) {
    return {
      nextFilters,
      feedback: 'End date cannot be earlier than the start date. It was moved forward to keep the range valid.'
    };
  }

  return {
    nextFilters,
    feedback: null
  };
}

const SummaryCard = memo(function SummaryCard({ label, value, detail }: SummaryCardData) {
  const decor = SUMMARY_CARD_DECOR[label] ?? SUMMARY_CARD_DECOR.Visits;

  return (
    <Card
      padding="compact"
      className="min-h-[13rem] border-line/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(251,247,242,0.82))] shadow-panel"
    >
      <div className={`absolute inset-x-0 top-0 h-1.5 bg-gradient-to-r ${decor.accent}`} />
      <div className="relative grid h-full gap-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <Eyebrow>{label}</Eyebrow>
            <MetricValue className="mt-5">{value}</MetricValue>
          </div>
          <Badge tone={decor.pillTone}>{decor.pillLabel}</Badge>
        </div>
        <MetricCopy className="mt-0 max-w-[24ch]">{detail}</MetricCopy>
      </div>
    </Card>
  );
});

function buildTrendSummary(points: TimeseriesPoint[], groupBy: TimeseriesGroupBy, reportingTimezone: string) {
  if (points.length === 0) {
    return 'No revenue buckets are available for the current reporting window.';
  }

  const strongestPoint = points.reduce<TimeseriesPoint>((current, point) => (point.revenue > current.revenue ? point : current), points[0]);
  const strongestLabel = groupBy === 'day' ? formatDateLabel(strongestPoint.date, reportingTimezone) : strongestPoint.date;

  return `${formatNumber(points.length)} buckets plotted. Strongest bucket ${strongestLabel} with ${formatCurrency(strongestPoint.revenue)} from ${formatNumber(strongestPoint.orders)} orders.`;
}

function buildCampaignMixSummary(data: Array<{ campaign: string; revenueShare: number; revenue: number }>) {
  if (data.length === 0) {
    return 'No campaign revenue share is available.';
  }

  return data
    .slice(0, 5)
    .map((row) => `${row.campaign}: ${row.revenueShare.toFixed(1)} percent share from ${formatCurrency(row.revenue)}.`)
    .join(' ');
}

function buildSourceMixSummary(
  data: Array<{ id: string; value: number; revenueLabel?: string }>
) {
  if (data.length === 0) {
    return 'No source contribution data is available.';
  }

  return data
    .slice(0, 5)
    .map((row) => `${row.id}: ${row.revenueLabel ?? formatCurrency(row.value)}.`)
    .join(' ');
}

function titleCaseToken(value: string) {
  return value
    .split('_')
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
}

function formatChannelLabel(source: string, medium: string) {
  return `${titleCaseToken(source || 'unknown')} / ${titleCaseToken(medium || 'unknown')}`;
}

const DashboardOverview = memo(function DashboardOverview({
  filters,
  groupBy,
  reportingTimezone,
  summary,
  campaigns,
  points,
  orderCount
}: {
  filters: ReportingFilters;
  groupBy: TimeseriesGroupBy;
  reportingTimezone: string;
  summary: SummaryTotals | null;
  campaigns: CampaignRow[];
  points: TimeseriesPoint[];
  orderCount: number;
}) {
  const totalRevenue = summary?.revenue ?? 0;
  const peakPoint = useMemo(
    () => points.reduce<TimeseriesPoint | null>((current, point) => (!current || point.revenue > current.revenue ? point : current), null),
    [points]
  );
  const averageRevenue = points.length > 0 ? totalRevenue / points.length : 0;
  const leadingCampaign = useMemo(
    () => campaigns.reduce<CampaignRow | null>((current, row) => (!current || row.revenue > current.revenue ? row : current), null),
    [campaigns]
  );
  const rangeLabel = useMemo(
    () => `${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`,
    [filters.endDate, filters.startDate, reportingTimezone]
  );

  return (
    <Panel
      title="Overview command center"
      description="A denser dashboard shell for the current reporting window, keeping controls, KPIs, and trend context together."
      wide
      className="overflow-hidden"
    >
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(18rem,0.85fr)]">
        <Card
          tone="accent"
          padding="card"
          className="min-h-[20rem] bg-[linear-gradient(135deg,rgba(255,255,255,0.98),rgba(246,223,211,0.82)_52%,rgba(220,239,237,0.72))]"
        >
          <div className="absolute right-[-3.5rem] top-[-3rem] h-40 w-40 rounded-full bg-brand/10 blur-3xl" />
          <div className="absolute bottom-[-4rem] left-[-2rem] h-48 w-48 rounded-full bg-teal/10 blur-3xl" />
          <div className="relative grid gap-6">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <Eyebrow>Reporting window</Eyebrow>
                <CardTitle className="mt-3 max-w-[15ch] text-display">
                  Performance snapshot for {rangeLabel}
                </CardTitle>
                <CardDescription className="mt-4 max-w-2xl text-lead">
                  Grouped {GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label.toLowerCase() ?? groupBy},
                  aligned to {reportingTimezone}, and tied to the existing summary, campaign, timeseries, and order feeds.
                </CardDescription>
              </div>
              <Badge tone="brand" className="px-4 py-2">
                {formatNumber(orderCount)} attributed orders
              </Badge>
            </div>

            <div className="grid gap-4 md:grid-cols-3">
              <Card padding="compact" className="border-white/70 bg-white/80 shadow-inset-soft">
                <Eyebrow>Peak bucket</Eyebrow>
                <p className="mt-4 font-display text-title text-ink">
                  {peakPoint
                    ? groupBy === 'day'
                      ? formatDateLabel(peakPoint.date, reportingTimezone)
                      : peakPoint.date
                    : 'N/A'}
                </p>
                <p className="mt-2 text-body text-ink-soft">
                  {peakPoint ? formatCurrency(peakPoint.revenue) : 'No revenue peak available yet'}
                </p>
              </Card>

              <Card padding="compact" className="border-white/70 bg-white/80 shadow-inset-soft">
                <Eyebrow>Average revenue</Eyebrow>
                <p className="mt-4 font-display text-title text-ink">{formatCurrency(averageRevenue)}</p>
                <p className="mt-2 text-body text-ink-soft">
                  {points.length > 0 ? `${formatNumber(points.length)} reporting buckets in view` : 'Awaiting timeseries data'}
                </p>
              </Card>

              <Card padding="compact" className="border-white/70 bg-white/80 shadow-inset-soft">
                <Eyebrow>Leading campaign</Eyebrow>
                <p className="mt-4 font-display text-title text-ink">{leadingCampaign?.campaign ?? 'N/A'}</p>
                <p className="mt-2 text-body text-ink-soft">
                  {leadingCampaign
                    ? `${formatCurrency(leadingCampaign.revenue)} from ${leadingCampaign.source} / ${leadingCampaign.medium}`
                    : 'No campaign leader available yet'}
                </p>
              </Card>
            </div>
          </div>
        </Card>

        <Card padding="card" className="bg-[linear-gradient(180deg,rgba(23,33,43,0.98),rgba(49,64,81,0.96))] text-white">
          <Eyebrow className="text-white/85">Operator notes</Eyebrow>
          <div className="mt-5 grid gap-4">
            <div className="rounded-card border border-white/10 bg-white/5 px-4 py-4">
              <p className="text-caption uppercase tracking-[0.14em] text-white/80">Revenue captured</p>
              <p className="mt-3 font-display text-display text-white">{formatCompactCurrency(totalRevenue)}</p>
              <p className="mt-2 text-body text-white/88">
                {summary?.roas == null ? 'ROAS pending spend data' : `${formatNumber(summary.roas)} ROAS on attributed revenue`}
              </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
              <div className="rounded-card border border-white/10 bg-white/5 px-4 py-4">
                <p className="text-caption uppercase tracking-[0.14em] text-white/80">Conversion rate</p>
                <p className="mt-2 font-display text-title text-white">{formatPercent(summary?.conversionRate)}</p>
              </div>
              <div className="rounded-card border border-white/10 bg-white/5 px-4 py-4">
                <p className="text-caption uppercase tracking-[0.14em] text-white/80">Campaign rows</p>
                <p className="mt-2 font-display text-title text-white">{formatNumber(campaigns.length)}</p>
              </div>
            </div>
          </div>
        </Card>
      </div>
    </Panel>
  );
});

const DashboardControlPanel = memo(function DashboardControlPanel({
  filters,
  onFiltersChange,
  groupBy,
  onGroupByChange,
  reportingTimezone,
  quickRanges,
  onApplyQuickRange,
  onClearFilters
}: {
  filters: ReportingFilters;
  onFiltersChange: (next: ReportingFilters) => void;
  groupBy: TimeseriesGroupBy;
  onGroupByChange: (value: TimeseriesGroupBy) => void;
  reportingTimezone: string;
  quickRanges: ReportingDashboardProps['quickRanges'];
  onApplyQuickRange: (range: Pick<ReportingFilters, 'startDate' | 'endDate'>) => void;
  onClearFilters: () => void;
}) {
  const [dateFeedback, setDateFeedback] = useState<string | null>(null);
  const preserveDateFeedbackRef = useRef(false);
  const scopeLabel = useMemo(
    () =>
      (filters.source ?? '').trim() || (filters.campaign ?? '').trim()
        ? `Filtered by ${[(filters.source ?? '').trim(), (filters.campaign ?? '').trim()].filter(Boolean).join(' / ')}`
        : 'All attributed traffic',
    [filters.campaign, filters.source]
  );
  const formattedRange = useMemo(
    () => `${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`,
    [filters.endDate, filters.startDate, reportingTimezone]
  );

  useEffect(() => {
    if (preserveDateFeedbackRef.current) {
      preserveDateFeedbackRef.current = false;
      return;
    }

    setDateFeedback(null);
  }, [filters.endDate, filters.startDate]);

  function handleDateChange(field: DateField, value: string) {
    const { nextFilters, feedback } = applyDateRangeChange(filters, field, value);

    if (feedback) {
      preserveDateFeedbackRef.current = true;
      setDateFeedback(feedback);
      onFiltersChange(nextFilters);
      return;
    }

    setDateFeedback(null);
    onFiltersChange(nextFilters);
  }

  return (
    <Card
      padding="compact"
      className="col-[1/-1] overflow-hidden border-line/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(237,245,242,0.78))] shadow-panel"
    >
      <div className="grid gap-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <Eyebrow>Reporting controls</Eyebrow>
            <div className="mt-2 flex flex-wrap items-center gap-2">
              <h2 className="font-display text-[1.35rem] leading-tight text-ink">Top control card</h2>
              <Badge tone="teal" className="min-h-[26px] px-2.5 py-0.5 text-[0.68rem]">
                {reportingTimezone}
              </Badge>
            </div>
            <p className="mt-2 max-w-3xl text-[0.95rem] leading-6 text-ink-muted">
              Tune dates, source filters, and chart grouping without giving the dashboard a full-height control section.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Badge tone="neutral" className="min-h-[26px] px-2.5 py-0.5 text-[0.68rem]">
              {GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label ?? groupBy}
            </Badge>
            <Button type="button" tone="ghost" className="min-h-[36px] px-3 py-1.5 text-label" onClick={onClearFilters}>
              Clear filters
            </Button>
          </div>
        </div>

        <div className="grid gap-3 xl:grid-cols-[minmax(0,1.8fr)_minmax(18rem,auto)] xl:items-end">
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            <Field label="Start date" htmlFor="start-date">
              <Input
                id="start-date"
                type="date"
                className="min-h-[40px] rounded-card px-3 py-2 text-[0.95rem]"
                value={filters.startDate}
                max={filters.endDate}
                onChange={(event) => handleDateChange('startDate', event.target.value)}
              />
            </Field>
            <Field label="End date" htmlFor="end-date">
              <Input
                id="end-date"
                type="date"
                className="min-h-[40px] rounded-card px-3 py-2 text-[0.95rem]"
                value={filters.endDate}
                min={filters.startDate}
                onChange={(event) => handleDateChange('endDate', event.target.value)}
              />
            </Field>
            <Field label="Source" htmlFor="source-filter">
              <Input
                id="source-filter"
                type="text"
                className="min-h-[40px] rounded-card px-3 py-2 text-[0.95rem]"
                placeholder="google, meta, facebook"
                value={filters.source}
                onChange={(event) => onFiltersChange({ ...filters, source: event.target.value })}
              />
            </Field>
            <Field label="Campaign" htmlFor="campaign-filter">
              <Input
                id="campaign-filter"
                type="text"
                className="min-h-[40px] rounded-card px-3 py-2 text-[0.95rem]"
                placeholder="spring-sale"
                value={filters.campaign}
                onChange={(event) => onFiltersChange({ ...filters, campaign: event.target.value })}
              />
            </Field>
            <Field label="Timeseries grouping" htmlFor="group-by">
              <Select
                id="group-by"
                className="min-h-[40px] rounded-card px-3 py-2 text-[0.95rem]"
                value={groupBy}
                onChange={(event) => onGroupByChange(event.target.value as TimeseriesGroupBy)}
              >
                {GROUP_BY_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </Select>
            </Field>
          </div>

          <div className="grid gap-3 rounded-card border border-line/60 bg-white/75 px-3 py-3 shadow-inset-soft">
            <div className="flex flex-wrap items-center gap-2">
              {quickRanges.map((preset) => (
                <Button
                  key={preset.label}
                  type="button"
                  tone="secondary"
                  className="min-h-[34px] min-w-[4.75rem] px-3 py-1 text-label"
                  onClick={() => onApplyQuickRange(preset.value(reportingTimezone))}
                >
                  {preset.label}
                </Button>
              ))}
            </div>

            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="min-w-0">
                <p className="text-label uppercase text-ink-muted">Current scope</p>
                <p className="mt-1 truncate text-[0.95rem] text-ink-soft xl:max-w-[22rem]">{scopeLabel}</p>
                <p className="mt-1 text-[0.82rem] text-ink-muted">{formattedRange}</p>
              </div>
            </div>

            {dateFeedback ? (
              <p className="text-[0.85rem] font-medium text-danger" role="status" aria-live="polite">
                {dateFeedback}
              </p>
            ) : null}
          </div>
        </div>
      </div>
    </Card>
  );
});

const TimeseriesTrendPanel = memo(function TimeseriesTrendPanel({
  points,
  groupBy,
  reportingTimezone,
  loading,
  error
}: {
  points: TimeseriesPoint[];
  groupBy: TimeseriesGroupBy;
  reportingTimezone: string;
  loading: boolean;
  error: string | null;
}) {
  const sortedPoints = useMemo(() => [...points].sort((left, right) => left.revenue - right.revenue).slice(-4).reverse(), [points]);
  const totalVisits = useMemo(() => points.reduce((sum, point) => sum + point.visits, 0), [points]);
  const totalOrders = useMemo(() => points.reduce((sum, point) => sum + point.orders, 0), [points]);
  const averageBucketRevenue = useMemo(
    () => (points.length > 0 ? points.reduce((sum, point) => sum + point.revenue, 0) / points.length : 0),
    [points]
  );
  const chartData = useMemo(
    () => [
      {
        id: 'Revenue',
        data: points.map((point) => {
          const bucketLabel = groupBy === 'day' ? formatDateLabel(point.date, reportingTimezone) : point.date;
          return {
            x: bucketLabel,
            y: point.revenue,
            orders: point.orders,
            visits: point.visits,
            bucketLabel
          };
        })
      }
    ],
    [groupBy, points, reportingTimezone]
  );

  return (
    <Panel
      title="Trend and pacing"
      description="Area trend highlights the revenue curve while keeping the strongest buckets visible beside the chart."
      wide
    >
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.45fr)_minmax(19rem,0.85fr)]">
        <Card tone="accent" padding="compact" className="shadow-inset-soft">
          <CardHeader className="items-center">
            <div>
              <Eyebrow>Revenue trend</Eyebrow>
              <CardTitle className="mt-3">Trend window by {GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label.toLowerCase() ?? groupBy}</CardTitle>
              <CardDescription className="mt-2">
                Revenue remains isolated from tables so grouping changes do not disturb campaign or order widgets.
              </CardDescription>
            </div>
            <Badge tone="teal">{formatNumber(points.length)} buckets</Badge>
          </CardHeader>

          <Suspense fallback={chartSuspenseFallback}>
            <NivoAreaChart
              data={chartData}
              height={320}
              loading={loading}
              error={error}
              empty={points.length === 0}
              emptyLabel="No timeseries data returned for this filter range."
              label="Revenue trend chart"
              description={`Revenue trend grouped by ${GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label.toLowerCase() ?? groupBy}.`}
              summary={buildTrendSummary(points, groupBy, reportingTimezone)}
              axisBottomLegend="Reporting bucket"
              axisLeftLegend="Revenue"
              yFormat={(value) => formatCompactCurrency(value)}
              margin={{ bottom: 64, left: 88 }}
            />
          </Suspense>
        </Card>

        <div className="grid gap-4">
          <Card padding="compact" className="border-line/60 bg-surface-alt/70">
            <Eyebrow>Pacing snapshot</Eyebrow>
            <div className="mt-4 grid gap-4 sm:grid-cols-3 xl:grid-cols-1">
              <div>
                <p className="text-label uppercase text-ink-muted">Average bucket</p>
                <p className="mt-2 font-display text-title text-ink">{formatCompactCurrency(averageBucketRevenue)}</p>
              </div>
              <div>
                <p className="text-label uppercase text-ink-muted">Visits tracked</p>
                <p className="mt-2 font-display text-title text-ink">{formatNumber(totalVisits)}</p>
              </div>
              <div>
                <p className="text-label uppercase text-ink-muted">Orders attributed</p>
                <p className="mt-2 font-display text-title text-ink">{formatNumber(totalOrders)}</p>
              </div>
            </div>
          </Card>

          <Card padding="compact" className="border-line/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(220,239,237,0.5))]">
            <Eyebrow>Top buckets</Eyebrow>
            <div className="mt-4 grid gap-3">
              {sortedPoints.length > 0 ? (
                sortedPoints.map((point) => (
                  <div
                    key={`${point.date}-${point.revenue}`}
                    className="flex flex-col gap-3 rounded-card border border-line/50 bg-white/80 px-4 py-3 sm:flex-row sm:items-start sm:justify-between"
                  >
                    <div>
                      <p className="text-body font-semibold text-ink">
                        {groupBy === 'day' ? formatDateLabel(point.date, reportingTimezone) : point.date}
                      </p>
                      <p className="mt-1 text-body text-ink-muted">
                        {formatNumber(point.orders)} orders from {formatNumber(point.visits)} visits
                      </p>
                    </div>
                    <p className="font-display text-title text-brand">{formatCompactCurrency(point.revenue)}</p>
                  </div>
                ))
              ) : (
                <p className="rounded-card border border-line/50 bg-white/80 px-4 py-4 text-body text-ink-muted">
                  No peak buckets available yet.
                </p>
              )}
            </div>
          </Card>
        </div>
      </div>
    </Panel>
  );
});

const ReportingDashboard = memo(function ReportingDashboard({
  filters,
  onFiltersChange,
  groupBy,
  onGroupByChange,
  reportingTimezone,
  quickRanges,
  onApplyQuickRange,
  onClearFilters,
  summaryCards,
  summarySection,
  campaignsSection,
  timeseriesSection,
  ordersSection,
  spendDetailsSection,
  onOpenOrderDetails
}: ReportingDashboardProps) {
  const campaigns = campaignsSection.data ?? [];
  const timeseriesPoints = timeseriesSection.data ?? [];
  const orders = ordersSection.data ?? [];
  const spendGroups = spendDetailsSection.data ?? [];
  const [campaignSearch, setCampaignSearch] = useState('');
  const [campaignSort, setCampaignSort] = useState<SortState<CampaignSortKey>>({
    key: 'revenue',
    direction: 'desc'
  });
  const [campaignPage, setCampaignPage] = useState(1);
  const [orderSearch, setOrderSearch] = useState('');
  const [orderSort, setOrderSort] = useState<SortState<OrderSortKey>>({
    key: 'processedAt',
    direction: 'desc'
  });
  const [orderPage, setOrderPage] = useState(1);
  const totalCampaignRevenue = useMemo(() => campaigns.reduce((sum, row) => sum + row.revenue, 0), [campaigns]);

  const campaignMixData = useMemo(
    () =>
      campaigns.map((row) => ({
        campaign: row.campaign,
        revenueShare: Number(((totalCampaignRevenue > 0 ? row.revenue / totalCampaignRevenue : 0) * 100).toFixed(2)),
        revenue: row.revenue,
        sourceMedium: `${row.source} / ${row.medium}`
      })),
    [campaigns, totalCampaignRevenue]
  );

  const sourceMixData = useMemo(() => {
    const sourceMixMap = campaigns.reduce<Record<string, { revenue: number; orders: number }>>((accumulator, row) => {
      const key = row.source || 'Unknown';
      const current = accumulator[key] ?? { revenue: 0, orders: 0 };
      current.revenue += row.revenue;
      current.orders += row.orders;
      accumulator[key] = current;
      return accumulator;
    }, {});

    return Object.entries(sourceMixMap)
      .map(([source, values]) => ({
        id: source,
        label: source,
        value: values.revenue,
        revenueLabel: `${formatCurrency(values.revenue)} from ${formatNumber(values.orders)} orders`
      }))
      .sort((left, right) => right.value - left.value);
  }, [campaigns]);

  const campaignHighlights = useMemo(
    () => [...campaigns].sort((left, right) => right.revenue - left.revenue).slice(0, 3),
    [campaigns]
  );
  const filteredCampaigns = useMemo(
    () =>
      campaigns.filter((row) =>
        matchesQuery(
          [row.campaign, row.source, row.medium, row.content, row.visits, row.orders, row.revenue],
          campaignSearch
        )
      ),
    [campaignSearch, campaigns]
  );
  const sortedCampaigns = useMemo(
    () =>
      sortRows(filteredCampaigns, campaignSort, {
        campaign: (row) => row.campaign,
        source: (row) => `${row.source} ${row.medium}`,
        visits: (row) => row.visits,
        orders: (row) => row.orders,
        revenue: (row) => row.revenue,
        conversionRate: (row) => row.conversionRate
      }),
    [campaignSort, filteredCampaigns]
  );
  const paginatedCampaigns = useMemo(
    () => paginateRows(sortedCampaigns, campaignPage, CAMPAIGN_PAGE_SIZE),
    [campaignPage, sortedCampaigns]
  );
  const filteredOrders = useMemo(
    () =>
      orders.filter((row) =>
        matchesQuery(
          [row.shopifyOrderId, row.source, row.medium, row.campaign, row.attributionReason, row.totalPrice],
          orderSearch
        )
      ),
    [orderSearch, orders]
  );
  const sortedOrders = useMemo(
    () =>
      sortRows(filteredOrders, orderSort, {
        order: (row) => row.shopifyOrderId,
        processedAt: (row) => row.processedAt ?? '',
        source: (row) => `${row.source ?? ''} ${row.medium ?? ''}`,
        campaign: (row) => row.campaign ?? '',
        totalPrice: (row) => row.totalPrice
      }),
    [filteredOrders, orderSort]
  );
  const paginatedOrders = useMemo(
    () => paginateRows(sortedOrders, orderPage, ORDER_PAGE_SIZE),
    [orderPage, sortedOrders]
  );
  const totalGroupedSpend = useMemo(
    () => spendGroups.reduce((sum, group) => sum + group.subtotal, 0),
    [spendGroups]
  );

  function toggleCampaignSort(key: CampaignSortKey) {
    setCampaignSort((current) => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
    }));
  }

  function toggleOrderSort(key: OrderSortKey) {
    setOrderSort((current) => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
    }));
  }

  return (
    <section className="grid gap-section">
      <DashboardOverview
        filters={filters}
        groupBy={groupBy}
        reportingTimezone={reportingTimezone}
        summary={summarySection.data}
        campaigns={campaigns}
        points={timeseriesPoints}
        orderCount={orders.length}
      />

      <DashboardControlPanel
        filters={filters}
        onFiltersChange={onFiltersChange}
        groupBy={groupBy}
        onGroupByChange={onGroupByChange}
        reportingTimezone={reportingTimezone}
        quickRanges={quickRanges}
        onApplyQuickRange={onApplyQuickRange}
        onClearFilters={onClearFilters}
      />

      <SectionState
        loading={summarySection.loading}
        error={summarySection.error}
        empty={!summarySection.data}
        emptyLabel="No summary totals were returned for this filter range."
      >
        <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-4">
          {summaryCards.map((card) => (
            <SummaryCard key={card.label} {...card} />
          ))}
        </div>
      </SectionState>

      <TimeseriesTrendPanel
        points={timeseriesPoints}
        groupBy={groupBy}
        reportingTimezone={reportingTimezone}
        loading={timeseriesSection.loading}
        error={timeseriesSection.error}
      />

      <div className="grid gap-section 2xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
        <Panel
          title="Campaign leaders"
          description="High-performing campaigns stay visible as quick-read cards before the full performance table."
        >
          <SectionState
            loading={campaignsSection.loading}
            error={campaignsSection.error}
            empty={!campaigns.length}
            emptyLabel="No campaign rows matched the current filters."
          >
            <>
              <div className="grid gap-4 xl:grid-cols-3">
                {campaignHighlights.map((row) => (
                  <Card
                    key={`${row.source}-${row.medium}-${row.campaign}`}
                    padding="compact"
                    className="border-line/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.95),rgba(246,223,211,0.42))]"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <Eyebrow>Top campaign</Eyebrow>
                        <p className="mt-3 font-display text-title text-ink">{row.campaign}</p>
                      </div>
                      <Badge tone="brand">{formatPercent(row.conversionRate)}</Badge>
                    </div>
                    <p className="mt-3 break-words text-body text-ink-soft">{row.source} / {row.medium}</p>
                    <p className="mt-5 font-display text-metric text-brand">{formatCompactCurrency(row.revenue)}</p>
                    <div className="mt-4 grid grid-cols-2 gap-3 text-body text-ink-muted">
                      <div className="rounded-card border border-line/50 bg-white/75 px-3 py-3">
                        <p className="text-label uppercase">Visits</p>
                        <p className="mt-1 font-semibold text-ink">{formatNumber(row.visits)}</p>
                      </div>
                      <div className="rounded-card border border-line/50 bg-white/75 px-3 py-3">
                        <p className="text-label uppercase">Orders</p>
                        <p className="mt-1 font-semibold text-ink">{formatNumber(row.orders)}</p>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>

              <DataTableToolbar
                title="Campaign performance"
                description="One shared table pattern now handles search, sticky headers, sort order, and paging for campaign rows."
                summary={
                  <>
                    <TableMeta currentCount={filteredCampaigns.length} totalCount={campaigns.length} label="campaign rows" />
                    <TablePagination
                      page={paginatedCampaigns.currentPage}
                      totalPages={paginatedCampaigns.totalPages}
                      onPageChange={setCampaignPage}
                    />
                  </>
                }
              >
                <TableFilterBar>
                  <TableSearchField
                    label="Search campaign rows"
                    value={campaignSearch}
                    onChange={(value) => {
                      setCampaignSearch(value);
                      setCampaignPage(1);
                    }}
                    placeholder="Campaign, source, medium, content"
                  />
                  <Field label="Sort by" htmlFor="campaign-table-sort">
                    <Select
                      id="campaign-table-sort"
                      value={`${campaignSort.key}:${campaignSort.direction}`}
                      onChange={(event) => {
                        const [key, direction] = event.target.value.split(':') as [CampaignSortKey, 'asc' | 'desc'];
                        setCampaignSort({ key, direction });
                        setCampaignPage(1);
                      }}
                    >
                      <option value="revenue:desc">Revenue ↓</option>
                      <option value="revenue:asc">Revenue ↑</option>
                      <option value="orders:desc">Orders ↓</option>
                      <option value="orders:asc">Orders ↑</option>
                      <option value="visits:desc">Visits ↓</option>
                      <option value="visits:asc">Visits ↑</option>
                      <option value="campaign:asc">Campaign A-Z</option>
                      <option value="campaign:desc">Campaign Z-A</option>
                      <option value="conversionRate:desc">CVR ↓</option>
                      <option value="conversionRate:asc">CVR ↑</option>
                    </Select>
                  </Field>
                </TableFilterBar>
              </DataTableToolbar>

              <TableWrap className="mt-6 max-h-[32rem]">
                <Table caption="Campaign performance">
                  <TableHead>
                    <TableRow>
                      <SortableTableHeaderCell
                        sorted={campaignSort.key === 'campaign'}
                        direction={campaignSort.direction}
                        onSort={() => toggleCampaignSort('campaign')}
                      >
                        Campaign
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={campaignSort.key === 'source'}
                        direction={campaignSort.direction}
                        onSort={() => toggleCampaignSort('source')}
                      >
                        Source
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={campaignSort.key === 'visits'}
                        direction={campaignSort.direction}
                        onSort={() => toggleCampaignSort('visits')}
                      >
                        Visits
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={campaignSort.key === 'orders'}
                        direction={campaignSort.direction}
                        onSort={() => toggleCampaignSort('orders')}
                      >
                        Orders
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={campaignSort.key === 'revenue'}
                        direction={campaignSort.direction}
                        onSort={() => toggleCampaignSort('revenue')}
                      >
                        Revenue
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={campaignSort.key === 'conversionRate'}
                        direction={campaignSort.direction}
                        onSort={() => toggleCampaignSort('conversionRate')}
                      >
                        CVR
                      </SortableTableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {paginatedCampaigns.rows.length === 0 ? (
                      <TableEmptyRow
                        colSpan={6}
                        title="No campaign rows found"
                        description="Try broadening the search or adjusting the dashboard filters."
                      />
                    ) : null}
                    {paginatedCampaigns.rows.map((row) => (
                      <TableRow key={`${row.source}-${row.medium}-${row.campaign}-${row.content ?? 'none'}`}>
                        <TableCell>
                          <PrimaryCell>
                            <strong>{row.campaign}</strong>
                            <span>{row.content ?? 'No content tag'}</span>
                          </PrimaryCell>
                        </TableCell>
                        <TableCell>{`${row.source} / ${row.medium}`}</TableCell>
                        <TableCell>{formatNumber(row.visits)}</TableCell>
                        <TableCell>{formatNumber(row.orders)}</TableCell>
                        <TableCell>{formatCurrency(row.revenue)}</TableCell>
                        <TableCell>{formatPercent(row.conversionRate)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableWrap>
            </>
          </SectionState>
        </Panel>

        <div className="grid gap-section">
          <Panel
            title="Campaign mix"
            description="Revenue share by campaign keeps the largest demand drivers easy to compare."
          >
            <Suspense fallback={chartSuspenseFallback}>
              <NivoBarChart
                data={campaignMixData}
                keys={['revenueShare']}
                indexBy="campaign"
                layout="horizontal"
                height={Math.max(campaignMixData.length * 52, 280)}
                loading={campaignsSection.loading}
                error={campaignsSection.error}
                empty={!campaignMixData.length}
                emptyLabel="No campaign mix available yet."
                label="Campaign revenue share chart"
                description="Horizontal bar chart showing the revenue share captured by each campaign."
                summary={buildCampaignMixSummary(campaignMixData)}
                axisBottomLegend="Revenue share"
                valueFormat={(value) => `${value.toFixed(1)}%`}
                margin={{ left: 104, bottom: 52 }}
              />
            </Suspense>
          </Panel>

          <Panel
            title="Source contribution"
            description="Aggregated campaign rows rolled up by source to expose where attributed revenue is concentrating."
          >
            <Suspense fallback={chartSuspenseFallback}>
              <NivoPieChart
                data={sourceMixData}
                height={300}
                loading={campaignsSection.loading}
                error={campaignsSection.error}
                empty={!sourceMixData.length}
                emptyLabel="No source contribution available yet."
                label="Source contribution chart"
                description="Pie chart showing attributed revenue distribution by traffic source."
                summary={buildSourceMixSummary(sourceMixData)}
                valueFormat={(value) => formatCompactCurrency(value)}
                margin={{ bottom: 64 }}
              />
            </Suspense>
          </Panel>
        </div>
      </div>

      <Panel
        title="Attributed orders"
        description="Order-level attribution rows remain intact so operators can drill into the full payload when something looks off."
        wide
      >
        <SectionState
          loading={ordersSection.loading}
          error={ordersSection.error}
          empty={!orders.length}
          emptyLabel="No attributed orders were returned for this range."
        >
          <>
            <DataTableToolbar
              title="Attributed order list"
              description="Shared list controls keep order lookup, sort, and pagination consistent with every other authenticated table."
              summary={
                <>
                  <TableMeta currentCount={filteredOrders.length} totalCount={orders.length} label="orders" />
                  <TablePagination
                    page={paginatedOrders.currentPage}
                    totalPages={paginatedOrders.totalPages}
                    onPageChange={setOrderPage}
                  />
                </>
              }
            >
              <TableFilterBar>
                <TableSearchField
                  label="Search orders"
                  value={orderSearch}
                  onChange={(value) => {
                    setOrderSearch(value);
                    setOrderPage(1);
                  }}
                  placeholder="Order ID, source, campaign, reason"
                />
                <Field label="Sort by" htmlFor="orders-table-sort">
                  <Select
                    id="orders-table-sort"
                    value={`${orderSort.key}:${orderSort.direction}`}
                    onChange={(event) => {
                      const [key, direction] = event.target.value.split(':') as [OrderSortKey, 'asc' | 'desc'];
                      setOrderSort({ key, direction });
                      setOrderPage(1);
                    }}
                  >
                    <option value="processedAt:desc">Processed ↓</option>
                    <option value="processedAt:asc">Processed ↑</option>
                    <option value="totalPrice:desc">Total ↓</option>
                    <option value="totalPrice:asc">Total ↑</option>
                    <option value="order:desc">Order ↓</option>
                    <option value="order:asc">Order ↑</option>
                    <option value="campaign:asc">Campaign A-Z</option>
                    <option value="campaign:desc">Campaign Z-A</option>
                  </Select>
                </Field>
              </TableFilterBar>
            </DataTableToolbar>

            <TableWrap className="max-h-[34rem]">
              <Table caption="Attributed orders">
                <TableHead>
                  <TableRow>
                    <SortableTableHeaderCell
                      sorted={orderSort.key === 'order'}
                      direction={orderSort.direction}
                      onSort={() => toggleOrderSort('order')}
                    >
                      Order
                    </SortableTableHeaderCell>
                    <SortableTableHeaderCell
                      sorted={orderSort.key === 'processedAt'}
                      direction={orderSort.direction}
                      onSort={() => toggleOrderSort('processedAt')}
                    >
                      Processed
                    </SortableTableHeaderCell>
                    <SortableTableHeaderCell
                      sorted={orderSort.key === 'source'}
                      direction={orderSort.direction}
                      onSort={() => toggleOrderSort('source')}
                    >
                      Source
                    </SortableTableHeaderCell>
                    <SortableTableHeaderCell
                      sorted={orderSort.key === 'campaign'}
                      direction={orderSort.direction}
                      onSort={() => toggleOrderSort('campaign')}
                    >
                      Campaign
                    </SortableTableHeaderCell>
                    <SortableTableHeaderCell
                      sorted={orderSort.key === 'totalPrice'}
                      direction={orderSort.direction}
                      onSort={() => toggleOrderSort('totalPrice')}
                    >
                      Total
                    </SortableTableHeaderCell>
                    <TableHeaderCell>Reason</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {paginatedOrders.rows.length === 0 ? (
                    <TableEmptyRow
                      colSpan={6}
                      title="No orders matched"
                      description="Try another search term or widen the reporting window."
                    />
                  ) : null}
                  {paginatedOrders.rows.map((row) => (
                    <TableRow key={row.shopifyOrderId}>
                      <TableCell>
                        <PrimaryCell>
                          <button
                            type="button"
                            className="w-fit rounded-pill border border-line/80 bg-surface px-3 py-1.5 font-semibold text-brand transition hover:-translate-y-0.5 hover:border-brand/30 hover:bg-brand-soft focus-visible:outline-none focus-visible:ring-4 focus-visible:ring-brand/20 focus-visible:ring-offset-2 focus-visible:ring-offset-surface"
                            onClick={() => onOpenOrderDetails(row.shopifyOrderId)}
                            aria-label={`Open order details for Shopify order ${row.shopifyOrderId}`}
                          >
                            #{row.shopifyOrderId}
                          </button>
                          <span>{row.medium ?? 'No medium'}</span>
                        </PrimaryCell>
                      </TableCell>
                      <TableCell>{formatDateTimeLabel(row.processedAt, reportingTimezone)}</TableCell>
                      <TableCell>{row.source ?? 'Unattributed'}</TableCell>
                      <TableCell>{row.campaign ?? 'No campaign'}</TableCell>
                      <TableCell>{formatCurrency(row.totalPrice)}</TableCell>
                      <TableCell>
                        <Badge tone="teal">{row.attributionReason}</Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableWrap>
          </>
        </SectionState>
      </Panel>

      <Panel
        title="Marketing spend detail"
        description="Bottom report keeps spend grouped by channel first, then by campaign, with visible subtotals for each media slice."
        wide
      >
        <SectionState
          loading={spendDetailsSection.loading}
          error={spendDetailsSection.error}
          empty={!spendGroups.length}
          emptyLabel="No marketing spend rows were returned for this range."
        >
          <>
            <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
              <div>
                <Eyebrow>Spend rollup</Eyebrow>
                <p className="mt-3 max-w-2xl text-body text-ink-soft">
                  Channel groupings follow the same reporting window and dashboard filters as the summary totals.
                </p>
              </div>
              <div className="grid gap-2 rounded-card border border-line/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(220,239,237,0.52))] px-4 py-4 text-right shadow-inset-soft">
                <p className="text-label uppercase text-ink-muted">Grouped spend total</p>
                <p className="font-display text-title text-brand">{formatCurrency(totalGroupedSpend)}</p>
                <p className="text-body text-ink-muted">
                  {formatNumber(spendGroups.length)} channels in the current window
                </p>
              </div>
            </div>

            <div className="mt-6 grid gap-4">
              {spendGroups.map((group) => (
                <Card
                  key={`${group.source}-${group.medium}`}
                  padding="compact"
                  className="border-line/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(246,223,211,0.34))]"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <Eyebrow>Channel</Eyebrow>
                      <p className="mt-3 font-display text-title text-ink">{formatChannelLabel(group.source, group.medium)}</p>
                      <p className="mt-2 text-body text-ink-muted">
                        {formatNumber(group.campaigns.length)} campaigns contributing spend
                      </p>
                    </div>
                    <Badge tone="teal">{formatCurrency(group.subtotal)} subtotal</Badge>
                  </div>

                  <TableWrap className="mt-5">
                    <Table caption={`Spend detail for ${formatChannelLabel(group.source, group.medium)}`}>
                      <TableHead>
                        <TableRow>
                          <TableHeaderCell>Campaign</TableHeaderCell>
                          <TableHeaderCell>Spend</TableHeaderCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {group.campaigns.map((campaign) => (
                          <TableRow key={`${group.source}-${group.medium}-${campaign.campaign}`}>
                            <TableCell>
                              <PrimaryCell>
                                <strong>{campaign.campaign}</strong>
                                <span>{formatChannelLabel(group.source, group.medium)}</span>
                              </PrimaryCell>
                            </TableCell>
                            <TableCell>{formatCurrency(campaign.spend)}</TableCell>
                          </TableRow>
                        ))}
                        <TableRow>
                          <TableCell className="font-semibold text-ink">Channel subtotal</TableCell>
                          <TableCell className="font-semibold text-ink">{formatCurrency(group.subtotal)}</TableCell>
                        </TableRow>
                      </TableBody>
                    </Table>
                  </TableWrap>
                </Card>
              ))}
            </div>
          </>
        </SectionState>
      </Panel>
    </section>
  );
});

export default ReportingDashboard;
