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
  SummaryTotals,
  TimeseriesGroupBy,
  TimeseriesPoint
} from '../lib/api';
import {
  Badge,
  Button,
  ButtonRow,
  Card,
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
  Select,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeaderCell,
  TableRow,
  TableWrap
} from './AuthenticatedUi';
import { NivoAreaChart, NivoBarChart, NivoPieChart } from './charts';

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
  onOpenOrderDetails: (shopifyOrderId: string) => void;
};

const GROUP_BY_OPTIONS: Array<{ value: TimeseriesGroupBy; label: string }> = [
  { value: 'day', label: 'Daily' },
  { value: 'source', label: 'By source' },
  { value: 'campaign', label: 'By campaign' }
];

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
  AOV: {
    accent: 'from-success/90 via-teal/80 to-brand/65',
    pillTone: 'success',
    pillLabel: 'Efficiency'
  }
};

function SummaryCard({ label, value, detail }: SummaryCardData) {
  const decor = SUMMARY_CARD_DECOR[label] ?? SUMMARY_CARD_DECOR.Visits;

  return (
    <Card
      padding="compact"
      className="ui-metric-card min-h-[13rem] border-line/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(251,247,242,0.82))] shadow-panel"
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
}

function DashboardOverview({
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
  const peakPoint = points.reduce<TimeseriesPoint | null>(
    (current, point) => (!current || point.revenue > current.revenue ? point : current),
    null
  );
  const averageRevenue = points.length > 0 ? totalRevenue / points.length : 0;
  const leadingCampaign = campaigns.reduce<CampaignRow | null>(
    (current, row) => (!current || row.revenue > current.revenue ? row : current),
    null
  );
  const rangeLabel = `${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`;

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
          <Eyebrow className="text-white/60">Operator notes</Eyebrow>
          <div className="mt-5 grid gap-4">
            <div className="rounded-card border border-white/10 bg-white/5 px-4 py-4">
              <p className="text-caption uppercase tracking-[0.14em] text-white/55">Revenue captured</p>
              <p className="mt-3 font-display text-display text-white">{formatCompactCurrency(totalRevenue)}</p>
              <p className="mt-2 text-body text-white/72">
                {summary?.roas == null ? 'ROAS pending spend data' : `${formatNumber(summary.roas)} ROAS on attributed revenue`}
              </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
              <div className="rounded-card border border-white/10 bg-white/5 px-4 py-4">
                <p className="text-caption uppercase tracking-[0.14em] text-white/55">Conversion rate</p>
                <p className="mt-2 font-display text-title text-white">{formatPercent(summary?.conversionRate)}</p>
              </div>
              <div className="rounded-card border border-white/10 bg-white/5 px-4 py-4">
                <p className="text-caption uppercase tracking-[0.14em] text-white/55">Campaign rows</p>
                <p className="mt-2 font-display text-title text-white">{formatNumber(campaigns.length)}</p>
              </div>
            </div>
          </div>
        </Card>
      </div>
    </Panel>
  );
}

function DashboardControlPanel({
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
  const scopeLabel =
    (filters.source ?? '').trim() || (filters.campaign ?? '').trim()
      ? `Filtered by ${[(filters.source ?? '').trim(), (filters.campaign ?? '').trim()].filter(Boolean).join(' / ')}`
      : 'All attributed traffic';

  return (
    <Panel
      title="Reporting controls"
      description="Tune the attribution window and reshape the overview without leaving the dashboard surface."
      wide
      className="overflow-hidden"
    >
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.45fr)_minmax(20rem,0.9fr)]">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          <Field label="Start date" htmlFor="start-date">
            <Input
              id="start-date"
              type="date"
              value={filters.startDate}
              onChange={(event) => onFiltersChange({ ...filters, startDate: event.target.value })}
            />
          </Field>
          <Field label="End date" htmlFor="end-date">
            <Input
              id="end-date"
              type="date"
              value={filters.endDate}
              onChange={(event) => onFiltersChange({ ...filters, endDate: event.target.value })}
            />
          </Field>
          <Field label="Source" htmlFor="source-filter">
            <Input
              id="source-filter"
              type="text"
              placeholder="google, meta, facebook"
              value={filters.source}
              onChange={(event) => onFiltersChange({ ...filters, source: event.target.value })}
            />
          </Field>
          <Field label="Campaign" htmlFor="campaign-filter">
            <Input
              id="campaign-filter"
              type="text"
              placeholder="spring-sale"
              value={filters.campaign}
              onChange={(event) => onFiltersChange({ ...filters, campaign: event.target.value })}
            />
          </Field>
          <Field label="Timeseries grouping" htmlFor="group-by">
            <Select id="group-by" value={groupBy} onChange={(event) => onGroupByChange(event.target.value as TimeseriesGroupBy)}>
              {GROUP_BY_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </Select>
          </Field>
        </div>

        <Card padding="compact" className="border-line/60 bg-[linear-gradient(180deg,rgba(237,245,242,0.7),rgba(255,255,255,0.92))]">
          <div className="flex items-start justify-between gap-3">
            <div>
              <Eyebrow>Quick ranges</Eyebrow>
              <p className="mt-3 max-w-[24ch] text-body text-ink-soft">
                Shortcuts respect the reporting timezone and immediately re-scope the dashboard.
              </p>
            </div>
            <Badge tone="teal">{reportingTimezone}</Badge>
          </div>

          <ButtonRow className="mt-5 gap-2">
            {quickRanges.map((preset) => (
              <Button
                key={preset.label}
                type="button"
                tone="secondary"
                className="min-w-[5.25rem] flex-1"
                onClick={() => onApplyQuickRange(preset.value(reportingTimezone))}
              >
                {preset.label}
              </Button>
            ))}
          </ButtonRow>

          <div className="mt-5 rounded-card border border-line/60 bg-white/80 px-4 py-4">
            <p className="text-label uppercase text-ink-muted">Current scope</p>
            <p className="mt-2 text-body text-ink-soft">{scopeLabel}</p>
            <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
              <Badge tone="neutral">
                {GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label ?? groupBy}
              </Badge>
              <Button type="button" tone="ghost" onClick={onClearFilters}>
                Clear filters
              </Button>
            </div>
          </div>
        </Card>
      </div>
    </Panel>
  );
}

function TimeseriesTrendPanel({
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
  const sortedPoints = [...points].sort((left, right) => left.revenue - right.revenue).slice(-4).reverse();
  const totalVisits = points.reduce((sum, point) => sum + point.visits, 0);
  const totalOrders = points.reduce((sum, point) => sum + point.orders, 0);
  const averageBucketRevenue = points.length > 0 ? points.reduce((sum, point) => sum + point.revenue, 0) / points.length : 0;
  const chartData = [
    {
      id: 'Revenue',
      data: points.map((point) => ({
        x: groupBy === 'day' ? formatDateLabel(point.date, reportingTimezone) : point.date,
        y: point.revenue,
        orders: point.orders,
        visits: point.visits,
        bucketLabel: groupBy === 'day' ? formatDateLabel(point.date, reportingTimezone) : point.date
      }))
    }
  ];

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

          <NivoAreaChart
            data={chartData}
            height={320}
            loading={loading}
            error={error}
            empty={points.length === 0}
            emptyLabel="No timeseries data returned for this filter range."
            axisBottomLegend="Reporting bucket"
            axisLeftLegend="Revenue"
            yFormat={(value) => formatCompactCurrency(value)}
            margin={{ bottom: 64, left: 88 }}
          />
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
                    className="flex items-start justify-between gap-3 rounded-card border border-line/50 bg-white/80 px-4 py-3"
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
}

export default function ReportingDashboard({
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
  onOpenOrderDetails
}: ReportingDashboardProps) {
  const campaigns = campaignsSection.data ?? [];
  const timeseriesPoints = timeseriesSection.data ?? [];
  const orders = ordersSection.data ?? [];
  const totalCampaignRevenue = campaigns.reduce((sum, row) => sum + row.revenue, 0);

  const campaignMixData = campaigns.map((row) => ({
    campaign: row.campaign,
    revenueShare: Number(((totalCampaignRevenue > 0 ? row.revenue / totalCampaignRevenue : 0) * 100).toFixed(2)),
    revenue: row.revenue,
    sourceMedium: `${row.source} / ${row.medium}`
  }));

  const sourceMixMap = campaigns.reduce<Record<string, { revenue: number; orders: number }>>((accumulator, row) => {
    const key = row.source || 'Unknown';
    const current = accumulator[key] ?? { revenue: 0, orders: 0 };
    current.revenue += row.revenue;
    current.orders += row.orders;
    accumulator[key] = current;
    return accumulator;
  }, {});

  const sourceMixData = Object.entries(sourceMixMap)
    .map(([source, values]) => ({
      id: source,
      label: source,
      value: values.revenue,
      revenueLabel: `${formatCurrency(values.revenue)} from ${formatNumber(values.orders)} orders`
    }))
    .sort((left, right) => right.value - left.value);

  const campaignHighlights = [...campaigns].sort((left, right) => right.revenue - left.revenue).slice(0, 3);

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
                    <p className="mt-3 text-body text-ink-soft">{row.source} / {row.medium}</p>
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

              <TableWrap className="mt-6">
                <Table caption="Campaign performance">
                  <TableHead>
                    <TableRow>
                      <TableHeaderCell>Campaign</TableHeaderCell>
                      <TableHeaderCell>Source</TableHeaderCell>
                      <TableHeaderCell>Visits</TableHeaderCell>
                      <TableHeaderCell>Orders</TableHeaderCell>
                      <TableHeaderCell>Revenue</TableHeaderCell>
                      <TableHeaderCell>CVR</TableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {campaigns.map((row) => (
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
              axisBottomLegend="Revenue share"
              valueFormat={(value) => `${value.toFixed(1)}%`}
              margin={{ left: 132, bottom: 52 }}
            />
          </Panel>

          <Panel
            title="Source contribution"
            description="Aggregated campaign rows rolled up by source to expose where attributed revenue is concentrating."
          >
            <NivoPieChart
              data={sourceMixData}
              height={300}
              loading={campaignsSection.loading}
              error={campaignsSection.error}
              empty={!sourceMixData.length}
              emptyLabel="No source contribution available yet."
              valueFormat={(value) => formatCompactCurrency(value)}
              margin={{ bottom: 84 }}
            />
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
          <TableWrap>
            <Table caption="Attributed orders">
              <TableHead>
                <TableRow>
                  <TableHeaderCell>Order</TableHeaderCell>
                  <TableHeaderCell>Processed</TableHeaderCell>
                  <TableHeaderCell>Source</TableHeaderCell>
                  <TableHeaderCell>Campaign</TableHeaderCell>
                  <TableHeaderCell>Total</TableHeaderCell>
                  <TableHeaderCell>Reason</TableHeaderCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {orders.map((row) => (
                  <TableRow key={row.shopifyOrderId}>
                    <TableCell>
                      <PrimaryCell>
                        <button
                          type="button"
                          className="w-fit rounded-pill border border-line/80 bg-surface px-3 py-1.5 font-semibold text-brand transition hover:-translate-y-0.5 hover:border-brand/30 hover:bg-brand-soft"
                          onClick={() => onOpenOrderDetails(row.shopifyOrderId)}
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
        </SectionState>
      </Panel>
    </section>
  );
}
