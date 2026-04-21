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
import { NivoAreaChart, NivoBarChart } from './charts';

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

function SummaryCard({ label, value, detail }: SummaryCardData) {
  return (
    <Card padding="compact" className="ui-metric-card">
      <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-brand via-brand/70 to-teal/70" />
      <Eyebrow>{label}</Eyebrow>
      <MetricValue>{value}</MetricValue>
      <MetricCopy>{detail}</MetricCopy>
    </Card>
  );
}

function TimeseriesChart({
  points,
  groupBy,
  reportingTimezone
}: {
  points: TimeseriesPoint[];
  groupBy: TimeseriesGroupBy;
  reportingTimezone: string;
}) {
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
    <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_18rem]">
      <Card tone="accent" padding="compact" className="shadow-inset-soft">
        <CardHeader className="items-center">
          <div>
            <Eyebrow>Trend window</Eyebrow>
            <p className="mt-2 font-display text-title text-ink">
              {formatCurrency(Math.max(...points.map((point) => point.revenue), 0))} max revenue
            </p>
          </div>
          <Badge tone="teal" className="px-3 py-2">
            {GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label ?? groupBy}
          </Badge>
        </CardHeader>
        <NivoAreaChart
          data={chartData}
          height={272}
          axisBottomLegend="Reporting bucket"
          axisLeftLegend="Revenue"
          yFormat={(value) => formatCompactCurrency(value)}
          margin={{ bottom: 64, left: 88 }}
        />
      </Card>

      <div className="grid content-start gap-3">
        {points.map((point) => (
          <Card key={point.date} padding="compact" className="border-line/60 bg-surface-alt/70">
            <div className="flex items-start justify-between gap-3">
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
          </Card>
        ))}
      </div>
    </div>
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
  const totalCampaignRevenue = (campaignsSection.data ?? []).reduce((sum, row) => sum + row.revenue, 0);
  const campaignMixData = (campaignsSection.data ?? []).map((row) => ({
    campaign: row.campaign,
    revenueShare: Number(((totalCampaignRevenue > 0 ? row.revenue / totalCampaignRevenue : 0) * 100).toFixed(2)),
    revenue: row.revenue,
    sourceMedium: `${row.source} / ${row.medium}`
  }));

  return (
    <section className="grid gap-section">
      <Panel
        title="Reporting controls"
        description="Tune the attribution window and narrow the view by traffic source or campaign without leaving the dashboard."
        wide
        className="overflow-hidden"
      >
        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.55fr)_minmax(18rem,0.95fr)]">
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
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

          <Card padding="compact" className="border-line/60 bg-surface-alt/60">
            <Eyebrow>Quick ranges</Eyebrow>
            <ButtonRow className="mt-4 gap-2">
              {quickRanges.map((preset) => (
                <Button
                  key={preset.label}
                  type="button"
                  tone="secondary"
                  className="min-w-[5.5rem] flex-1"
                  onClick={() => onApplyQuickRange(preset.value(reportingTimezone))}
                >
                  {preset.label}
                </Button>
              ))}
            </ButtonRow>
            <div className="mt-4 flex items-center justify-between gap-3 rounded-card border border-line/50 bg-white/70 px-4 py-3">
              <div>
                <p className="text-label uppercase text-ink-muted">Current scope</p>
                <p className="mt-1 text-body text-ink-soft">
                  {(filters.source ?? '').trim() || (filters.campaign ?? '').trim()
                    ? `Filtered by ${[(filters.source ?? '').trim(), (filters.campaign ?? '').trim()].filter(Boolean).join(' / ')}`
                    : 'All attributed traffic'}
                </p>
              </div>
              <Button type="button" tone="ghost" onClick={onClearFilters}>
                Clear filters
              </Button>
            </div>
          </Card>
        </div>
      </Panel>

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

      <Panel
        title="Revenue trend"
        description="Time-based revenue stays decoupled from the rest of the reporting tables, so grouping changes do not disturb the other widgets."
        wide
      >
        <SectionState
          loading={timeseriesSection.loading}
          error={timeseriesSection.error}
          empty={!timeseriesSection.data?.length}
          emptyLabel="No timeseries data returned for this filter range."
        >
          <TimeseriesChart
            points={timeseriesSection.data ?? []}
            groupBy={groupBy}
            reportingTimezone={reportingTimezone}
          />
        </SectionState>
      </Panel>

      <div className="grid gap-section xl:grid-cols-[minmax(0,1.45fr)_minmax(18rem,0.85fr)]">
        <Panel
          title="Campaign performance"
          description="Top campaign rows ordered by revenue, matching the reporting API contract."
        >
          <SectionState
            loading={campaignsSection.loading}
            error={campaignsSection.error}
            empty={!campaignsSection.data?.length}
            emptyLabel="No campaign rows matched the current filters."
          >
            <TableWrap>
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
                  {(campaignsSection.data ?? []).map((row) => (
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
          </SectionState>
        </Panel>

        <Panel
          title="Campaign mix"
          description="Revenue share highlights the campaigns carrying the selected reporting window."
        >
          <NivoBarChart
            data={campaignMixData}
            keys={['revenueShare']}
            indexBy="campaign"
            layout="horizontal"
            height={Math.max(campaignMixData.length * 52, 260)}
            loading={campaignsSection.loading}
            error={campaignsSection.error}
            empty={!campaignMixData.length}
            emptyLabel="No campaign mix available yet."
            axisBottomLegend="Revenue share"
            valueFormat={(value) => `${value.toFixed(1)}%`}
            margin={{ left: 116, bottom: 52 }}
          />
        </Panel>
      </div>

      <Panel
        title="Attributed orders"
        description="Order-level attribution rows let you inspect which source and campaign received credit before drilling into the full payload."
        wide
      >
        <SectionState
          loading={ordersSection.loading}
          error={ordersSection.error}
          empty={!ordersSection.data?.length}
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
                {(ordersSection.data ?? []).map((row) => (
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
                      <Badge tone="teal">
                        {row.attributionReason}
                      </Badge>
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
