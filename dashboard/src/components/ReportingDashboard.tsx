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
  Field,
  Input,
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

function SummaryCard({ label, value, detail }: SummaryCardData) {
  return (
    <Card padding="compact" className="border-line/70">
      <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-brand via-brand/70 to-teal/70" />
      <p className="text-caption uppercase tracking-[0.16em] text-ink-muted">{label}</p>
      <p className="mt-4 font-display text-[clamp(2rem,4vw,2.9rem)] leading-none tracking-[-0.05em] text-ink">
        {value}
      </p>
      <p className="mt-3 text-body text-ink-soft">{detail}</p>
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
  const path = buildSeriesPath(points);
  const maxRevenue = Math.max(...points.map((point) => point.revenue), 1);

  return (
    <div className="grid gap-6 xl:grid-cols-[minmax(0,1.4fr)_18rem]">
      <Card tone="accent" padding="compact" className="shadow-inset-soft">
        <CardHeader className="items-center">
          <div>
            <p className="text-caption uppercase tracking-[0.14em] text-ink-muted">Trend window</p>
            <p className="mt-2 font-display text-title text-ink">{formatCurrency(maxRevenue)} max revenue</p>
          </div>
          <Badge tone="teal" className="px-3 py-2">
            {GROUP_BY_OPTIONS.find((option) => option.value === groupBy)?.label ?? groupBy}
          </Badge>
        </CardHeader>
        <svg viewBox="0 0 320 160" className="h-[17rem] w-full overflow-visible" aria-label="Revenue timeseries">
          <defs>
            <linearGradient id="revenueFill" x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor="rgba(203, 99, 50, 0.32)" />
              <stop offset="100%" stopColor="rgba(203, 99, 50, 0.03)" />
            </linearGradient>
          </defs>
          {[0, 1, 2, 3].map((index) => {
            const y = 24 + index * 30;
            return (
              <line
                key={y}
                x1="0"
                x2="320"
                y1={y}
                y2={y}
                stroke="rgba(23, 33, 43, 0.08)"
                strokeDasharray="3 5"
              />
            );
          })}
          {path ? <path d={`${path} L 320 144 L 0 144 Z`} fill="url(#revenueFill)" /> : null}
          {path ? (
            <path
              d={path}
              fill="none"
              stroke="#cb6332"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="3"
            />
          ) : null}
          {points.map((point, index) => {
            const x = points.length === 1 ? 320 : (index / (points.length - 1)) * 320;
            const y = 144 - (point.revenue / maxRevenue) * 120;

            return (
              <g key={`${point.date}-${index}`}>
                <circle cx={x} cy={y} fill="#ffffff" r="6" stroke="#cb6332" strokeWidth="2" />
                <circle cx={x} cy={y} fill="#cb6332" r="2.5" />
              </g>
            );
          })}
        </svg>
      </Card>

      <div className="grid content-start gap-3">
        {points.map((point) => (
          <Card key={point.date} padding="compact" className="border-line/60 bg-surface-alt/70">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="font-semibold text-ink">
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
            <p className="text-caption uppercase tracking-[0.16em] text-ink-muted">Quick ranges</p>
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
                <p className="text-[0.82rem] font-semibold uppercase tracking-[0.08em] text-ink-muted">Current scope</p>
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
          <SectionState
            loading={campaignsSection.loading}
            error={campaignsSection.error}
            empty={!campaignsSection.data?.length}
            emptyLabel="No campaign mix available yet."
          >
            <div className="grid gap-3">
              {(campaignsSection.data ?? []).map((row) => {
                const share = totalCampaignRevenue > 0 ? row.revenue / totalCampaignRevenue : 0;

                return (
                  <Card key={`${row.source}-${row.campaign}`} padding="compact" className="border-line/60 bg-surface-alt/55">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="truncate font-semibold text-ink">{row.campaign}</p>
                        <p className="mt-1 text-body text-ink-muted">{`${row.source} / ${row.medium}`}</p>
                      </div>
                      <div className="shrink-0 text-right">
                        <p className="font-semibold text-ink">{formatPercent(share)}</p>
                        <p className="mt-1 text-body text-ink-muted">{formatCompactCurrency(row.revenue)}</p>
                      </div>
                    </div>
                    <div className="mt-4 h-3 overflow-hidden rounded-pill bg-canvas-tint">
                      <div
                        className="h-full rounded-pill bg-gradient-to-r from-brand to-teal"
                        style={{ width: `${Math.max(share * 100, 2)}%` }}
                      />
                    </div>
                  </Card>
                );
              })}
            </div>
          </SectionState>
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
