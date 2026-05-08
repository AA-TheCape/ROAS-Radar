import React, { useMemo, useState } from 'react';

import type {
  AttributionChannelTotalRow,
  AttributionChannelTotalsResponse,
  AttributionExplainabilityResponse,
  AttributionExplainabilityTouchpoint,
  AttributionFilters,
  AttributionResultRow
} from '../lib/api';
import { formatCurrency, formatDateTimeLabel, formatNumber } from '../lib/format';
import type { AttributionCreditRecordV1, AttributionExplainRecordV1, AttributionModelKey } from '../../../packages/attribution-schema/index.js';
import {
  Badge,
  Button,
  ButtonRow,
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
  DetailList,
  Eyebrow,
  Field,
  FieldGrid,
  Input,
  MetricCopy,
  MetricValue,
  Panel,
  SectionState,
  StatusPill,
  Table,
  TableBody,
  TableCell,
  TableEmptyRow,
  TableHead,
  TableHeaderCell,
  TableRow,
  TableWrap
} from './AuthenticatedUi';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type AttributionDashboardProps = {
  filters: AttributionFilters;
  onFiltersChange: (next: AttributionFilters) => void;
  onClearFilters: () => void;
  activeModel: AttributionModelKey;
  onActiveModelChange: (modelKey: AttributionModelKey) => void;
  reportingTimezone: string;
  resultsSection: AsyncSection<AttributionResultRow[]>;
  channelTotalsSection: AsyncSection<AttributionChannelTotalsResponse>;
  explainabilitySection: AsyncSection<AttributionExplainabilityResponse>;
  selectedOrderId: string | null;
  onInspectOrder: (orderId: string, runId: string) => void;
};

const MODEL_ORDER: AttributionModelKey[] = [
  'first_touch',
  'last_touch',
  'last_non_direct',
  'linear',
  'clicks_only',
  'hinted_fallback_only'
];

const MODEL_META: Record<
  AttributionModelKey,
  {
    label: string;
    description: string;
    tone: 'brand' | 'teal' | 'warning' | 'success' | 'neutral';
  }
> = {
  first_touch: {
    label: 'First touch',
    description: 'Credits the earliest eligible touchpoint in canonical journey order.',
    tone: 'brand'
  },
  last_touch: {
    label: 'Last touch',
    description: 'Credits the latest eligible touchpoint, including direct revisits.',
    tone: 'teal'
  },
  last_non_direct: {
    label: 'Last non-direct',
    description: 'Suppresses direct traffic whenever a non-direct eligible touch exists.',
    tone: 'success'
  },
  linear: {
    label: 'Linear',
    description: 'Splits credit evenly across all eligible touchpoints in the journey.',
    tone: 'warning'
  },
  clicks_only: {
    label: 'Clicks only',
    description: 'Restricts attribution to eligible clicks and ignores view touchpoints.',
    tone: 'brand'
  },
  hinted_fallback_only: {
    label: 'Hinted fallback only',
    description: 'Only credits qualifying Shopify marketing hints when deterministic evidence is absent.',
    tone: 'neutral'
  }
};

function parseDecimal(value: string | number | null | undefined): number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }

  const parsed = Number.parseFloat(value ?? '0');
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatModelLabel(modelKey: AttributionModelKey): string {
  return MODEL_META[modelKey].label;
}

function formatAllocationStatus(status: AttributionResultRow['record']['allocation_status']): string {
  if (status === 'no_eligible_touches') {
    return 'No eligible touches';
  }

  if (status === 'blocked_by_deterministic') {
    return 'Blocked by deterministic evidence';
  }

  if (status === 'unattributed') {
    return 'Unattributed';
  }

  return 'Attributed';
}

function formatChannelLabel(source: string | null | undefined, medium: string | null | undefined): string {
  if (!source && !medium) {
    return 'Unspecified';
  }

  return `${source ?? 'Unknown'} / ${medium ?? 'Unknown'}`;
}

function buildModelTotals(rows: AttributionChannelTotalRow[]) {
  return rows.reduce<Record<AttributionModelKey, { revenue: number; orders: number; credits: number }>>(
    (accumulator, row) => {
      const current = accumulator[row.modelKey] ?? { revenue: 0, orders: 0, credits: 0 };
      current.revenue += parseDecimal(row.revenueCredited);
      current.orders += row.orderCount;
      current.credits += parseDecimal(row.creditWeightTotal);
      accumulator[row.modelKey] = current;
      return accumulator;
    },
    {
      first_touch: { revenue: 0, orders: 0, credits: 0 },
      last_touch: { revenue: 0, orders: 0, credits: 0 },
      last_non_direct: { revenue: 0, orders: 0, credits: 0 },
      linear: { revenue: 0, orders: 0, credits: 0 },
      clicks_only: { revenue: 0, orders: 0, credits: 0 },
      hinted_fallback_only: { revenue: 0, orders: 0, credits: 0 }
    }
  );
}

function buildComparisonRows(rows: AttributionChannelTotalRow[]) {
  const map = new Map<
    string,
    {
      key: string;
      source: string | null;
      medium: string | null;
      label: string;
      totalRevenue: number;
      cells: Partial<Record<AttributionModelKey, AttributionChannelTotalRow>>;
    }
  >();

  for (const row of rows) {
    const key = `${row.source ?? 'none'}::${row.medium ?? 'none'}`;
    const existing = map.get(key) ?? {
      key,
      source: row.source,
      medium: row.medium,
      label: formatChannelLabel(row.source, row.medium),
      totalRevenue: 0,
      cells: {}
    };

    existing.totalRevenue += parseDecimal(row.revenueCredited);
    existing.cells[row.modelKey] = row;
    map.set(key, existing);
  }

  return Array.from(map.values()).sort((left, right) => right.totalRevenue - left.totalRevenue || left.label.localeCompare(right.label));
}

function buildActiveSummary(rows: AttributionResultRow[]) {
  return rows.reduce(
    (accumulator, row) => {
      accumulator.orderCount += 1;
      accumulator.revenue += parseDecimal(row.record.total_revenue_credited);
      accumulator.attributedCount += row.record.allocation_status === 'attributed' ? 1 : 0;
      accumulator.touchpoints += row.record.touchpoint_count_considered;
      return accumulator;
    },
    {
      orderCount: 0,
      revenue: 0,
      attributedCount: 0,
      touchpoints: 0
    }
  );
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <Card padding="compact" className="border-line/70">
      <Eyebrow>{label}</Eyebrow>
      <MetricValue>{value}</MetricValue>
      <MetricCopy>{detail}</MetricCopy>
    </Card>
  );
}

function ExplanationMetrics({
  summary,
  explainability,
  reportingTimezone
}: {
  summary: AttributionExplainabilityResponse['summaries'][number] | null;
  explainability: AttributionExplainabilityResponse;
  reportingTimezone: string;
}) {
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      <MetricCard
        label="Allocation status"
        value={summary ? formatAllocationStatus(summary.allocation_status) : 'Not available'}
        detail={summary ? `${formatNumber(summary.touchpoint_count_considered)} touchpoints considered` : 'No model summary was returned.'}
      />
      <MetricCard
        label="Credited revenue"
        value={summary ? formatCurrency(parseDecimal(summary.total_revenue_credited)) : formatCurrency(null)}
        detail={summary ? `${formatNumber(summary.eligible_click_count)} clicks · ${formatNumber(summary.eligible_view_count)} views` : 'No eligible counts returned.'}
      />
      <MetricCard
        label="Selected run"
        value={explainability.run.id.slice(0, 8)}
        detail={`Created ${formatDateTimeLabel(explainability.run.createdAtUtc, reportingTimezone)}`}
      />
      <MetricCard
        label="Run window"
        value={`${explainability.run.lookbackClickWindowDays}d click / ${explainability.run.lookbackViewWindowDays}d view`}
        detail={explainability.run.windowStartUtc ? `Run window started ${formatDateTimeLabel(explainability.run.windowStartUtc, reportingTimezone)}` : 'Using the v1 lookback contract.'}
      />
    </div>
  );
}

function TouchpointTable({
  touchpoints,
  reportingTimezone
}: {
  touchpoints: AttributionExplainabilityTouchpoint[];
  reportingTimezone: string;
}) {
  return (
    <TableWrap className="max-h-[28rem]">
      <Table caption="Attribution touchpoints">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Time</TableHeaderCell>
            <TableHeaderCell>Touchpoint</TableHeaderCell>
            <TableHeaderCell>Eligibility</TableHeaderCell>
            <TableHeaderCell>Evidence</TableHeaderCell>
            <TableHeaderCell>Reason</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {touchpoints.length === 0 ? (
            <TableEmptyRow
              colSpan={5}
              title="No touchpoints returned"
              description="The selected order did not return any normalized attribution touchpoints."
            />
          ) : null}
          {touchpoints.map((touchpoint) => (
            <TableRow key={touchpoint.touchpointId}>
              <TableCell>{formatDateTimeLabel(touchpoint.touchpointOccurredAtUtc, reportingTimezone)}</TableCell>
              <TableCell>
                <div className="grid gap-1 text-body text-ink-muted">
                  <strong className="text-ink">{formatChannelLabel(touchpoint.source, touchpoint.medium)}</strong>
                  <span>
                    {touchpoint.engagementType} · {touchpoint.touchpointSourceKind}
                    {touchpoint.clickIdType && touchpoint.clickIdValue ? ` · ${touchpoint.clickIdType}: ${touchpoint.clickIdValue}` : ''}
                  </span>
                </div>
              </TableCell>
              <TableCell>
                <Badge tone={touchpoint.isEligible ? 'success' : 'warning'}>
                  {touchpoint.isEligible ? 'Eligible' : 'Excluded'}
                </Badge>
              </TableCell>
              <TableCell>{touchpoint.evidenceSource}</TableCell>
              <TableCell>{touchpoint.ineligibilityReason ?? touchpoint.attributionReason ?? 'Not available'}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableWrap>
  );
}

function CreditsTable({
  credits,
  reportingTimezone
}: {
  credits: AttributionCreditRecordV1[];
  reportingTimezone: string;
}) {
  return (
    <TableWrap className="max-h-[24rem]">
      <Table caption="Attribution credits">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Position</TableHeaderCell>
            <TableHeaderCell>Touchpoint</TableHeaderCell>
            <TableHeaderCell>Occurred at</TableHeaderCell>
            <TableHeaderCell>Revenue credit</TableHeaderCell>
            <TableHeaderCell>Weight</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {credits.length === 0 ? (
            <TableEmptyRow
              colSpan={5}
              title="No credits returned"
              description="This model did not credit any touchpoints for the selected order."
            />
          ) : null}
          {credits.map((credit) => (
            <TableRow key={`${credit.touchpoint_id}-${credit.touchpoint_position}`}>
              <TableCell>{formatNumber(credit.touchpoint_position)}</TableCell>
              <TableCell>
                <div className="grid gap-1 text-body text-ink-muted">
                  <strong className="text-ink">{formatChannelLabel(credit.source, credit.medium)}</strong>
                  <span>{credit.attribution_reason}</span>
                </div>
              </TableCell>
              <TableCell>{formatDateTimeLabel(credit.occurred_at_utc, reportingTimezone)}</TableCell>
              <TableCell>{formatCurrency(parseDecimal(credit.revenue_credit))}</TableCell>
              <TableCell>{parseDecimal(credit.credit_weight).toFixed(2)}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableWrap>
  );
}

function ExplainabilityTable({ rows }: { rows: AttributionExplainRecordV1[] }) {
  return (
    <TableWrap className="max-h-[24rem]">
      <Table caption="Explainability decisions">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Stage</TableHeaderCell>
            <TableHeaderCell>Decision</TableHeaderCell>
            <TableHeaderCell>Touchpoint</TableHeaderCell>
            <TableHeaderCell>Reason</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.length === 0 ? (
            <TableEmptyRow
              colSpan={4}
              title="No explainability rows returned"
              description="No audit decisions were returned for the selected order and model."
            />
          ) : null}
          {rows.map((row, index) => (
            <TableRow key={`${row.model_key ?? 'order'}-${row.touchpoint_id ?? 'none'}-${index}`}>
              <TableCell>{row.explain_stage}</TableCell>
              <TableCell>{row.decision}</TableCell>
              <TableCell>{row.touchpoint_id ?? 'Order-level'}</TableCell>
              <TableCell>{row.decision_reason}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableWrap>
  );
}

export default function AttributionDashboard({
  filters,
  onFiltersChange,
  onClearFilters,
  activeModel,
  onActiveModelChange,
  reportingTimezone,
  resultsSection,
  channelTotalsSection,
  explainabilitySection,
  selectedOrderId,
  onInspectOrder
}: AttributionDashboardProps) {
  const [orderPage, setOrderPage] = useState(1);
  const activeResults = resultsSection.data ?? [];
  const activeSummary = useMemo(() => buildActiveSummary(activeResults), [activeResults]);
  const channelTotals = channelTotalsSection.data?.rows ?? [];
  const modelTotals = useMemo(() => buildModelTotals(channelTotals), [channelTotals]);
  const comparisonRows = useMemo(() => buildComparisonRows(channelTotals), [channelTotals]);
  const pageSize = 8;
  const paginatedOrders = useMemo(() => {
    const start = (orderPage - 1) * pageSize;
    return activeResults.slice(start, start + pageSize);
  }, [activeResults, orderPage]);
  const totalOrderPages = Math.max(1, Math.ceil(activeResults.length / pageSize));

  const activeExplainability = explainabilitySection.data;
  const selectedSummary =
    activeExplainability?.summaries.find((summary) => summary.model_key === activeModel) ?? null;
  const selectedCredits =
    activeExplainability?.credits.filter((credit) => credit.model_key === activeModel) ?? [];
  const selectedExplainRows =
    activeExplainability?.explainability.filter((row) => row.model_key === null || row.model_key === activeModel) ?? [];

  const lookbackClickWindowDays =
    channelTotalsSection.data?.lookbackClickWindowDays ??
    resultsSection.data?.[0]?.run.lookbackClickWindowDays ??
    28;
  const lookbackViewWindowDays =
    channelTotalsSection.data?.lookbackViewWindowDays ??
    resultsSection.data?.[0]?.run.lookbackViewWindowDays ??
    7;

  return (
    <section className="grid gap-section">
      <Panel
        title="Attribution engine v1"
        description="These six models are alternative views of the same orders. Compare them side-by-side, then inspect one order without mixing winner rules or fallback semantics."
        wide
      >
        <div className="grid gap-6">
          <div className="flex flex-wrap items-center gap-3">
            <Badge tone="brand">Alternative views, not one universal truth</Badge>
            <StatusPill>{lookbackClickWindowDays}-day click lookback</StatusPill>
            <StatusPill>{lookbackViewWindowDays}-day view lookback</StatusPill>
          </div>

          <FieldGrid className="xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)_minmax(0,1fr)]">
            <Field label="Date range" description="Attribution results are scoped by order occurrence date in the reporting timezone." wide>
              <div className="grid gap-3 sm:grid-cols-2">
                <Input
                  type="date"
                  value={filters.startDate}
                  onChange={(event) => onFiltersChange({ ...filters, startDate: event.target.value })}
                />
                <Input
                  type="date"
                  value={filters.endDate}
                  onChange={(event) => onFiltersChange({ ...filters, endDate: event.target.value })}
                />
              </div>
            </Field>
            <Field label="Source / medium" description="Use these filters before comparing channels so every model is operating on the same scoped set.">
              <div className="grid gap-3 sm:grid-cols-2">
                <Input
                  placeholder="google"
                  value={filters.source ?? ''}
                  onChange={(event) => onFiltersChange({ ...filters, source: event.target.value })}
                />
                <Input
                  placeholder="cpc"
                  value={filters.medium ?? ''}
                  onChange={(event) => onFiltersChange({ ...filters, medium: event.target.value })}
                />
              </div>
            </Field>
            <Field label="Campaign / order" description="Narrow the comparison when you need one campaign cohort or a single order drill-in.">
              <div className="grid gap-3 sm:grid-cols-2">
                <Input
                  placeholder="spring-sale"
                  value={filters.campaign ?? ''}
                  onChange={(event) => onFiltersChange({ ...filters, campaign: event.target.value })}
                />
                <Input
                  placeholder="order-1001"
                  value={filters.orderId ?? ''}
                  onChange={(event) => onFiltersChange({ ...filters, orderId: event.target.value })}
                />
              </div>
            </Field>
          </FieldGrid>

          <ButtonRow>
            <Button type="button" tone="ghost" onClick={onClearFilters}>
              Clear attribution filters
            </Button>
          </ButtonRow>
        </div>
      </Panel>

      <Panel
        title="Model selector"
        description="Switch the active model for the order table and rationale panel. The comparison matrix below always keeps all six models visible side-by-side."
        wide
      >
        <div className="grid gap-4 xl:grid-cols-3">
          {MODEL_ORDER.map((modelKey) => {
            const model = MODEL_META[modelKey];
            const totals = modelTotals[modelKey];
            const active = modelKey === activeModel;

            return (
              <button
                key={modelKey}
                type="button"
                onClick={() => {
                  setOrderPage(1);
                  onActiveModelChange(modelKey);
                }}
                className={[
                  'rounded-panel border p-panel text-left transition',
                  active
                    ? 'border-teal/35 bg-teal-soft/65 shadow-panel'
                    : 'border-line/70 bg-surface-alt/65 hover:-translate-y-0.5 hover:border-brand/25 hover:bg-surface'
                ].join(' ')}
                aria-pressed={active}
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <Eyebrow>{model.label}</Eyebrow>
                    <CardTitle className="mt-2">{formatCurrency(totals.revenue)}</CardTitle>
                    <CardDescription>{model.description}</CardDescription>
                  </div>
                  <Badge tone={model.tone}>{active ? 'Active' : 'Available'}</Badge>
                </div>
                <div className="mt-4 grid gap-2 text-body text-ink-muted sm:grid-cols-2">
                  <span>{formatNumber(totals.orders)} orders with non-zero credit</span>
                  <span>{parseDecimal(totals.credits).toFixed(2)} total credit weight</span>
                </div>
              </button>
            );
          })}
        </div>
      </Panel>

      <Panel
        title="Channel comparison"
        description="Revenue and order counts are grouped by credited channel. This is the side-by-side view that keeps model logic explicit instead of collapsing everything into one blended total."
        wide
      >
        <SectionState
          loading={channelTotalsSection.loading}
          error={channelTotalsSection.error}
          empty={comparisonRows.length === 0}
          emptyLabel="No attributed channel credits were returned for the current filters."
        >
          <TableWrap className="max-h-[34rem]">
            <Table caption="Attributed channel comparison">
              <TableHead>
                <TableRow>
                  <TableHeaderCell>Channel</TableHeaderCell>
                  {MODEL_ORDER.map((modelKey) => (
                    <TableHeaderCell key={modelKey}>{formatModelLabel(modelKey)}</TableHeaderCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {comparisonRows.map((row) => (
                  <TableRow key={row.key}>
                    <TableCell>
                      <div className="grid gap-1 text-body text-ink-muted">
                        <strong className="text-ink">{row.label}</strong>
                        <span>{formatCurrency(row.totalRevenue)} total across all model views</span>
                      </div>
                    </TableCell>
                    {MODEL_ORDER.map((modelKey) => {
                      const cell = row.cells[modelKey];
                      return (
                        <TableCell key={`${row.key}-${modelKey}`}>
                          {cell ? (
                            <div className="grid gap-1 text-body text-ink-muted">
                              <strong className="text-ink">{formatCurrency(parseDecimal(cell.revenueCredited))}</strong>
                              <span>{formatNumber(cell.orderCount)} orders</span>
                            </div>
                          ) : (
                            <span className="text-ink-muted">No credit</span>
                          )}
                        </TableCell>
                      );
                    })}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableWrap>
        </SectionState>
      </Panel>

      <div className="grid gap-section xl:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
        <Panel
          title={`${formatModelLabel(activeModel)} order outputs`}
          description={MODEL_META[activeModel].description}
          wide={false}
        >
          <div className="grid gap-4">
            <div className="grid gap-4 sm:grid-cols-3">
              <MetricCard
                label="Orders"
                value={formatNumber(activeSummary.orderCount)}
                detail={`${formatNumber(activeSummary.attributedCount)} attributed under this model`}
              />
              <MetricCard
                label="Revenue"
                value={formatCurrency(activeSummary.revenue)}
                detail="Summed from model summary rows"
              />
              <MetricCard
                label="Touchpoints"
                value={formatNumber(activeSummary.touchpoints)}
                detail="Eligible touchpoints considered across the visible rows"
              />
            </div>

            <SectionState
              loading={resultsSection.loading}
              error={resultsSection.error}
              empty={activeResults.length === 0}
              emptyLabel={`No ${formatModelLabel(activeModel).toLowerCase()} results matched the current filters.`}
            >
              <div className="grid gap-4">
                <div className="flex flex-wrap items-center justify-between gap-3 text-body text-ink-muted">
                  <span>{formatNumber(activeResults.length)} order summaries returned for the active model</span>
                  <span>Page {formatNumber(orderPage)} of {formatNumber(totalOrderPages)}</span>
                </div>
                <TableWrap className="max-h-[34rem]">
                  <Table caption="Attribution order results">
                    <TableHead>
                      <TableRow>
                        <TableHeaderCell>Order</TableHeaderCell>
                        <TableHeaderCell>Status</TableHeaderCell>
                        <TableHeaderCell>Winning touchpoint</TableHeaderCell>
                        <TableHeaderCell>Credited revenue</TableHeaderCell>
                        <TableHeaderCell>Eligible counts</TableHeaderCell>
                        <TableHeaderCell>Rationale</TableHeaderCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {paginatedOrders.length === 0 ? (
                        <TableEmptyRow
                          colSpan={6}
                          title="No orders on this page"
                          description="Try changing the filters or switching to another model."
                        />
                      ) : null}
                      {paginatedOrders.map((row) => (
                        <TableRow key={`${row.record.run_id}-${row.record.order_id}`}>
                          <TableCell>
                            <div className="grid gap-1 text-body text-ink-muted">
                              <strong className="text-ink">{row.record.order_id}</strong>
                              <span>{formatDateTimeLabel(row.orderOccurredAtUtc, reportingTimezone)}</span>
                            </div>
                          </TableCell>
                          <TableCell>
                            <Badge tone={row.record.allocation_status === 'attributed' ? 'success' : 'warning'}>
                              {formatAllocationStatus(row.record.allocation_status)}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <div className="grid gap-1 text-body text-ink-muted">
                              <strong className="text-ink">
                                {row.primaryTouchpoint
                                  ? formatChannelLabel(row.primaryTouchpoint.source, row.primaryTouchpoint.medium)
                                  : 'No credited touchpoint'}
                              </strong>
                              <span>{row.record.winner_evidence_source ?? 'No winner evidence source'}</span>
                            </div>
                          </TableCell>
                          <TableCell>{formatCurrency(parseDecimal(row.record.total_revenue_credited))}</TableCell>
                          <TableCell>
                            {formatNumber(row.record.eligible_click_count)} clicks · {formatNumber(row.record.eligible_view_count)} views
                          </TableCell>
                          <TableCell>
                            <div className="grid gap-3">
                              <span className="text-body text-ink-muted">
                                {row.record.winner_attribution_reason ?? 'No winner reason was returned.'}
                              </span>
                              <Button
                                type="button"
                                tone="ghost"
                                onClick={() => onInspectOrder(row.record.order_id, row.record.run_id)}
                                aria-label={`Inspect rationale for order ${row.record.order_id}`}
                              >
                                Inspect rationale
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableWrap>
                <div className="flex flex-wrap gap-3">
                  <Button
                    type="button"
                    tone="ghost"
                    disabled={orderPage <= 1}
                    onClick={() => setOrderPage((current) => Math.max(1, current - 1))}
                  >
                    Previous page
                  </Button>
                  <Button
                    type="button"
                    tone="ghost"
                    disabled={orderPage >= totalOrderPages}
                    onClick={() => setOrderPage((current) => Math.min(totalOrderPages, current + 1))}
                  >
                    Next page
                  </Button>
                </div>
              </div>
            </SectionState>
          </div>
        </Panel>

        <Panel
          title="Order rationale"
          description="Inspect one order with the active model applied. Candidate touchpoints, credited rows, and explainability decisions all stay on the same screen."
          wide={false}
        >
          <SectionState
            loading={explainabilitySection.loading}
            error={explainabilitySection.error}
            empty={!activeExplainability}
            emptyLabel={selectedOrderId ? `No explainability payload was returned for order ${selectedOrderId}.` : 'Choose an order from the active model table to inspect its rationale.'}
          >
            <div className="grid gap-5">
              {activeExplainability ? (
                <>
                  <div className="flex flex-wrap items-center justify-between gap-3 rounded-card border border-line/60 bg-surface-alt/55 px-4 py-4">
                    <div>
                      <Eyebrow>Selected order</Eyebrow>
                      <CardTitle className="mt-2">{activeExplainability.orderId}</CardTitle>
                      <CardDescription>
                        {formatModelLabel(activeModel)} is shown here. Other models remain available in the comparison matrix above.
                      </CardDescription>
                    </div>
                    <Badge tone="brand">{activeExplainability.selectedRunReason === 'explicit_run_id' ? 'Explicit run' : 'Latest run'}</Badge>
                  </div>

                  <ExplanationMetrics
                    summary={selectedSummary}
                    explainability={activeExplainability}
                    reportingTimezone={reportingTimezone}
                  />

                  <Card padding="compact" className="border-line/70">
                    <CardHeader>
                      <div>
                        <CardTitle>Model outcome</CardTitle>
                        <CardDescription>The active model summary stays separate from touchpoint and audit detail so the winner rule remains explicit.</CardDescription>
                      </div>
                    </CardHeader>
                    <DetailList className="xl:grid-cols-2">
                      <div>
                        <dt>Winner selection rule</dt>
                        <dd>{selectedSummary?.winner_selection_rule ?? activeModel}</dd>
                      </div>
                      <div>
                        <dt>Lookback rule</dt>
                        <dd>{selectedSummary?.lookback_rule_applied ?? 'mixed'}</dd>
                      </div>
                      <div>
                        <dt>Winner reason</dt>
                        <dd>{selectedSummary?.winner_attribution_reason ?? 'Not available'}</dd>
                      </div>
                      <div>
                        <dt>Direct suppression applied</dt>
                        <dd>{selectedSummary?.direct_suppression_applied ? 'Yes' : 'No'}</dd>
                      </div>
                      <div>
                        <dt>Deterministic block applied</dt>
                        <dd>{selectedSummary?.deterministic_block_applied ? 'Yes' : 'No'}</dd>
                      </div>
                      <div>
                        <dt>Normalization failures</dt>
                        <dd>{formatNumber(selectedSummary?.normalization_failures_count ?? 0)}</dd>
                      </div>
                    </DetailList>
                  </Card>

                  <Card padding="compact" className="border-line/70">
                    <CardHeader>
                      <div>
                        <CardTitle>Eligible touchpoints</CardTitle>
                        <CardDescription>These rows show what entered the decision process before the active model picked a winner or split credit.</CardDescription>
                      </div>
                    </CardHeader>
                    <TouchpointTable touchpoints={activeExplainability.touchpoints} reportingTimezone={reportingTimezone} />
                  </Card>

                  <Card padding="compact" className="border-line/70">
                    <CardHeader>
                      <div>
                        <CardTitle>Credited rows</CardTitle>
                        <CardDescription>Touchpoints with non-zero credit under the active model only.</CardDescription>
                      </div>
                    </CardHeader>
                    <CreditsTable credits={selectedCredits} reportingTimezone={reportingTimezone} />
                  </Card>

                  <Card padding="compact" className="border-line/70">
                    <CardHeader>
                      <div>
                        <CardTitle>Explainability audit trail</CardTitle>
                        <CardDescription>Decision records from candidate extraction, eligibility filtering, scoring, and fallback handling.</CardDescription>
                      </div>
                    </CardHeader>
                    <ExplainabilityTable rows={selectedExplainRows} />
                  </Card>
                </>
              ) : null}
            </div>
          </SectionState>
        </Panel>
      </div>
    </section>
  );
}
