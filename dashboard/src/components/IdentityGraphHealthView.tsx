import React, { useEffect, useMemo, useState } from 'react';

import type {
  IdentityConflictRow,
  IdentityConflictsResponse,
  IdentityHealthFilters,
  IdentityHealthOverviewResponse
} from '../lib/api';
import { formatDateLabel, formatDateTimeLabel, formatNumber } from '../lib/format';
import {
  Badge,
  Button,
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
  EmptyState,
  Eyebrow,
  Field,
  FieldGrid,
  Input,
  MetricCopy,
  MetricValue,
  Panel,
  SectionState,
  Select
} from './AuthenticatedUi';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type IdentityGraphHealthViewProps = {
  filters: IdentityHealthFilters;
  onFiltersChange: (next: IdentityHealthFilters) => void;
  onRefresh: () => void;
  reportingTimezone: string;
  overviewSection: AsyncSection<IdentityHealthOverviewResponse>;
  conflictsSection: AsyncSection<IdentityConflictsResponse>;
};

const SOURCE_OPTIONS = [
  { value: '', label: 'All sources' },
  { value: 'shopify_order_webhook', label: 'Shopify webhook' },
  { value: 'tracking_event', label: 'Tracking event' },
  { value: 'backfill', label: 'Backfill' },
  { value: 'admin_repair', label: 'Admin repair' },
  { value: 'tracking_sessions', label: 'Tracking sessions' },
  { value: 'tracking_events', label: 'Tracking events' },
  { value: 'shopify_orders', label: 'Shopify orders' },
  { value: 'shopify_customers', label: 'Shopify customers' }
] as const;

function titleizeToken(value: string | null | undefined) {
  if (!value) {
    return 'Unknown';
  }

  return value
    .split('_')
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
}

function formatConflictBadgeTone(status: IdentityConflictRow['journeyStatus']) {
  if (status === 'conflicted') {
    return 'danger';
  }

  if (status === 'quarantined') {
    return 'warning';
  }

  return 'neutral';
}

function MetricCard({
  label,
  value,
  detail,
  tone
}: {
  label: string;
  value: string;
  detail: string;
  tone: 'brand' | 'teal' | 'warning' | 'danger';
}) {
  return (
    <Card padding="compact" className="min-h-[11.5rem] border-line/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(240,246,242,0.86))]">
      <CardHeader className="mb-2">
        <div>
          <Eyebrow>{label}</Eyebrow>
          <MetricValue className="mt-4">{value}</MetricValue>
        </div>
        <Badge tone={tone}>{label}</Badge>
      </CardHeader>
      <MetricCopy className="mt-0">{detail}</MetricCopy>
    </Card>
  );
}

function DailySeriesSummary({
  points,
  reportingTimezone
}: {
  points: IdentityHealthOverviewResponse['series'];
  reportingTimezone: string;
}) {
  if (points.length === 0) {
    return (
      <EmptyState
        title="No identity traffic in range"
        description="No identity-ingestion activity was recorded for the selected filters."
        compact
      />
    );
  }

  const maxValue = Math.max(
    ...points.map((point) => Math.max(point.linked, point.conflicts, point.mergeRuns, point.quarantinedNodes, 1))
  );

  return (
    <div className="grid gap-3">
      {points.map((point) => {
        const linkedWidth = Math.max(4, Math.round((point.linked / maxValue) * 100));
        const conflictWidth = Math.max(0, Math.round((point.conflicts / maxValue) * 100));

        return (
          <div key={point.date} className="grid gap-2 rounded-card border border-line/60 bg-surface-alt/55 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="font-semibold text-ink">{formatDateLabel(point.date, reportingTimezone)}</p>
                <p className="text-body text-ink-muted">
                  {formatNumber(point.linked)} linked, {formatNumber(point.conflicts)} conflicts, {formatNumber(point.mergeRuns)} merge runs
                </p>
              </div>
              <Badge tone={point.conflicts > 0 ? 'warning' : 'teal'}>
                {formatNumber(point.quarantinedNodes)} quarantined
              </Badge>
            </div>
            <div className="grid gap-2">
              <div className="h-2 overflow-hidden rounded-pill bg-canvas-tint">
                <div className="h-full rounded-pill bg-teal" style={{ width: `${linkedWidth}%` }} />
              </div>
              <div className="h-2 overflow-hidden rounded-pill bg-canvas-tint">
                <div className="h-full rounded-pill bg-warning" style={{ width: `${conflictWidth}%` }} />
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function ConflictDrilldown({
  conflicts,
  reportingTimezone
}: {
  conflicts: IdentityConflictRow[];
  reportingTimezone: string;
}) {
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(conflicts[0]?.edgeId ?? null);

  useEffect(() => {
    setSelectedEdgeId(conflicts[0]?.edgeId ?? null);
  }, [conflicts]);

  const selectedConflict = useMemo(
    () => conflicts.find((conflict) => conflict.edgeId === selectedEdgeId) ?? conflicts[0] ?? null,
    [conflicts, selectedEdgeId]
  );

  if (conflicts.length === 0) {
    return (
      <EmptyState
        title="No conflicts in range"
        description="The current filter window did not return any quarantined or conflicted identity edges."
        compact
      />
    );
  }

  return (
    <div className="grid gap-5 xl:grid-cols-[minmax(0,1.25fr)_minmax(18rem,0.75fr)]">
      <div className="min-w-0 overflow-auto overscroll-x-contain rounded-card border border-line/60 bg-surface/65">
        <table className="min-w-[44rem] border-collapse [&_td]:border-b [&_td]:border-line/50 [&_td]:px-4 [&_td]:py-4 [&_td]:text-left [&_td]:align-top [&_td]:text-body [&_th]:border-b [&_th]:border-line/50 [&_th]:px-4 [&_th]:py-4 [&_th]:text-left [&_th]:text-caption [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.14em] [&_th]:text-ink-muted">
          <caption className="sr-only">Identity graph conflicts</caption>
          <thead>
            <tr>
              <th scope="col">Conflict</th>
              <th scope="col">Node</th>
              <th scope="col">Journey</th>
              <th scope="col">Observed</th>
            </tr>
          </thead>
          <tbody>
            {conflicts.map((conflict) => {
              const selected = conflict.edgeId === selectedConflict?.edgeId;

              return (
                <tr key={conflict.edgeId} className={selected ? 'bg-brand-soft/45' : undefined}>
                  <td>
                    <button
                      type="button"
                      className="grid gap-2 text-left"
                      onClick={() => setSelectedEdgeId(conflict.edgeId)}
                      aria-label={`Open conflict ${conflict.conflictCode}`}
                    >
                      <strong className="text-ink">{titleizeToken(conflict.conflictCode)}</strong>
                      <span className="text-ink-muted">
                        {titleizeToken(conflict.evidenceSource)} from {conflict.sourceTable ?? 'unknown source'}
                      </span>
                    </button>
                  </td>
                  <td>
                    <div className="grid gap-1">
                      <span className="font-semibold text-ink">{titleizeToken(conflict.nodeType)}</span>
                      <span className="text-ink-muted">{conflict.nodeKey}</span>
                    </div>
                  </td>
                  <td>
                    <div className="grid gap-2">
                      <span className="text-ink">{conflict.journeyId}</span>
                      <Badge tone={formatConflictBadgeTone(conflict.journeyStatus)}>{conflict.journeyStatus}</Badge>
                    </div>
                  </td>
                  <td>{formatDateTimeLabel(conflict.updatedAt, reportingTimezone)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <Card tone="accent" className="h-full">
        <CardHeader>
          <div>
            <Eyebrow>Conflict drill-down</Eyebrow>
            <CardTitle className="mt-3">{titleizeToken(selectedConflict?.conflictCode ?? 'Conflict')}</CardTitle>
            <CardDescription className="mt-3">
              Inspect the affected node, owning journey, and source record before repair or replay.
            </CardDescription>
          </div>
        </CardHeader>

        {selectedConflict ? (
          <dl className="grid gap-4 text-body text-ink-soft [&_dt]:text-caption [&_dt]:font-semibold [&_dt]:uppercase [&_dt]:tracking-[0.14em] [&_dt]:text-ink-muted">
            <div>
              <dt>Journey</dt>
              <dd>{selectedConflict.journeyId}</dd>
            </div>
            <div>
              <dt>Authoritative Shopify customer</dt>
              <dd>{selectedConflict.authoritativeShopifyCustomerId ?? 'None attached'}</dd>
            </div>
            <div>
              <dt>Node</dt>
              <dd>
                {titleizeToken(selectedConflict.nodeType)}: {selectedConflict.nodeKey}
              </dd>
            </div>
            <div>
              <dt>Source record</dt>
              <dd>
                {selectedConflict.sourceTable ?? 'unknown'} / {selectedConflict.sourceRecordId ?? 'n/a'}
              </dd>
            </div>
            <div>
              <dt>Evidence source</dt>
              <dd>{selectedConflict.evidenceSource}</dd>
            </div>
            <div>
              <dt>First observed</dt>
              <dd>{formatDateTimeLabel(selectedConflict.firstObservedAt, reportingTimezone)}</dd>
            </div>
            <div>
              <dt>Last observed</dt>
              <dd>{formatDateTimeLabel(selectedConflict.lastObservedAt, reportingTimezone)}</dd>
            </div>
          </dl>
        ) : null}
      </Card>
    </div>
  );
}

export default function IdentityGraphHealthView({
  filters,
  onFiltersChange,
  onRefresh,
  reportingTimezone,
  overviewSection,
  conflictsSection
}: IdentityGraphHealthViewProps) {
  const summary = overviewSection.data?.summary ?? null;
  const latestRun = overviewSection.data?.backfill.latestRun ?? null;

  return (
    <section className="grid gap-section">
      <Panel
        title="Identity graph health"
        description="Operational view for merge activity, quarantines, unlinked session load, and the latest identity graph backfill status."
        wide
      >
        <div className="grid gap-6">
          <Card tone="teal" className="overflow-hidden">
            <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
              <div>
                <Eyebrow>Operator filters</Eyebrow>
                <CardTitle className="mt-3">Identity stitch telemetry</CardTitle>
                <CardDescription className="mt-3">
                  Filter by UTC date range and evidence source to inspect merge volume, conflicts, and session linkage pressure.
                </CardDescription>
              </div>
              <Button type="button" tone="secondary" onClick={onRefresh}>
                Refresh health metrics
              </Button>
            </div>

            <FieldGrid className="mt-6">
              <Field label="Start date">
                <Input
                  type="date"
                  value={filters.startDate}
                  onChange={(event) => onFiltersChange({ ...filters, startDate: event.target.value })}
                />
              </Field>
              <Field label="End date">
                <Input
                  type="date"
                  value={filters.endDate}
                  onChange={(event) => onFiltersChange({ ...filters, endDate: event.target.value })}
                />
              </Field>
              <Field label="Source">
                <Select
                  value={filters.source ?? ''}
                  onChange={(event) => onFiltersChange({ ...filters, source: event.target.value })}
                >
                  {SOURCE_OPTIONS.map((option) => (
                    <option key={option.value || 'all'} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </Select>
              </Field>
            </FieldGrid>
          </Card>

          <SectionState
            loading={overviewSection.loading}
            error={overviewSection.error}
            empty={!summary}
            emptyLabel="No identity health metrics were returned."
          >
            <div className="grid gap-6">
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <MetricCard
                  label="Merge runs"
                  value={formatNumber(summary?.mergeRuns)}
                  detail={`${formatNumber(summary?.rehomedNodes)} nodes re-homed into stronger journeys.`}
                  tone="brand"
                />
                <MetricCard
                  label="Conflicts"
                  value={formatNumber(summary?.conflictIngestions)}
                  detail={`${formatNumber(summary?.unresolvedConflicts)} conflict edges still active.`}
                  tone="danger"
                />
                <MetricCard
                  label="Unlinked sessions"
                  value={formatNumber(summary?.unlinkedSessions)}
                  detail={`${formatNumber(summary?.linkedSessions)} sessions already attached in the same window.`}
                  tone="warning"
                />
                <MetricCard
                  label="Backfill runs"
                  value={formatNumber(overviewSection.data?.backfill.completedRuns)}
                  detail={`${formatNumber(overviewSection.data?.backfill.activeRuns)} active and ${formatNumber(overviewSection.data?.backfill.failedRuns)} failed.`}
                  tone="teal"
                />
              </div>

              <div className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(18rem,0.85fr)]">
                <Card>
                  <CardHeader>
                    <div>
                      <Eyebrow>Daily activity</Eyebrow>
                      <CardTitle className="mt-3">Merge and conflict volume</CardTitle>
                      <CardDescription className="mt-3">
                        Daily linked outcomes, conflict spikes, and quarantine volume for the selected filter window.
                      </CardDescription>
                    </div>
                  </CardHeader>
                  <DailySeriesSummary points={overviewSection.data?.series ?? []} reportingTimezone={reportingTimezone} />
                </Card>

                <Card tone="accent">
                  <CardHeader>
                    <div>
                      <Eyebrow>Backfill status</Eyebrow>
                      <CardTitle className="mt-3">Latest identity graph run</CardTitle>
                      <CardDescription className="mt-3">
                        Use this to confirm whether backfill is still processing or whether the latest run needs manual repair.
                      </CardDescription>
                    </div>
                  </CardHeader>

                  {latestRun ? (
                    <dl className="grid gap-4 text-body text-ink-soft [&_dt]:text-caption [&_dt]:font-semibold [&_dt]:uppercase [&_dt]:tracking-[0.14em] [&_dt]:text-ink-muted">
                      <div>
                        <dt>Status</dt>
                        <dd>
                          <Badge
                            tone={
                              latestRun.status === 'completed'
                                ? 'success'
                                : latestRun.status === 'failed'
                                  ? 'danger'
                                  : 'warning'
                            }
                          >
                            {latestRun.status}
                          </Badge>
                        </dd>
                      </div>
                      <div>
                        <dt>Run ID</dt>
                        <dd>{latestRun.runId}</dd>
                      </div>
                      <div>
                        <dt>Worker</dt>
                        <dd>{latestRun.workerId}</dd>
                      </div>
                      <div>
                        <dt>Requested by</dt>
                        <dd>{latestRun.requestedBy}</dd>
                      </div>
                      <div>
                        <dt>Sources</dt>
                        <dd>{latestRun.sources.length > 0 ? latestRun.sources.join(', ') : 'All configured sources'}</dd>
                      </div>
                      <div>
                        <dt>Started</dt>
                        <dd>{formatDateTimeLabel(latestRun.startedAt, reportingTimezone)}</dd>
                      </div>
                      <div>
                        <dt>Completed</dt>
                        <dd>{latestRun.completedAt ? formatDateTimeLabel(latestRun.completedAt, reportingTimezone) : 'Still running'}</dd>
                      </div>
                      {latestRun.errorMessage ? (
                        <div>
                          <dt>Error</dt>
                          <dd>{latestRun.errorMessage}</dd>
                        </div>
                      ) : null}
                    </dl>
                  ) : (
                    <EmptyState
                      title="No backfill run in range"
                      description="Widen the date window if you need to inspect an older identity graph backfill run."
                      compact
                    />
                  )}
                </Card>
              </div>
            </div>
          </SectionState>
        </div>
      </Panel>

      <Panel
        title="Conflict drill-down"
        description="Recent quarantined and conflicted identity edges. Select a row to inspect the affected journey and source record."
        wide
      >
        <SectionState
          loading={conflictsSection.loading}
          error={conflictsSection.error}
          empty={(conflictsSection.data?.conflicts.length ?? 0) === 0}
          emptyLabel="No conflicts were returned for the selected window."
        >
          <ConflictDrilldown conflicts={conflictsSection.data?.conflicts ?? []} reportingTimezone={reportingTimezone} />
        </SectionState>
      </Panel>
    </section>
  );
}
