import type React from 'react';
import { useEffect, useMemo, useState } from 'react';

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
  FieldGrid,
  Form,
  Input,
  MetricCopy,
  MetricValue,
  Panel,
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
import {
  fetchExposureCoverage,
  fetchMmmExport,
  fetchMmmModelRuns,
  type ExposureCoverageResponse,
  type MmmExportQuery,
  type MmmExportResponse,
  type MmmExportRow,
  type MmmModelRunsResponse,
  type MmmReadinessStatus
} from '../lib/api';
import { formatCurrency, formatDateLabel, formatDateTimeLabel, formatNumber, formatPercent } from '../lib/format';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type MmmReadinessDashboardProps = {
  reportingTimezone: string;
};

type ChecklistStatus = 'pass' | 'warn' | 'fail' | 'pending';

type ChecklistItem = {
  label: string;
  owner: string;
  status: ChecklistStatus;
  detail: string;
};

type OwnerApproval = {
  owner: string;
  status: ChecklistStatus;
  detail: string;
};

const MS_PER_DAY = 24 * 60 * 60 * 1000;
const FRESHNESS_WARN_HOURS = 36;
const DEFAULT_LIMIT = 250;
const ATTRIBUTION_MODEL_OPTIONS = [
  'last_touch',
  'first_touch',
  'linear',
  'time_decay',
  'position_based',
  'rule_based_weighted'
] as const;

function createSection<T>(overrides: Partial<AsyncSection<T>> = {}): AsyncSection<T> {
  return {
    data: null,
    loading: false,
    error: null,
    ...overrides
  };
}

function formatDateInput(date: Date, reportingTimezone: string): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: reportingTimezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  return year && month && day ? `${year}-${month}-${day}` : date.toISOString().slice(0, 10);
}

function buildDefaultQuery(reportingTimezone: string): MmmExportQuery {
  const end = new Date();
  const start = new Date(end.getTime() - 89 * MS_PER_DAY);

  return {
    startDate: formatDateInput(start, reportingTimezone),
    endDate: formatDateInput(end, reportingTimezone),
    attributionModel: 'last_touch',
    limit: DEFAULT_LIMIT
  };
}

function hoursSince(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }

  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return (Date.now() - timestamp) / (60 * 60 * 1000);
}

function latestDate(values: Array<string | null | undefined>): string | null {
  return values.reduce<string | null>((latest, value) => {
    if (!value) {
      return latest;
    }

    return latest === null || value > latest ? value : latest;
  }, null);
}

function statusTone(status: ChecklistStatus): 'success' | 'warning' | 'danger' | 'neutral' | 'teal' {
  if (status === 'pass') {
    return 'success';
  }

  if (status === 'warn') {
    return 'warning';
  }

  if (status === 'fail') {
    return 'danger';
  }

  return 'neutral';
}

function readinessTone(status: MmmReadinessStatus): 'success' | 'warning' | 'danger' {
  if (status === 'ready') {
    return 'success';
  }

  return status === 'partial' ? 'warning' : 'danger';
}

function formatStatus(status: ChecklistStatus | MmmReadinessStatus): string {
  return status.replace(/_/g, ' ');
}

function isFresh(value: string | null): boolean {
  const ageHours = hoursSince(value);
  return ageHours !== null && ageHours <= FRESHNESS_WARN_HOURS;
}

export function deriveMmmChecklist(exportData: MmmExportResponse | null): ChecklistItem[] {
  if (!exportData) {
    return [
      {
        label: 'MMM mart schema',
        owner: 'Analytics',
        status: 'pending',
        detail: 'Waiting for the MMM export response.'
      },
      {
        label: 'Readiness window',
        owner: 'Product',
        status: 'pending',
        detail: 'Waiting for requested-date coverage.'
      },
      {
        label: 'Input freshness',
        owner: 'Frontend',
        status: 'pending',
        detail: 'Waiting for mart freshness timestamps.'
      }
    ];
  }

  const latestSpendSync = latestDate(exportData.rows.map((row) => row.spendLastSyncedAt));
  const latestShopifyIngest = latestDate(exportData.rows.map((row) => row.shopifyLastIngestedAt));
  const latestAttributionCompute = latestDate(exportData.rows.map((row) => row.attributionLastComputedAt));
  const freshnessPass =
    isFresh(latestSpendSync) && isFresh(latestShopifyIngest) && isFresh(latestAttributionCompute);

  return [
    {
      label: 'MMM mart schema',
      owner: 'Analytics',
      status: exportData.schemaVersion === 'mmm_daily_input_mart_v1' ? 'pass' : 'fail',
      detail: exportData.schemaVersion
    },
    {
      label: 'Readiness window',
      owner: 'Product',
      status:
        exportData.readiness.status === 'ready'
          ? 'pass'
          : exportData.readiness.status === 'partial'
            ? 'warn'
            : 'fail',
      detail: `${formatNumber(exportData.readiness.includedDateCount)} included days, ${formatNumber(
        exportData.readiness.excludedDateWindows.length
      )} excluded windows.`
    },
    {
      label: 'Paid media inputs',
      owner: 'Analytics',
      status: exportData.rows.some((row) => row.martRowType === 'paid_media') ? 'pass' : 'fail',
      detail: `${formatNumber(exportData.rows.filter((row) => row.martRowType === 'paid_media').length)} paid media rows sampled.`
    },
    {
      label: 'Attribution inputs',
      owner: 'Analytics',
      status: exportData.rows.some((row) => row.martRowType === 'attribution') ? 'pass' : 'warn',
      detail: `${formatNumber(exportData.rows.filter((row) => row.martRowType === 'attribution').length)} attribution rows sampled.`
    },
    {
      label: 'Input freshness',
      owner: 'Frontend',
      status: freshnessPass ? 'pass' : 'warn',
      detail: `Latest spend ${latestSpendSync ?? 'N/A'}, Shopify ${latestShopifyIngest ?? 'N/A'}, attribution ${latestAttributionCompute ?? 'N/A'}.`
    }
  ];
}

function deriveOwnerApprovals(checklist: ChecklistItem[], modelRuns: MmmModelRunsResponse | null): OwnerApproval[] {
  const productReady = checklist.find((item) => item.owner === 'Product')?.status;
  const analyticsFailures = checklist.filter((item) => item.owner === 'Analytics' && item.status === 'fail');
  const freshness = checklist.find((item) => item.label === 'Input freshness')?.status;

  return [
    {
      owner: 'Product',
      status: productReady === 'pass' ? 'pass' : productReady === 'warn' ? 'warn' : 'fail',
      detail: productReady === 'pass' ? 'Readiness window has full coverage.' : 'Readiness exclusions need sign-off.'
    },
    {
      owner: 'Analytics',
      status: analyticsFailures.length === 0 ? 'pass' : 'fail',
      detail:
        analyticsFailures.length === 0
          ? 'Schema and input rows are present for review.'
          : `${formatNumber(analyticsFailures.length)} analytics gate failed.`
    },
    {
      owner: 'Frontend',
      status: freshness === 'pass' ? 'pass' : 'warn',
      detail: freshness === 'pass' ? 'Freshness cards have current source timestamps.' : 'Freshness telemetry is incomplete or stale.'
    },
    {
      owner: 'Modeling',
      status: modelRuns && modelRuns.rows.length > 0 ? 'pass' : 'pending',
      detail:
        modelRuns && modelRuns.rows.length > 0
          ? `${formatNumber(modelRuns.rows.length)} baseline run output rows are available.`
          : 'Waiting on the model-runs read API.'
    }
  ];
}

function summarizeRows(rows: MmmExportRow[]) {
  return rows.reduce(
    (totals, row) => ({
      spend: totals.spend + row.spend,
      revenue: totals.revenue + row.shopifyRevenue + row.attributionCreditRevenue,
      impressions: totals.impressions + row.impressions,
      clicks: totals.clicks + row.clicks,
      orders: totals.orders + row.shopifyOrders + row.attributionCreditOrders
    }),
    {
      spend: 0,
      revenue: 0,
      impressions: 0,
      clicks: 0,
      orders: 0
    }
  );
}

export default function MmmReadinessDashboard({ reportingTimezone }: MmmReadinessDashboardProps) {
  const [query, setQuery] = useState<MmmExportQuery>(() => buildDefaultQuery(reportingTimezone));
  const [draftQuery, setDraftQuery] = useState<MmmExportQuery>(() => buildDefaultQuery(reportingTimezone));
  const [exportSection, setExportSection] = useState<AsyncSection<MmmExportResponse>>(createSection({ loading: true }));
  const [modelRunsSection, setModelRunsSection] = useState<AsyncSection<MmmModelRunsResponse>>(createSection({ loading: true }));
  const [exposureCoverageSection, setExposureCoverageSection] = useState<AsyncSection<ExposureCoverageResponse>>(
    createSection({ loading: true })
  );

  useEffect(() => {
    let cancelled = false;
    setExportSection(createSection({ loading: true }));
    setModelRunsSection(createSection({ loading: true }));
    setExposureCoverageSection(createSection({ loading: true }));

    fetchMmmExport(query)
      .then((response) => {
        if (!cancelled) {
          setExportSection(createSection({ data: response }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setExportSection(createSection({ error: error.message }));
        }
      });

    fetchMmmModelRuns({
      startDate: query.startDate,
      endDate: query.endDate,
      attributionModel: query.attributionModel,
      limit: 5
    })
      .then((response) => {
        if (!cancelled) {
          setModelRunsSection(createSection({ data: response }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setModelRunsSection(createSection({ error: error.message }));
        }
      });

    fetchExposureCoverage({
      startDate: query.startDate,
      endDate: query.endDate
    })
      .then((response) => {
        if (!cancelled) {
          setExposureCoverageSection(createSection({ data: response }));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setExposureCoverageSection(createSection({ error: error.message }));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [query]);

  const checklist = useMemo(() => deriveMmmChecklist(exportSection.data), [exportSection.data]);
  const approvals = useMemo(
    () => deriveOwnerApprovals(checklist, modelRunsSection.data),
    [checklist, modelRunsSection.data]
  );
  const summary = useMemo(() => summarizeRows(exportSection.data?.rows ?? []), [exportSection.data?.rows]);
  const latestSpendSync = latestDate((exportSection.data?.rows ?? []).map((row) => row.spendLastSyncedAt));
  const latestShopifyIngest = latestDate((exportSection.data?.rows ?? []).map((row) => row.shopifyLastIngestedAt));
  const latestAttributionCompute = latestDate((exportSection.data?.rows ?? []).map((row) => row.attributionLastComputedAt));
  const readiness = exportSection.data?.readiness;
  const exposureTotals = exposureCoverageSection.data?.totals;

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setQuery({
      ...draftQuery,
      limit: DEFAULT_LIMIT
    });
  }

  return (
    <section className="grid gap-section">
      <Panel
        title="MMM readiness"
        description="Readiness, approval, freshness, and baseline model status for the approved MMM input mart."
        wide
      >
        <Form onSubmit={handleSubmit}>
          <FieldGrid>
            <Field label="Start date">
              <Input
                type="date"
                value={draftQuery.startDate}
                onChange={(event) => setDraftQuery((current) => ({ ...current, startDate: event.target.value }))}
              />
            </Field>
            <Field label="End date">
              <Input
                type="date"
                value={draftQuery.endDate}
                onChange={(event) => setDraftQuery((current) => ({ ...current, endDate: event.target.value }))}
              />
            </Field>
            <Field label="Attribution model">
              <Select
                value={draftQuery.attributionModel ?? ''}
                onChange={(event) =>
                  setDraftQuery((current) => ({
                    ...current,
                    attributionModel: event.target.value || undefined
                  }))
                }
              >
                <option value="">All models</option>
                {ATTRIBUTION_MODEL_OPTIONS.map((model) => (
                  <option key={model} value={model}>
                    {model.replace(/_/g, ' ')}
                  </option>
                ))}
              </Select>
            </Field>
            <Field label="Platform">
              <Select
                value={draftQuery.platform ?? ''}
                onChange={(event) =>
                  setDraftQuery((current) => ({
                    ...current,
                    platform: (event.target.value || undefined) as MmmExportQuery['platform']
                  }))
                }
              >
                <option value="">All platforms</option>
                <option value="meta">Meta</option>
                <option value="google">Google</option>
                <option value="taxonomy">Taxonomy</option>
              </Select>
            </Field>
          </FieldGrid>
          <ButtonRow>
            <Button type="submit" disabled={exportSection.loading}>
              Refresh MMM status
            </Button>
          </ButtonRow>
        </Form>
      </Panel>

      <div className="grid gap-card lg:grid-cols-4">
        <Card padding="compact" tone="teal">
          <Eyebrow>Readiness</Eyebrow>
          <MetricValue>{readiness ? formatStatus(readiness.status) : 'Loading'}</MetricValue>
          <MetricCopy>
            {readiness
              ? `${formatNumber(readiness.includedDateCount)} included days, generated ${formatDateTimeLabel(
                  readiness.generationTimestamp,
                  reportingTimezone
                )}.`
              : 'Checking requested-window coverage.'}
          </MetricCopy>
        </Card>
        <Card padding="compact">
          <Eyebrow>MMM rows</Eyebrow>
          <MetricValue>{formatNumber(exportSection.data?.pagination.totalRows)}</MetricValue>
          <MetricCopy>{formatNumber(exportSection.data?.pagination.returned)} sampled rows in this dashboard load.</MetricCopy>
        </Card>
        <Card padding="compact">
          <Eyebrow>Spend</Eyebrow>
          <MetricValue>{formatCurrency(summary.spend)}</MetricValue>
          <MetricCopy>{formatNumber(summary.impressions)} impressions and {formatNumber(summary.clicks)} clicks.</MetricCopy>
        </Card>
        <Card padding="compact">
          <Eyebrow>Outcome signal</Eyebrow>
          <MetricValue>{formatCurrency(summary.revenue)}</MetricValue>
          <MetricCopy>{formatNumber(summary.orders)} blended Shopify and attribution orders.</MetricCopy>
        </Card>
      </div>

      <div className="grid gap-card xl:grid-cols-[1.1fr_0.9fr]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Foundation checklist</CardTitle>
              <CardDescription>Deterministic gates derived from the MMM export contract and sampled mart rows.</CardDescription>
            </div>
            {readiness ? <Badge tone={readinessTone(readiness.status)}>{formatStatus(readiness.status)}</Badge> : null}
          </CardHeader>
          <SectionState
            loading={exportSection.loading}
            error={exportSection.error}
            empty={checklist.length === 0}
            emptyLabel="No checklist items are available."
          >
            <div className="grid gap-3">
              {checklist.map((item) => (
                <div key={item.label} className="grid gap-3 rounded-card border border-line/60 bg-surface-alt/65 p-4 sm:grid-cols-[1fr_auto]">
                  <div>
                    <p className="font-semibold text-ink">{item.label}</p>
                    <p className="mt-1 text-body text-ink-muted">{item.detail}</p>
                  </div>
                  <div className="flex flex-wrap items-center gap-2 sm:justify-end">
                    <Badge tone="neutral">{item.owner}</Badge>
                    <Badge tone={statusTone(item.status)}>{formatStatus(item.status)}</Badge>
                  </div>
                </div>
              ))}
            </div>
          </SectionState>
        </Card>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Owner approvals</CardTitle>
              <CardDescription>Product, Analytics, Frontend, and Modeling approval status for the selected window.</CardDescription>
            </div>
          </CardHeader>
          <div className="grid gap-3">
            {approvals.map((approval) => (
              <div key={approval.owner} className="rounded-card border border-line/60 bg-surface-alt/65 p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <p className="font-semibold text-ink">{approval.owner}</p>
                  <Badge tone={statusTone(approval.status)}>{formatStatus(approval.status)}</Badge>
                </div>
                <p className="mt-2 text-body text-ink-muted">{approval.detail}</p>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div className="grid gap-card xl:grid-cols-3">
        {[
          ['Spend freshness', latestSpendSync],
          ['Shopify freshness', latestShopifyIngest],
          ['Attribution freshness', latestAttributionCompute]
        ].map(([label, value]) => (
          <Card key={label} padding="compact">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <Eyebrow>{label}</Eyebrow>
                <MetricValue>{formatDateTimeLabel(value, reportingTimezone)}</MetricValue>
              </div>
              <Badge tone={isFresh(value) ? 'success' : 'warning'}>{isFresh(value) ? 'fresh' : 'stale'}</Badge>
            </div>
          </Card>
        ))}
      </div>

      <div className="grid gap-card xl:grid-cols-[0.78fr_1.22fr]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Exposure coverage</CardTitle>
              <CardDescription>View and impression ingestion coverage for 7-day view attribution readiness.</CardDescription>
            </div>
            <Badge tone={exposureTotals && exposureTotals.identityResolutionRate !== null && exposureTotals.identityResolutionRate >= 0.8 ? 'success' : 'warning'}>
              {formatPercent(exposureTotals?.identityResolutionRate ?? null)}
            </Badge>
          </CardHeader>
          <SectionState
            loading={exposureCoverageSection.loading}
            error={exposureCoverageSection.error}
            empty={!exposureCoverageSection.data || exposureCoverageSection.data.rows.length === 0}
            emptyLabel="No exposure events were returned for this window."
            compact
          >
            <div className="grid gap-3 sm:grid-cols-3">
              <div className="rounded-card border border-line/60 bg-surface-alt/65 p-4">
                <Eyebrow>Total</Eyebrow>
                <MetricValue>{formatNumber(exposureTotals?.totalExposures)}</MetricValue>
                <MetricCopy>{formatNumber(exposureTotals?.validExposures)} valid exposure events.</MetricCopy>
              </div>
              <div className="rounded-card border border-line/60 bg-surface-alt/65 p-4">
                <Eyebrow>Identity</Eyebrow>
                <MetricValue>{formatPercent(exposureTotals?.identityResolutionRate ?? null)}</MetricValue>
                <MetricCopy>{formatNumber(exposureTotals?.identityResolvedExposures)} linked to identity graph.</MetricCopy>
              </div>
              <div className="rounded-card border border-line/60 bg-surface-alt/65 p-4">
                <Eyebrow>Campaign metadata</Eyebrow>
                <MetricValue>{formatPercent(exposureTotals?.campaignMetadataResolutionRate ?? null)}</MetricValue>
                <MetricCopy>{formatNumber(exposureTotals?.campaignMetadataResolvedExposures)} campaign joins resolved.</MetricCopy>
              </div>
            </div>
          </SectionState>
        </Card>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Exposure breakdown</CardTitle>
              <CardDescription>Daily validity, identity, and campaign metadata coverage by platform and exposure type.</CardDescription>
            </div>
          </CardHeader>
          <SectionState
            loading={exposureCoverageSection.loading}
            error={exposureCoverageSection.error}
            empty={!exposureCoverageSection.data || exposureCoverageSection.data.rows.length === 0}
            emptyLabel="No exposure breakdown rows were returned for this window."
            compact
          >
            <TableWrap>
              <Table caption="Exposure coverage breakdown">
                <TableHead>
                  <TableRow>
                    <TableHeaderCell>Date</TableHeaderCell>
                    <TableHeaderCell>Platform</TableHeaderCell>
                    <TableHeaderCell>Type</TableHeaderCell>
                    <TableHeaderCell>Total</TableHeaderCell>
                    <TableHeaderCell>Identity</TableHeaderCell>
                    <TableHeaderCell>Metadata</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {(exposureCoverageSection.data?.rows ?? []).slice(0, 12).map((row) => (
                    <TableRow key={`${row.date}-${row.sourcePlatform}-${row.exposureType}`}>
                      <TableCell>{formatDateLabel(row.date, reportingTimezone)}</TableCell>
                      <TableCell>{row.sourcePlatform.replace(/_/g, ' ')}</TableCell>
                      <TableCell>{row.exposureType}</TableCell>
                      <TableCell>{formatNumber(row.totalExposures)}</TableCell>
                      <TableCell>{formatPercent(row.identityResolutionRate)}</TableCell>
                      <TableCell>{formatPercent(row.campaignMetadataResolutionRate)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableWrap>
          </SectionState>
        </Card>
      </div>

      <div className="grid gap-card xl:grid-cols-[0.9fr_1.1fr]">
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Readiness-window failures</CardTitle>
              <CardDescription>Excluded dates that block or weaken model-training readiness.</CardDescription>
            </div>
          </CardHeader>
          <SectionState
            loading={exportSection.loading}
            error={exportSection.error}
            empty={(readiness?.excludedDateWindows.length ?? 0) === 0}
            emptyLabel="No excluded windows were returned for this MMM query."
            compact
          >
            <TableWrap>
              <Table caption="MMM readiness failures">
                <TableHead>
                  <TableRow>
                    <TableHeaderCell>Date</TableHeaderCell>
                    <TableHeaderCell>Reason</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {(readiness?.excludedDateWindows ?? []).map((window) => (
                    <TableRow key={`${window.startDate}-${window.reason}`}>
                      <TableCell>{formatDateLabel(window.startDate, reportingTimezone)}</TableCell>
                      <TableCell>{window.reason.replace(/_/g, ' ')}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableWrap>
          </SectionState>
        </Card>

        <Card>
          <CardHeader>
            <div>
              <CardTitle>Baseline model outputs</CardTitle>
              <CardDescription>Latest deterministic `baseline_linear_mmm_v1` run summaries when the model-runs API is available.</CardDescription>
            </div>
          </CardHeader>
          <SectionState
            loading={modelRunsSection.loading}
            error={
              modelRunsSection.error?.includes('404') || modelRunsSection.error?.includes('Cannot GET')
                ? null
                : modelRunsSection.error
            }
            empty={!modelRunsSection.data || modelRunsSection.data.rows.length === 0}
            emptyLabel={
              modelRunsSection.error?.includes('404') || modelRunsSection.error?.includes('Cannot GET')
                ? 'The MMM model-runs read API is not available yet. This panel is typed and ready to render baseline outputs once `/api/reporting/mmm/model-runs` exists.'
                : 'No baseline MMM runs were returned for this training window.'
            }
            compact
          >
            <TableWrap>
              <Table caption="MMM baseline model runs">
                <TableHead>
                  <TableRow>
                    <TableHeaderCell>Run</TableHeaderCell>
                    <TableHeaderCell>Status</TableHeaderCell>
                    <TableHeaderCell>Training window</TableHeaderCell>
                    <TableHeaderCell>Completed</TableHeaderCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {(modelRunsSection.data?.rows ?? []).map((run) => (
                    <TableRow key={run.id}>
                      <TableCell>{run.modelVersion}</TableCell>
                      <TableCell>
                        <Badge tone={run.runStatus === 'completed' ? 'success' : 'danger'}>{run.runStatus}</Badge>
                      </TableCell>
                      <TableCell>
                        {formatDateLabel(run.trainingStartDate, reportingTimezone)} to{' '}
                        {formatDateLabel(run.trainingEndDate, reportingTimezone)}
                      </TableCell>
                      <TableCell>{formatDateTimeLabel(run.completedAt, reportingTimezone)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableWrap>
          </SectionState>
        </Card>
      </div>
    </section>
  );
}
