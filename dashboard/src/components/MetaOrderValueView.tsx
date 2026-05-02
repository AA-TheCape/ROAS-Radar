import React, { useEffect, useMemo, useState } from 'react';

import {
  fetchMetaOrderValue,
  type MetaOrderValueResponse,
  type MetaOrderValueRow,
  type MetaOrderValueSortBy,
  type MetaOrderValueSortDirection
} from '../lib/api';
import { formatDateLabel, formatNumber } from '../lib/format';
import {
  Badge,
  Button,
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
  DataTableToolbar,
  Eyebrow,
  Field,
  Input,
  MetricCopy,
  MetricValue,
  Panel,
  PrimaryCell,
  SectionState,
  Select,
  SortableTableHeaderCell,
  Table,
  TableBody,
  TableCell,
  TableEmptyRow,
  TableFilterBar,
  TableHead,
  TableHeaderCell,
  TableMeta,
  TablePagination,
  TableRow,
  TableSearchField,
  TableWrap
} from './AuthenticatedUi';

const DEFAULT_REPORTING_TIMEZONE = 'America/Los_Angeles';
const PAGE_SIZE = 8;
const MS_PER_DAY = 24 * 60 * 60 * 1000;

type MetaOrderValueViewProps = {
  reportingTimezone: string;
};

type MetaOrderValueFilters = {
  startDate: string;
  endDate: string;
  campaignSearch: string;
  actionType: string;
  sortBy: MetaOrderValueSortBy;
  sortDirection: MetaOrderValueSortDirection;
  page: number;
};

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

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

function formatDateInput(date: Date, reportingTimezone = DEFAULT_REPORTING_TIMEZONE): string {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: reportingTimezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);

  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    return date.toISOString().slice(0, 10);
  }

  return `${year}-${month}-${day}`;
}

function buildRange(
  days: number,
  reportingTimezone = DEFAULT_REPORTING_TIMEZONE
): Pick<MetaOrderValueFilters, 'startDate' | 'endDate'> {
  const end = new Date();
  const start = new Date(end.getTime() - (days - 1) * MS_PER_DAY);

  return {
    startDate: formatDateInput(start, reportingTimezone),
    endDate: formatDateInput(end, reportingTimezone)
  };
}

function createDefaultFilters(reportingTimezone = DEFAULT_REPORTING_TIMEZONE): MetaOrderValueFilters {
  return {
    ...buildRange(30, reportingTimezone),
    campaignSearch: '',
    actionType: '',
    sortBy: 'reportDate',
    sortDirection: 'desc',
    page: 1
  };
}

function normalizeDateRange(filters: MetaOrderValueFilters): MetaOrderValueFilters {
  if (filters.startDate && filters.endDate && filters.startDate > filters.endDate) {
    return {
      ...filters,
      endDate: filters.startDate
    };
  }

  return filters;
}

function formatCurrencyForCode(value: number | null | undefined, currencyCode: string | null | undefined) {
  if (value == null || !Number.isFinite(value)) {
    return 'N/A';
  }

  const currency = currencyCode?.trim() || 'USD';

  try {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency,
      maximumFractionDigits: value >= 100 ? 0 : 2
    }).format(value);
  } catch {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: value >= 100 ? 0 : 2
    }).format(value);
  }
}

function formatDateTimeZoneLabel(date: string, reportingTimezone: string): string {
  const match = date.match(/^(\d{4})-(\d{2})-(\d{2})$/);

  if (!match) {
    return reportingTimezone;
  }

  const [, year, month, day] = match;
  const displayDate = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), 12));
  const timeZoneName = new Intl.DateTimeFormat('en-US', {
    timeZone: reportingTimezone,
    timeZoneName: 'short'
  })
    .formatToParts(displayDate)
    .find((part) => part.type === 'timeZoneName')
    ?.value;

  return `12:00 AM ${timeZoneName ?? reportingTimezone}`;
}

function formatSelectionModeLabel(mode: MetaOrderValueRow['canonicalSelectionMode']) {
  switch (mode) {
    case 'fallback':
      return 'Fallback';
    case 'priority':
      return 'Priority';
    default:
      return 'No match';
  }
}

function formatActionType(value: string | null) {
  return value ?? 'No purchase-like action type';
}

function sortOptionLabel(sortBy: MetaOrderValueSortBy, sortDirection: MetaOrderValueSortDirection) {
  return `${sortBy}:${sortDirection}`;
}

function parseSortOption(value: string): { sortBy: MetaOrderValueSortBy; sortDirection: MetaOrderValueSortDirection } {
  const [sortBy, sortDirection] = value.split(':') as [MetaOrderValueSortBy, MetaOrderValueSortDirection];

  return {
    sortBy,
    sortDirection
  };
}

function toggleSort(
  current: Pick<MetaOrderValueFilters, 'sortBy' | 'sortDirection'>,
  key: MetaOrderValueSortBy
): Pick<MetaOrderValueFilters, 'sortBy' | 'sortDirection'> {
  if (current.sortBy === key) {
    return {
      sortBy: key,
      sortDirection: current.sortDirection === 'desc' ? 'asc' : 'desc'
    };
  }

  return {
    sortBy: key,
    sortDirection: key === 'campaignName' || key === 'actionType' ? 'asc' : 'desc'
  };
}

function SummaryCard({
  label,
  value,
  detail,
  tone = 'default'
}: {
  label: string;
  value: string;
  detail: string;
  tone?: 'default' | 'accent' | 'teal';
}) {
  return (
    <Card padding="compact" tone={tone} className="min-h-[11.5rem] border-line/60">
      <Eyebrow>{label}</Eyebrow>
      <MetricValue className="mt-4">{value}</MetricValue>
      <MetricCopy className="mt-2">{detail}</MetricCopy>
    </Card>
  );
}

export default function MetaOrderValueView({ reportingTimezone }: MetaOrderValueViewProps) {
  const [filters, setFilters] = useState<MetaOrderValueFilters>(() => createDefaultFilters(reportingTimezone));
  const [section, setSection] = useState<AsyncSection<MetaOrderValueResponse>>(createLoadingSection());

  useEffect(() => {
    setFilters((current) => {
      if (current.startDate || current.endDate) {
        return current;
      }

      return createDefaultFilters(reportingTimezone);
    });
  }, [reportingTimezone]);

  useEffect(() => {
    let cancelled = false;

    setSection(createLoadingSection());

    fetchMetaOrderValue({
      startDate: filters.startDate,
      endDate: filters.endDate,
      campaignSearch: filters.campaignSearch,
      actionType: filters.actionType,
      sortBy: filters.sortBy,
      sortDirection: filters.sortDirection,
      limit: PAGE_SIZE,
      offset: (filters.page - 1) * PAGE_SIZE
    })
      .then((response) => {
        if (!cancelled) {
          setSection(createResolvedSection(response));
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setSection(createErroredSection(error.message));
        }
      });

    return () => {
      cancelled = true;
    };
  }, [filters]);

  const response = section.data;
  const rows = response?.rows ?? [];
  const totalRows = response?.pagination.totalRows ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const currencyCode = response?.rows.find((row) => row.currency)?.currency ?? 'USD';
  const rangeLabel = `${formatDateLabel(filters.startDate, reportingTimezone)} to ${formatDateLabel(filters.endDate, reportingTimezone)}`;
  const actionTypes = useMemo(
    () =>
      Array.from(
        new Set(
          rows
            .map((row) => row.canonicalActionType)
            .filter((value): value is string => Boolean(value))
        )
      ).sort((left, right) => left.localeCompare(right)),
    [rows]
  );
  const actionTypeOptions = useMemo(() => {
    if (!filters.actionType || actionTypes.includes(filters.actionType)) {
      return actionTypes;
    }

    return [...actionTypes, filters.actionType].sort((left, right) => left.localeCompare(right));
  }, [actionTypes, filters.actionType]);

  function updateFilters(updater: (current: MetaOrderValueFilters) => MetaOrderValueFilters) {
    setFilters((current) => normalizeDateRange(updater(current)));
  }

  function handleDateChange(field: 'startDate' | 'endDate', value: string) {
    updateFilters((current) => ({
      ...current,
      [field]: value,
      page: 1
    }));
  }

  function handleQuickRange(days: number) {
    const range = buildRange(days, reportingTimezone);
    updateFilters((current) => ({
      ...current,
      ...range,
      page: 1
    }));
  }

  return (
    <section className="grid gap-section">
      <Panel
        title="Meta order value"
        description="Daily Meta-attributed revenue stays isolated here so operators can inspect campaign-day revenue rows, fallback action types, and spend context without mixing them into Shopify order-level attribution."
        wide
      >
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.1fr)_minmax(20rem,0.9fr)]">
          <Card className="border-line/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(246,223,211,0.36))]">
            <CardHeader className="mb-0">
              <div>
                <Eyebrow>Live API view</Eyebrow>
                <CardTitle className="mt-3">Meta attributed revenue explorer</CardTitle>
                <CardDescription className="max-w-2xl">
                  Querying `/api/reporting/meta-order-value` with server-side sorting and paging. Report dates render in
                  the configured reporting timezone.
                </CardDescription>
              </div>
              <Badge tone="teal">{reportingTimezone}</Badge>
            </CardHeader>

            <div className="mt-6 grid gap-4 lg:grid-cols-2">
              <Field label="Start date" htmlFor="meta-order-value-start-date">
                <Input
                  id="meta-order-value-start-date"
                  type="date"
                  value={filters.startDate}
                  max={filters.endDate}
                  onChange={(event) => handleDateChange('startDate', event.target.value)}
                />
              </Field>
              <Field label="End date" htmlFor="meta-order-value-end-date">
                <Input
                  id="meta-order-value-end-date"
                  type="date"
                  value={filters.endDate}
                  min={filters.startDate}
                  onChange={(event) => handleDateChange('endDate', event.target.value)}
                />
              </Field>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              {[
                { label: 'Last 7D', days: 7 },
                { label: 'Last 30D', days: 30 },
                { label: 'Last 90D', days: 90 }
              ].map((range) => (
                <Button
                  key={range.label}
                  type="button"
                  tone="ghost"
                  className="min-h-[36px] px-3 py-1.5 text-label"
                  onClick={() => handleQuickRange(range.days)}
                >
                  {range.label}
                </Button>
              ))}
            </div>
          </Card>

          <Card tone="teal" className="border-line/60">
            <Eyebrow>Reporting frame</Eyebrow>
            <MetricValue className="mt-4">{rangeLabel}</MetricValue>
            <MetricCopy className="mt-3 max-w-[36ch]">
              The table below shows one campaign-day per row using Meta conversion reporting and the account attribution
              setting contract.
            </MetricCopy>
            <div className="mt-5 grid gap-3 sm:grid-cols-2">
              <div className="rounded-card border border-line/60 bg-white/80 px-4 py-4">
                <p className="text-label uppercase text-ink-muted">Rows in window</p>
                <p className="mt-1 font-display text-title text-ink">{formatNumber(totalRows)}</p>
              </div>
              <div className="rounded-card border border-line/60 bg-white/80 px-4 py-4">
                <p className="text-label uppercase text-ink-muted">Active action types</p>
                <p className="mt-1 font-display text-title text-ink">{formatNumber(actionTypes.length)}</p>
              </div>
            </div>
          </Card>
        </div>

        <SectionState
          loading={section.loading}
          error={section.error}
          empty={!response}
          emptyLabel="Meta order value totals were not returned for the selected reporting window."
        >
          <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <SummaryCard
              label="Attributed revenue"
              value={formatCurrencyForCode(response?.totals.attributedRevenue, currencyCode)}
              detail={`${formatNumber(response?.totals.purchaseCount)} purchases across ${rangeLabel}`}
              tone="accent"
            />
            <SummaryCard
              label="Purchase count"
              value={formatNumber(response?.totals.purchaseCount)}
              detail="Canonical purchase counts stay aligned to the selected action type."
            />
            <SummaryCard
              label="Spend"
              value={formatCurrencyForCode(response?.totals.spend, currencyCode)}
              detail="Spend is stored on the same campaign-day row as the revenue aggregate."
              tone="teal"
            />
            <SummaryCard
              label="ROAS"
              value={response?.totals.roas == null ? 'N/A' : formatNumber(response.totals.roas)}
              detail="Uses Meta-attributed revenue totals divided by spend for the selected window."
            />
          </div>
        </SectionState>

        <div className="mt-6">
          <SectionState
            loading={section.loading}
            error={section.error}
            empty={!response || response.pagination.totalRows === 0}
            emptyLabel="No Meta order value rows matched the current range or filters."
          >
            <>
              <DataTableToolbar
                title="Campaign-day breakdown"
                description="Server-backed sorting and paging keep the rows stable when the result set gets large, while surfacing the canonical action type chosen for each daily aggregate."
                summary={
                  <>
                    <TableMeta currentCount={rows.length} totalCount={totalRows} label="campaign-day rows" />
                    <TablePagination page={filters.page} totalPages={totalPages} onPageChange={(page) => updateFilters((current) => ({ ...current, page }))} />
                  </>
                }
              >
                <TableFilterBar>
                  <TableSearchField
                    label="Search campaigns"
                    value={filters.campaignSearch}
                    onChange={(value) =>
                      updateFilters((current) => ({
                        ...current,
                        campaignSearch: value,
                        page: 1
                      }))
                    }
                    placeholder="Campaign name or ID"
                  />
                  <Field label="Action type" htmlFor="meta-order-value-action-type">
                    <Select
                      id="meta-order-value-action-type"
                      value={filters.actionType}
                      onChange={(event) =>
                        updateFilters((current) => ({
                          ...current,
                          actionType: event.target.value,
                          page: 1
                        }))
                      }
                    >
                      <option value="">All purchase-like types</option>
                      {actionTypeOptions.map((actionType) => (
                        <option key={actionType} value={actionType}>
                          {actionType}
                        </option>
                      ))}
                    </Select>
                  </Field>
                  <Field label="Sort by" htmlFor="meta-order-value-sort">
                    <Select
                      id="meta-order-value-sort"
                      value={sortOptionLabel(filters.sortBy, filters.sortDirection)}
                      onChange={(event) => {
                        const nextSort = parseSortOption(event.target.value);
                        updateFilters((current) => ({
                          ...current,
                          ...nextSort,
                          page: 1
                        }));
                      }}
                    >
                      <option value="reportDate:desc">Report date ↓</option>
                      <option value="reportDate:asc">Report date ↑</option>
                      <option value="attributedRevenue:desc">Revenue ↓</option>
                      <option value="attributedRevenue:asc">Revenue ↑</option>
                      <option value="purchaseCount:desc">Purchase count ↓</option>
                      <option value="purchaseCount:asc">Purchase count ↑</option>
                      <option value="spend:desc">Spend ↓</option>
                      <option value="spend:asc">Spend ↑</option>
                      <option value="roas:desc">ROAS ↓</option>
                      <option value="roas:asc">ROAS ↑</option>
                      <option value="campaignName:asc">Campaign A-Z</option>
                      <option value="campaignName:desc">Campaign Z-A</option>
                    </Select>
                  </Field>
                </TableFilterBar>
              </DataTableToolbar>

              <div className="mt-6 grid gap-4 md:hidden">
                {rows.map((row) => (
                  <Card
                    key={`${row.date}-${row.campaignId}-${row.canonicalActionType ?? 'none'}`}
                    padding="compact"
                    className="border-line/60 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(220,239,237,0.28))]"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <Eyebrow>Order reporting day</Eyebrow>
                        <p className="mt-2 font-display text-title text-ink">{formatDateLabel(row.date, reportingTimezone)}</p>
                        <p className="mt-1 text-body text-ink-muted">{formatDateTimeZoneLabel(row.date, reportingTimezone)}</p>
                      </div>
                      <Badge tone={row.canonicalSelectionMode === 'fallback' ? 'warning' : 'teal'}>
                        {formatSelectionModeLabel(row.canonicalSelectionMode)}
                      </Badge>
                    </div>

                    <div className="mt-4">
                      <p className="font-semibold text-ink">{row.campaignName ?? 'Unnamed campaign'}</p>
                      <p className="mt-1 break-all text-body text-ink-muted">{row.campaignId}</p>
                    </div>

                    <div className="mt-5 grid grid-cols-2 gap-3">
                      <div className="rounded-card border border-line/60 bg-white/75 px-3 py-3">
                        <p className="text-label uppercase text-ink-muted">Revenue</p>
                        <p className="mt-1 font-semibold text-ink">{formatCurrencyForCode(row.attributedRevenue, row.currency)}</p>
                      </div>
                      <div className="rounded-card border border-line/60 bg-white/75 px-3 py-3">
                        <p className="text-label uppercase text-ink-muted">Spend</p>
                        <p className="mt-1 font-semibold text-ink">{formatCurrencyForCode(row.spend, row.currency)}</p>
                      </div>
                      <div className="rounded-card border border-line/60 bg-white/75 px-3 py-3">
                        <p className="text-label uppercase text-ink-muted">Count</p>
                        <p className="mt-1 font-semibold text-ink">{formatNumber(row.purchaseCount)}</p>
                      </div>
                      <div className="rounded-card border border-line/60 bg-white/75 px-3 py-3">
                        <p className="text-label uppercase text-ink-muted">ROAS</p>
                        <p className="mt-1 font-semibold text-ink">
                          {row.roas == null ? formatNumber(row.calculatedRoas) : formatNumber(row.roas)}
                        </p>
                      </div>
                    </div>

                    <div className="mt-4 rounded-card border border-line/60 bg-white/75 px-3 py-3 text-body text-ink-soft">
                      <p className="text-label uppercase text-ink-muted">Action type</p>
                      <p className="mt-1 font-semibold text-ink">{formatActionType(row.canonicalActionType)}</p>
                    </div>
                  </Card>
                ))}
              </div>

              <TableWrap className="mt-6 hidden max-h-[36rem] md:block">
                <Table caption="Meta campaign-day order value rows">
                  <TableHead>
                    <TableRow>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'reportDate'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'reportDate'),
                            page: 1
                          }))
                        }
                      >
                        Order date/time
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'campaignName'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'campaignName'),
                            page: 1
                          }))
                        }
                      >
                        Campaign
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'attributedRevenue'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'attributedRevenue'),
                            page: 1
                          }))
                        }
                      >
                        Revenue
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'purchaseCount'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'purchaseCount'),
                            page: 1
                          }))
                        }
                      >
                        Count
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'spend'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'spend'),
                            page: 1
                          }))
                        }
                      >
                        Spend
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'roas'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'roas'),
                            page: 1
                          }))
                        }
                      >
                        ROAS
                      </SortableTableHeaderCell>
                      <SortableTableHeaderCell
                        sorted={filters.sortBy === 'actionType'}
                        direction={filters.sortDirection}
                        onSort={() =>
                          updateFilters((current) => ({
                            ...current,
                            ...toggleSort(current, 'actionType'),
                            page: 1
                          }))
                        }
                      >
                        Action type
                      </SortableTableHeaderCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {rows.length === 0 ? (
                      <TableEmptyRow
                        colSpan={7}
                        title="No Meta order value rows found"
                        description="Try widening the date range or removing campaign filters."
                      />
                    ) : null}
                    {rows.map((row) => (
                      <TableRow key={`${row.date}-${row.campaignId}-${row.canonicalActionType ?? 'none'}`}>
                        <TableCell>
                          <PrimaryCell>
                            <strong>{formatDateLabel(row.date, reportingTimezone)}</strong>
                            <span>{formatDateTimeZoneLabel(row.date, reportingTimezone)}</span>
                          </PrimaryCell>
                        </TableCell>
                        <TableCell>
                          <PrimaryCell>
                            <strong>{row.campaignName ?? 'Unnamed campaign'}</strong>
                            <span>{row.campaignId}</span>
                          </PrimaryCell>
                        </TableCell>
                        <TableCell>{formatCurrencyForCode(row.attributedRevenue, row.currency)}</TableCell>
                        <TableCell>{formatNumber(row.purchaseCount)}</TableCell>
                        <TableCell>{formatCurrencyForCode(row.spend, row.currency)}</TableCell>
                        <TableCell>{row.roas == null ? formatNumber(row.calculatedRoas) : formatNumber(row.roas)}</TableCell>
                        <TableCell>
                          <PrimaryCell className="gap-1">
                            <strong>{formatActionType(row.canonicalActionType)}</strong>
                            <span>{formatSelectionModeLabel(row.canonicalSelectionMode)}</span>
                          </PrimaryCell>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableWrap>
            </>
          </SectionState>
        </div>
      </Panel>
    </section>
  );
}
