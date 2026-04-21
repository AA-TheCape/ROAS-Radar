import React, { useMemo, useState, type ReactNode } from 'react';

import type { OrderDetailsResponse } from '../lib/api';
import { formatCurrency, formatDateTimeLabel, formatNumber } from '../lib/format';
import {
  Badge,
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
  DataTableToolbar,
  DetailList,
  Eyebrow,
  EmptyState,
  MetricCopy,
  MetricValue,
  PrimaryCell,
  SectionState,
  SortableTableHeaderCell,
  StatusPill,
  Table,
  TableBody,
  TableCell,
  TableEmptyRow,
  TableFilterBar,
  TableHead,
  TableHeaderCell,
  TableMeta,
  TablePagination,
  TableSearchField,
  TableRow,
  TableWrap,
  Tooltip
} from './AuthenticatedUi';
import { matchesQuery, paginateRows, sortRows, type SortState } from '../lib/dataTable';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type OrderDetailsViewProps = {
  selectedOrderId: string | null;
  reportingTimezone: string;
  orderDetailsSection: AsyncSection<OrderDetailsResponse>;
};

function formatOptionalDateTime(value: string | null | undefined, reportingTimezone: string): string {
  return value ? formatDateTimeLabel(value, reportingTimezone) : 'Not available';
}

function formatOptionalValue(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === '') {
    return 'Not available';
  }

  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }

  return String(value);
}

function formatJsonValue(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <Card padding="compact" className="border-line/70">
      <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-brand via-brand/75 to-teal/70" />
      <Eyebrow>{label}</Eyebrow>
      <MetricValue>{value}</MetricValue>
      <MetricCopy>{detail}</MetricCopy>
    </Card>
  );
}

function DetailCard({
  title,
  description,
  children
}: {
  title: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <Card className="bg-surface/88">
      <CardHeader>
        <div>
          <CardTitle>{title}</CardTitle>
          {description ? <CardDescription>{description}</CardDescription> : null}
        </div>
      </CardHeader>
      {children}
    </Card>
  );
}

function JsonViewer({ value }: { value: unknown }) {
  return (
    <pre className="max-h-[28rem] overflow-auto rounded-card border border-white/8 bg-[#132130] p-4 font-mono text-[0.78rem] leading-6 text-slate-200 shadow-inset-soft">
      {formatJsonValue(value)}
    </pre>
  );
}

export default function OrderDetailsView({
  selectedOrderId,
  reportingTimezone,
  orderDetailsSection
}: OrderDetailsViewProps) {
  const data = orderDetailsSection.data;
  const order = data?.order;
  const lineItems = data?.lineItems ?? [];
  const attributionCredits = data?.attributionCredits ?? [];
  const [lineItemSearch, setLineItemSearch] = useState('');
  const [lineItemSort, setLineItemSort] = useState<
    SortState<'title' | 'sku' | 'quantity' | 'price' | 'vendor'>
  >({
    key: 'title',
    direction: 'asc'
  });
  const [lineItemPage, setLineItemPage] = useState(1);
  const [creditSearch, setCreditSearch] = useState('');
  const [creditSort, setCreditSort] = useState<
    SortState<'model' | 'position' | 'source' | 'campaign' | 'time' | 'revenueCredit' | 'weight'>
  >({
    key: 'revenueCredit',
    direction: 'desc'
  });
  const [creditPage, setCreditPage] = useState(1);
  const attributedRevenue = attributionCredits.reduce((sum, credit) => sum + credit.revenueCredit, 0);
  const filteredLineItems = useMemo(
    () =>
      lineItems.filter((item) =>
        matchesQuery(
          [item.title, item.variantTitle, item.sku, item.vendor, item.fulfillmentStatus, item.quantity, item.price],
          lineItemSearch
        )
      ),
    [lineItemSearch, lineItems]
  );
  const sortedLineItems = useMemo(
    () =>
      sortRows(filteredLineItems, lineItemSort, {
        title: (item) => item.title ?? '',
        sku: (item) => item.sku ?? '',
        quantity: (item) => item.quantity,
        price: (item) => item.price,
        vendor: (item) => item.vendor ?? ''
      }),
    [filteredLineItems, lineItemSort]
  );
  const paginatedLineItems = useMemo(() => paginateRows(sortedLineItems, lineItemPage, 6), [lineItemPage, sortedLineItems]);
  const filteredCredits = useMemo(
    () =>
      attributionCredits.filter((credit) =>
        matchesQuery(
          [
            credit.attributionModel,
            credit.source,
            credit.medium,
            credit.campaign,
            credit.attributionReason,
            credit.clickIdType,
            credit.clickIdValue
          ],
          creditSearch
        )
      ),
    [attributionCredits, creditSearch]
  );
  const sortedCredits = useMemo(
    () =>
      sortRows(filteredCredits, creditSort, {
        model: (credit) => credit.attributionModel,
        position: (credit) => credit.touchpointPosition,
        source: (credit) => `${credit.source ?? ''} ${credit.medium ?? ''}`,
        campaign: (credit) => credit.campaign ?? '',
        time: (credit) => credit.touchpointOccurredAt ?? '',
        revenueCredit: (credit) => credit.revenueCredit,
        weight: (credit) => credit.creditWeight
      }),
    [creditSort, filteredCredits]
  );
  const paginatedCredits = useMemo(() => paginateRows(sortedCredits, creditPage, 6), [creditPage, sortedCredits]);

  function toggleLineItemSort(key: 'title' | 'sku' | 'quantity' | 'price' | 'vendor') {
    setLineItemSort((current) => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
    }));
  }

  function toggleCreditSort(key: 'model' | 'position' | 'source' | 'campaign' | 'time' | 'revenueCredit' | 'weight') {
    setCreditSort((current) => ({
      key,
      direction: current.key === key && current.direction === 'desc' ? 'asc' : 'desc'
    }));
  }

  return (
    <SectionState
      loading={orderDetailsSection.loading}
      error={orderDetailsSection.error}
      empty={!data}
      emptyLabel={selectedOrderId ? `No details were loaded for order ${selectedOrderId}.` : 'No order selected.'}
    >
      <div className="grid gap-section">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard
            label="Shopify order"
            value={formatOptionalValue(order?.shopifyOrderNumber)}
            detail={formatOptionalValue(order?.shopifyOrderId)}
          />
          <MetricCard
            label="Total revenue"
            value={formatCurrency(order?.totalPrice)}
            detail={`Subtotal ${formatCurrency(order?.subtotalPrice)}`}
          />
          <MetricCard
            label="Line items"
            value={formatNumber(lineItems.length)}
            detail={`${formatNumber(lineItems.reduce((sum, item) => sum + item.quantity, 0))} units across the order`}
          />
          <MetricCard
            label="Attributed credits"
            value={formatNumber(attributionCredits.length)}
            detail={`${formatCurrency(attributedRevenue)} total credited revenue`}
          />
        </div>

        <div className="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,1.2fr)_minmax(0,0.95fr)]">
          <DetailCard title="Order overview" description="Primary order identifiers, commercial totals, and fulfillment state.">
            <DetailList className="xl:grid-cols-2">
              <div>
                <dt>Shopify order ID</dt>
                <dd>{formatOptionalValue(order?.shopifyOrderId)}</dd>
              </div>
              <div>
                <dt>Order number</dt>
                <dd>{formatOptionalValue(order?.shopifyOrderNumber)}</dd>
              </div>
              <div>
                <dt>Currency</dt>
                <dd>{formatOptionalValue(order?.currencyCode)}</dd>
              </div>
              <div>
                <dt>Source name</dt>
                <dd>{formatOptionalValue(order?.sourceName)}</dd>
              </div>
              <div>
                <dt>Subtotal</dt>
                <dd>{formatCurrency(order?.subtotalPrice)}</dd>
              </div>
              <div>
                <dt>Total</dt>
                <dd>{formatCurrency(order?.totalPrice)}</dd>
              </div>
              <div>
                <dt>Financial status</dt>
                <dd>{formatOptionalValue(order?.financialStatus)}</dd>
              </div>
              <div>
                <dt>Fulfillment status</dt>
                <dd>{formatOptionalValue(order?.fulfillmentStatus)}</dd>
              </div>
            </DetailList>
          </DetailCard>

          <DetailCard title="Customer and linkage" description="Identity stitching fields captured across Shopify and ROAS Radar.">
            <DetailList className="xl:grid-cols-2">
              <div>
                <dt>Shopify customer ID</dt>
                <dd>{formatOptionalValue(order?.shopifyCustomerId)}</dd>
              </div>
              <div>
                <dt>Customer identity ID</dt>
                <dd>{formatOptionalValue(order?.customerIdentityId)}</dd>
              </div>
              <div>
                <dt>Email</dt>
                <dd>{formatOptionalValue(order?.email)}</dd>
              </div>
              <div>
                <dt>Email hash</dt>
                <dd className="break-all">{formatOptionalValue(order?.emailHash)}</dd>
              </div>
              <div>
                <dt>Landing session ID</dt>
                <dd className="break-all">{formatOptionalValue(order?.landingSessionId)}</dd>
              </div>
              <div>
                <dt>Checkout token</dt>
                <dd className="break-all">{formatOptionalValue(order?.checkoutToken)}</dd>
              </div>
              <div className="md:col-[1/-1]">
                <dt>Cart token</dt>
                <dd className="break-all">{formatOptionalValue(order?.cartToken)}</dd>
              </div>
            </DetailList>
          </DetailCard>

          <DetailCard title="Timestamps" description="Operational dates shown in the active reporting timezone.">
            <DetailList className="grid-cols-1">
              <div>
                <dt>Processed</dt>
                <dd>{formatOptionalDateTime(order?.processedAt, reportingTimezone)}</dd>
              </div>
              <div>
                <dt>Created in Shopify</dt>
                <dd>{formatOptionalDateTime(order?.createdAtShopify, reportingTimezone)}</dd>
              </div>
              <div>
                <dt>Updated in Shopify</dt>
                <dd>{formatOptionalDateTime(order?.updatedAtShopify, reportingTimezone)}</dd>
              </div>
              <div>
                <dt>Ingested</dt>
                <dd>{formatOptionalDateTime(order?.ingestedAt, reportingTimezone)}</dd>
              </div>
            </DetailList>
          </DetailCard>
        </div>

        <DetailCard title="Line items" description="Commercial line-item detail from Shopify, including variant metadata and ingestion flags.">
          <DataTableToolbar
            title="Line item records"
            description="Search and sort the Shopify line items without losing the existing order-detail content."
            summary={
              <>
                <TableMeta currentCount={filteredLineItems.length} totalCount={lineItems.length} label="line items" />
                <TablePagination
                  page={paginatedLineItems.currentPage}
                  totalPages={paginatedLineItems.totalPages}
                  onPageChange={setLineItemPage}
                />
              </>
            }
          >
            <TableFilterBar>
              <TableSearchField
                label="Search line items"
                value={lineItemSearch}
                onChange={(value) => {
                  setLineItemSearch(value);
                  setLineItemPage(1);
                }}
                placeholder="Title, SKU, vendor, variant"
              />
            </TableFilterBar>
          </DataTableToolbar>
          <TableWrap className="max-h-[30rem]">
            <Table caption="Order line items">
              <TableHead>
                <TableRow>
                  <SortableTableHeaderCell
                    sorted={lineItemSort.key === 'title'}
                    direction={lineItemSort.direction}
                    onSort={() => toggleLineItemSort('title')}
                  >
                    Title
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={lineItemSort.key === 'sku'}
                    direction={lineItemSort.direction}
                    onSort={() => toggleLineItemSort('sku')}
                  >
                    SKU
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={lineItemSort.key === 'quantity'}
                    direction={lineItemSort.direction}
                    onSort={() => toggleLineItemSort('quantity')}
                  >
                    Qty
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={lineItemSort.key === 'price'}
                    direction={lineItemSort.direction}
                    onSort={() => toggleLineItemSort('price')}
                  >
                    Price
                  </SortableTableHeaderCell>
                  <TableHeaderCell>Discount</TableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={lineItemSort.key === 'vendor'}
                    direction={lineItemSort.direction}
                    onSort={() => toggleLineItemSort('vendor')}
                  >
                    Vendor
                  </SortableTableHeaderCell>
                  <TableHeaderCell>Flags</TableHeaderCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {paginatedLineItems.rows.length === 0 ? (
                  <TableEmptyRow
                    colSpan={7}
                    title="No line items found"
                    description="No stored line items match the current search."
                  />
                ) : null}
                {paginatedLineItems.rows.map((item) => (
                  <TableRow key={item.shopifyLineItemId}>
                    <TableCell>
                      <PrimaryCell>
                        <strong>{item.title ?? 'Untitled line item'}</strong>
                        <span>{item.variantTitle ?? 'No variant title'}</span>
                      </PrimaryCell>
                    </TableCell>
                    <TableCell>{formatOptionalValue(item.sku)}</TableCell>
                    <TableCell>{formatNumber(item.quantity)}</TableCell>
                    <TableCell>{formatCurrency(item.price)}</TableCell>
                    <TableCell>{formatCurrency(item.totalDiscount)}</TableCell>
                    <TableCell>{formatOptionalValue(item.vendor)}</TableCell>
                    <TableCell>
                      <div className="grid gap-1 text-body text-ink-muted">
                        <span>{formatOptionalValue(item.fulfillmentStatus)}</span>
                        <span>
                          Shipping {formatOptionalValue(item.requiresShipping)} · Taxable {formatOptionalValue(item.taxable)}
                        </span>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableWrap>
        </DetailCard>

        <DetailCard title="Attribution credits" description="Per-touchpoint revenue allocation stored for this order across attribution models.">
          <DataTableToolbar
            title="Attribution credit records"
            description="The same shared list pattern is used here for touchpoint-level inspection."
            summary={
              <>
                <TableMeta currentCount={filteredCredits.length} totalCount={attributionCredits.length} label="credits" />
                <TablePagination
                  page={paginatedCredits.currentPage}
                  totalPages={paginatedCredits.totalPages}
                  onPageChange={setCreditPage}
                />
              </>
            }
          >
            <TableFilterBar>
              <TableSearchField
                label="Search attribution credits"
                value={creditSearch}
                onChange={(value) => {
                  setCreditSearch(value);
                  setCreditPage(1);
                }}
                placeholder="Model, source, campaign, reason"
              />
            </TableFilterBar>
          </DataTableToolbar>
          <TableWrap className="max-h-[32rem]">
            <Table caption="Attribution credits">
              <TableHead>
                <TableRow>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'model'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('model')}
                  >
                    Model
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'position'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('position')}
                  >
                    Position
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'source'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('source')}
                  >
                    Source / medium
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'campaign'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('campaign')}
                  >
                    Campaign
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'time'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('time')}
                  >
                    Touchpoint time
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'revenueCredit'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('revenueCredit')}
                  >
                    Revenue credit
                  </SortableTableHeaderCell>
                  <SortableTableHeaderCell
                    sorted={creditSort.key === 'weight'}
                    direction={creditSort.direction}
                    onSort={() => toggleCreditSort('weight')}
                  >
                    Weight
                  </SortableTableHeaderCell>
                  <TableHeaderCell>Reason</TableHeaderCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {paginatedCredits.rows.length === 0 ? (
                  <TableEmptyRow
                    colSpan={8}
                    title="No attribution credits found"
                    description="No stored credits match the current search."
                  />
                ) : null}
                {paginatedCredits.rows.map((credit) => (
                  <TableRow key={`${credit.attributionModel}-${credit.touchpointPosition}-${credit.sessionId ?? 'none'}`}>
                    <TableCell>
                      <PrimaryCell>
                        <strong>{credit.attributionModel}</strong>
                        <span>{credit.isPrimary ? 'Primary touchpoint' : 'Supporting touchpoint'}</span>
                      </PrimaryCell>
                    </TableCell>
                    <TableCell>{formatNumber(credit.touchpointPosition)}</TableCell>
                    <TableCell>
                      <PrimaryCell className="gap-0.5">
                        <strong>{`${credit.source ?? 'Unknown'} / ${credit.medium ?? 'Unknown'}`}</strong>
                        <span>{credit.clickIdType && credit.clickIdValue ? `${credit.clickIdType}: ${credit.clickIdValue}` : 'No click ID stored'}</span>
                      </PrimaryCell>
                    </TableCell>
                    <TableCell>{credit.campaign ?? 'No campaign'}</TableCell>
                    <TableCell>{formatOptionalDateTime(credit.touchpointOccurredAt, reportingTimezone)}</TableCell>
                    <TableCell>{formatCurrency(credit.revenueCredit)}</TableCell>
                    <TableCell>{formatNumber(credit.creditWeight)}</TableCell>
                    <TableCell>
                      <Badge tone="brand">
                        {credit.attributionReason}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableWrap>
        </DetailCard>

        <div className="grid gap-4 xl:grid-cols-2">
          <DetailCard title="Raw order payload" description="Stored Shopify payload for the order record.">
            <JsonViewer value={order?.rawPayload ?? {}} />
          </DetailCard>

          <DetailCard title="Raw line item payloads" description="Per-line-item Shopify payloads, grouped in insertion order.">
            {lineItems.length === 0 ? (
              <EmptyState
                title="No raw payloads stored"
                description="No raw line item payloads were stored for this order."
                compact
                tone="muted"
              />
            ) : (
              <div className="grid gap-4">
                {lineItems.map((item, index) => (
                  <div key={`${item.shopifyLineItemId}-raw`} className="grid gap-3">
                    <div className="flex flex-wrap items-center justify-between gap-3 rounded-card border border-line/60 bg-surface-alt/60 px-4 py-3">
                      <div>
                        <p className="text-body font-semibold text-ink">{item.title ?? `Line item ${index + 1}`}</p>
                        <p className="text-body text-ink-muted">
                          {formatOptionalValue(item.variantTitle)} · Qty {formatNumber(item.quantity)}
                        </p>
                      </div>
                      <Tooltip content="Shopify line item identifier">
                        <StatusPill>{formatOptionalValue(item.shopifyLineItemId)}</StatusPill>
                      </Tooltip>
                    </div>
                    <JsonViewer value={item.rawPayload} />
                  </div>
                ))}
              </div>
            )}
          </DetailCard>
        </div>
      </div>
    </SectionState>
  );
}
